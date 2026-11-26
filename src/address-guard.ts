import type { HttpClient } from 'effect/http';

import { Context, Effect, Option } from 'effect';
import { FetchHttpClient, HttpClientRequest, HttpClientResponse } from 'effect/http';
import { lookup } from 'node:dns/promises';

import { cancelResponseBody } from './body.ts';
import { tagHttpError } from './error-meta.ts';
import { HttpRequestError } from './errors.ts';
import { isPublicIpAddress, parseIpAddress } from './ip.ts';
import { DEFAULT_MAX_REDIRECTS } from './policy.ts';
import {
  blocksBodyReplay,
  isCrossOrigin,
  redirectLocation,
  redirectRequest,
  replaysStreamBody,
  safelistedHeaderInit,
} from './redirects.ts';
import { markRedirectedResponse } from './retry.ts';
import { prepareRequest, sendRequest } from './transport.ts';

/**
 * Resolves a hostname to every IPv4 and IPv6 address it has, as address strings.
 */
export type ResolveAddresses = (hostname: string) => Promise<ReadonlyArray<string>>;

/**
 * Settings for one guarded request.
 */
export type AddressGuardParams = {
  /**
   * Logical upstream identifier used in error messages.
   */
  readonly upstream: string;

  /**
   * Redirect hops followed before the request is refused; defaults to `DEFAULT_MAX_REDIRECTS`.
   */
  readonly maxRedirects?: number | undefined;

  /**
   * Whether the first URL and every redirect hop must pass `assertPublicUrl`; defaults to `true`.
   * With `false` redirects are still followed here, with the same header and body rules, but any
   * address may be reached.
   */
  readonly checkAddresses?: boolean | undefined;
};

/**
 * URL schemes a guarded request may use.
 */
const ALLOWED_PROTOCOLS: ReadonlySet<string> = new Set(['http:', 'https:']);

/**
 * Hostnames that only resolve on the local machine or a private network.
 */
const NON_PUBLIC_HOSTNAMES: ReadonlySet<string> = new Set(['localhost']);

/**
 * Domain suffixes that only resolve on the local machine or a private network: RFC 6761
 * `localhost`, RFC 6762 `local`, the ICANN-reserved `internal`, RFC 8375 `home.arpa`, and the
 * common `lan` convention.
 */
const NON_PUBLIC_SUFFIXES: ReadonlyArray<string> = [
  '.localhost',
  '.local',
  '.internal',
  '.home.arpa',
  '.lan',
];

/**
 * URL brackets around an IPv6 literal hostname.
 */
const HOSTNAME_BRACKETS_PATTERN = /^\[(.*)\]$/u;

/**
 * Characters no transport may send in a header value: carriage return, line feed and NUL.
 */
const UNSAFE_HEADER_VALUE_PATTERN = /[\r\n\0]/u;

/**
 * A single trailing dot of a fully qualified hostname.
 */
const TRAILING_DOT_PATTERN = /\.$/u;

/**
 * Resolves through the system resolver (`getaddrinfo`), returning every answer in resolver order.
 */
const systemResolveAddresses: ResolveAddresses = async (hostname) => {
  const answers = await lookup(hostname, { all: true, order: 'verbatim' });
  return answers.map((answer) => answer.address);
};

/**
 * Resolver used by the public-address guard. It defaults to the system resolver through
 * `node:dns/promises`; tests provide a stub with `Effect.provideService(AddressResolver, …)`.
 */
export const AddressResolver = Context.Reference<ResolveAddresses>(
  '@yieldkit/http/AddressResolver',
  { defaultValue: () => systemResolveAddresses },
);

/**
 * Fails unless `url` is safe to request: the protocol is `http:` or `https:`, there is no username
 * or password, a hostname is public (not `localhost`, `*.localhost`, `*.local`, `*.internal`,
 * `*.home.arpa`, `*.lan` or a single label), an IP literal is public, and a name resolves to at
 * least one address with every address public. A refusal is an `HttpRequestError` with status null,
 * outcome `blocked`, never retried and ignored by the circuit breaker; its message names the origin
 * only. A DNS failure or empty answer fails closed as a retryable `network_error`.
 */
export const assertPublicUrl = (
  url: URL,
  upstream: string,
): Effect.Effect<void, HttpRequestError> => {
  const hostname = normalizeHostname(url.hostname);
  const reason = refusalReason(url, hostname);
  if (reason !== null) {
    return Effect.fail(refusal(upstream, url, reason));
  }

  if (parseIpAddress(hostname) !== null) {
    return Effect.void;
  }

  return Effect.flatMap(resolveAddresses(hostname, url, upstream), (addresses) =>
    addresses.every((address) => isPublicIpAddress(address))
      ? Effect.void
      : Effect.fail(refusal(upstream, url, 'resolves to a non-public address')),
  );
};

/**
 * Sends `request` and follows its redirects here, never in fetch (`redirect: 'manual'` is merged
 * into any ambient `FetchHttpClient.RequestInit`), so the same rules apply to every hop whatever
 * the transport. The client's request transforms (`HttpClient.mapRequest`, `mapRequestEffect`) run
 * once per call (one attempt), before the first hop, and every check below reads the request they
 * produce: a URL still relative after them fails as an invalid URL, and a header value with a
 * carriage return, line feed or NUL character is refused before anything is sent. Redirect hops run
 * only the client's response side (`postprocess`), so a credential a transform adds is never
 * re-added on a redirect hop, and a transform that signs the URL is not re-run on a hop, so a
 * signed URL is only valid for the first hop. The transforms run outside response transforms
 * (`HttpClient.transformResponse`), so a service a response transform provides is not visible to a
 * request transform; provide it to the caller instead.
 *
 * A 301, 302, 303, 307 or 308 with a `Location` cancels the hop body, counts against
 * `maxRedirects`, resolves the location against the current URL and rewrites the request (see
 * `redirectRequest`): credentials and custom headers, including those a transform added, never
 * cross to another origin, and fetch defaults from an ambient `RequestInit` are cut to the same
 * safelist after such a hop. A hop that would replay a non-empty body to another origin, or replay
 * a one-shot body (see `isOneShotBody`) at all, is refused. A 3xx without `Location` is returned as
 * is. The returned response is the last hop's, so its `url` is the final URL. These rules need a
 * transport that returns each 3xx as it is (`FetchHttpClient` and `BunHttpClient` do with
 * `redirect: 'manual'`); a transport that follows redirects on its own applies its own rules, and
 * only with `checkAddresses` are its redirected responses refused.
 *
 * With `checkAddresses` (the default) the first URL and every hop must pass `assertPublicUrl`, and
 * a response the transport itself reports as redirected (its own redirect handling, such as a
 * client wrapped in `HttpClient.followRedirects`) is refused, because the guard never saw those
 * hops. The guard checks and then connects, and every transport resolves the name again after the
 * check, so DNS rebinding between the check and the connection is narrowed but not closed.
 */
export const guardedExecute = Effect.fnUntraced(function* (
  client: HttpClient.HttpClient,
  request: HttpClientRequest.HttpClientRequest,
  params: AddressGuardParams,
): Effect.fn.Return<HttpClientResponse.HttpClientResponse, HttpRequestError> {
  const { upstream } = params;
  const checkAddresses = params.checkAddresses ?? true;
  const maxRedirects = params.maxRedirects ?? DEFAULT_MAX_REDIRECTS;
  const prepared = yield* prepareRequest(client, request, upstream);
  const unsafeHeader = Object.keys(prepared.headers).find((name) =>
    UNSAFE_HEADER_VALUE_PATTERN.test(prepared.headers[name] ?? ''),
  );
  if (unsafeHeader !== undefined) {
    return yield* refusedRequest(
      upstream,
      `header ${unsafeHeader} contains a line break or NUL character`,
    );
  }

  const ambientInit = yield* Effect.serviceOption(FetchHttpClient.RequestInit);
  const sameOriginInit: RequestInit = {
    ...Option.getOrUndefined(ambientInit),
    redirect: 'manual',
  };
  const crossOriginInit: RequestInit = { ...sameOriginInit };
  const crossOriginHeaders = safelistedHeaderInit(sameOriginInit.headers);
  if (crossOriginHeaders !== undefined) {
    crossOriginInit.headers = crossOriginHeaders;
  }

  let crossedOrigin = false;
  let hopRequest = prepared;
  let url = yield* requestUrl(prepared, upstream);
  for (let redirects = 0; ; redirects += 1) {
    if (checkAddresses) {
      yield* assertPublicUrl(url, upstream);
    }

    const response = yield* sendRequest(client, hopRequest, upstream).pipe(
      Effect.provideService(
        FetchHttpClient.RequestInit,
        crossedOrigin ? crossOriginInit : sameOriginInit,
      ),
    );
    if (checkAddresses && transportFollowedRedirect(response, url)) {
      yield* cancelResponseBody(response);
      return yield* refusal(upstream, url, 'the transport followed a redirect the guard never saw');
    }

    const location = redirectLocation(response);
    if (location === null) {
      if (redirects > 0) {
        markRedirectedResponse(response);
      }

      return response;
    }

    yield* cancelResponseBody(response);
    if (redirects >= maxRedirects) {
      return yield* refusal(upstream, url, `more than ${String(maxRedirects)} redirects`);
    }

    const next = yield* resolveRedirectTarget({
      location,
      current: url,
      request: hopRequest,
      status: response.status,
      upstream,
    });
    hopRequest = redirectRequest({
      request: hopRequest,
      status: response.status,
      from: url,
      to: next,
    });
    crossedOrigin ||= isCrossOrigin(url, next);
    url = next;
  }
});

/**
 * Returns why following a `status` redirect from `from` to `to` must be refused because of the
 * request body, or null when the hop may be followed.
 */
const bodyReplayRefusal = (
  request: HttpClientRequest.HttpClientRequest,
  status: number,
  from: URL,
  to: URL,
): string | null => {
  if (replaysStreamBody(request, status)) {
    return `a ${String(status)} redirect would replay a one-shot body`;
  }

  if (blocksBodyReplay({ request, status, from, to })) {
    return `a ${String(status)} redirect to another origin would forward the request body`;
  }

  return null;
};

/**
 * Returns `true` when the transport followed a redirect on its own: the Web response says it was
 * redirected, or the response reports a URL other than the one requested.
 */
const transportFollowedRedirect = (
  response: HttpClientResponse.HttpClientResponse,
  requested: URL,
): boolean => {
  const web = HttpClientResponse.toWeb(response);
  if (Option.isSome(web) && web.value.redirected) {
    return true;
  }

  const expected = new URL(requested.href);
  expected.hash = '';
  return response.url !== '' && response.url !== expected.href;
};

/**
 * Lower-cases `hostname` and strips IPv6 brackets and a trailing dot, so `[::1]` and `localhost.`
 * are judged as `::1` and `localhost`.
 */
const normalizeHostname = (hostname: string): string =>
  hostname.replace(HOSTNAME_BRACKETS_PATTERN, '$1').replace(TRAILING_DOT_PATTERN, '').toLowerCase();

/**
 * Resolves the `Location` of a redirect from `current` and fails with a refusal when the hop may
 * not be followed: a scheme other than http(s), URL credentials, or a body that would be replayed
 * where it must not be.
 */
const resolveRedirectTarget = Effect.fnUntraced(function* (params: {
  readonly location: string;
  readonly current: URL;
  readonly request: HttpClientRequest.HttpClientRequest;
  readonly status: number;
  readonly upstream: string;
}): Effect.fn.Return<URL, HttpRequestError> {
  const { location, current, request, status, upstream } = params;
  const next = yield* resolveLocation(location, current, upstream);
  const hopRefusal =
    redirectTargetRefusal(next) ?? bodyReplayRefusal(request, status, current, next);
  if (hopRefusal !== null) {
    return yield* refusal(upstream, current, hopRefusal);
  }

  return next;
});

/**
 * Returns why a redirect to `url` is refused whatever `checkAddresses` says, or null when it may be
 * followed. Only `http:` and `https:` hops without URL credentials are followed, as in the Fetch
 * standard: the transport may serve other schemes (Bun's fetch reads `file:` URLs), and an upstream
 * must never be able to point the client at them.
 */
const redirectTargetRefusal = (url: URL): string | null => {
  if (!ALLOWED_PROTOCOLS.has(url.protocol)) {
    return `redirect to protocol ${url.protocol} is not allowed`;
  }

  return url.username === '' && url.password === ''
    ? null
    : 'credentials in a redirect URL are not allowed';
};

/**
 * Returns why `url` is refused before any lookup (scheme, credentials, a non-public IP literal or a
 * local hostname), or null when it may proceed. `hostname` is the normalized hostname of `url`.
 */
const refusalReason = (url: URL, hostname: string): string | null => {
  if (!ALLOWED_PROTOCOLS.has(url.protocol)) {
    return `protocol ${url.protocol} is not allowed`;
  }

  if (url.username !== '' || url.password !== '') {
    return 'credentials in the URL are not allowed';
  }

  if (parseIpAddress(hostname) !== null) {
    return isPublicIpAddress(hostname) ? null : 'not a public address';
  }

  return isPublicHostname(hostname) ? null : 'not a public hostname';
};

/**
 * Returns `true` for a multi-label DNS name outside the local and private suffixes. Expects a
 * normalized hostname that is not an IP literal.
 */
const isPublicHostname = (hostname: string): boolean => {
  const labels = hostname.split('.');
  if (labels.length < 2 || labels.some((label) => label.length === 0)) {
    return false;
  }

  if (NON_PUBLIC_HOSTNAMES.has(hostname)) {
    return false;
  }

  return !NON_PUBLIC_SUFFIXES.some((suffix) => hostname.endsWith(suffix));
};

/**
 * Resolves `hostname` with the current `AddressResolver`. A lookup error or an empty answer fails
 * closed with a retryable `network_error`.
 */
const resolveAddresses = Effect.fnUntraced(function* (
  hostname: string,
  url: URL,
  upstream: string,
): Effect.fn.Return<ReadonlyArray<string>, HttpRequestError> {
  const resolve = yield* AddressResolver;
  const addresses = yield* Effect.tryPromise({
    try: () => resolve(hostname),
    catch: (cause) => lookupFailure(upstream, url, describeLookupError(cause)),
  });
  if (addresses.length === 0) {
    return yield* lookupFailure(upstream, url, 'no addresses');
  }

  return addresses;
});

/**
 * Returns the absolute URL of `request`, or a refusal when it has none (a relative URL without a
 * base).
 */
const requestUrl = (
  request: HttpClientRequest.HttpClientRequest,
  upstream: string,
): Effect.Effect<URL, HttpRequestError> =>
  Effect.fromOption(HttpClientRequest.toUrl(request), () =>
    refusedRequest(upstream, 'request URL is not absolute'),
  );

/**
 * Resolves a redirect `Location` against the current URL, or fails with a refusal when it is not a
 * valid URL. The location is left out of the message because it may carry a secret.
 */
const resolveLocation = (
  location: string,
  current: URL,
  upstream: string,
): Effect.Effect<URL, HttpRequestError> =>
  Effect.try({
    try: () => new URL(location, current),
    catch: () => refusal(upstream, current, 'invalid redirect location'),
  });

/**
 * Describes `url` by its origin only, never its path, query or credentials. Schemes without an
 * origin, such as `file:`, are described by scheme and host.
 */
const describeOrigin = (url: URL): string =>
  url.origin === 'null' ? `${url.protocol}//${url.host}` : url.origin;

/**
 * Builds the refusal for a URL the guard rejects: never retried and ignored by the circuit breaker.
 */
const refusal = (upstream: string, url: URL, reason: string): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({
      upstream,
      status: null,
      message: `${upstream} refused ${describeOrigin(url)}: ${reason}`,
    }),
    { outcome: 'blocked', retryable: false, circuit: 'ignore' },
  );

/**
 * Builds the refusal for a request refused before it has a URL to name (an unparseable URL, an
 * unsafe header value), without echoing the URL or the value.
 */
const refusedRequest = (upstream: string, reason: string): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({ upstream, status: null, message: `${upstream} refused: ${reason}` }),
    { outcome: 'blocked', retryable: false, circuit: 'ignore' },
  );

/**
 * Builds the transient error for a failed or empty DNS lookup. It is retried, and the guard runs
 * again on the retry.
 */
const lookupFailure = (upstream: string, url: URL, reason: string): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({
      upstream,
      status: null,
      message: `${upstream} could not resolve ${describeOrigin(url)}: ${reason}`,
    }),
    { outcome: 'network_error' },
  );

/**
 * Returns the error code of a failed lookup (`ENOTFOUND`, `EAI_AGAIN`), or a generic reason.
 */
const describeLookupError = (cause: unknown): string => {
  if (cause instanceof Error && 'code' in cause && typeof cause.code === 'string') {
    return cause.code;
  }

  return 'lookup failed';
};
