import type { Tracer } from 'effect';
import type { HttpClientError, HttpClientResponse } from 'effect/http';

import { isSensitiveQueryParamName } from '@yieldkit/logger';
import { Cause, Clock, Effect, Exit, Option, References } from 'effect';
import { HttpClient, HttpClientRequest, UrlParams } from 'effect/http';

import type { HttpErrorMeta } from './error-meta.ts';
import type { SentValueMask } from './sent-values.ts';

import { tagHttpError } from './error-meta.ts';
import { HttpRequestError } from './errors.ts';
import { currentSentValueMask } from './sent-values.ts';
import { redactUrlForLogs, requestRedactedNames } from './url-redaction.ts';

/**
 * Undici error codes for timeouts reported by the request transport.
 */
const UNDICI_TIMEOUT_CODES: ReadonlySet<unknown> = new Set([
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_BODY_TIMEOUT',
]);

/**
 * Returns the names of the query parameters of `request` that the logs mask (see
 * `isSensitiveQueryParamName`), so Effect masks the same values in the URLs of `HttpClientError`
 * reasons. A request whose URL cannot be built has none.
 */
const credentialParamNames = (
  request: HttpClientRequest.HttpClientRequest,
): ReadonlyArray<string> => {
  const url = HttpClientRequest.toUrl(request);
  return Option.isNone(url)
    ? []
    : [...new Set(url.value.searchParams.keys())].filter(isSensitiveQueryParamName);
};

/**
 * Absolute URLs of any scheme (`https://…`, `ftp://…`) inside free text, cut down to their origin
 * before the text is quoted.
 */
const ABSOLUTE_URL_RE = /\b[a-z][a-z0-9+.-]*:\/\/[^\s"'<>]+/gi;

/**
 * Returns the origin of `url`, or `<invalid url>` when it does not parse. The path, query, fragment
 * and userinfo are dropped, so nothing the caller put in the URL can leak through it.
 */
const describeRequestOrigin = (url: string): string => {
  try {
    const parsed = new URL(url);
    return parsed.origin === 'null' ? `${parsed.protocol}//${parsed.host}` : parsed.origin;
  } catch {
    return '<invalid url>';
  }
};

/**
 * Cleans free text taken from an underlying error before it is quoted: the credentials the request
 * sent are masked with `mask`, then every absolute URL becomes its origin. Other text is kept as
 * the transport wrote it.
 */
const describeUntrustedText = (text: string, mask: SentValueMask): string =>
  mask(text).replaceAll(ABSOLUTE_URL_RE, (url) => describeRequestOrigin(url));

/**
 * Returns the text that explains why a client error happened: its own description, or else the code
 * and message of the underlying cause, each cleaned with `describeUntrustedText`. Returns `null`
 * when there is nothing to say.
 */
const describeReasonDetail = (
  reason: HttpClientError.HttpClientErrorReason,
  mask: SentValueMask,
): string | null => {
  if (reason.description !== undefined && reason.description !== '') {
    return describeUntrustedText(reason.description, mask);
  }

  const cause: unknown = reason.cause;
  if (!(cause instanceof Error)) {
    return null;
  }

  const code = 'code' in cause ? cause.code : undefined;
  const detail =
    typeof code === 'string' && code !== '' && !cause.message.includes(code)
      ? `${cause.message} (${code})`
      : cause.message;
  return detail === '' ? null : describeUntrustedText(detail, mask);
};

/**
 * `SentValueMask` that changes nothing, for describing an error without its request's credentials.
 */
const keepText: SentValueMask = (text) => text;

/**
 * Describes a client error without its URL: the reason (`Transport error`, `InvalidUrl error`), its
 * cleaned detail (with the credentials the request sent masked by `mask`, when given), the method
 * and the request origin, as in `Transport error: connection reset (GET https://api.example.com)`.
 */
export const describeClientError = (
  error: HttpClientError.HttpClientError,
  mask: SentValueMask = keepText,
): string => {
  const { reason } = error;
  const label = `${reason._tag.replace(/Error$/, '')} error`;
  const detail = describeReasonDetail(reason, mask);
  const status = error.response === undefined ? '' : `HTTP ${String(error.response.status)}, `;
  const target = `${status}${reason.request.method} ${describeRequestOrigin(reason.request.url)}`;
  return detail === null ? `${label} (${target})` : `${label}: ${detail} (${target})`;
};

/**
 * Picks the metadata of a client error. A failure with a response is classified by its status. A
 * request that could not be built (bad URL, body that failed to encode) is never retried and never
 * counts against the circuit breaker. A transport failure is a `network_error`, or a `timeout` when
 * the transport gave up waiting.
 */
const clientErrorMeta = (error: HttpClientError.HttpClientError): HttpErrorMeta => {
  const { reason } = error;
  if (error.response !== undefined) {
    return {};
  }

  if (reason._tag === 'InvalidUrlError' || reason._tag === 'EncodeError') {
    return { outcome: 'blocked', retryable: false, circuit: 'ignore' };
  }

  const cause: unknown = reason.cause;
  const undiciTimeout =
    cause instanceof Error && 'code' in cause && UNDICI_TIMEOUT_CODES.has(cause.code);
  return { outcome: Cause.isTimeoutError(cause) || undiciTimeout ? 'timeout' : 'network_error' };
};

/**
 * Builds the `HttpRequestError` for a client error: the response status when there is one, a
 * message from `describeClientError` with the sent credentials masked by `mask`, and the metadata
 * of `clientErrorMeta`.
 */
const clientFailure = (
  cause: HttpClientError.HttpClientError,
  upstream: string,
  mask: SentValueMask,
): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({
      upstream,
      status: cause.response?.status ?? null,
      message: `${upstream} request failed: ${describeClientError(cause, mask)}`,
    }),
    clientErrorMeta(cause),
  );

/**
 * Runs the request transforms of `client` (`HttpClient.mapRequest`, `mapRequestEffect` and the
 * like, its `preprocess`) on `request` once, giving the request as it will be sent. A failing
 * transform becomes an `HttpRequestError` built as `sendRequest` builds one, with the credentials
 * of the request as the transforms left it masked, so a token an earlier transform added is masked
 * too.
 */
export const prepareRequest = (
  client: HttpClient.HttpClient,
  request: HttpClientRequest.HttpClientRequest,
  upstream: string,
): Effect.Effect<HttpClientRequest.HttpClientRequest, HttpRequestError> =>
  Effect.catch(client.preprocess(request), (cause) =>
    Effect.flatMap(currentSentValueMask(cause.request), (mask) =>
      Effect.fail(clientFailure(cause, upstream, mask)),
    ),
  );

/**
 * Port implied by a URL scheme when the URL names none.
 */
const DEFAULT_PORTS = new Map([
  ['http:', 80],
  ['https:', 443],
]);

/**
 * Lowest status OpenTelemetry marks as a failed client span.
 */
const FIRST_ERROR_STATUS = 400;

/**
 * First status past the 5xx range. Effect's own client span marks only 4xx and 5xx as failed.
 */
const PAST_ERROR_STATUS = 600;

/**
 * Brackets around an IPv6 literal hostname, which the semantic conventions leave out.
 */
const IPV6_BRACKETS_RE = /^\[|\]$/g;

/**
 * Sets the URL attributes of a client span (the OpenTelemetry names Effect uses: `url.full`,
 * `url.path`, `url.query`, `url.scheme`, `server.address`, `server.port`). The URL is first masked
 * as Effect's own client span masks it (`UrlParams.redactUrl` with `redactedNames` and the names of
 * `Redacted` query parameters, see `requestRedactedNames`), then with `redactUrlForLogs`, so the
 * span masks everything either masks and never carries userinfo or the fragment. A URL that cannot
 * be built sets no URL attributes.
 */
const addUrlAttributes = (
  span: Tracer.Span,
  request: HttpClientRequest.HttpClientRequest,
  redactedNames: ReadonlyArray<string | RegExp>,
): void => {
  const url = HttpClientRequest.toUrl(request);
  if (Option.isNone(url)) {
    return;
  }

  const redacted = redactUrlForLogs(
    UrlParams.redactUrl(url.value.href, requestRedactedNames(request, redactedNames)),
  );
  span.attribute('url.full', redacted);
  if (!URL.canParse(redacted)) {
    return;
  }

  const parsed = new URL(redacted);
  span.attribute('url.path', parsed.pathname);
  const queryStart = redacted.indexOf('?');
  if (queryStart !== -1 && queryStart < redacted.length - 1) {
    span.attribute('url.query', redacted.slice(queryStart + 1));
  }

  span.attribute('url.scheme', parsed.protocol.slice(0, -1));
  span.attribute('server.address', parsed.hostname.replaceAll(IPV6_BRACKETS_RE, ''));
  const port = parsed.port === '' ? DEFAULT_PORTS.get(parsed.protocol) : Number(parsed.port);
  if (port !== undefined) {
    span.attribute('server.port', port);
  }
};

/**
 * Records the response status on the client span. A 4xx or 5xx also sets `error.type` to the status
 * code and ends the span at once with a failed exit, so exporters give it the Error status the
 * OpenTelemetry HTTP client conventions ask for while the response itself still succeeds, as
 * Effect's own client span does. The failure is a sanitised `HttpRequestError` carrying only the
 * upstream and status, never the request or response. Ending honours
 * `References.TracerTimingEnabled`; the later end by `Effect.useSpan` is then a no-op.
 */
const recordResponseStatus = Effect.fnUntraced(function* (
  span: Tracer.Span,
  response: HttpClientResponse.HttpClientResponse,
  upstream: string,
) {
  span.attribute('http.response.status_code', response.status);
  if (response.status < FIRST_ERROR_STATUS || response.status >= PAST_ERROR_STATUS) {
    return;
  }

  span.attribute('error.type', String(response.status));
  const timingEnabled = yield* References.TracerTimingEnabled;
  const endTime = timingEnabled ? yield* Clock.currentTimeNanos : BigInt(0);
  const failure = new HttpRequestError({
    upstream,
    status: response.status,
    message: `${upstream} responded with HTTP ${String(response.status)}`,
  });
  span.end(endTime, Exit.fail(failure));
});

/**
 * Returns the `error.type` of a failed request: the response status when one arrived, or else the
 * tag of the failure reason (`TransportError`, `InvalidUrlError`, `EncodeError`), a low-cardinality
 * identifier as the OpenTelemetry HTTP client conventions ask for.
 */
const errorType = (cause: HttpClientError.HttpClientError): string =>
  cause.response === undefined ? cause.reason._tag : String(cause.response.status);

/**
 * Sends one request through `client` without trace propagation headers, mapping a transport or
 * client failure to an `HttpRequestError` carrying the response status when there is one. The
 * request must already be prepared (see `prepareRequest`): only the client's `postprocess` runs
 * here, so its request transforms never run again on a redirect hop. The error message names the
 * method and origin only, never the path or query, and the failure is tagged with its metric
 * outcome so classification never reads the message.
 *
 * The request runs in a client span of this package instead of Effect's own: its URL attributes are
 * masked as Effect's span masks them (`UrlParams.CurrentRedactedNames`, `Redacted` query values)
 * and as the logs mask them (`redactUrlForLogs`). The error message masks the credentials the
 * request sent (`currentSentValueMask`) wherever the transport's detail quotes them. A 4xx or 5xx
 * response and a failure end it with a failed exit and set `error.type` (see `recordResponseStatus`
 * and `errorType`); the exit holds the sanitised `HttpRequestError`, never the raw transport cause
 * or the unredacted URL. The span honours `HttpClient.SpanNameGenerator` and is skipped when
 * `HttpClient.TracerDisabledWhen` matches the request.
 */
export const sendRequest = Effect.fnUntraced(function* (
  client: HttpClient.HttpClient,
  request: HttpClientRequest.HttpClientRequest,
  upstream: string,
): Effect.fn.Return<HttpClientResponse.HttpClientResponse, HttpRequestError> {
  const redactedNames = yield* UrlParams.CurrentRedactedNames;
  const tracerDisabledWhen = yield* HttpClient.TracerDisabledWhen;
  const spanName = yield* HttpClient.SpanNameGenerator;
  const execute = client.postprocess(Effect.succeed(request)).pipe(
    Effect.provideService(HttpClient.TracerPropagationEnabled, false),
    Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
    Effect.provideService(UrlParams.CurrentRedactedNames, [
      ...redactedNames,
      ...credentialParamNames(request),
    ]),
  );
  const mask = yield* currentSentValueMask(request);
  const sanitize = Effect.mapError((cause: HttpClientError.HttpClientError) =>
    clientFailure(cause, upstream, mask),
  );
  if (tracerDisabledWhen(request)) {
    return yield* sanitize(execute);
  }

  return yield* Effect.useSpan(spanName(request), { kind: 'client' }, (span) => {
    span.attribute('http.request.method', request.method);
    addUrlAttributes(span, request, redactedNames);
    const traced = execute.pipe(
      Effect.tap((response) => recordResponseStatus(span, response, upstream)),
      Effect.tapError((cause) =>
        Effect.sync(() => {
          if (cause.response !== undefined) {
            span.attribute('http.response.status_code', cause.response.status);
          }

          span.attribute('error.type', errorType(cause));
        }),
      ),
      sanitize,
    );
    return Effect.withParentSpan(traced, span);
  });
});
