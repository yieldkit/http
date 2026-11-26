import type { Duration, Redacted } from 'effect';
import type { HttpClient } from 'effect/http';

import { Effect, Option } from 'effect';
import { Headers, HttpClientRequest } from 'effect/http';

import type { HttpRequestError } from './errors.ts';
import type { ExecuteRequestBaseParams } from './execute.ts';
import type { ResolvedHttpPolicy } from './policy.ts';

import * as Policy from './policy.ts';

/**
 * Per-request overrides accepted by `ResilientHttpClient` convenience methods.
 */
export interface HttpRequestOptions {
  /**
   * Additional HTTP headers merged over the policy's `defaultHeaders`.
   */
  readonly headers?: Readonly<Record<string, string>> | undefined;

  /**
   * Query string parameters appended to the request URL.
   */
  readonly urlParams?: Readonly<Record<string, string | undefined>> | undefined;

  /**
   * Per-attempt timeout override.
   */
  readonly attemptTimeout?: Duration.Input | undefined;

  /**
   * Whole-operation deadline override: rate-limiter waits, every attempt, backoff and `Retry-After`
   * sleeps. It runs inside the circuit breaker, so running out of it counts as a breaker failure,
   * unless it ran out while the first attempt was still waiting for a rate-limit token. It does not
   * cover the `maxConcurrency` queue wait or the `use` phase of `withWebResponse`. For `stream` it
   * also covers reading the body: the stream fails once the whole call has run this long.
   */
  readonly operationTimeout?: Duration.Input | undefined;

  /**
   * For `stream` and `withWebResponse`: the longest wait for the next body chunk before the body
   * fails as stalled (outcome `timeout`). Defaults to `DEFAULT_IDLE_TIMEOUT` (30 seconds).
   */
  readonly idleTimeout?: Duration.Input | undefined;

  /**
   * Declares a POST or PATCH request safe to repeat, so a 5xx, a timeout or a transport error is
   * retried. Without it only a rate limit is retried for those methods.
   */
  readonly idempotent?: boolean | undefined;

  /**
   * Treats an HTTP 404 as success for `getText`, `getBytes`, `getResponse`, `withWebResponse` and
   * `execute`: the body is returned under the usual caps, there is no retry, and the metric outcome
   * is `not_found`. JSON methods ignore it; use `getOptionalJson`.
   */
  readonly acceptNotFound?: boolean | undefined;

  /**
   * Stale-if-error override for `getCachedJson`: the maximum age of a cached payload that may be
   * served when the upstream fails, or `false` to disable it for this request.
   */
  readonly staleIfError?: Duration.Input | false | undefined;

  /**
   * Maximum response body byte limit override.
   */
  readonly maxBodyBytes?: number | undefined;

  /**
   * Optional Bearer authentication token (`Redacted` or plain string).
   */
  readonly bearerToken?: Redacted.Redacted | string | undefined;

  /**
   * Optional `If-None-Match` ETag header value for conditional requests.
   */
  readonly ifNoneMatch?: string | null | undefined;

  /**
   * Optional `If-Modified-Since` timestamp header value for conditional requests.
   */
  readonly ifModifiedSince?: string | null | undefined;
}

/**
 * The parts of a client runtime needed to build request parameters.
 */
export type RequestRuntime = {
  /**
   * Underlying Effect `HttpClient` transport.
   */
  readonly httpClient: HttpClient.HttpClient;

  /**
   * Resolved policy for the upstream.
   */
  readonly policy: ResolvedHttpPolicy;
};

/**
 * Returns `value` when it is a valid duration, otherwise `fallback`.
 */
const validDurationOr = (
  value: Duration.Input | undefined,
  fallback: Duration.Input,
): Duration.Input =>
  value !== undefined && Policy.checkedDuration(value) !== null ? value : fallback;

/**
 * Lists what is wrong with the validated per-request options, or an empty list.
 */
const requestOptionIssues = (options: HttpRequestOptions): Array<string> => {
  const issues: Array<string> = [];
  for (const [name, value] of [
    ['attemptTimeout', options.attemptTimeout],
    ['operationTimeout', options.operationTimeout],
    ['idleTimeout', options.idleTimeout],
    ['staleIfError', options.staleIfError === false ? undefined : options.staleIfError],
  ] as const) {
    if (value !== undefined && Policy.checkedDuration(value) === null) {
      issues.push(`${name} must be a finite, non-negative duration`);
    }
  }

  if (
    options.maxBodyBytes !== undefined &&
    !Policy.POSITIVE_SAFE_INTEGER.accepts(options.maxBodyBytes)
  ) {
    issues.push(`maxBodyBytes must be ${Policy.POSITIVE_SAFE_INTEGER.description}`);
  }

  return issues;
};

/**
 * Returns the typed failure for invalid per-request options (`maxBodyBytes` that is zero, `NaN` or
 * infinite, a timeout such as `'1e3 millis'`), or `null` when they are valid. The failure is a
 * refusal before anything is sent: never retried and ignored by the circuit breaker.
 */
export const requestOptionsError = (
  upstream: string,
  options: HttpRequestOptions | undefined,
): HttpRequestError | null => {
  const issues = options === undefined ? [] : requestOptionIssues(options);
  return issues.length === 0
    ? null
    : Policy.invalidConfigError(upstream, 'request options', issues);
};

/**
 * Validates the resolved policy and the per-request options before a request is built. It fails
 * with the typed `HttpRequestError` of the first problem found (`policy.configError`, then
 * `requestOptionsError`), so an invalid value never reaches the transport, hangs or dies.
 */
export const validateRequestConfig = (
  policy: ResolvedHttpPolicy,
  options: HttpRequestOptions | undefined,
): Effect.Effect<void, HttpRequestError> => {
  const error = policy.configError ?? requestOptionsError(policy.upstream, options);
  return error === null ? Effect.void : Effect.fail(error);
};

/**
 * Resolves the timeouts, body cap and request flags from per-request options over the policy. An
 * invalid per-request value falls back to the policy value, so the parameters never carry a `NaN`,
 * infinite or unparseable limit; `validateRequestConfig` reports it as a typed failure.
 */
const resolveRequestLimits = (
  policy: ResolvedHttpPolicy,
  options: HttpRequestOptions | undefined,
): Pick<
  ExecuteRequestBaseParams,
  'attemptTimeout' | 'operationTimeout' | 'maxBodyBytes' | 'idempotent' | 'acceptNotFound'
> => ({
  attemptTimeout: validDurationOr(options?.attemptTimeout, policy.attemptTimeout),
  operationTimeout: validDurationOr(options?.operationTimeout, policy.operationTimeout),
  maxBodyBytes:
    options?.maxBodyBytes !== undefined &&
    Policy.POSITIVE_SAFE_INTEGER.accepts(options.maxBodyBytes)
      ? options.maxBodyBytes
      : policy.maxBodyBytes,
  idempotent: options?.idempotent,
  acceptNotFound: options?.acceptNotFound,
});

/**
 * Applies per-request headers, query parameters, Bearer token, and conditional validators to a
 * request.
 */
const applyRequestOptions = (
  initial: HttpClientRequest.HttpClientRequest,
  options: HttpRequestOptions,
): HttpClientRequest.HttpClientRequest => {
  let request =
    options.headers !== undefined
      ? HttpClientRequest.setHeaders(initial, options.headers)
      : initial;
  if (options.urlParams !== undefined) {
    for (const [key, value] of Object.entries(options.urlParams)) {
      if (value !== undefined) {
        request = HttpClientRequest.appendUrlParam(request, key, value);
      }
    }
  }

  if (options.bearerToken !== undefined) {
    request = HttpClientRequest.bearerToken(request, options.bearerToken);
  }

  if (options.ifNoneMatch !== undefined && options.ifNoneMatch !== null) {
    request = HttpClientRequest.setHeader(request, 'if-none-match', options.ifNoneMatch);
  }

  if (options.ifModifiedSince !== undefined && options.ifModifiedSince !== null) {
    request = HttpClientRequest.setHeader(request, 'if-modified-since', options.ifModifiedSince);
  }

  return request;
};

/**
 * Returns the policy headers a request gets: `User-Agent` from `policy.userAgent` (when set), then
 * `defaultHeaders` over it. The default headers are bound to the base URL's origin: a request to
 * another origin (an absolute URL from a pagination link or a redirect target the caller passes on)
 * gets only the `User-Agent`, so credentials in `defaultHeaders` never reach a host the policy did
 * not name. Without a `baseUrl` every request gets them.
 */
const policyHeaders = (
  policy: ResolvedHttpPolicy,
  request: HttpClientRequest.HttpClientRequest,
): Headers.Headers => {
  const userAgent = Headers.fromInput(
    policy.userAgent === null ? {} : { 'user-agent': policy.userAgent },
  );
  if (!isBaseOrigin(policy.baseUrl, request)) {
    return userAgent;
  }

  return Headers.setAll(userAgent, policy.defaultHeaders);
};

/**
 * Returns `true` when `request` goes to the origin of `baseUrl`, when there is no base URL, or when
 * the request URL does not parse (the transport rejects such a request anyway). A base URL that
 * does not parse names no origin, so it gives `false` and the default headers stay off the request
 * (the policy check rejects such a base as well).
 */
const isBaseOrigin = (
  baseUrl: string | null,
  request: HttpClientRequest.HttpClientRequest,
): boolean => {
  if (baseUrl === null || baseUrl.length === 0) {
    return true;
  }

  const target = HttpClientRequest.toUrl(request);
  if (Option.isNone(target)) {
    return true;
  }

  if (!URL.canParse(baseUrl)) {
    return false;
  }

  return new URL(baseUrl).origin === target.value.origin;
};

/**
 * Adds each of `defaults` that `request` does not already carry, so a header the caller put on its
 * own request (`execute` with an `Authorization` or `Accept`) is never replaced by a policy
 * default.
 */
const addMissingHeaders = (
  request: HttpClientRequest.HttpClientRequest,
  defaults: Headers.Headers,
): HttpClientRequest.HttpClientRequest =>
  Object.entries(defaults)
    .filter(([name]) => Option.isNone(Headers.get(request.headers, name)))
    .reduce((current, [name, value]) => HttpClientRequest.setHeader(current, name, value), request);

/**
 * Builds `ExecuteRequestBaseParams` by combining the upstream policy with per-request options. The
 * header order, from weakest to strongest, is: the policy's `userAgent`, its `defaultHeaders` (only
 * for the base URL's origin, see `policyHeaders`), `Accept: application/json` when
 * `defaultAcceptJson` is set and no `Accept` is set yet, the headers already on `baseRequest`, then
 * `options.headers`, `bearerToken` and the conditional validators.
 */
export const prepareRequestParams = (
  runtime: RequestRuntime,
  baseRequest: HttpClientRequest.HttpClientRequest,
  options?: HttpRequestOptions,
  defaultAcceptJson = false,
): ExecuteRequestBaseParams => {
  const { httpClient, policy } = runtime;
  let request = HttpClientRequest.setUrl(
    baseRequest,
    Policy.resolveRequestUrl(policy.baseUrl, baseRequest.url),
  );
  request = addMissingHeaders(request, policyHeaders(policy, request));
  if (defaultAcceptJson && Option.isNone(Headers.get(request.headers, 'accept'))) {
    request = HttpClientRequest.acceptJson(request);
  }

  if (options !== undefined) {
    request = applyRequestOptions(request, options);
  }

  return {
    client: httpClient,
    request,
    upstream: policy.upstream,
    ...resolveRequestLimits(policy, options),
    treatQuotaExhausted403AsRateLimit: policy.treatQuotaExhausted403AsRateLimit,
    treat403AsRateLimit: policy.treat403AsRateLimit,
    detectHtmlEdgeBlocks: policy.detectHtmlEdgeBlocks,
    retry: policy.retry,
    rateLimit: policy.rateLimit ?? undefined,
    circuitBreaker: policy.circuitBreaker ?? undefined,
    publicAddressesOnly: policy.publicAddressesOnly,
    maxRedirects: policy.maxRedirects,
  };
};
