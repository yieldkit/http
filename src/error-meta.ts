import type { HttpError } from './errors.ts';

/**
 * Low-cardinality outcome label recorded on `resilient_http_requests_total` and latency histograms.
 */
export type HttpOutcomeLabel =
  | 'success'
  | 'not_modified'
  | 'not_found'
  | 'rate_limited'
  | 'client_error'
  | 'server_error'
  | 'timeout'
  | 'network_error'
  | 'decode_error'
  | 'unexpected_status'
  | 'circuit_open'
  | 'body_too_large'
  | 'blocked';

/**
 * Classification attached to an error by the code that built it, so metrics, retries and the
 * circuit breaker never have to guess from the error message.
 */
export type HttpErrorMeta = {
  /**
   * Metric outcome label, used before any status or message heuristic.
   */
  readonly outcome?: HttpOutcomeLabel | undefined;

  /**
   * `false` stops the retry schedule even when the status would count as transient.
   */
  readonly retryable?: boolean | undefined;

  /**
   * `'ignore'` keeps the failure from counting against the circuit breaker.
   */
  readonly circuit?: 'ignore' | undefined;

  /**
   * Whole seconds of the `Retry-After` header on a retryable status error (a 503, a 408). The retry
   * schedule waits at least this long, and a wait longer than `maxInlineRetryAfterSeconds` is not
   * retried at all. A `HttpRateLimitedError` carries its own `retryAfterSeconds` instead.
   */
  readonly retryAfterSeconds?: number | undefined;

  /**
   * `true` when the response that produced the error came from a redirect hop rather than from the
   * original request. A rate limit on such a hop says nothing about the original request, so a POST
   * or PATCH is not replayed for it.
   */
  readonly redirected?: boolean | undefined;
};

/**
 * Metadata keyed by error instance. A `WeakMap` keeps the error shape unchanged and lets the
 * metadata be collected with the error.
 */
const HTTP_ERROR_META = new WeakMap<HttpError, HttpErrorMeta>();

/**
 * Attaches `meta` to `error`, merged over any metadata it already has, and returns `error`.
 */
export const tagHttpError = <E extends HttpError>(error: E, meta: HttpErrorMeta): E => {
  HTTP_ERROR_META.set(error, { ...HTTP_ERROR_META.get(error), ...meta });
  return error;
};

/**
 * Returns the metadata attached to `error`, or an empty object when it has none.
 */
export const readHttpErrorMeta = (error: HttpError): HttpErrorMeta =>
  HTTP_ERROR_META.get(error) ?? {};
