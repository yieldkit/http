import { Schema } from 'effect';

/**
 * Raised when the upstream throttles a request: an HTTP 429, a 403 with rate-limit headers, or a
 * refusal by the local rate limiter.
 */
export class HttpRateLimitedError extends Schema.TaggedError<
  HttpRateLimitedError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('HttpRateLimitedError', {
  /**
   * Name of the upstream (the client's `policy.upstream`), such as `example-api`.
   */
  upstream: Schema.String,

  /**
   * Seconds to wait before retrying, from `Retry-After` or the rate-limit reset headers, or `null`
   * when the response gave none.
   */
  retryAfterSeconds: Schema.NullOr(Schema.Finite),

  /**
   * Description of the rate limit, safe to log.
   */
  message: Schema.String,
}) {}

/**
 * Raised when a request fails: the transport fails, the response has a non-2xx status, the body is
 * too large or cannot be decoded, the circuit is open, or the request is blocked.
 */
export class HttpRequestError extends Schema.TaggedError<
  HttpRequestError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('HttpRequestError', {
  /**
   * Name of the upstream (the client's `policy.upstream`), such as `example-api`.
   */
  upstream: Schema.String,

  /**
   * HTTP status associated with the failure, including a response's status on decode failures and
   * 503 when the circuit refuses a call. Failures without an associated status use `null`.
   */
  status: Schema.NullOr(Schema.Int),

  /**
   * Short description of the failure: the upstream, the status or reason and, for a non-2xx
   * response, the upstream request id. It never quotes the response body, and URLs in it carry no
   * path or query.
   */
  message: Schema.String,

  /**
   * Start of the response body, for a non-2xx response or a body that failed to decode: control
   * characters and runs of whitespace collapsed, a JSON body with the values under sensitive key
   * names masked, and every credential the request sent masked wherever it appears. Other text is
   * kept as the upstream wrote it, so treat it as untrusted. `undefined` when there is no body to
   * quote.
   */
  bodySnippet: Schema.optional(Schema.String),

  /**
   * Why the body did not match the schema, for a decode failure: the schema issue with the
   * credentials the request sent masked, cut at 500 characters. Issues can quote body values, so
   * treat it as untrusted like `bodySnippet`. `undefined` for any other failure.
   */
  decodeIssue: Schema.optional(Schema.String),
}) {}

/**
 * Union of the errors outbound HTTP operations fail with.
 */
export type HttpError = HttpRateLimitedError | HttpRequestError;

/**
 * Type guard for `HttpRateLimitedError`, built once.
 */
const isHttpRateLimitedError = Schema.is(HttpRateLimitedError);

/**
 * Type guard for `HttpRequestError`, built once.
 */
const isHttpRequestError = Schema.is(HttpRequestError);

/**
 * Returns `true` when `value` is one of this package's errors, as found in a squashed `Cause`. Any
 * other value (a defect, an interruption, a foreign error) gives `false`.
 */
export const isHttpError = (value: unknown): value is HttpError =>
  isHttpRateLimitedError(value) || isHttpRequestError(value);
