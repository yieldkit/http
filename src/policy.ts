import { Duration, Effect, Option } from 'effect';

import { tagHttpError } from './error-meta.ts';
import { HttpRequestError } from './errors.ts';

/**
 * Default per-attempt request timeout covering transport headers and response body reading.
 */
export const REQUEST_TIMEOUT = '15 seconds' as const;

/**
 * Default whole-operation deadline covering rate-limiter waits, every attempt, retry backoff and
 * `Retry-After` sleeps. Without it the worst case was about 62 seconds with the defaults and no
 * `Retry-After`, and longer with `Retry-After` sleeps or a larger `maxRetries`.
 */
export const DEFAULT_OPERATION_TIMEOUT = '60 seconds' as const;

/**
 * Longest wait for the next body chunk in `stream` and `withWebResponse` before the body fails as
 * stalled.
 */
export const DEFAULT_IDLE_TIMEOUT = '30 seconds' as const;

/**
 * Default number of redirect hops a request follows before it is refused.
 */
export const DEFAULT_MAX_REDIRECTS = 5;

/**
 * Default maximum number of automatic retry attempts for transient HTTP or transport errors.
 */
export const DEFAULT_MAX_RETRIES = 3;

/**
 * Default initial exponential backoff delay before the first retry attempt.
 */
export const DEFAULT_BASE_DELAY = '250 millis' as const;

/**
 * Default upper bound in seconds for exponential retry backoff delays.
 */
export const DEFAULT_MAX_BACKOFF_SECONDS = 5;

/**
 * Maximum `Retry-After` delay in seconds that will be slept inline during a retry loop.
 */
export const DEFAULT_MAX_INLINE_RETRY_AFTER_SECONDS = 10;

/**
 * Default policy for retrying HTTP 429 / rate-limit responses inline: disabled by default so rate
 * limits surface immediately unless a client opts in.
 */
export const DEFAULT_RETRY_ON_RATE_LIMIT = false;

/**
 * Fallback cooldown in seconds when an HTTP 403 edge/burst block omits `Retry-After`.
 */
export const DEFAULT_EDGE_403_RETRY_AFTER_SECONDS = 60;

/**
 * Default upper bound in bytes for buffered text or byte response bodies (2 MiB).
 */
export const DEFAULT_MAX_BODY_BYTES = 2 * 1024 * 1024;

/**
 * Consecutive failures after which `HttpResilienceLive` opens an upstream's circuit.
 */
export const DEFAULT_CIRCUIT_CONSECUTIVE_FAILURES = 5;

/**
 * First open period of a circuit in `HttpResilienceLive`. Each reopening after a failed probe
 * doubles it, for as long as `DEFAULT_CIRCUIT_OPEN_BACKOFF_LIMIT`.
 */
export const DEFAULT_CIRCUIT_OPEN_DURATION = '5 seconds' as const;

/**
 * How long the open period of a circuit in `HttpResilienceLive` keeps doubling.
 */
export const DEFAULT_CIRCUIT_OPEN_BACKOFF_LIMIT = '5 minutes' as const;

/**
 * Configures exponential backoff, jitter, and inline `Retry-After` thresholds for an upstream.
 */
export interface HttpRetryPolicy {
  /**
   * Maximum retry attempts after the initial request fails with a transient error.
   */
  readonly maxRetries: number;

  /**
   * Base delay for exponential backoff calculation.
   */
  readonly baseDelay: Duration.Input;

  /**
   * Maximum backoff cap in seconds, applied after jitter and before `Retry-After` floors.
   */
  readonly maxBackoffSeconds: number;

  /**
   * Maximum `Retry-After` delay in seconds eligible for inline retry sleeping. A 429, 503 or other
   * transient status asking for longer is not retried.
   */
  readonly maxInlineRetryAfterSeconds: number;

  /**
   * Whether HTTP 429 / `HttpRateLimitedError` responses with a short `Retry-After` are retried
   * inline. Defaults to `DEFAULT_RETRY_ON_RATE_LIMIT` (`false`).
   */
  readonly retryOnRateLimit?: boolean | undefined;
}

/**
 * Fully resolved `HttpRetryPolicy` with all optional fields normalized to concrete defaults.
 */
export interface ResolvedHttpRetryPolicy {
  /**
   * Maximum retry attempts after the initial request fails with a transient error.
   */
  readonly maxRetries: number;

  /**
   * Base delay for exponential backoff calculation.
   */
  readonly baseDelay: Duration.Input;

  /**
   * Maximum backoff cap in seconds, applied after jitter and before `Retry-After` floors.
   */
  readonly maxBackoffSeconds: number;

  /**
   * Maximum `Retry-After` delay in seconds eligible for inline retry sleeping.
   */
  readonly maxInlineRetryAfterSeconds: number;

  /**
   * Whether HTTP 429 / `HttpRateLimitedError` responses with a short `Retry-After` are retried
   * inline.
   */
  readonly retryOnRateLimit: boolean;
}

/**
 * Configures proactive client-side rate limiting via Effect `RateLimiter`.
 */
export interface HttpRateLimitPolicy {
  /**
   * Optional shared rate-limiter bucket key; defaults to `ratelimit:${upstream}`.
   */
  readonly key?: string | undefined;

  /**
   * Maximum number of requests permitted within `window`.
   */
  readonly limit: number;

  /**
   * Refill or fixed-window duration for token consumption.
   */
  readonly window: Duration.Input;

  /**
   * Rate-limiting algorithm (`token-bucket` smooths bursts; `fixed-window` enforces discrete
   * windows).
   */
  readonly algorithm?: 'token-bucket' | 'fixed-window' | undefined;

  /**
   * Action when tokens are exhausted. Every attempt takes a token, retries included. `delay`, the
   * default, waits and asks the limiter again, reserving no token while it waits but keeping the
   * call's `maxConcurrency` permit; `fail` raises `HttpRateLimitedError` at once, which is not
   * retried and is ignored by the circuit breaker.
   */
  readonly onExceeded?: 'delay' | 'fail' | undefined;
}

/**
 * Configures per-upstream fault isolation via Effect `CircuitBreaker`. When the breaker trips and
 * how long it stays open are set where the `CircuitBreaker` service is built (see
 * `makeHttpResilienceLayer`), because one breaker serves every key.
 */
export interface HttpCircuitBreakerPolicy {
  /**
   * Optional circuit key; defaults to `circuit:${upstream}`. Upstreams that share a key share one
   * circuit.
   */
  readonly key?: string | undefined;
}

/**
 * Normalized circuit breaker configuration with all defaults applied.
 */
export interface ResolvedHttpCircuitBreakerPolicy {
  /**
   * Resolved circuit breaker store key.
   */
  readonly key: string;
}

/**
 * Provider-agnostic HTTP and scraping policy for an upstream or endpoint group.
 */
export interface HttpPolicy {
  /**
   * Logical upstream identifier used in metrics, logs, rate-limit errors, and cache keys.
   */
  readonly upstream: string;

  /**
   * Optional base URL prepended to relative request paths. It must be an absolute `http:` or
   * `https:` URL without a query or fragment. An invalid base is reported in `configError` and
   * every request fails.
   */
  readonly baseUrl?: string | undefined;

  /**
   * `User-Agent` header sent on every request that does not set its own. Omitted, the client adds
   * none and the transport's default (such as Bun's `Bun/<version>`) goes out.
   */
  readonly userAgent?: string | undefined;

  /**
   * Headers added to every request for this upstream that does not set them itself. With a
   * `baseUrl` they go only to requests on the base URL's origin, so credentials here never reach
   * another host through an absolute URL.
   */
  readonly defaultHeaders?: Readonly<Record<string, string>> | undefined;

  /**
   * Per-attempt timeout covering headers and body consumption; defaults to `REQUEST_TIMEOUT`.
   */
  readonly attemptTimeout?: Duration.Input | undefined;

  /**
   * Deadline for the whole operation: rate-limiter waits, every attempt including its body read,
   * backoff and `Retry-After` sleeps; defaults to `DEFAULT_OPERATION_TIMEOUT`. A retry whose delay
   * would end at or after the deadline is not started: the last real error is returned instead of
   * sleeping to the deadline. It runs inside the circuit breaker, so running out of it counts as a
   * breaker failure, unless it ran out while the first attempt was still waiting for a rate-limit
   * token. It does not cover the `maxConcurrency` queue wait or the `use` phase of
   * `withWebResponse`.
   */
  readonly operationTimeout?: Duration.Input | undefined;

  /**
   * Maximum response body size in bytes for text and binary reads; defaults to
   * `DEFAULT_MAX_BODY_BYTES`.
   */
  readonly maxBodyBytes?: number | undefined;

  /**
   * Opt-in stale-if-error for `getCachedJson`: the maximum age of a cached payload that may be
   * served when the upstream fails with a rate limit, 5xx, timeout, network, decode, open-circuit
   * or oversized-body error. Omitted disables it. A request can override it with
   * `HttpRequestOptions.staleIfError`.
   */
  readonly staleIfError?: Duration.Input | undefined;

  /**
   * Opt-in server-side request forgery guard: every request and redirect hop must use `http:` or
   * `https:`, carry no URL credentials, and reach only a public hostname whose every resolved
   * address is public. Defaults to `false`. Redirects are followed by this package, never by fetch,
   * whether the guard is on or off; with the guard on, a response the transport itself redirected
   * (`HttpClient.followRedirects`) is refused, because its hops were never checked.
   */
  readonly publicAddressesOnly?: boolean | undefined;

  /**
   * Redirect hops a request follows before it is refused; defaults to `DEFAULT_MAX_REDIRECTS`.
   */
  readonly maxRedirects?: number | undefined;

  /**
   * Optional maximum concurrent in-flight requests enforced via an Effect `Semaphore` bulkhead. A
   * call holds its permit for its whole run, rate-limiter waits, retries, backoff and `Retry-After`
   * sleeps included, so size it for calls sleeping in backoff too: at 100 with 20% 503s, throughput
   * fell from 3,606 to 907 calls per second on localhost.
   */
  readonly maxConcurrency?: number | undefined;

  /**
   * Treats HTTP 403 responses with `x-ratelimit-remaining: 0` as `HttpRateLimitedError`.
   */
  readonly treatQuotaExhausted403AsRateLimit?: boolean | undefined;

  /**
   * Unconditionally treats HTTP 403 responses as `HttpRateLimitedError` (for burst-throttling
   * APIs).
   */
  readonly treat403AsRateLimit?: boolean | undefined;

  /**
   * Inspects HTTP 403 HTML bodies for CDN/WAF challenge signatures and maps matches to
   * `HttpRateLimitedError`.
   */
  readonly detectHtmlEdgeBlocks?: boolean | undefined;

  /**
   * Custom retry policy overrides.
   */
  readonly retry?: Partial<HttpRetryPolicy> | undefined;

  /**
   * Optional proactive rate-limiter policy.
   */
  readonly rateLimit?: HttpRateLimitPolicy | undefined;

  /**
   * Optional circuit breaker policy.
   */
  readonly circuitBreaker?: HttpCircuitBreakerPolicy | undefined;
}

/**
 * Fully resolved `HttpPolicy` with all optional fields normalized to concrete defaults.
 */
export interface ResolvedHttpPolicy {
  /**
   * Logical upstream identifier.
   */
  readonly upstream: string;

  /**
   * Resolved base URL or `null` when requests supply full URLs.
   */
  readonly baseUrl: string | null;

  /**
   * `User-Agent` header added to requests that lack one, or `null` to leave the transport default.
   */
  readonly userAgent: string | null;

  /**
   * Default headers map.
   */
  readonly defaultHeaders: Readonly<Record<string, string>>;

  /**
   * Per-attempt timeout duration.
   */
  readonly attemptTimeout: Duration.Input;

  /**
   * Whole-operation deadline duration.
   */
  readonly operationTimeout: Duration.Input;

  /**
   * Maximum response body byte limit.
   */
  readonly maxBodyBytes: number;

  /**
   * Maximum age of a cached payload `getCachedJson` may serve when the upstream fails, or `null`
   * when stale-if-error is disabled.
   */
  readonly staleIfError: Duration.Input | null;

  /**
   * Whether every request and redirect hop passes the public-address guard.
   */
  readonly publicAddressesOnly: boolean;

  /**
   * Redirect hops a request follows before it is refused.
   */
  readonly maxRedirects: number;

  /**
   * Bulkhead concurrency limit or `null` when unbounded.
   */
  readonly maxConcurrency: number | null;

  /**
   * Whether HTTP 403 with zero remaining quota is classified as rate limiting.
   */
  readonly treatQuotaExhausted403AsRateLimit: boolean;

  /**
   * Whether all HTTP 403 responses are classified as rate limiting.
   */
  readonly treat403AsRateLimit: boolean;

  /**
   * Whether HTTP 403 HTML bodies are scanned for CDN/WAF block signatures.
   */
  readonly detectHtmlEdgeBlocks: boolean;

  /**
   * Resolved retry schedule configuration.
   */
  readonly retry: ResolvedHttpRetryPolicy;

  /**
   * Resolved proactive rate-limit configuration or `null`.
   */
  readonly rateLimit: HttpRateLimitPolicy | null;

  /**
   * Resolved circuit breaker configuration or `null`.
   */
  readonly circuitBreaker: ResolvedHttpCircuitBreakerPolicy | null;

  /**
   * Typed failure describing every invalid policy value, or `null` when the policy is valid. An
   * invalid value is replaced by its default in this resolved policy so nothing hangs or dies, and
   * the request path fails with this error before sending (see `validateRequestConfig`).
   */
  readonly configError: HttpRequestError | null;
}

/**
 * Accepts or rejects a number, with the wording used when it is rejected.
 */
export type NumberCheck = {
  /**
   * Returns `true` when `value` is acceptable.
   */
  readonly accepts: (value: number) => boolean;

  /**
   * Describes what is acceptable, for error messages.
   */
  readonly description: string;
};

/**
 * A positive safe integer: concurrency limits, byte caps and counts that must be at least one.
 */
export const POSITIVE_SAFE_INTEGER: NumberCheck = {
  /**
   * Accepts integers from 1 up to `Number.MAX_SAFE_INTEGER`.
   */
  accepts: (value) => Number.isSafeInteger(value) && value > 0,

  /**
   * Wording for error messages.
   */
  description: 'a positive safe integer',
};

/**
 * A non-negative safe integer: retry and redirect counts, where zero disables the feature.
 */
export const NON_NEGATIVE_SAFE_INTEGER: NumberCheck = {
  /**
   * Accepts integers from 0 up to `Number.MAX_SAFE_INTEGER`.
   */
  accepts: (value) => Number.isSafeInteger(value) && value >= 0,

  /**
   * Wording for error messages.
   */
  description: 'a non-negative safe integer',
};

/**
 * A finite number of seconds that is not negative.
 */
export const NON_NEGATIVE_FINITE: NumberCheck = {
  /**
   * Accepts finite numbers from 0 up; rejects `NaN` and `Infinity`.
   */
  accepts: (value) => Number.isFinite(value) && value >= 0,

  /**
   * Wording for error messages.
   */
  description: 'a finite, non-negative number',
};

/**
 * Returns `true` when `input` holds a `NaN` anywhere: a `NaN` number, or a `NaN` component of a
 * `[seconds, nanos]` tuple or a duration object, which `Duration.fromInput` would read as zero.
 * Takes `unknown` because untyped configuration can pass anything, `null` included.
 */
const hasNaNComponent = (input: unknown): boolean => {
  if (typeof input === 'number') {
    return Number.isNaN(input);
  }

  if (Array.isArray(input)) {
    return input.some((component: unknown) => Number.isNaN(component));
  }

  if (typeof input === 'object' && input !== null && !Duration.isDuration(input)) {
    return Object.values(input).some(
      (component) => typeof component === 'number' && Number.isNaN(component),
    );
  }

  return false;
};

/**
 * Parses `input` with the checked `Duration` API and returns the duration, or `null` when it does
 * not parse (such as `'1e3 millis'`, which `Effect.timeout` would turn into a defect, or a `null`
 * from untyped configuration), is or holds `NaN` (also inside a tuple or object), or is negative or
 * infinite. Never throws.
 */
export const checkedDuration = (input: Duration.Input): Duration.Duration | null => {
  if (hasNaNComponent(input)) {
    return null;
  }

  const parsed = Duration.fromInput(input);
  if (Option.isNone(parsed)) {
    return null;
  }

  const duration = parsed.value;
  return Duration.isFinite(duration) && !Duration.isNegative(duration) ? duration : null;
};

/**
 * Builds the typed failure for invalid configuration. It is a refusal before anything is sent:
 * never retried and ignored by the circuit breaker.
 */
export const invalidConfigError = (
  upstream: string,
  scope: string,
  issues: ReadonlyArray<string>,
): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({
      upstream,
      status: null,
      message: `${upstream} has invalid ${scope}: ${issues.join('; ')}`,
    }),
    { outcome: 'blocked', retryable: false, circuit: 'ignore' },
  );

/**
 * Returns `value` when it passes `check`, or `undefined` when it is omitted or rejected, recording
 * an issue for a rejected value. Callers apply the default with `??`.
 */
const validNumber = (
  issues: Array<string>,
  name: string,
  value: number | undefined,
  check: NumberCheck,
): number | undefined => {
  if (value === undefined || check.accepts(value)) {
    return value;
  }

  issues.push(`${name} must be ${check.description}`);
  return undefined;
};

/**
 * Returns `value` when it is a valid duration, or `undefined` when it is omitted or rejected,
 * recording an issue for a rejected value. Callers apply the default with `??`.
 */
const validDuration = (
  issues: Array<string>,
  name: string,
  value: Duration.Input | undefined,
): Duration.Input | undefined => {
  if (value === undefined || checkedDuration(value) !== null) {
    return value;
  }

  issues.push(`${name} must be a finite, non-negative duration`);
  return undefined;
};

/**
 * Merges partial retry overrides with package defaults, replacing invalid values by the defaults.
 */
const resolveHttpRetryPolicy = (
  issues: Array<string>,
  retry?: Partial<HttpRetryPolicy>,
): ResolvedHttpRetryPolicy => ({
  maxRetries:
    validNumber(issues, 'retry.maxRetries', retry?.maxRetries, NON_NEGATIVE_SAFE_INTEGER) ??
    DEFAULT_MAX_RETRIES,
  baseDelay: validDuration(issues, 'retry.baseDelay', retry?.baseDelay) ?? DEFAULT_BASE_DELAY,
  maxBackoffSeconds:
    validNumber(issues, 'retry.maxBackoffSeconds', retry?.maxBackoffSeconds, NON_NEGATIVE_FINITE) ??
    DEFAULT_MAX_BACKOFF_SECONDS,
  maxInlineRetryAfterSeconds:
    validNumber(
      issues,
      'retry.maxInlineRetryAfterSeconds',
      retry?.maxInlineRetryAfterSeconds,
      NON_NEGATIVE_FINITE,
    ) ?? DEFAULT_MAX_INLINE_RETRY_AFTER_SECONDS,
  retryOnRateLimit: retry?.retryOnRateLimit ?? DEFAULT_RETRY_ON_RATE_LIMIT,
});

/**
 * Normalizes optional circuit breaker settings for an upstream: the key defaults to
 * `circuit:<upstream>`. An explicit null from untyped input is invalid.
 */
const resolveHttpCircuitBreakerPolicy = (
  issues: Array<string>,
  upstream: string,
  circuitBreaker?: HttpCircuitBreakerPolicy | null,
): ResolvedHttpCircuitBreakerPolicy | null => {
  if (circuitBreaker === null) {
    issues.push('circuitBreaker must be an object');
  }

  if (circuitBreaker === undefined || circuitBreaker === null) {
    return null;
  }

  return { key: circuitBreaker.key ?? `circuit:${upstream}` };
};

/**
 * Keeps the rate-limit policy when its limit and window are usable. An invalid one is dropped,
 * because a limiter built from a `NaN` or zero window cannot work.
 */
const resolveHttpRateLimitPolicy = (
  issues: Array<string>,
  rateLimit?: HttpRateLimitPolicy | null,
): HttpRateLimitPolicy | null => {
  if (rateLimit === null) {
    issues.push('rateLimit must be an object');
  }

  if (rateLimit === undefined || rateLimit === null) {
    return null;
  }

  const before = issues.length;
  if (!POSITIVE_SAFE_INTEGER.accepts(rateLimit.limit)) {
    issues.push(`rateLimit.limit must be ${POSITIVE_SAFE_INTEGER.description}`);
  }

  const window = checkedDuration(rateLimit.window);
  if (window === null || Duration.isZero(window)) {
    issues.push('rateLimit.window must be a finite, positive duration');
  }

  return issues.length === before ? rateLimit : null;
};

/**
 * Resolves the public-address guard settings: off by default, with `DEFAULT_MAX_REDIRECTS` hops.
 */
const resolveAddressGuardPolicy = (
  issues: Array<string>,
  policy: HttpPolicy,
): Pick<ResolvedHttpPolicy, 'publicAddressesOnly' | 'maxRedirects'> => ({
  publicAddressesOnly: policy.publicAddressesOnly ?? false,
  maxRedirects:
    validNumber(issues, 'maxRedirects', policy.maxRedirects, NON_NEGATIVE_SAFE_INTEGER) ??
    DEFAULT_MAX_REDIRECTS,
});

/**
 * The limits an executor takes directly in `ExecuteRequestBaseParams`, checked like a policy.
 */
export type HttpRequestLimits = Pick<
  HttpPolicy,
  | 'attemptTimeout'
  | 'operationTimeout'
  | 'maxBodyBytes'
  | 'maxRedirects'
  | 'retry'
  | 'rateLimit'
  | 'circuitBreaker'
>;

/**
 * Lists every invalid value in `limits`, worded like `configError`, or returns an empty list. The
 * rules are those of `resolveHttpPolicy`.
 */
const httpRequestLimitIssues = (limits: HttpRequestLimits): Array<string> => {
  const issues: Array<string> = [];
  validDuration(issues, 'attemptTimeout', limits.attemptTimeout);
  validDuration(issues, 'operationTimeout', limits.operationTimeout);
  validNumber(issues, 'maxBodyBytes', limits.maxBodyBytes, POSITIVE_SAFE_INTEGER);
  validNumber(issues, 'maxRedirects', limits.maxRedirects, NON_NEGATIVE_SAFE_INTEGER);
  resolveHttpRetryPolicy(issues, limits.retry);
  resolveHttpRateLimitPolicy(issues, limits.rateLimit);
  return issues;
};

/**
 * Fails with the typed `invalidConfigError` (`<upstream> has invalid request parameters: <fields>`)
 * when `limits` holds an invalid value: a `'1e3 millis'` timeout, a `NaN` or infinite
 * `maxBodyBytes`, a `NaN` retry count. The public executors call it before anything is sent,
 * because they take these limits unchecked: an unparseable timeout would otherwise die in
 * `Effect.timeout`, and a `NaN` cap would read a body of any size.
 */
export const validateRequestLimits = (
  upstream: string,
  limits: HttpRequestLimits,
): Effect.Effect<void, HttpRequestError> => {
  const issues = httpRequestLimitIssues(limits);
  return issues.length === 0
    ? Effect.void
    : Effect.fail(invalidConfigError(upstream, 'request parameters', issues));
};

/**
 * Returns `baseUrl`, listing it in `issues` when it is set but not an absolute `http:` or `https:`
 * URL (`api.example.com/v1` without a scheme), because the default headers are bound to its origin
 * and a base that does not parse names none. A query or fragment would swallow the appended path.
 * An empty or missing base means no base URL. An explicit null from untyped input is invalid.
 */
const checkedBaseUrl = (
  issues: Array<string>,
  baseUrl: string | null | undefined,
): string | null => {
  if (baseUrl === undefined || baseUrl === '') {
    return baseUrl ?? null;
  }

  if (baseUrl === null || !ABSOLUTE_HTTP_URL_PATTERN.test(baseUrl) || !URL.canParse(baseUrl)) {
    issues.push('baseUrl must be an absolute http or https URL');
  } else if (baseUrl.includes('?') || baseUrl.includes('#')) {
    issues.push('baseUrl must not contain a query or fragment');
  }

  return baseUrl;
};

/**
 * Normalizes an `HttpPolicy` into a `ResolvedHttpPolicy` with concrete defaults. It never throws:
 * an invalid number or duration (`maxConcurrency` 0 or `NaN`, an infinite `maxBodyBytes`, a `'1e3
 * millis'` timeout) is replaced by its default, so nothing hangs or dies, and every rejected value
 * is listed in `configError` for the request path to fail with. A `baseUrl` that is not an absolute
 * `http(s)` URL without a query or fragment is kept as given but listed too. Explicit null values
 * for `baseUrl`, `rateLimit` and `circuitBreaker` are rejected.
 */
export const resolveHttpPolicy = (policy: HttpPolicy): ResolvedHttpPolicy => {
  const issues: Array<string> = [];
  const resolved = {
    upstream: policy.upstream,
    baseUrl: checkedBaseUrl(issues, policy.baseUrl),
    userAgent: policy.userAgent ?? null,
    defaultHeaders: policy.defaultHeaders ?? {},
    attemptTimeout:
      validDuration(issues, 'attemptTimeout', policy.attemptTimeout) ?? REQUEST_TIMEOUT,
    operationTimeout:
      validDuration(issues, 'operationTimeout', policy.operationTimeout) ??
      DEFAULT_OPERATION_TIMEOUT,
    maxBodyBytes:
      validNumber(issues, 'maxBodyBytes', policy.maxBodyBytes, POSITIVE_SAFE_INTEGER) ??
      DEFAULT_MAX_BODY_BYTES,
    staleIfError: validDuration(issues, 'staleIfError', policy.staleIfError) ?? null,
    ...resolveAddressGuardPolicy(issues, policy),
    maxConcurrency:
      validNumber(issues, 'maxConcurrency', policy.maxConcurrency, POSITIVE_SAFE_INTEGER) ?? null,
    treatQuotaExhausted403AsRateLimit: policy.treatQuotaExhausted403AsRateLimit ?? false,
    treat403AsRateLimit: policy.treat403AsRateLimit ?? false,
    detectHtmlEdgeBlocks: policy.detectHtmlEdgeBlocks ?? false,
    retry: resolveHttpRetryPolicy(issues, policy.retry),
    rateLimit: resolveHttpRateLimitPolicy(issues, policy.rateLimit),
    circuitBreaker: resolveHttpCircuitBreakerPolicy(issues, policy.upstream, policy.circuitBreaker),
  };
  return {
    ...resolved,
    configError: issues.length === 0 ? null : invalidConfigError(policy.upstream, 'policy', issues),
  };
};

/**
 * An absolute `http:` or `https:` URL prefix; URL schemes are case-insensitive.
 */
const ABSOLUTE_HTTP_URL_PATTERN = /^https?:\/\//iu;

/**
 * Resolves a relative path or absolute URL against an optional base URL.
 */
export const resolveRequestUrl = (baseUrl: string | null, pathOrUrl: string): string => {
  if (ABSOLUTE_HTTP_URL_PATTERN.test(pathOrUrl)) {
    return pathOrUrl;
  }

  if (baseUrl === null || baseUrl.length === 0) {
    return pathOrUrl;
  }

  const normalizedBase = baseUrl.replace(/\/+$/, '');
  const normalizedPath = pathOrUrl.startsWith('/') ? pathOrUrl : `/${pathOrUrl}`;
  return `${normalizedBase}${normalizedPath}`;
};
