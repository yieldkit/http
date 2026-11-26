import { Duration, Effect, Option, Schedule } from 'effect';
import { HttpClientRequest, HttpClientResponse } from 'effect/http';

import type { HttpError } from './errors.ts';
import type { HttpRetryPolicy } from './policy.ts';

import { readHttpErrorMeta } from './error-meta.ts';
import {
  DEFAULT_BASE_DELAY,
  DEFAULT_MAX_BACKOFF_SECONDS,
  DEFAULT_MAX_INLINE_RETRY_AFTER_SECONDS,
  DEFAULT_MAX_RETRIES,
  DEFAULT_RETRY_ON_RATE_LIMIT,
} from './policy.ts';

/**
 * Returns `true` when an HTTP status code represents a transient upstream condition worth retrying.
 * HTTP 429 is included only when `retryOnRateLimit` is `true` (default `false`).
 */
export const isTransientHttpStatus = (
  status: number,
  retryOnRateLimit: boolean = DEFAULT_RETRY_ON_RATE_LIMIT,
): boolean =>
  status === 408 ||
  status === 425 ||
  (retryOnRateLimit && status === 429) ||
  (status >= 500 && status <= 504) ||
  (status >= 520 && status <= 524);

/**
 * Returns the upstream `Retry-After` wait of `error` in whole seconds: the `retryAfterSeconds` of a
 * rate limit, or the one tagged on a retryable status error such as a 503. `null` when the upstream
 * named no wait.
 */
export const readRetryAfterSeconds = (error: HttpError): number | null => {
  if (error._tag === 'HttpRateLimitedError') {
    return error.retryAfterSeconds;
  }

  return readHttpErrorMeta(error).retryAfterSeconds ?? null;
};

/**
 * Determines whether an error from this package is eligible for inline retry within the current
 * call. An error tagged `retryable: false` (a decode failure, an oversized body) is never retried,
 * and neither is one whose upstream `Retry-After` is longer than `maxInlineRetryAfterSeconds`.
 * Rate-limit errors (`HttpRateLimitedError` and HTTP 429) are retried only when `retryOnRateLimit`
 * is `true` (default `false`).
 */
export const isTransientHttpError = (
  error: HttpError,
  maxInlineRetryAfterSeconds: number = DEFAULT_MAX_INLINE_RETRY_AFTER_SECONDS,
  retryOnRateLimit: boolean = DEFAULT_RETRY_ON_RATE_LIMIT,
): boolean => {
  if (readHttpErrorMeta(error).retryable === false) {
    return false;
  }

  const retryAfterSeconds = readRetryAfterSeconds(error);
  if (retryAfterSeconds !== null && retryAfterSeconds > maxInlineRetryAfterSeconds) {
    return false;
  }

  if (error._tag === 'HttpRateLimitedError') {
    return retryOnRateLimit;
  }

  return error.status === null || isTransientHttpStatus(error.status, retryOnRateLimit);
};

/**
 * HTTP methods that are safe or idempotent, so repeating them after a failure of unknown outcome
 * cannot repeat a side effect. Any other method (POST, PATCH) is retried only when the server
 * refused before acting.
 */
export const IDEMPOTENT_METHODS: ReadonlySet<string> = new Set([
  'GET',
  'HEAD',
  'OPTIONS',
  'PUT',
  'DELETE',
  'TRACE',
]);

/**
 * Returns the URL of `request` with any fragment removed, the form `response.url` reports, or
 * `null` when it cannot be built.
 */
const requestHref = (request: HttpClientRequest.HttpClientRequest): string | null => {
  const url = HttpClientRequest.toUrl(request);
  if (Option.isNone(url)) {
    return null;
  }

  url.value.hash = '';
  return url.value.href;
};

/**
 * Responses the address guard got after following at least one redirect hop itself.
 */
const guardRedirected = new WeakSet<HttpClientResponse.HttpClientResponse>();

/**
 * Records that the address guard followed a redirect to get `response`.
 */
export const markRedirectedResponse = (response: HttpClientResponse.HttpClientResponse): void => {
  guardRedirected.add(response);
};

/**
 * Returns `true` when `response` did not answer `request` itself but a redirect hop: the address
 * guard followed one, its request has another method (a 303 turned a POST into a GET), the Web
 * response says it was redirected (a transport followed the hop itself, even back to the same URL),
 * or it reports a URL other than the one sent. The URL sent is `request`'s, or, when that is still
 * relative (a client transform such as `prependUrl` completes it), the URL of the request the
 * response answers. When neither can be built the response counts as redirected, so a POST is never
 * replayed on a guess.
 */
export const isRedirectedResponse = (
  request: HttpClientRequest.HttpClientRequest,
  response: HttpClientResponse.HttpClientResponse,
): boolean => {
  if (guardRedirected.has(response) || response.request.method !== request.method) {
    return true;
  }

  const web = HttpClientResponse.toWeb(response);
  if (Option.isSome(web) && web.value.redirected) {
    return true;
  }

  const href = requestHref(request) ?? requestHref(response.request);
  return href === null || response.url !== href;
};

/**
 * Decides whether a failed attempt of `method` may be retried. A transient error is retried for an
 * idempotent method, or for any method when the caller declared the request `idempotent`. For POST
 * and PATCH only `HttpRateLimitedError` is retried, because the server refused before acting, and
 * only when that refusal answered the original request: a rate limit from a redirect hop (a 303 to
 * a GET that returned 429) means the POST itself may already have been acted on.
 */
export const isRetryableForMethod = (params: {
  readonly error: HttpError;
  readonly method: string;
  readonly idempotent?: boolean | undefined;
  readonly maxInlineRetryAfterSeconds?: number | undefined;
  readonly retryOnRateLimit?: boolean | undefined;
}): boolean => {
  const { error, method } = params;
  if (!isTransientHttpError(error, params.maxInlineRetryAfterSeconds, params.retryOnRateLimit)) {
    return false;
  }

  return (
    params.idempotent === true ||
    IDEMPOTENT_METHODS.has(method.toUpperCase()) ||
    (error._tag === 'HttpRateLimitedError' && readHttpErrorMeta(error).redirected !== true)
  );
};

/**
 * Options for `createRetrySchedule` that come from the running operation, not the policy.
 */
export type RetryScheduleOptions = {
  /**
   * Epoch milliseconds at which the whole operation runs out. A retry whose delay would end at or
   * after it is not started, so the caller gets the last real error instead of an operation
   * timeout. Omit it for no deadline.
   */
  readonly deadlineMs?: number | undefined;
};

/**
 * Builds a jittered exponential retry schedule over any error type. Each delay is the exponential
 * backoff with jitter, cut to `maxBackoffSeconds`, then raised to `retryAfterSeconds(error)` when
 * that returns a wait. With `deadlineMs` the schedule stops instead of sleeping into the deadline.
 * Use it to retry errors that are not this package's, such as a caller's own domain errors.
 */
export const createBackoffSchedule = <E>(
  retryAfterSeconds: (error: E) => number | null,
  policy?: Partial<HttpRetryPolicy>,
  options?: RetryScheduleOptions,
): Schedule.Schedule<Duration.Duration, E> => {
  const maxRetries = policy?.maxRetries ?? DEFAULT_MAX_RETRIES;
  const baseDelay = policy?.baseDelay ?? DEFAULT_BASE_DELAY;
  const maxBackoff = Duration.seconds(policy?.maxBackoffSeconds ?? DEFAULT_MAX_BACKOFF_SECONDS);
  const deadlineMs = options?.deadlineMs ?? Number.POSITIVE_INFINITY;

  return Schedule.exponential(baseDelay).pipe(
    Schedule.jittered,
    Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.min(duration, maxBackoff))),
    Schedule.setInputType<E>(),
    Schedule.modifyDelay(({ duration, input }) => {
      const seconds = retryAfterSeconds(input);
      if (seconds !== null) {
        return Effect.succeed(Duration.max(duration, Duration.seconds(seconds)));
      }

      return Effect.succeed(duration);
    }),
    Schedule.while(({ now, duration }) => now + Duration.toMillis(duration) < deadlineMs),
    Schedule.upTo({ times: maxRetries }),
  );
};

/**
 * Builds the retry schedule for this package's errors: `createBackoffSchedule` with the wait from
 * `readRetryAfterSeconds` (a rate limit, or a 503 tagged with its `Retry-After` header).
 */
export const createRetrySchedule = (
  policy?: Partial<HttpRetryPolicy>,
  options?: RetryScheduleOptions,
): Schedule.Schedule<Duration.Duration, HttpError> =>
  createBackoffSchedule(readRetryAfterSeconds, policy, options);
