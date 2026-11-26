import { Duration, Option } from 'effect';

import { tagHttpError } from './error-meta.ts';
import { HttpRequestError } from './errors.ts';

/**
 * Formats a `Duration.Input` value into a human-readable timeout string for error messages.
 */
export const formatTimeoutInput = (timeout: Duration.Input): string =>
  typeof timeout === 'string' || typeof timeout === 'number'
    ? String(timeout)
    : Duration.format(Duration.fromInputUnsafe(timeout));

/**
 * Builds the error for an attempt that ran past `timeout`. It is transient and retried.
 */
export const attemptTimeoutError = (upstream: string, timeout: Duration.Input): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({
      upstream,
      status: null,
      message: `${upstream} request timed out after ${formatTimeoutInput(timeout)}`,
    }),
    { outcome: 'timeout' },
  );

/**
 * Builds the error for an operation that ran past its whole-operation deadline. It is never
 * retried. When no attempt was sent (`sent` is false) the whole deadline went to the client-side
 * rate limiter, so the error is kept out of the circuit breaker: the upstream never failed.
 */
export const operationTimeoutError = (
  upstream: string,
  timeout: Duration.Input,
  progress: { readonly attempts: number; readonly sent: boolean },
): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({
      upstream,
      status: null,
      message: `${upstream} operation timed out after ${formatTimeoutInput(timeout)} (${String(progress.attempts)} attempts)`,
    }),
    { outcome: 'timeout', retryable: false, circuit: progress.sent ? undefined : 'ignore' },
  );

/**
 * Returns the operation deadline in milliseconds, or `null` when `timeout` is not a valid duration.
 * `validateRequestLimits` refuses such a timeout before the first attempt, so `null` is only a
 * fallback that leaves the retry schedule without a deadline.
 */
export const operationBudgetMillis = (timeout: Duration.Input): number | null =>
  Option.match(Duration.fromInput(timeout), {
    onNone: () => null,
    onSome: (duration) => Duration.toMillis(duration),
  });
