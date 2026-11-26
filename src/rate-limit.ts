import { Duration, Effect, Option } from 'effect';
import { RateLimiter } from 'effect/persistence';

import type { HttpError } from './errors.ts';
import type { HttpRateLimitPolicy } from './policy.ts';

import { tagHttpError } from './error-meta.ts';
import { HttpRateLimitedError, HttpRequestError } from './errors.ts';
import * as Metrics from './metrics.ts';

/**
 * Shortest wait between two admission checks in `delay` mode, so a limiter that answers with a zero
 * `retryAfter` cannot spin the fiber.
 */
const MIN_ADMISSION_POLL = Duration.millis(1);

/**
 * Builds the error for a request the client-side limiter refused in `fail` mode. Nothing was sent,
 * so it is never retried (the caller asked to fail rather than wait) and the circuit breaker
 * ignores it.
 */
const localRefusalError = (upstream: string, retryAfter: Duration.Duration): HttpRateLimitedError =>
  tagHttpError(
    new HttpRateLimitedError({
      upstream,
      retryAfterSeconds: Math.ceil(Duration.toMillis(retryAfter) / 1000),
      message: `${upstream} client rate limit exceeded`,
    }),
    { retryable: false, circuit: 'ignore' },
  );

/**
 * Builds the error for a limiter whose store failed. The upstream was never called, so the circuit
 * breaker ignores it.
 */
const limiterStoreError = (upstream: string, message: string): HttpRequestError =>
  tagHttpError(new HttpRequestError({ upstream, status: null, message }), {
    outcome: 'network_error',
    circuit: 'ignore',
  });

/**
 * Asks `limiter` for one permit under `options`. A refusal fails at once with `failOnRefusal`, and
 * otherwise waits the limiter's `retryAfter` and asks again. A store failure fails with
 * `limiterStoreError`.
 */
const admit = (params: {
  readonly limiter: RateLimiter.RateLimiter;
  readonly options: Parameters<RateLimiter.RateLimiter['consume']>[0];
  readonly upstream: string;
  readonly failOnRefusal: boolean;
}): Effect.Effect<void, HttpError> =>
  params.limiter.consume(params.options).pipe(
    Effect.asVoid,
    Effect.catch((error) => {
      if (error.reason._tag !== 'RateLimitExceeded') {
        return Effect.fail(limiterStoreError(params.upstream, error.reason.message));
      }

      const { retryAfter } = error.reason;
      if (params.failOnRefusal) {
        return Effect.andThen(
          Metrics.recordHttpRateLimitMetric(params.upstream),
          Effect.fail(localRefusalError(params.upstream, retryAfter)),
        );
      }

      return Effect.andThen(
        Effect.sleep(Duration.max(retryAfter, MIN_ADMISSION_POLL)),
        Effect.suspend(() => admit(params)),
      );
    }),
  );

/**
 * Consumes a permit from `RateLimiter.RateLimiter` when present in the Effect context.
 *
 * The limiter is always asked with `onExceeded: 'fail'`, which takes a permit only when one is
 * free. With the policy's `fail` strategy a refusal fails at once with a `HttpRateLimitedError`
 * that is not retried and not counted by the circuit breaker. With `delay` (the default) the call
 * sleeps for the limiter's `retryAfter` and asks again, so a wait cut short by the operation
 * deadline holds no permit and later requests are not starved.
 */
export const consumeRateLimitToken = (
  upstream: string,
  rateLimit: HttpRateLimitPolicy | undefined,
): Effect.Effect<void, HttpError> => {
  if (rateLimit === undefined) {
    return Effect.void;
  }

  return Effect.gen(function* () {
    const limiterOpt = yield* Effect.serviceOption(RateLimiter.RateLimiter);
    if (Option.isNone(limiterOpt)) {
      return;
    }

    yield* admit({
      limiter: limiterOpt.value,
      options: {
        key: rateLimit.key ?? `ratelimit:${upstream}`,
        limit: rateLimit.limit,
        window: rateLimit.window,
        algorithm: rateLimit.algorithm ?? 'token-bucket',
        onExceeded: 'fail',
      },
      upstream,
      failOnRefusal: rateLimit.onExceeded === 'fail',
    });
  });
};
