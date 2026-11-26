import { Cause, Duration, Effect, Exit, Option } from 'effect';
import { CircuitBreaker } from 'effect/persistence';

import type { HttpError } from './errors.ts';
import type { HttpCircuitBreakerPolicy } from './policy.ts';

import { readHttpErrorMeta, tagHttpError } from './error-meta.ts';
import { HttpRequestError, isHttpError } from './errors.ts';
import { isTransientHttpStatus } from './retry.ts';

/**
 * Options for `classifyCircuitExit` and `withCircuitBreaker`.
 */
export type CircuitClassifyOptions<A> = {
  /**
   * Returns `true` for a success the breaker should ignore, such as a 404 accepted as a value. It
   * neither resets the consecutive-failure count nor closes a half-open probe.
   */
  readonly isIgnoredSuccess?: ((value: A) => boolean) | undefined;
};

/**
 * Classifies a completed request `Exit` into a `CircuitBreaker.Outcome`. A success counts as
 * healthy, a transient failure (no status, or a retried status) and a rate limit count against the
 * upstream, and anything that says nothing about it is ignored: a refusal before sending, a
 * non-transient 4xx, a 404 accepted as a value. When the call is the half-open probe
 * (`context.probe`), a response the upstream did send (an accepted 404, a non-transient 4xx) closes
 * the circuit instead of being ignored, because it shows the upstream answers; otherwise a
 * recovered upstream that keeps answering 404 would stay half-open forever.
 */
export const classifyCircuitExit = <A>(
  exit: Exit.Exit<A, HttpError>,
  options?: CircuitClassifyOptions<A>,
  context?: { readonly probe: boolean },
): CircuitBreaker.Outcome => {
  const respondedOutcome =
    context?.probe === true ? CircuitBreaker.success : CircuitBreaker.ignored;
  if (Exit.isSuccess(exit)) {
    return options?.isIgnoredSuccess?.(exit.value) === true
      ? respondedOutcome
      : CircuitBreaker.success;
  }

  const squashed = Cause.squash(exit.cause);
  if (!isHttpError(squashed) || readHttpErrorMeta(squashed).circuit === 'ignore') {
    return CircuitBreaker.ignored;
  }

  if (squashed._tag === 'HttpRequestError') {
    if (squashed.status === null || isTransientHttpStatus(squashed.status, true)) {
      return CircuitBreaker.failure();
    }

    return respondedOutcome;
  }

  return CircuitBreaker.failure(
    squashed.retryAfterSeconds !== null
      ? { retryAfter: Duration.seconds(squashed.retryAfterSeconds) }
      : undefined,
  );
};

/**
 * Builds the error for a call the breaker rejected: an open circuit, a half-open circuit whose
 * probe is still running, or a failing breaker store. It reports a 503 labelled `circuit_open`;
 * `reason` is the rejection's tag, such as `CircuitOpen`.
 */
const circuitOpenError = (upstream: string, reason: string): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({
      upstream,
      status: 503,
      message: `${upstream} circuit breaker is open (${reason})`,
    }),
    { outcome: 'circuit_open' },
  );

/**
 * Guards an effect with `CircuitBreaker.CircuitBreaker` when configured and present in context.
 */
export const withCircuitBreaker = <A, R>(
  effect: Effect.Effect<A, HttpError, R>,
  upstream: string,
  circuitBreaker: HttpCircuitBreakerPolicy | undefined,
  options?: CircuitClassifyOptions<A>,
): Effect.Effect<A, HttpError, R> => {
  if (circuitBreaker === undefined) {
    return effect;
  }

  return Effect.gen(function* () {
    const breakerOpt = yield* Effect.serviceOption(CircuitBreaker.CircuitBreaker);
    if (Option.isNone(breakerOpt)) {
      return yield* effect;
    }

    return yield* breakerOpt.value
      .execute(effect, {
        key: circuitBreaker.key ?? `circuit:${upstream}`,
        classify: (exit, context) => classifyCircuitExit(exit, options, context),
      })
      .pipe(
        Effect.catchTag('CircuitBreakerError', (error) =>
          Effect.fail(circuitOpenError(upstream, error.reason._tag)),
        ),
      );
  });
};
