import { Clock, Duration, Effect, Exit, Fiber, Layer, Metric } from 'effect';
import { CircuitBreaker, RateLimiter } from 'effect/persistence';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import type { HttpRateLimitPolicy } from './policy.ts';

import { classifyCircuitExit } from './circuit.ts';
import { exitErrorMeta, exitOutcome, squashExitFailure } from './http.fixture.ts';
import { httpRateLimitsTotal } from './metrics.ts';
import { consumeRateLimitToken } from './rate-limit.ts';

type ConsumeOptions = Parameters<RateLimiter.RateLimiter['consume']>[0];

const POLICY: HttpRateLimitPolicy = { limit: 2, window: '2 seconds' };

/**
 * Builds a `RateLimiter` whose `consume` is `consume`, so tests control exactly what the limiter
 * answers. The adaptive methods are not used by the package.
 */
const makeLimiter = (consume: RateLimiter.RateLimiter['consume']): RateLimiter.RateLimiter => ({
  [RateLimiter.TypeId]: RateLimiter.TypeId,
  consume,
  adaptiveConsume: () => Effect.die('adaptiveConsume is not used'),
  adaptiveFeedback: () => Effect.die('adaptiveFeedback is not used'),
});

/**
 * Builds a consume result that waits `delayMillis` before the request may go out.
 */
const allowedAfter = (delayMillis: number): RateLimiter.ConsumeResult => ({
  delay: Duration.millis(delayMillis),
  limit: 2,
  remaining: 1,
  resetAfter: Duration.millis(delayMillis),
});

/**
 * Builds the error a limiter raises when `onExceeded` is `fail` and the limit is used up.
 */
const exceeded = (retryAfterMillis: number): RateLimiter.RateLimiterError =>
  new RateLimiter.RateLimiterError({
    reason: new RateLimiter.RateLimitExceeded({
      key: 'k',
      retryAfter: Duration.millis(retryAfterMillis),
      limit: 2,
      remaining: 0,
    }),
  });

/**
 * Builds the layer of a real in-memory limiter on the test clock. The store is built on that same
 * clock, so its background sweep does not read the real time and drop every entry.
 */
const makeMemoryLimiterLayer = (): Layer.Layer<RateLimiter.RateLimiter> =>
  RateLimiter.layer.pipe(
    Layer.provide(RateLimiter.layerStoreMemory),
    Layer.provideMerge(TestClock.layer()),
  );

/**
 * Reads `resilient_http_rate_limits_total` for `upstream`.
 */
const readRateLimitCount = (upstream: string): Effect.Effect<number> =>
  Effect.map(
    Metric.value(Metric.withAttributes(httpRateLimitsTotal, { upstream })),
    (state) => state.count,
  );

describe('consumeRateLimitToken', () => {
  it('does nothing without a policy, even when a limiter is provided', async () => {
    let consumed = 0;
    const limiter = makeLimiter(() => {
      consumed += 1;
      return Effect.succeed(allowedAfter(0));
    });
    await Effect.runPromise(
      consumeRateLimitToken('test:rl-none', undefined).pipe(
        Effect.provideService(RateLimiter.RateLimiter, limiter),
      ),
    );
    expect(consumed).toBe(0);
  });

  it('does nothing when the context has no RateLimiter', async () => {
    const exit = await Effect.runPromiseExit(consumeRateLimitToken('test:rl-absent', POLICY));
    expect(exit).toEqual(Exit.void);
  });

  it('asks the limiter with the default key and algorithm, always in fail mode', async () => {
    const seen: Array<ConsumeOptions> = [];
    const limiter = makeLimiter((options) => {
      seen.push(options);
      return Effect.succeed(allowedAfter(0));
    });
    await Effect.runPromise(
      consumeRateLimitToken('test:rl-defaults', POLICY).pipe(
        Effect.provideService(RateLimiter.RateLimiter, limiter),
      ),
    );
    expect(seen).toEqual([
      {
        key: 'ratelimit:test:rl-defaults',
        limit: 2,
        window: '2 seconds',
        algorithm: 'token-bucket',
        onExceeded: 'fail',
      },
    ]);
  });

  it('passes an explicit key, algorithm and strategy through', async () => {
    const seen: Array<ConsumeOptions> = [];
    const limiter = makeLimiter((options) => {
      seen.push(options);
      return Effect.succeed(allowedAfter(0));
    });
    await Effect.runPromise(
      consumeRateLimitToken('test:rl-explicit', {
        key: 'shared-bucket',
        limit: 5,
        window: '1 minute',
        algorithm: 'fixed-window',
        onExceeded: 'fail',
      }).pipe(Effect.provideService(RateLimiter.RateLimiter, limiter)),
    );
    expect(seen).toEqual([
      {
        key: 'shared-bucket',
        limit: 5,
        window: '1 minute',
        algorithm: 'fixed-window',
        onExceeded: 'fail',
      },
    ]);
  });

  it('fails with HttpRateLimitedError and counts it when the limit is exceeded', async () => {
    const upstream = 'test:rl-exceeded';
    const limiter = makeLimiter(() => Effect.fail(exceeded(1500)));
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          consumeRateLimitToken(upstream, { ...POLICY, onExceeded: 'fail' }),
        );
        return { exit, count: yield* readRateLimitCount(upstream) };
      }).pipe(Effect.provideService(RateLimiter.RateLimiter, limiter)),
    );
    expect(squashExitFailure(result.exit)).toMatchObject({
      _tag: 'HttpRateLimitedError',
      upstream,
      retryAfterSeconds: 2,
      message: `${upstream} client rate limit exceeded`,
    });
    expect(result.count).toBe(1);
  });

  it('marks a fail-mode refusal as not retried and ignored by the circuit breaker', async () => {
    const limiter = makeLimiter(() => Effect.fail(exceeded(1500)));
    const exit = await Effect.runPromiseExit(
      consumeRateLimitToken('test:rl-refusal-meta', { ...POLICY, onExceeded: 'fail' }).pipe(
        Effect.provideService(RateLimiter.RateLimiter, limiter),
      ),
    );
    expect(exitErrorMeta(exit)).toMatchObject({ retryable: false, circuit: 'ignore' });
    expect(exitOutcome(exit)).toBe('rate_limited');
    expect(classifyCircuitExit(exit)).toBe(CircuitBreaker.ignored);
  });

  it('rounds the retry delay up to whole seconds, even for a sub-second wait', async () => {
    const limiter = makeLimiter(() => Effect.fail(exceeded(1)));
    const exit = await Effect.runPromiseExit(
      consumeRateLimitToken('test:rl-round', { ...POLICY, onExceeded: 'fail' }).pipe(
        Effect.provideService(RateLimiter.RateLimiter, limiter),
      ),
    );
    expect(squashExitFailure(exit)).toMatchObject({ retryAfterSeconds: 1 });
  });

  it('fails the second request of a real fail-strategy limiter with the wait for a token', async () => {
    const upstream = 'test:rl-real-fail';
    const policy: HttpRateLimitPolicy = {
      limit: 1,
      window: '10 seconds',
      onExceeded: 'fail',
    };
    const layer = makeMemoryLimiterLayer();
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const first = yield* Effect.exit(consumeRateLimitToken(upstream, policy));
        const second = yield* Effect.exit(consumeRateLimitToken(upstream, policy));
        return { first, second };
      }).pipe(Effect.provide(layer)),
    );
    expect(result.first).toEqual(Exit.void);
    expect(squashExitFailure(result.second)).toMatchObject({
      _tag: 'HttpRateLimitedError',
      upstream,
      retryAfterSeconds: 10,
    });
  });

  it('in delay mode, waits out a refusal and asks again before returning', async () => {
    let asked = 0;
    const limiter = makeLimiter(() => {
      asked += 1;
      return asked === 1 ? Effect.fail(exceeded(5000)) : Effect.succeed(allowedAfter(0));
    });
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(consumeRateLimitToken('test:rl-delay', POLICY));
        yield* TestClock.adjust('4 seconds');
        const early = fiber.pollUnsafe();
        yield* TestClock.adjust('1 second');
        const exit = yield* Fiber.await(fiber);
        return { early, exit };
      }).pipe(
        Effect.provideService(RateLimiter.RateLimiter, limiter),
        Effect.provide(TestClock.layer()),
      ),
    );
    expect(result.early).toBeUndefined();
    expect(result.exit).toEqual(Exit.void);
    expect(asked).toBe(2);
  });

  it('holds no permit for a delay-mode wait that is cut short', async () => {
    const upstream = 'test:rl-cut-wait';
    const policy: HttpRateLimitPolicy = { limit: 1, window: '10 seconds' };
    const finishedAt = await Effect.runPromise(
      Effect.gen(function* () {
        yield* consumeRateLimitToken(upstream, policy);
        const waiter = yield* Effect.forkChild(consumeRateLimitToken(upstream, policy));
        yield* TestClock.adjust('1 second');
        yield* Fiber.interrupt(waiter);
        yield* TestClock.adjust('9 seconds');
        const next = yield* Effect.forkChild(
          Effect.andThen(consumeRateLimitToken(upstream, policy), Clock.currentTimeMillis),
        );
        yield* TestClock.adjust('15 seconds');
        return yield* Fiber.join(next);
      }).pipe(Effect.provide(makeMemoryLimiterLayer())),
    );
    expect(finishedAt).toBe(10_000);
  });

  it('does not sleep when the limiter allows the request immediately', async () => {
    const limiter = makeLimiter(() => Effect.succeed(allowedAfter(0)));
    const exit = await Effect.runPromiseExit(
      consumeRateLimitToken('test:rl-immediate', POLICY).pipe(
        Effect.provideService(RateLimiter.RateLimiter, limiter),
        Effect.provide(TestClock.layer()),
      ),
    );
    expect(exit).toEqual(Exit.void);
  });

  it('delays the third request of a real token bucket until a token is refilled', async () => {
    const upstream = 'test:rl-real-delay';
    const layer = makeMemoryLimiterLayer();
    const finishedAt = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          Effect.forEach([1, 2, 3], () =>
            Effect.andThen(consumeRateLimitToken(upstream, POLICY), Clock.currentTimeMillis),
          ),
        );
        yield* TestClock.adjust('3 seconds');
        return yield* Fiber.join(fiber);
      }).pipe(Effect.provide(layer)),
    );
    expect(finishedAt.slice(0, 2)).toEqual([0, 0]);
    expect(finishedAt[2]).toBeGreaterThanOrEqual(1000);
  });

  it('maps any other limiter failure to an HttpRequestError without counting a rate limit', async () => {
    const upstream = 'test:rl-store-down';
    const limiter = makeLimiter(() =>
      Effect.fail(
        new RateLimiter.RateLimiterError({
          reason: new RateLimiter.RateLimitStoreError({ message: 'redis is down' }),
        }),
      ),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const exit = yield* Effect.exit(consumeRateLimitToken(upstream, POLICY));
        return { exit, count: yield* readRateLimitCount(upstream) };
      }).pipe(Effect.provideService(RateLimiter.RateLimiter, limiter)),
    );
    expect(squashExitFailure(result.exit)).toMatchObject({
      _tag: 'HttpRequestError',
      upstream,
      status: null,
      message: 'redis is down',
    });
    expect(result.count).toBe(0);
    expect(exitErrorMeta(result.exit)).toMatchObject({
      outcome: 'network_error',
      circuit: 'ignore',
    });
  });
});
