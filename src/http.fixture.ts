import type { Duration } from 'effect';

import { Cause, Effect, Exit, Fiber, Metric, Option, Schema } from 'effect';
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/http';
import { TestClock } from 'effect/testing';

import type { ResilientHttpClient } from './client.ts';
import type { HttpOutcomeLabel, HttpErrorMeta } from './error-meta.ts';
import type { HttpPolicy } from './policy.ts';

import { makeResilientHttpClient } from './client.ts';
import { readHttpErrorMeta } from './error-meta.ts';
import { HttpRateLimitedError, HttpRequestError } from './errors.ts';
import { classifyErrorOutcome, httpRequestsTotal } from './metrics.ts';

/**
 * Retry policy with near-zero backoff so retry tests run on the real clock.
 */
export const FAST_RETRY = { maxRetries: 2, baseDelay: '1 millis' } as const;

/**
 * Small JSON payload used by the HTTP client tests.
 */
export const ItemPayloadSchema = Schema.Struct({
  id: Schema.Int,
  title: Schema.String,
});

/**
 * Builds an Effect HTTP client that answers from `handler` and counts attempts.
 */
export const makeMockHttpClient = (
  handler: (
    request: HttpClientRequest.HttpClientRequest,
    url: URL,
    attempt: number,
  ) => Effect.Effect<Response>,
): HttpClient.HttpClient => {
  let attempt = 0;
  return HttpClient.make((request, url) =>
    Effect.gen(function* () {
      attempt += 1;
      const response = yield* handler(request, url, attempt);
      return HttpClientResponse.fromWeb(request, response);
    }),
  );
};

/**
 * Returns the first typed failure of `exit`, or null when it succeeded or ended without one (a
 * defect or an interruption).
 */
export const squashExitFailure = <E>(exit: Exit.Exit<unknown, E>): E | null =>
  Exit.isFailure(exit) ? Option.getOrNull(Cause.findErrorOption(exit.cause)) : null;

/**
 * Counters recorded by a `makeTrackedStream` body.
 */
export type StreamTracker = {
  /**
   * Bytes handed to the reader so far.
   */
  pulledBytes: number;

  /**
   * Whether the reader cancelled the stream.
   */
  cancelled: boolean;
};

/**
 * Body built by `makeTrackedStream` and the counters it updates as it is read.
 */
export type TrackedStream = {
  /**
   * Body to hand to a `Response`; read it at most once.
   */
  readonly stream: ReadableStream<Uint8Array>;

  /**
   * Live counters, mutated as the reader pulls or cancels `stream`.
   */
  readonly tracker: StreamTracker;
};

/**
 * Builds a pull-based body of `totalBytes` bytes, produced `chunkBytes` at a time from `prefix`
 * followed by spaces, that records how much was read and whether it was cancelled. With `stall:
 * true` the stream never produces a chunk. `highWaterMark: 0` keeps the runtime from reading ahead,
 * so `pulledBytes` counts only what a reader asked for.
 */
export const makeTrackedStream = (params: {
  readonly totalBytes: number;
  readonly chunkBytes: number;
  readonly prefix?: string | undefined;
  readonly stall?: boolean | undefined;
}): TrackedStream => {
  const tracker: StreamTracker = { pulledBytes: 0, cancelled: false };
  const content = new Uint8Array(params.totalBytes).fill(0x20);
  content.set(new TextEncoder().encode(params.prefix ?? ''));
  const stream = new ReadableStream<Uint8Array>(
    {
      pull: (controller) => {
        if (params.stall === true) {
          return new Promise<void>(() => {});
        }

        if (tracker.pulledBytes >= params.totalBytes) {
          controller.close();
          return Promise.resolve();
        }

        const end = Math.min(tracker.pulledBytes + params.chunkBytes, params.totalBytes);
        controller.enqueue(content.slice(tracker.pulledBytes, end));
        tracker.pulledBytes = end;
        return Promise.resolve();
      },
      cancel: () => {
        tracker.cancelled = true;
      },
    },
    { highWaterMark: 0 },
  );
  return { stream, tracker };
};

/**
 * Builds a Web `Response` whose `url` reports `url`, as a fetch response does after redirects.
 */
export const withResponseUrl = (response: Response, url: string): Response => {
  Object.defineProperty(response, 'url', { value: url });
  return response;
};

/**
 * Returns the metric outcome label of a failed `exit`, or null when it succeeded or failed with
 * something other than an http error.
 */
export const exitOutcome = (exit: Exit.Exit<unknown, unknown>): HttpOutcomeLabel | null => {
  const failure = Exit.isFailure(exit) ? Cause.squash(exit.cause) : null;
  if (Schema.is(HttpRequestError)(failure) || Schema.is(HttpRateLimitedError)(failure)) {
    return classifyErrorOutcome(failure);
  }

  return null;
};

/**
 * Returns the message of the error a failed `exit` squashes to, or null when it succeeded or the
 * failure is not an `Error`.
 */
export const exitMessage = (exit: Exit.Exit<unknown, unknown>): string | null => {
  const failure = Exit.isFailure(exit) ? Cause.squash(exit.cause) : null;
  return failure instanceof Error ? failure.message : null;
};

/**
 * Builds a `ResilientHttpClient` over `http` for `policy`.
 */
export const makeClientOver = (
  http: HttpClient.HttpClient,
  policy: HttpPolicy,
): Effect.Effect<ResilientHttpClient> =>
  makeResilientHttpClient(policy).pipe(Effect.provideService(HttpClient.HttpClient, http));

/**
 * Reads `resilient_http_requests_total` for a request of `upstream` with `method` and `outcome`.
 */
export const readRequestCount = (
  upstream: string,
  method: string,
  outcome: string,
): Effect.Effect<number> =>
  Effect.map(
    Metric.value(Metric.withAttributes(httpRequestsTotal, { upstream, method, outcome })),
    (state) => state.count,
  );

/**
 * Builds a mock transport that always answers `503` with `Retry-After: 4` and records the number of
 * attempts in `counter`.
 */
export const makeAlways503 = (counter: {
  attempts: number;
}): ReturnType<typeof makeMockHttpClient> =>
  makeMockHttpClient((_req, _url, attempt) =>
    Effect.sync(() => {
      counter.attempts = attempt;
      return new Response('Unavailable', { status: 503, headers: { 'retry-after': '4' } });
    }),
  );

/**
 * Wraps a Web `Response` as the `HttpClientResponse` of `request`, without any transport.
 */
export const makeClientResponse = (
  response: Response,
  request: HttpClientRequest.HttpClientRequest = HttpClientRequest.get('https://example.com/file'),
): HttpClientResponse.HttpClientResponse => HttpClientResponse.fromWeb(request, response);

/**
 * Returns the metadata attached to the http error a failed `exit` squashes to, or null when it
 * succeeded or failed with something other than an http error.
 */
export const exitErrorMeta = (exit: Exit.Exit<unknown, unknown>): HttpErrorMeta | null => {
  const failure = Exit.isFailure(exit) ? Cause.squash(exit.cause) : null;
  if (Schema.is(HttpRequestError)(failure) || Schema.is(HttpRateLimitedError)(failure)) {
    return readHttpErrorMeta(failure);
  }

  return null;
};

/**
 * Forks `effect` and runs it until it can go no further without time passing, then returns its
 * fiber. The fork waits on a one-millisecond test-clock sleep that this helper then adjusts past;
 * `TestClock.adjust` returns only once the woken fiber, and every fiber it resumed or forked, has
 * suspended. So on return a request has reached the transport or is parked on a bulkhead, and every
 * timeout it started is registered with the clock, without any wall-clock wait. Needs
 * `TestClock.layer()` (not `TestClock.make`), and leaves the clock one millisecond later.
 */
export const forkUntilSuspended = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<Fiber.Fiber<A, E>, never, R> =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(Effect.andThen(Effect.sleep('1 millis'), effect));
    yield* TestClock.adjust('1 millis');
    return fiber;
  });

/**
 * Whether `fiber` has finished, successfully or not, at the moment this runs.
 */
export const isFiberDone = <A, E>(fiber: Fiber.Fiber<A, E>): Effect.Effect<boolean> =>
  Effect.sync(() => fiber.pollUnsafe() !== undefined);

/**
 * Runs `effect` on a test clock that moves forward by `advance` once `effect` has suspended, and
 * resolves with its exit. Every timeout and retry delay that falls due within `advance` fires in
 * order, so a test of a timeout neither waits in real time nor depends on how fast the machine is;
 * an `effect` still waiting on something later than `advance` never resolves.
 */
export const runPastTimeouts = <A, E>(
  effect: Effect.Effect<A, E>,
  advance: Duration.Input,
): Promise<Exit.Exit<A, E>> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fiber = yield* forkUntilSuspended(effect);
      yield* TestClock.adjust(advance);
      return yield* Fiber.await(fiber);
    }).pipe(Effect.provide(TestClock.layer())),
  );
