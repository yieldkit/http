import { Deferred, Effect, Exit, Fiber, Stream } from 'effect';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import type { HttpPolicy } from './policy.ts';

import {
  FAST_RETRY,
  exitMessage,
  exitOutcome,
  forkUntilSuspended,
  makeClientOver,
  makeMockHttpClient,
  makeTrackedStream,
  readRequestCount,
  runPastTimeouts,
} from './http.fixture.ts';

/**
 * Policy shared by the stream tests; each test sets its own `upstream`.
 */
const basePolicy = (upstream: string): HttpPolicy => ({
  upstream,
  baseUrl: 'https://files.example.com',
  retry: FAST_RETRY,
});

/**
 * Collects a byte stream into its total length and number of chunks.
 */
const measure = <E>(
  stream: Stream.Stream<Uint8Array, E>,
): Effect.Effect<{ readonly bytes: number; readonly chunks: number }, E> =>
  Stream.runFold(
    stream,
    () => ({ bytes: 0, chunks: 0 }),
    (acc, chunk) => ({ bytes: acc.bytes + chunk.byteLength, chunks: acc.chunks + 1 }),
  );

/**
 * Builds a body that never ends: every pull gets a 64-byte chunk at once, so it never stalls.
 */
const makeEndlessStream = (): ReadableStream<Uint8Array> =>
  new ReadableStream<Uint8Array>(
    {
      pull: (controller) => {
        controller.enqueue(new Uint8Array(64));
      },
    },
    { highWaterMark: 0 },
  );

describe('stream', () => {
  it('delivers the body chunk by chunk and reads it only as it is pulled', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 3000, chunkBytes: 1000 });
    let requests = 0;
    const transport = makeMockHttpClient(() =>
      Effect.sync(() => {
        requests += 1;
        return new Response(stream);
      }),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, basePolicy('test:stream-ok'));
        const body = http.stream('/archive.bin');
        const before = requests;
        const measured = yield* measure(body);
        return { before, measured };
      }),
    );
    expect(result.before).toBe(0);
    expect(result.measured).toEqual({ bytes: 3000, chunks: 3 });
    expect(tracker.pulledBytes).toBe(3000);
    expect(requests).toBe(1);
  });

  it('fails past maxBodyBytes as body_too_large and cancels the body', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 10_000, chunkBytes: 1000 });
    const transport = makeMockHttpClient(() => Effect.succeed(new Response(stream)));
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, basePolicy('test:stream-cap'));
        return yield* measure(http.stream('/big', { maxBodyBytes: 2500 }));
      }),
    );
    expect(exitOutcome(exit)).toBe('body_too_large');
    expect(tracker.pulledBytes).toBe(3000);
    expect(tracker.cancelled).toBe(true);
  });

  it('refuses a declared Content-Length over the cap before reading anything', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 10_000, chunkBytes: 1000 });
    const transport = makeMockHttpClient(() =>
      Effect.succeed(new Response(stream, { headers: { 'content-length': '10000' } })),
    );
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, basePolicy('test:stream-declared'));
        return yield* measure(http.stream('/big', { maxBodyBytes: 2500 }));
      }),
    );
    expect(exitOutcome(exit)).toBe('body_too_large');
    expect(tracker.pulledBytes).toBe(0);
  });

  it('fails a body that sends nothing for idleTimeout as a stalled timeout', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 10, chunkBytes: 10, stall: true });
    const transport = makeMockHttpClient(() => Effect.succeed(new Response(stream)));
    const exit = await runPastTimeouts(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, basePolicy('test:stream-idle'));
        return yield* measure(http.stream('/slow', { idleTimeout: '50 millis' }));
      }),
      '50 millis',
    );
    expect(exitOutcome(exit)).toBe('timeout');
    expect(exitMessage(exit)).toContain('response body stalled: no data for 50 millis');
    expect(tracker.cancelled).toBe(true);
  });

  it('fails a body that keeps flowing once the whole call passes operationTimeout', async () => {
    const transport = makeMockHttpClient(() => Effect.succeed(new Response(makeEndlessStream())));
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, basePolicy('test:stream-total'));
        return yield* measure(
          http.stream('/endless', {
            operationTimeout: '150 millis',
            idleTimeout: '1 second',
            maxBodyBytes: 1024 ** 3,
          }),
        );
      }),
    );
    expect(exitOutcome(exit)).toBe('timeout');
    expect(exitMessage(exit)).toContain('ran past the operation timeout (150 millis)');
  });

  it('retries a transient status before the body starts', async () => {
    let attempts = 0;
    const transport = makeMockHttpClient((_request, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return attempt === 1
          ? new Response('busy', { status: 503 })
          : new Response(new Uint8Array(64));
      }),
    );
    const measured = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, basePolicy('test:stream-retry'));
        return yield* measure(http.stream('/file'));
      }),
    );
    expect(measured.bytes).toBe(64);
    expect(attempts).toBe(2);
  });

  it('retries under maxConcurrency 1 without waiting on its own permit', async () => {
    const transport = makeMockHttpClient((_request, _url, attempt) =>
      Effect.succeed(
        attempt === 1 ? new Response('busy', { status: 503 }) : new Response(new Uint8Array(8)),
      ),
    );
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, {
          ...basePolicy('test:stream-retry-permit'),
          maxConcurrency: 1,
        });
        return yield* measure(http.stream('/file'));
      }).pipe(Effect.timeout('2 seconds')),
    );
    expect(exit).toEqual(Exit.succeed({ bytes: 8, chunks: 1 }));
  });

  it('does not retry once part of the body has been delivered', async () => {
    let attempts = 0;
    const transport = makeMockHttpClient((_request, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        let sent = false;
        return new Response(
          new ReadableStream<Uint8Array>({
            pull: (controller) => {
              if (sent) {
                controller.error(new Error('connection reset'));
                return;
              }

              sent = true;
              controller.enqueue(new Uint8Array(16));
            },
          }),
        );
      }),
    );
    const received: Array<number> = [];
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, basePolicy('test:stream-no-replay'));
        return yield* Stream.runForEach(http.stream('/file'), (chunk) =>
          Effect.sync(() => received.push(chunk.byteLength)),
        );
      }),
    );
    expect(exitOutcome(exit)).toBe('network_error');
    expect(received).toEqual([16]);
    expect(attempts).toBe(1);
  });

  it('cancels the body when the consumer stops early', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 10_000, chunkBytes: 1000 });
    const transport = makeMockHttpClient(() => Effect.succeed(new Response(stream)));
    const measured = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, basePolicy('test:stream-take'));
        return yield* measure(Stream.take(http.stream('/big'), 2));
      }),
    );
    expect(measured.chunks).toBe(2);
    expect(tracker.pulledBytes).toBeLessThan(10_000);
    expect(tracker.cancelled).toBe(true);
  });

  it('gives an empty stream for a 204', async () => {
    const transport = makeMockHttpClient(() => Effect.succeed(new Response(null, { status: 204 })));
    const measured = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, basePolicy('test:stream-204'));
        return yield* measure(http.stream('/none'));
      }),
    );
    expect(measured).toEqual({ bytes: 0, chunks: 0 });
  });

  it('records the request metric once, when the body ends, with the body outcome', async () => {
    const upstream = 'test:stream-metric';
    const counts = await Effect.runPromise(
      Effect.gen(function* () {
        const ok = makeMockHttpClient(() => Effect.succeed(new Response(new Uint8Array(10))));
        const http = yield* makeClientOver(ok, basePolicy(upstream));
        const body = http.stream('/ok');
        const beforeRun = yield* readRequestCount(upstream, 'GET', 'success');
        yield* measure(body);
        const afterSuccess = yield* readRequestCount(upstream, 'GET', 'success');
        const big = makeMockHttpClient(() => Effect.succeed(new Response(new Uint8Array(4096))));
        const capped = yield* makeClientOver(big, basePolicy(upstream));
        yield* Effect.exit(measure(capped.stream('/big', { maxBodyBytes: 1024 })));
        const tooLarge = yield* readRequestCount(upstream, 'GET', 'body_too_large');
        const successAfterFailure = yield* readRequestCount(upstream, 'GET', 'success');
        return { beforeRun, afterSuccess, tooLarge, successAfterFailure };
      }),
    );
    expect(counts).toEqual({ beforeRun: 0, afterSuccess: 1, tooLarge: 1, successAfterFailure: 1 });
  });

  it('holds the bulkhead permit until the stream ends', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const firstRequested = yield* Deferred.make<void>();
        const controllers: Array<ReadableStreamDefaultController<Uint8Array>> = [];
        const held = new ReadableStream<Uint8Array>({
          start: (controller) => {
            controllers.push(controller);
          },
        });
        const paths: Array<string> = [];
        const transport = makeMockHttpClient((_request, url) =>
          Effect.gen(function* () {
            paths.push(url.pathname);
            if (url.pathname === '/held') {
              yield* Deferred.succeed(firstRequested, undefined);
              return new Response(held);
            }

            return new Response('second');
          }),
        );
        const http = yield* makeClientOver(transport, {
          ...basePolicy('test:stream-permit'),
          maxConcurrency: 1,
        });
        const first = yield* Effect.forkChild(measure(http.stream('/held')));
        yield* Deferred.await(firstRequested);
        // On return the second request has reached the transport or is parked on the bulkhead.
        const second = yield* forkUntilSuspended(http.getText('/second'));
        const pathsWhileHeld = [...paths];
        for (const controller of controllers) {
          controller.enqueue(new Uint8Array(1));
          controller.close();
        }

        yield* Fiber.join(first);
        const text = yield* Fiber.join(second);
        return { pathsWhileHeld, text };
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(result.pathsWhileHeld).toEqual(['/held']);
    expect(result.text).toBe('second');
  });
});

describe('stream request metric', () => {
  it('counts a 404 accepted with acceptNotFound as not_found, not success', async () => {
    const upstream = 'test:stream-not-found';
    const transport = makeMockHttpClient(() =>
      Effect.succeed(new Response('missing', { status: 404 })),
    );
    const counts = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, basePolicy(upstream));
        yield* measure(http.stream('/gone', { acceptNotFound: true }));
        return {
          notFound: yield* readRequestCount(upstream, 'GET', 'not_found'),
          success: yield* readRequestCount(upstream, 'GET', 'success'),
        };
      }),
    );
    expect(counts).toEqual({ notFound: 1, success: 0 });
  });
});
