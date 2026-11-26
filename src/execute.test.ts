import { captureLogs } from '@yieldkit/logger/testing';
import {
  Cause,
  Clock,
  Effect,
  Exit,
  Fiber,
  Layer,
  Metric,
  Option,
  Redacted,
  Scheduler,
  Stream,
  Tracer,
} from 'effect';
import {
  HttpBody,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
  UrlParams,
} from 'effect/http';
import { RateLimiter } from 'effect/persistence';
import { TestClock } from 'effect/testing';
import { Readable } from 'node:stream';
import { describe, expect, it } from 'vite-plus/test';

import {
  executeBytesRequest,
  executeConditionalJsonRequest,
  executeJsonRequest,
  executeOptionalJsonRequest,
  executeTextRequest,
} from './execute.ts';
import {
  exitErrorMeta,
  exitMessage,
  exitOutcome,
  FAST_RETRY,
  forkUntilSuspended,
  isFiberDone,
  ItemPayloadSchema,
  makeAlways503,
  makeMockHttpClient,
  makeTrackedStream,
  readRequestCount,
  runPastTimeouts,
  squashExitFailure,
  withResponseUrl,
} from './http.fixture.ts';
import { httpActiveRequests, httpRateLimitsTotal, httpRetriesTotal } from './metrics.ts';

/**
 * Seconds of simulated time the deadline test advances, one second at a time.
 */
const DEADLINE_STEPS = 14;

/**
 * Builds a mock transport that always answers `503` without `Retry-After` and records the number of
 * attempts in `counter`, for retry tests that run on the real clock.
 */
const makeBare503 = (counter: { attempts: number }): ReturnType<typeof makeMockHttpClient> =>
  makeMockHttpClient((_req, _url, attempt) =>
    Effect.sync(() => {
      counter.attempts = attempt;
      return new Response('Unavailable', { status: 503 });
    }),
  );

/**
 * Builds a mock transport whose attempts never answer, recording the number of attempts in
 * `counter`.
 */
const makeHungClient = (counter: { attempts: number }): ReturnType<typeof makeMockHttpClient> =>
  makeMockHttpClient((_req, _url, attempt) =>
    Effect.suspend(() => {
      counter.attempts = attempt;
      return Effect.never;
    }),
  );

/**
 * Reads `resilient_http_retries_total` for `upstream`, summed over every reason.
 */
const readRetryCount = (upstream: string): Effect.Effect<number> =>
  Effect.map(
    Effect.forEach(['rate_limit', 'server_error', 'timeout', 'network_error'], (reason) =>
      Metric.value(Metric.withAttributes(httpRetriesTotal, { upstream, reason })),
    ),
    (states) => states.reduce((total, state) => total + state.count, 0),
  );

/**
 * Builds the layer of a real in-memory rate limiter on the test clock. The store is built on that
 * same clock, so its background sweep does not read the real time and drop every entry.
 */
const makeMemoryLimiterLayer = (): Layer.Layer<RateLimiter.RateLimiter> =>
  RateLimiter.layer.pipe(
    Layer.provide(RateLimiter.layerStoreMemory),
    Layer.provideMerge(TestClock.layer()),
  );

/**
 * Builds a POST to `/items` with a JSON body.
 */
const makePostItem = (): HttpClientRequest.HttpClientRequest =>
  HttpClientRequest.bodyJsonUnsafe(HttpClientRequest.post('https://api.example.com/items'), {
    a: 1,
  });

describe('executeJsonRequest', () => {
  it('does not retry a 429 inline by default even with a short Retry-After', async () => {
    let calls = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        calls = attempt;
        return new Response('Rate limited', {
          status: 429,
          headers: { 'retry-after': '1' },
        });
      }),
    );
    const exit = await Effect.runPromiseExit(
      executeJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/item/7'),
        schema: ItemPayloadSchema,
        upstream: 'test:no-retry-429-default',
      }),
    );
    expect(calls).toBe(1);
    expect(Exit.isFailure(exit)).toBe(true);
    expect(squashExitFailure(exit)).toMatchObject({
      _tag: 'HttpRateLimitedError',
      upstream: 'test:no-retry-429-default',
      retryAfterSeconds: 1,
    });
  });

  it('retries a 503 and a short 429 when retryOnRateLimit is true, then returns the decoded body', async () => {
    let calls = 0;
    const sentAt: Array<number> = [];
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.gen(function* () {
        calls = attempt;
        sentAt.push(yield* Clock.currentTimeMillis);
        if (attempt === 1) {
          return new Response('Unavailable', { status: 503 });
        }

        if (attempt === 2) {
          return new Response('Rate limited', {
            status: 429,
            headers: { 'retry-after': '2' },
          });
        }

        return Response.json(
          { id: 7, title: 'Recovered' },
          {
            status: 200,
            headers: { 'content-type': 'application/json' },
          },
        );
      }),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          executeJsonRequest({
            client,
            request: HttpClientRequest.get('https://api.example.com/item/7'),
            schema: ItemPayloadSchema,
            upstream: 'test:retry',
            retry: { retryOnRateLimit: true },
          }),
        );
        yield* TestClock.adjust('1 second');
        yield* TestClock.adjust('5 seconds');
        return yield* Fiber.join(fiber);
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(calls).toBe(3);
    expect(result).toEqual({ id: 7, title: 'Recovered' });
    // The 429 asked for 2 seconds, far above the ~0.5 s backoff of the second retry.
    const floorWaitMs = Math.round((sentAt[2] ?? 0) - (sentAt[1] ?? 0));
    expect(sentAt).toHaveLength(3);
    expect(floorWaitMs).toBeGreaterThanOrEqual(2000);
    expect(floorWaitMs).toBeLessThan(2100);
  });

  it('waits the Retry-After of a 503 before retrying it', async () => {
    const sentAt: Array<number> = [];
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.gen(function* () {
        sentAt.push(yield* Clock.currentTimeMillis);
        if (attempt === 1) {
          return new Response('Maintenance', { status: 503, headers: { 'retry-after': '3' } });
        }

        return new Response('{"id":1,"title":"back"}');
      }),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          executeJsonRequest({
            client,
            request: HttpClientRequest.get('https://api.example.com/item/1'),
            schema: ItemPayloadSchema,
            upstream: 'test:503-retry-after',
          }),
        );
        yield* TestClock.adjust('5 seconds');
        return yield* Fiber.join(fiber);
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(result).toEqual({ id: 1, title: 'back' });
    expect(sentAt).toEqual([0, 3000]);
  });

  it('fails a 503 whose Retry-After is over the inline cap after one attempt', async () => {
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return new Response('Maintenance', { status: 503, headers: { 'retry-after': '120' } });
      }),
    );
    const exit = await Effect.runPromiseExit(
      executeJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/item/1'),
        schema: ItemPayloadSchema,
        upstream: 'test:503-long-retry-after',
      }),
    );
    expect(attempts).toBe(1);
    expect(squashExitFailure(exit)).toMatchObject({ _tag: 'HttpRequestError', status: 503 });
    expect(exitErrorMeta(exit)?.retryAfterSeconds).toBe(120);
  });

  it('fails a 429 whose Retry-After is longer than 10 seconds without retrying even with retryOnRateLimit', async () => {
    let calls = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        calls = attempt;
        return new Response('Too Many Requests', {
          status: 429,
          headers: { 'retry-after': '45' },
        });
      }),
    );
    const exit = await Effect.runPromiseExit(
      executeJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/items'),
        schema: ItemPayloadSchema,
        upstream: 'test:long-429',
        retry: { retryOnRateLimit: true },
      }),
    );
    expect(calls).toBe(1);
    expect(Exit.isFailure(exit)).toBe(true);
    expect(squashExitFailure(exit)).toMatchObject({
      _tag: 'HttpRateLimitedError',
      upstream: 'test:long-429',
      retryAfterSeconds: 45,
    });
  });
});

describe('executeTextRequest', () => {
  it('releases the unread body when interrupted immediately after headers', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 16, chunkBytes: 8 });
    let signal: AbortSignal | undefined;
    let headersArrived = false;
    let yielded = false;
    const scheduler = new (class extends Scheduler.MixedScheduler {
      override shouldYield(fiber: Fiber.Fiber<unknown, unknown>) {
        if (headersArrived && !yielded) {
          yielded = true;
          queueMicrotask(() => requestFiber.interruptUnsafe());
          return true;
        }

        return super.shouldYield(fiber);
      }
    })();
    const client = HttpClient.make((request, _url, abortSignal) =>
      Effect.sync(() => {
        signal = abortSignal;
        const response = HttpClientResponse.fromWeb(request, new Response(stream));
        headersArrived = true;
        return response;
      }),
    );
    const requestFiber = Effect.runFork(
      executeTextRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/item'),
        upstream: 'test:interrupt-after-headers',
        retry: { maxRetries: 0 },
      }),
      { scheduler },
    );
    const exit = await Effect.runPromise(Fiber.await(requestFiber));
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
    expect(tracker.pulledBytes).toBe(0);
    expect(tracker.cancelled || signal?.aborted).toBe(true);
  });

  it('rejects a body whose Content-Length or streamed size exceeds the cap', async () => {
    const contentLengthClient = makeMockHttpClient(() =>
      Effect.succeed(
        new Response('tiny', {
          status: 200,
          headers: { 'content-length': '99999' },
        }),
      ),
    );
    const headerExit = await Effect.runPromiseExit(
      executeTextRequest({
        client: contentLengthClient,
        request: HttpClientRequest.get('https://example.com/big-header'),
        upstream: 'test:bounded',
        maxBodyBytes: 128,
        retry: { maxRetries: 0 },
      }),
    );
    expect(Exit.isFailure(headerExit)).toBe(true);
    expect(squashExitFailure(headerExit)).toMatchObject({
      _tag: 'HttpRequestError',
      upstream: 'test:bounded',
      status: 200,
    });

    const streamClient = makeMockHttpClient(() =>
      Effect.succeed(new Response('x'.repeat(512), { status: 200 })),
    );
    const streamExit = await Effect.runPromiseExit(
      executeTextRequest({
        client: streamClient,
        request: HttpClientRequest.get('https://example.com/big-stream'),
        upstream: 'test:bounded',
        maxBodyBytes: 128,
        retry: { maxRetries: 0 },
      }),
    );
    expect(Exit.isFailure(streamExit)).toBe(true);
    expect(squashExitFailure(streamExit)).toMatchObject({
      _tag: 'HttpRequestError',
      upstream: 'test:bounded',
      status: 200,
    });
  });
});

describe('executeConditionalJsonRequest', () => {
  it('returns NotModified when If-None-Match matches', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(
        new Response(null, {
          status: 304,
          headers: { etag: '"v1-etag"' },
        }),
      ),
    );
    const result = await Effect.runPromise(
      executeConditionalJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/story/42'),
        schema: ItemPayloadSchema,
        upstream: 'test:etag',
        ifNoneMatch: '"v1-etag"',
      }),
    );
    expect(result).toEqual({
      _tag: 'NotModified',
      etag: '"v1-etag"',
      lastModified: null,
      cacheControl: null,
    });
  });
});

describe('executeJsonRequest failures that are never retried', () => {
  it.each([
    ['invalid JSON', '{not json'],
    ['a schema mismatch', JSON.stringify({ id: 'seven', title: 'Wrong' })],
  ])('makes one attempt for %s', async (_case, body) => {
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return new Response(body);
      }),
    );
    const exit = await Effect.runPromiseExit(
      executeJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/item'),
        schema: ItemPayloadSchema,
        upstream: 'test:json-decode',
        retry: FAST_RETRY,
      }),
    );
    expect(exitOutcome(exit)).toBe('decode_error');
    expect(attempts).toBe(1);
  });

  it('makes one attempt for a body whose Content-Length is over the cap', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 4096, chunkBytes: 256 });
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return new Response(stream, {
          headers: { 'content-type': 'application/json', 'content-length': '4096' },
        });
      }),
    );
    const exit = await Effect.runPromiseExit(
      executeJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/big'),
        schema: ItemPayloadSchema,
        upstream: 'test:json-cap',
        maxBodyBytes: 1024,
        retry: FAST_RETRY,
      }),
    );
    expect(exitOutcome(exit)).toBe('body_too_large');
    expect(attempts).toBe(1);
    expect(tracker.pulledBytes).toBe(0);
  });
});

describe('invalid executor parameters', () => {
  it.each([
    ['attemptTimeout', { attemptTimeout: '1e3 millis' }],
    ['operationTimeout', { operationTimeout: '1e3 millis' }],
    ['maxBodyBytes', { maxBodyBytes: Number.NaN }],
    ['maxBodyBytes', { maxBodyBytes: Number.POSITIVE_INFINITY }],
    ['retry.maxRetries', { retry: { maxRetries: Number.NaN } }],
    ['maxRedirects', { maxRedirects: -1 }],
    ['retry.baseDelay', { retry: { baseDelay: '1e3 millis' } }],
    ['rateLimit.limit', { rateLimit: { limit: 0, window: '1 second' } }],
  ] as const)('fails before sending with a typed error naming %s', async (field, invalid) => {
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return new Response('ok');
      }),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/item'),
        upstream: 'test:invalid-params',
        ...invalid,
      }),
    );
    expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(false);
    expect(squashExitFailure(exit)).toMatchObject({ _tag: 'HttpRequestError', status: null });
    expect(exitMessage(exit)).toContain('test:invalid-params has invalid request parameters: ');
    expect(exitMessage(exit)).toContain(`${field} must be`);
    expect(exitOutcome(exit)).toBe('blocked');
    expect(exitErrorMeta(exit)).toMatchObject({ retryable: false, circuit: 'ignore' });
    expect(attempts).toBe(0);
  });

  it('keeps the body cap of executeBytesRequest when maxBodyBytes is NaN', async () => {
    const { stream, tracker } = makeTrackedStream({
      totalBytes: 8 * 1024 * 1024,
      chunkBytes: 1 << 20,
    });
    const exit = await Effect.runPromiseExit(
      executeBytesRequest({
        client: makeMockHttpClient(() => Effect.succeed(new Response(stream))),
        request: HttpClientRequest.get('https://api.example.com/blob'),
        upstream: 'test:invalid-cap',
        maxBodyBytes: Number.NaN,
      }),
    );
    expect(exitOutcome(exit)).toBe('blocked');
    expect(tracker.pulledBytes).toBe(0);
  });
});

describe('operation deadline', () => {
  it('fails at the deadline with outcome timeout and fewer attempts than maxRetries + 1', async () => {
    const upstream = 'test:deadline';
    const counter = { attempts: 0 };
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          Effect.exit(
            executeJsonRequest({
              client: makeHungClient(counter),
              request: HttpClientRequest.get('https://api.example.com/item'),
              schema: ItemPayloadSchema,
              upstream,
              attemptTimeout: '3 seconds',
              operationTimeout: '10 seconds',
              retry: { maxRetries: 10, baseDelay: '1 second', maxBackoffSeconds: 1 },
            }),
          ),
        );
        for (let step = 0; step < DEADLINE_STEPS; step += 1) {
          yield* TestClock.adjust('1 second');
        }

        const exit = yield* Fiber.join(fiber);
        const timeouts = yield* readRequestCount(upstream, 'GET', 'timeout');
        return { exit, timeouts };
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(Exit.isFailure(result.exit)).toBe(true);
    expect(exitOutcome(result.exit)).toBe('timeout');
    expect(squashExitFailure(result.exit)).toMatchObject({
      _tag: 'HttpRequestError',
      upstream,
      status: null,
    });
    expect(exitMessage(result.exit)).toContain(`${upstream} operation timed out after 10 seconds`);
    expect(exitMessage(result.exit)).toContain(`(${String(counter.attempts)} attempts)`);
    expect(counter.attempts).toBeGreaterThan(0);
    expect(counter.attempts).toBeLessThan(11);
    expect(result.timeouts).toBe(1);
  });

  it('returns a rate limit whose Retry-After ends after the deadline instead of sleeping', async () => {
    const upstream = 'test:deadline-429';
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return new Response('Slow down', { status: 429, headers: { 'retry-after': '5' } });
      }),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          Effect.exit(
            executeTextRequest({
              client,
              request: HttpClientRequest.get('https://api.example.com/item'),
              upstream,
              operationTimeout: '2 seconds',
              retry: { retryOnRateLimit: true },
            }),
          ),
        );
        yield* TestClock.adjust('3 seconds');
        const exit = yield* Fiber.join(fiber);
        const retries = yield* readRetryCount(upstream);
        return { exit, retries };
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(attempts).toBe(1);
    expect(squashExitFailure(result.exit)).toMatchObject({
      _tag: 'HttpRateLimitedError',
      upstream,
      retryAfterSeconds: 5,
    });
    expect(exitOutcome(result.exit)).toBe('rate_limited');
    expect(result.retries).toBe(0);
  });

  it('returns the last 503 when the next backoff would end after the deadline', async () => {
    const upstream = 'test:deadline-503';
    const counter = { attempts: 0 };
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          Effect.exit(
            executeJsonRequest({
              client: makeAlways503(counter),
              request: HttpClientRequest.get('https://api.example.com/item'),
              schema: ItemPayloadSchema,
              upstream,
              operationTimeout: '5 seconds',
              retry: { maxRetries: 10, baseDelay: '4 seconds', maxBackoffSeconds: 5 },
            }),
          ),
        );
        for (let step = 0; step < 7; step += 1) {
          yield* TestClock.adjust('1 second');
        }

        return yield* Fiber.join(fiber);
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(counter.attempts).toBe(2);
    expect(squashExitFailure(result)).toMatchObject({ _tag: 'HttpRequestError', status: 503 });
    expect(exitOutcome(result)).toBe('server_error');
  });

  it('does not count a retry whose attempt never went out before the deadline', async () => {
    const upstream = 'test:deadline-uncounted-retry';
    const counter = { attempts: 0 };
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          Effect.exit(
            executeJsonRequest({
              client: makeBare503(counter),
              request: HttpClientRequest.get('https://api.example.com/item'),
              schema: ItemPayloadSchema,
              upstream,
              operationTimeout: '5 seconds',
              rateLimit: { key: 'deadline-uncounted', limit: 1, window: '1 minute' },
            }),
          ),
        );
        for (let step = 0; step < 7; step += 1) {
          yield* TestClock.adjust('1 second');
        }

        const exit = yield* Fiber.join(fiber);
        const retries = yield* readRetryCount(upstream);
        return { exit, retries };
      }).pipe(Effect.provide(makeMemoryLimiterLayer())),
    );
    expect(counter.attempts).toBe(1);
    expect(exitMessage(result.exit)).toContain('operation timed out');
    expect(result.retries).toBe(0);
  });
});

describe('client-side rate limiter', () => {
  it('fails a request refused in fail mode at once, without retrying or sending it', async () => {
    const upstream = 'test:limiter-fail';
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return new Response('ok');
      }),
    );
    const params = {
      client,
      request: HttpClientRequest.get('https://api.example.com/item'),
      upstream,
      rateLimit: { key: 'limiter-fail', limit: 1, window: '2 seconds', onExceeded: 'fail' },
    } as const;
    // Before the fix the refusal was retried and the request sent once the 2 second window passed:
    // the refused call must be over before any time passes, and stay unsent after the window.
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const first = yield* executeTextRequest(params);
        const refused = yield* forkUntilSuspended(Effect.exit(executeTextRequest(params)));
        const doneWithoutTime = yield* isFiberDone(refused);
        yield* TestClock.adjust('1 minute');
        const second = yield* Fiber.join(refused);
        return { first, second, doneWithoutTime };
      }).pipe(Effect.provide(makeMemoryLimiterLayer())),
    );
    expect(result.first).toBe('ok');
    expect(result.doneWithoutTime).toBe(true);
    expect(attempts).toBe(1);
    expect(squashExitFailure(result.second)).toMatchObject({
      _tag: 'HttpRateLimitedError',
      retryAfterSeconds: 2,
    });
    expect(exitErrorMeta(result.second)).toMatchObject({ retryable: false, circuit: 'ignore' });
  });
});

describe('rate-limited responses', () => {
  it.each([
    ['a 429', 429, { 'retry-after': '45' }, {}],
    [
      'a 403 with no quota left',
      403,
      { 'x-ratelimit-remaining': '0', 'retry-after': '45' },
      { treatQuotaExhausted403AsRateLimit: true },
    ],
    [
      'any 403 under treat403AsRateLimit',
      403,
      { 'retry-after': '45' },
      { treat403AsRateLimit: true },
    ],
  ])('cancels the body of %s without reading it', async (_case, status, headers, flags) => {
    const upstream = 'test:rate-limit-cancel';
    const { stream, tracker } = makeTrackedStream({ totalBytes: 4096, chunkBytes: 256 });
    const client = makeMockHttpClient(() =>
      Effect.succeed(new Response(stream, { status, headers })),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/item'),
        upstream,
        ...flags,
      }),
    );
    expect(squashExitFailure(exit)).toMatchObject({ _tag: 'HttpRateLimitedError' });
    expect(tracker.cancelled).toBe(true);
    expect(tracker.pulledBytes).toBe(0);
  });

  it('counts each upstream rate limit once', async () => {
    const upstream = 'test:rate-limit-count';
    const client = makeMockHttpClient(() =>
      Effect.succeed(new Response('slow', { status: 429, headers: { 'retry-after': '45' } })),
    );
    const count = await Effect.runPromise(
      Effect.gen(function* () {
        yield* Effect.exit(
          executeTextRequest({
            client,
            request: HttpClientRequest.get('https://api.example.com/item'),
            upstream,
          }),
        );
        const state = yield* Metric.value(Metric.withAttributes(httpRateLimitsTotal, { upstream }));
        return state.count;
      }),
    );
    expect(count).toBe(1);
  });
});

describe('http.request.retry log', () => {
  it('logs each retry once, when it goes out, with credential query values masked', async () => {
    const upstream = 'test:retry-log';
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() =>
        attempt === 1
          ? new Response('Unavailable', { status: 503 })
          : new Response('{"id":1,"title":"ok"}'),
      ),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const captured = yield* captureLogs(
          executeJsonRequest({
            client,
            request: HttpClientRequest.get(
              'https://api.example.com/items?%74oken=s3cr3t&creds%5Btoken%5D=k3y&q=ok',
            ),
            schema: ItemPayloadSchema,
            upstream,
            retry: FAST_RETRY,
          }),
          { minLevel: 'Debug' },
        );
        const retries = yield* readRetryCount(upstream);
        return { ...captured, retries };
      }),
    );
    const lines = result.logs.filter((entry) => entry.message === 'http.request.retry');
    expect(lines).toHaveLength(1);
    expect(lines[0]?.annotations).toMatchObject({
      upstream,
      method: 'GET',
      attempt: 1,
      reason: 'server_error',
      errorTag: 'HttpRequestError',
    });
    expect(lines[0]?.annotations.url).toBe(
      'https://api.example.com/items?%74oken=<redacted>&creds%5Btoken%5D=<redacted>&q=ok',
    );
    expect(JSON.stringify(result.logs)).not.toMatch(/s3cr3t|k3y/);
    expect(result.retries).toBe(1);
  });
});

describe('http.request.retry log with redacted names', () => {
  it('masks a query parameter named in UrlParams.CurrentRedactedNames', async () => {
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() =>
        attempt === 1 ? new Response('Unavailable', { status: 503 }) : new Response('ok'),
      ),
    );
    const result = await Effect.runPromise(
      captureLogs(
        executeTextRequest({
          client,
          request: HttpClientRequest.get('https://api.example.com/items?tenant=ACME_SECRET&q=ok'),
          upstream: 'test:retry-log-names',
          retry: FAST_RETRY,
        }),
        { minLevel: 'Debug' },
      ).pipe(Effect.provideService(UrlParams.CurrentRedactedNames, ['tenant'])),
    );
    const line = result.logs.find((entry) => entry.message === 'http.request.retry');
    expect(line?.annotations.url).toBe('https://api.example.com/items?tenant=REDACTED&q=ok');
    expect(JSON.stringify(result.logs)).not.toContain('ACME_SECRET');
  });

  it('masks named query parameters on a relative request completed by a client transform', async () => {
    const transport = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() =>
        attempt === 1 ? new Response('Unavailable', { status: 503 }) : new Response('ok'),
      ),
    );
    const client = HttpClient.mapRequest(
      transport,
      HttpClientRequest.prependUrl('https://api.example.com'),
    );
    const result = await Effect.runPromise(
      captureLogs(
        executeTextRequest({
          client,
          request: HttpClientRequest.get('/items?tenant=ACME_SECRET&q=ok', {
            urlParams: { ticket: Redacted.make('TICKET_SECRET') },
          }),
          upstream: 'test:retry-log-relative',
          retry: FAST_RETRY,
        }),
        { minLevel: 'Debug' },
      ).pipe(Effect.provideService(UrlParams.CurrentRedactedNames, ['tenant'])),
    );
    const line = result.logs.find((entry) => entry.message === 'http.request.retry');
    expect(result.value).toBe('ok');
    expect(line?.annotations.url).toBe('/items?tenant=REDACTED&q=ok&ticket=REDACTED');
  });
});

describe('retries by method', () => {
  it('makes exactly one attempt for a POST 503', async () => {
    const counter = { attempts: 0 };
    const exit = await Effect.runPromiseExit(
      executeJsonRequest({
        client: makeAlways503(counter),
        request: makePostItem(),
        schema: ItemPayloadSchema,
        upstream: 'test:post-503',
        retry: FAST_RETRY,
      }),
    );
    expect(counter.attempts).toBe(1);
    expect(squashExitFailure(exit)).toMatchObject({ _tag: 'HttpRequestError', status: 503 });
  });

  it('retries a POST 429 whose Retry-After is 0 when retryOnRateLimit is true', async () => {
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        if (attempt === 1) {
          return new Response('Slow down', { status: 429, headers: { 'retry-after': '0' } });
        }

        return new Response('{"id":1,"title":"ok"}');
      }),
    );
    const result = await Effect.runPromise(
      executeJsonRequest({
        client,
        request: makePostItem(),
        schema: ItemPayloadSchema,
        upstream: 'test:post-429',
        retry: { ...FAST_RETRY, retryOnRateLimit: true },
      }),
    );
    expect(attempts).toBe(2);
    expect(result).toEqual({ id: 1, title: 'ok' });
  });

  it('does not replay a POST for a rate limit on the page a redirect led to', async () => {
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return withResponseUrl(
          new Response('Slow down', { status: 429, headers: { 'retry-after': '0' } }),
          'https://api.example.com/jobs/1',
        );
      }),
    );
    const exit = await Effect.runPromiseExit(
      executeJsonRequest({
        client,
        request: makePostItem(),
        schema: ItemPayloadSchema,
        upstream: 'test:post-303-429',
        retry: { ...FAST_RETRY, retryOnRateLimit: true },
      }),
    );
    expect(attempts).toBe(1);
    expect(squashExitFailure(exit)).toMatchObject({ _tag: 'HttpRateLimitedError' });
    expect(exitErrorMeta(exit)?.redirected).toBe(true);
  });

  it('still retries a GET whose rate limit came from a redirect target when retryOnRateLimit is true', async () => {
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        if (attempt === 1) {
          return withResponseUrl(
            new Response('Slow down', { status: 429, headers: { 'retry-after': '0' } }),
            'https://cdn.example.com/items',
          );
        }

        return new Response('{"id":1,"title":"ok"}');
      }),
    );
    const result = await Effect.runPromise(
      executeJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/items'),
        schema: ItemPayloadSchema,
        upstream: 'test:get-redirect-429',
        retry: { ...FAST_RETRY, retryOnRateLimit: true },
      }),
    );
    expect(attempts).toBe(2);
    expect(result).toEqual({ id: 1, title: 'ok' });
  });

  it('retries a POST 503 when the request is marked idempotent', async () => {
    const counter = { attempts: 0 };
    const exit = await Effect.runPromiseExit(
      executeJsonRequest({
        client: makeBare503(counter),
        request: makePostItem(),
        schema: ItemPayloadSchema,
        upstream: 'test:post-idempotent',
        retry: FAST_RETRY,
        idempotent: true,
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(counter.attempts).toBe(3);
  });

  it('still retries a GET 503', async () => {
    const counter = { attempts: 0 };
    await Effect.runPromiseExit(
      executeJsonRequest({
        client: makeBare503(counter),
        request: HttpClientRequest.get('https://api.example.com/items'),
        schema: ItemPayloadSchema,
        upstream: 'test:get-503',
        retry: FAST_RETRY,
      }),
    );
    expect(counter.attempts).toBe(3);
  });
});

describe('executeOptionalJsonRequest', () => {
  it('gives none for a 404 after one attempt, cancels the body and records not_found', async () => {
    const upstream = 'test:optional-404';
    const { stream, tracker } = makeTrackedStream({ totalBytes: 4096, chunkBytes: 256 });
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return new Response(stream, { status: 404 });
      }),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const value = yield* executeOptionalJsonRequest({
          client,
          request: HttpClientRequest.get('https://api.example.com/item/9'),
          schema: ItemPayloadSchema,
          upstream,
          retry: FAST_RETRY,
        });
        const notFound = yield* readRequestCount(upstream, 'GET', 'not_found');
        return { value, notFound };
      }),
    );
    expect(Option.isNone(result.value)).toBe(true);
    expect(attempts).toBe(1);
    expect(result.notFound).toBe(1);
    expect(tracker.pulledBytes).toBe(0);
    expect(tracker.cancelled).toBe(true);
  });

  it('gives some for a 200', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(new Response('{"id":3,"title":"Found"}')),
    );
    const result = await Effect.runPromise(
      executeOptionalJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/item/3'),
        schema: ItemPayloadSchema,
        upstream: 'test:optional-200',
      }),
    );
    expect(result).toEqual(Option.some({ id: 3, title: 'Found' }));
  });

  it('fails on a 500 like executeJsonRequest', async () => {
    const client = makeMockHttpClient(() => Effect.succeed(new Response('boom', { status: 500 })));
    const exit = await Effect.runPromiseExit(
      executeOptionalJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/item/3'),
        schema: ItemPayloadSchema,
        upstream: 'test:optional-500',
        retry: { maxRetries: 0 },
      }),
    );
    expect(exitOutcome(exit)).toBe('server_error');
  });
});

describe('acceptNotFound', () => {
  it('returns the 404 body from executeTextRequest and counts not_found', async () => {
    const upstream = 'test:text-404';
    const client = makeMockHttpClient(() =>
      Effect.succeed(new Response('page is gone', { status: 404 })),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const text = yield* executeTextRequest({
          client,
          request: HttpClientRequest.get('https://api.example.com/missing'),
          upstream,
          acceptNotFound: true,
        });
        const notFound = yield* readRequestCount(upstream, 'GET', 'not_found');
        return { text, notFound };
      }),
    );
    expect(result.text).toBe('page is gone');
    expect(result.notFound).toBe(1);
  });

  it('still fails executeTextRequest with status 404 without acceptNotFound', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(new Response('page is gone', { status: 404 })),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/missing'),
        upstream: 'test:text-404-strict',
      }),
    );
    expect(squashExitFailure(exit)).toMatchObject({ _tag: 'HttpRequestError', status: 404 });
    expect(exitOutcome(exit)).toBe('client_error');
  });

  it('reads a 404 body under the byte cap', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(new Response('x'.repeat(512), { status: 404 })),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/missing'),
        upstream: 'test:text-404-cap',
        maxBodyBytes: 128,
        acceptNotFound: true,
      }),
    );
    expect(exitOutcome(exit)).toBe('body_too_large');
  });

  it('is ignored by executeJsonRequest, which fails a 404', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(new Response('{"id":1,"title":"t"}', { status: 404 })),
    );
    const exit = await Effect.runPromiseExit(
      executeJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/missing'),
        schema: ItemPayloadSchema,
        upstream: 'test:json-404',
        acceptNotFound: true,
        retry: { maxRetries: 0 },
      }),
    );
    expect(squashExitFailure(exit)).toMatchObject({ status: 404 });
  });
});

/**
 * Builds a PUT to `url` whose body is a one-shot stream of `text`: a second read yields nothing,
 * like a request body piped from a socket or a file handle.
 */
const oneShotPut = (url: string, text: string): HttpClientRequest.HttpClientRequest => {
  const chunks = [new TextEncoder().encode(text)][Symbol.iterator]();
  const oneShot: Iterable<Uint8Array> = { [Symbol.iterator]: () => chunks };
  return HttpClientRequest.put(url).pipe(
    HttpClientRequest.setBody(HttpBody.stream(Stream.fromIterable(oneShot))),
  );
};

/**
 * Reads the body of a request the mock transport received as text.
 */
const receivedBody = (request: HttpClientRequest.HttpClientRequest): Effect.Effect<string> =>
  request.body._tag === 'Stream'
    ? Stream.mkString(Stream.decodeText(Stream.orDie(request.body.stream)))
    : Effect.succeed('');

describe('one-shot stream bodies', () => {
  it('never retries a request whose stream body the first attempt consumed', async () => {
    const bodies: Array<string> = [];
    const client = makeMockHttpClient((request, _url, attempt) =>
      Effect.map(receivedBody(request), (body) => {
        bodies.push(body);
        return new Response(attempt === 1 ? 'busy' : 'stored', {
          status: attempt === 1 ? 503 : 200,
        });
      }),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({
        client,
        request: oneShotPut('https://api.example.com/objects/1', 'abc'),
        upstream: 'test:stream-put',
        retry: FAST_RETRY,
      }),
    );
    expect(bodies).toEqual(['abc']);
    expect(squashExitFailure(exit)).toMatchObject({ status: 503 });
  });

  it('never retries a request whose raw body is a Node Readable', async () => {
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return new Response('busy', { status: 503 });
      }),
    );
    const request = HttpClientRequest.put('https://api.example.com/objects/1').pipe(
      HttpClientRequest.setBody(
        HttpBody.raw(Readable.from(['abc']), { contentType: 'text/plain' }),
      ),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({ client, request, upstream: 'test:raw-put', retry: FAST_RETRY }),
    );
    expect(attempts).toBe(1);
    expect(squashExitFailure(exit)).toMatchObject({ status: 503 });
  });

  it('refuses a 307 that would replay a consumed stream body, even on the same origin', async () => {
    const bodies: Array<string> = [];
    const client = makeMockHttpClient((request, url) =>
      Effect.map(receivedBody(request), (body) => {
        bodies.push(body);
        return url.pathname === '/objects/1'
          ? new Response(null, { status: 307, headers: { location: '/objects/2' } })
          : new Response('stored');
      }),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({
        client,
        request: oneShotPut('https://api.example.com/objects/1', 'abc'),
        upstream: 'test:stream-307',
      }),
    );
    expect(bodies).toEqual(['abc']);
    expect(exitOutcome(exit)).toBe('blocked');
    expect(exitMessage(exit)).toContain('a 307 redirect would replay a one-shot body');
  });

  it('refuses a 307 that would replay a consumed synchronous generator body', async () => {
    const bytes = new TextEncoder().encode('draft');
    const generator = (function* () {
      yield bytes;
    })();
    const bodies: Array<string> = [];
    const client = makeMockHttpClient((request, url) =>
      Effect.sync(() => {
        if (request.body._tag === 'Raw' && request.body.body === generator) {
          const decoder = new TextDecoder();
          bodies.push(Array.from(generator, (chunk) => decoder.decode(chunk)).join(''));
        }

        return url.pathname === '/item'
          ? new Response(null, { status: 307, headers: { location: '/result' } })
          : new Response('stored');
      }),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({
        client,
        request: HttpClientRequest.setBody(
          HttpClientRequest.put('https://api.example.com/item'),
          HttpBody.raw(generator),
        ),
        upstream: 'test:generator-307',
        retry: { maxRetries: 0 },
      }),
    );
    expect(bodies).toEqual(['draft']);
    expect(exitOutcome(exit)).toBe('blocked');
  });
});

describe('redirect query redaction', () => {
  it('keeps a Redacted query value masked in the error snippet and span after a redirect', async () => {
    const secret = 'r-8c1f5a0e';
    const client = makeMockHttpClient((_request, url) =>
      Effect.sync(() =>
        url.pathname === '/search'
          ? new Response(null, { status: 301, headers: { location: `/search/?ref=${secret}` } })
          : new Response(`unknown ref ${url.searchParams.get('ref')}`, { status: 400 }),
      ),
    );
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });
    const exit = await Effect.runPromiseExit(
      executeTextRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/search', {
          urlParams: { ref: Redacted.make(secret) },
        }),
        upstream: 'test:redirect-redaction',
        retry: { maxRetries: 0 },
      }).pipe(Effect.provideService(Tracer.Tracer, tracer)),
    );
    expect(squashExitFailure(exit)).toMatchObject({
      status: 400,
      bodySnippet: 'unknown ref <redacted>',
    });
    const hop = spans.find((span) => span.attributes.get('url.path') === '/search/');
    expect(hop?.attributes.get('url.full')).toBe('https://api.example.com/search/?ref=REDACTED');
    expect(hop?.attributes.get('url.query')).toBe('ref=REDACTED');
  });
});

describe('error bodies that stall', () => {
  it('fails a 403 whose body stalls past the attempt timeout as the 403, not a retried timeout', async () => {
    let sent = 0;
    const client = makeMockHttpClient(() =>
      Effect.sync(() => {
        sent += 1;
        const { stream } = makeTrackedStream({ totalBytes: 1024, chunkBytes: 64, stall: true });
        return new Response(stream, { status: 403, headers: { 'content-type': 'text/html' } });
      }),
    );
    const exit = await runPastTimeouts(
      executeTextRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/private'),
        upstream: 'test:stalled-403',
        attemptTimeout: '50 millis',
        detectHtmlEdgeBlocks: true,
        retry: FAST_RETRY,
      }),
      '50 millis',
    );
    expect(sent).toBe(1);
    expect(squashExitFailure(exit)).toMatchObject({
      status: 403,
      message: 'test:stalled-403 responded with HTTP 403',
    });
    expect(exitOutcome(exit)).toBe('client_error');
  });
});

describe('request metric on interruption', () => {
  it('restores the active gauge when interrupted immediately after it increments', async () => {
    const upstream = 'test:interrupt-active-gauge';
    const registry: Metric.MetricRegistry = new Map();
    const gauge = Metric.withAttributes(httpActiveRequests, { upstream });
    const readGauge = () =>
      Effect.runSync(
        Metric.value(gauge).pipe(Effect.provideService(Metric.MetricRegistry, registry)),
      ).value;
    let sent = 0;
    let yielded = false;
    const scheduler = new (class extends Scheduler.MixedScheduler {
      override shouldYield(fiber: Fiber.Fiber<unknown, unknown>) {
        if (!yielded && readGauge() === 1) {
          yielded = true;
          queueMicrotask(() => fiber.interruptUnsafe());
          return true;
        }

        return super.shouldYield(fiber);
      }
    })();
    const client = makeMockHttpClient(() =>
      Effect.suspend(() => {
        sent += 1;
        return Effect.never;
      }),
    );
    const fiber = Effect.runFork(
      executeTextRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/item'),
        upstream,
      }).pipe(Effect.provideService(Metric.MetricRegistry, registry)),
      { scheduler },
    );
    const exit = await Effect.runPromise(Fiber.await(fiber));
    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
    expect(yielded).toBe(true);
    expect({ sent, gauge: readGauge() }).toEqual({ sent: 0, gauge: 0 });
  });

  it('records a request its caller interrupts while it waits on the transport', async () => {
    const upstream = 'test:interrupted-metric';
    const client = makeMockHttpClient(() => Effect.never);
    const count = await Effect.runPromise(
      Effect.gen(function* () {
        const request = yield* forkUntilSuspended(
          executeTextRequest({
            client,
            request: HttpClientRequest.get('https://api.example.com/hang'),
            upstream,
          }),
        );
        yield* Fiber.interrupt(request);
        return yield* readRequestCount(upstream, 'GET', 'network_error');
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(count).toBe(1);
  });
});
