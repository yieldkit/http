import { Cause, Clock, Effect, Exit, Fiber, Layer, Metric, Ref } from 'effect';
import { Headers, HttpClient, HttpClientRequest } from 'effect/http';
import { CircuitBreaker, RateLimiter } from 'effect/persistence';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import {
  ResilientHttp,
  HttpResilienceLive,
  makeHttpResilienceLayer,
  makeResilientHttpClient,
} from './client.ts';
import { EtagCache } from './conditional.ts';
import {
  exitErrorMeta,
  exitMessage,
  exitOutcome,
  ItemPayloadSchema,
  makeClientOver,
  makeMockHttpClient,
  squashExitFailure,
} from './http.fixture.ts';
import { httpRequestDurationMs, httpRequestsTotal, httpRetriesTotal } from './metrics.ts';
import { DEFAULT_CIRCUIT_CONSECUTIVE_FAILURES } from './policy.ts';

/**
 * Seconds of simulated time each hung call is given: past the 60 second default deadline.
 */
const HUNG_CALL_STEPS = 61;

describe('makeResilientHttpClient', () => {
  it('treats a quota-exhausted 403 and an HTML edge block as rate limits', async () => {
    const nowEpochSec = Math.floor(Date.UTC(2026, 0, 15, 12, 1, 0) / 1000);
    const quotaClient = makeMockHttpClient(() =>
      Effect.succeed(
        Response.json(
          { message: 'API rate limit exceeded' },
          {
            status: 403,
            headers: {
              'content-type': 'application/json',
              'x-ratelimit-remaining': '0',
              'x-ratelimit-reset': String(nowEpochSec),
            },
          },
        ),
      ),
    );
    const quotaExit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        yield* TestClock.setTime(Date.UTC(2026, 0, 15, 12, 0, 0));
        const upstreamClient = yield* makeResilientHttpClient({
          upstream: 'test:quota-api',
          baseUrl: 'https://api.example.com',
          treatQuotaExhausted403AsRateLimit: true,
        });
        return yield* upstreamClient.getJson('/items', ItemPayloadSchema);
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, quotaClient),
        Effect.provide(TestClock.layer()),
      ),
    );
    expect(Exit.isFailure(quotaExit)).toBe(true);
    expect(squashExitFailure(quotaExit)).toMatchObject({
      _tag: 'HttpRateLimitedError',
      upstream: 'test:quota-api',
      retryAfterSeconds: 60,
    });

    const edgeClient = makeMockHttpClient(() =>
      Effect.succeed(
        new Response(
          '<!DOCTYPE html><html><title>Attention Required! | Cloudflare</title></html>',
          {
            status: 403,
            headers: { 'content-type': 'text/html; charset=utf-8' },
          },
        ),
      ),
    );
    const edgeExit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const upstreamClient = yield* makeResilientHttpClient({
          upstream: 'test:html-scraper',
          baseUrl: 'https://www.example.com',
          detectHtmlEdgeBlocks: true,
        });
        return yield* upstreamClient.getText('/trending');
      }).pipe(Effect.provideService(HttpClient.HttpClient, edgeClient)),
    );
    expect(Exit.isFailure(edgeExit)).toBe(true);
    expect(squashExitFailure(edgeExit)).toMatchObject({
      _tag: 'HttpRateLimitedError',
      upstream: 'test:html-scraper',
      retryAfterSeconds: 60,
    });
  });

  it('serves a cached payload after a 304', async () => {
    let seenIfNoneMatch: string | null = null;
    const mockHttp = makeMockHttpClient((request) =>
      Effect.sync(() => {
        const inm = Headers.get(request.headers, 'if-none-match');
        seenIfNoneMatch = inm._tag === 'Some' ? inm.value : null;
        if (seenIfNoneMatch === '"v1-etag"') {
          return new Response(null, {
            status: 304,
            headers: { etag: '"v1-etag"' },
          });
        }

        return Response.json(
          { id: 42, title: 'Cached Story' },
          {
            status: 200,
            headers: {
              'content-type': 'application/json',
              etag: '"v1-etag"',
            },
          },
        );
      }),
    );
    const cachedOutcome = await Effect.runPromise(
      Effect.gen(function* () {
        const httpFactory = yield* ResilientHttp;
        const upstreamClient = yield* httpFactory.client({
          upstream: 'test:cached',
          baseUrl: 'https://api.example.com',
        });
        const first = yield* upstreamClient.getCachedJson('/story/42', ItemPayloadSchema);
        const second = yield* upstreamClient.getCachedJson('/story/42', ItemPayloadSchema);
        return { first, second, seenIfNoneMatch };
      }).pipe(
        Effect.provide(
          ResilientHttp.layer.pipe(
            Layer.provide(Layer.succeed(HttpClient.HttpClient, mockHttp)),
            Layer.provide(EtagCache.layer),
          ),
        ),
      ),
    );
    expect(cachedOutcome.first).toEqual({ id: 42, title: 'Cached Story' });
    expect(cachedOutcome.second).toEqual({ id: 42, title: 'Cached Story' });
    expect(cachedOutcome.seenIfNoneMatch).toBe('"v1-etag"');
  });

  it('delays a third request when the token bucket is empty', async () => {
    const timestamps: Array<number> = [];
    const mockHttp = makeMockHttpClient(() =>
      Effect.gen(function* () {
        const now = yield* Clock.currentTimeMillis;
        timestamps.push(now);
        return Response.json(
          { id: timestamps.length, title: 'ok' },
          {
            status: 200,
            headers: { 'content-type': 'application/json' },
          },
        );
      }),
    );
    const layer = Layer.mergeAll(
      Layer.succeed(HttpClient.HttpClient, mockHttp),
      RateLimiter.layer.pipe(
        Layer.provide(RateLimiter.layerStoreMemory),
        Layer.provideMerge(TestClock.layer()),
      ),
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        const upstreamClient = yield* makeResilientHttpClient({
          upstream: 'test:ratelimiter',
          baseUrl: 'https://api.example.com',
          rateLimit: {
            key: 'test-bucket',
            limit: 2,
            window: '2 seconds',
            algorithm: 'token-bucket',
            onExceeded: 'delay',
          },
        });
        const fiber = yield* Effect.forkChild(
          Effect.gen(function* () {
            yield* upstreamClient.getJson('/1', ItemPayloadSchema);
            yield* upstreamClient.getJson('/2', ItemPayloadSchema);
            yield* upstreamClient.getJson('/3', ItemPayloadSchema);
          }),
        );
        yield* TestClock.adjust('3 seconds');
        return yield* Fiber.join(fiber);
      }).pipe(Effect.provide(layer)),
    );
    expect(timestamps).toHaveLength(3);
    expect(timestamps[0]).toBe(0);
    expect(timestamps[1]).toBe(0);
    expect(timestamps[2]).toBeGreaterThanOrEqual(1000);
  });

  it('opens the circuit after consecutive 5xx responses and probes again after the open duration', async () => {
    let transportCalls = 0;
    let shouldFail = true;
    const mockHttp = makeMockHttpClient(() =>
      Effect.sync(() => {
        transportCalls += 1;
        if (shouldFail) {
          return new Response('Gateway Timeout', { status: 504 });
        }

        return Response.json(
          { id: 99, title: 'Healthy' },
          {
            status: 200,
            headers: { 'content-type': 'application/json' },
          },
        );
      }),
    );
    const breakerLayer = CircuitBreaker.layer({
      tripPolicy: CircuitBreaker.consecutive(2),
      openDuration: '10 seconds',
    }).pipe(Layer.provide(CircuitBreaker.layerStoreMemory()));
    const layer = Layer.mergeAll(
      Layer.succeed(HttpClient.HttpClient, mockHttp),
      breakerLayer,
      TestClock.layer(),
    );
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const upstreamClient = yield* makeResilientHttpClient({
          upstream: 'test:circuit',
          baseUrl: 'https://api.example.com',
          retry: { maxRetries: 0 },
          circuitBreaker: { key: 'test:circuit-key' },
        });
        const exit1 = yield* Effect.exit(upstreamClient.getJson('/item', ItemPayloadSchema));
        const exit2 = yield* Effect.exit(upstreamClient.getJson('/item', ItemPayloadSchema));
        const callsAfterTrip = transportCalls;
        const fastFailExit = yield* Effect.exit(upstreamClient.getJson('/item', ItemPayloadSchema));
        const callsAfterFastFail = transportCalls;
        shouldFail = false;
        yield* TestClock.adjust('11 seconds');
        const recovered = yield* upstreamClient.getJson('/item', ItemPayloadSchema);
        return {
          exit1Failed: Exit.isFailure(exit1),
          exit2Failed: Exit.isFailure(exit2),
          callsAfterTrip,
          fastFailExit,
          callsAfterFastFail,
          recovered,
        };
      }).pipe(Effect.provide(layer)),
    );
    expect(outcome.exit1Failed).toBe(true);
    expect(outcome.exit2Failed).toBe(true);
    expect(outcome.callsAfterTrip).toBe(2);
    expect(outcome.callsAfterFastFail).toBe(2);
    expect(Exit.isFailure(outcome.fastFailExit)).toBe(true);
    expect(squashExitFailure(outcome.fastFailExit)).toMatchObject({
      _tag: 'HttpRequestError',
      upstream: 'test:circuit',
      status: 503,
    });
    expect(outcome.recovered).toEqual({ id: 99, title: 'Healthy' });
  });

  it('opens the shared circuit when hung calls run out of the default operation deadline', async () => {
    let transportCalls = 0;
    const hungHttp = makeMockHttpClient(() =>
      Effect.suspend(() => {
        transportCalls += 1;
        return Effect.never;
      }),
    );
    const layer = Layer.mergeAll(
      Layer.succeed(HttpClient.HttpClient, hungHttp),
      HttpResilienceLive,
      TestClock.layer(),
    );
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const upstreamClient = yield* makeResilientHttpClient({
          upstream: 'test:hung-circuit',
          baseUrl: 'https://api.example.com',
          circuitBreaker: {},
        });
        const hungExits: Array<Exit.Exit<unknown, unknown>> = [];
        for (let call = 0; call < DEFAULT_CIRCUIT_CONSECUTIVE_FAILURES; call += 1) {
          const fiber = yield* Effect.forkChild(
            Effect.exit(upstreamClient.getJson('/item', ItemPayloadSchema)),
          );
          for (let step = 0; step < HUNG_CALL_STEPS; step += 1) {
            yield* TestClock.adjust('1 second');
          }

          hungExits.push(yield* Fiber.join(fiber));
        }

        const callsAfterTrip = transportCalls;
        const fastFailExit = yield* Effect.exit(upstreamClient.getJson('/item', ItemPayloadSchema));
        return { hungExits, callsAfterTrip, fastFailExit, callsAfterFastFail: transportCalls };
      }).pipe(Effect.provide(layer)),
    );
    for (const exit of outcome.hungExits) {
      expect(squashExitFailure(exit)).toMatchObject({ _tag: 'HttpRequestError', status: null });
      expect(exitMessage(exit)).toContain('operation timed out');
    }

    expect(outcome.callsAfterFastFail).toBe(outcome.callsAfterTrip);
    expect(exitOutcome(outcome.fastFailExit)).toBe('circuit_open');
  });

  it('ignores a 404 accepted as a value when counting circuit failures', async () => {
    let status = 503;
    const mockHttp = makeMockHttpClient(() => Effect.sync(() => new Response('{}', { status })));
    const breakerLayer = CircuitBreaker.layer({
      tripPolicy: CircuitBreaker.consecutive(2),
      openDuration: '10 seconds',
    }).pipe(Layer.provide(CircuitBreaker.layerStoreMemory()));
    const layer = Layer.mergeAll(
      Layer.succeed(HttpClient.HttpClient, mockHttp),
      breakerLayer,
      TestClock.layer(),
    );
    const fastFailExit = await Effect.runPromise(
      Effect.gen(function* () {
        const upstreamClient = yield* makeResilientHttpClient({
          upstream: 'test:not-found-circuit',
          baseUrl: 'https://api.example.com',
          retry: { maxRetries: 0 },
          circuitBreaker: {},
        });
        yield* Effect.exit(upstreamClient.getText('/item'));
        status = 404;
        yield* upstreamClient.getText('/item', { acceptNotFound: true });
        status = 503;
        yield* Effect.exit(upstreamClient.getText('/item'));
        status = 200;
        return yield* Effect.exit(upstreamClient.getText('/item'));
      }).pipe(Effect.provide(layer)),
    );
    expect(exitOutcome(fastFailExit)).toBe('circuit_open');
  });

  it('keeps a deadline spent waiting for a rate-limit token out of the circuit breaker', async () => {
    let transportCalls = 0;
    const mockHttp = makeMockHttpClient(() =>
      Effect.sync(() => {
        transportCalls += 1;
        return Response.json(
          { id: 1, title: 'ok' },
          {
            status: 200,
            headers: { 'content-type': 'application/json' },
          },
        );
      }),
    );
    const breakerLayer = CircuitBreaker.layer({
      tripPolicy: CircuitBreaker.consecutive(2),
      openDuration: '1 hour',
    }).pipe(Layer.provide(CircuitBreaker.layerStoreMemory()));
    const layer = Layer.mergeAll(
      Layer.succeed(HttpClient.HttpClient, mockHttp),
      RateLimiter.layer.pipe(
        Layer.provide(RateLimiter.layerStoreMemory),
        Layer.provideMerge(TestClock.layer()),
      ),
      breakerLayer,
    );
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const upstreamClient = yield* makeResilientHttpClient({
          upstream: 'test:limiter-deadline',
          baseUrl: 'https://api.example.com',
          operationTimeout: '5 seconds',
          circuitBreaker: {},
          rateLimit: {
            key: 'limiter-deadline',
            limit: 1,
            window: '1 minute',
            algorithm: 'token-bucket',
            onExceeded: 'delay',
          },
        });
        const fiber = yield* Effect.forkChild(
          Effect.gen(function* () {
            yield* upstreamClient.getJson('/1', ItemPayloadSchema);
            const starved = [
              yield* Effect.exit(upstreamClient.getJson('/2', ItemPayloadSchema)),
              yield* Effect.exit(upstreamClient.getJson('/3', ItemPayloadSchema)),
            ];
            // Starved calls hold no permit, so only the bucket refill is waited out.
            yield* Effect.sleep('2 minutes');
            const afterRefill = yield* Effect.exit(upstreamClient.getJson('/4', ItemPayloadSchema));
            return { starved, afterRefill };
          }),
        );
        yield* TestClock.adjust('20 minutes');
        return yield* Fiber.join(fiber);
      }).pipe(Effect.provide(layer)),
    );
    for (const exit of outcome.starved) {
      expect(exitMessage(exit)).toContain('operation timed out');
    }

    expect(Exit.isSuccess(outcome.afterRefill)).toBe(true);
    expect(transportCalls).toBe(2);
  });

  it('holds concurrent requests to maxConcurrency', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const activeRef = yield* Ref.make(0);
        const maxObservedRef = yield* Ref.make(0);
        const slowClient = makeMockHttpClient(() =>
          Effect.gen(function* () {
            const current = yield* Ref.updateAndGet(activeRef, (n) => n + 1);
            yield* Ref.update(maxObservedRef, (prev) => Math.max(prev, current));
            yield* Effect.sleep('100 millis');
            yield* Ref.update(activeRef, (n) => n - 1);
            return Response.json(
              { id: 1, title: 'done' },
              {
                status: 200,
                headers: { 'content-type': 'application/json' },
              },
            );
          }),
        );
        const upstreamClient = yield* makeResilientHttpClient({
          upstream: 'test:bulkhead',
          baseUrl: 'https://api.example.com',
          maxConcurrency: 2,
        }).pipe(Effect.provideService(HttpClient.HttpClient, slowClient));
        const fiber = yield* Effect.forkChild(
          Effect.all(
            [
              upstreamClient.getJson('/1', ItemPayloadSchema),
              upstreamClient.getJson('/2', ItemPayloadSchema),
              upstreamClient.getJson('/3', ItemPayloadSchema),
              upstreamClient.getJson('/4', ItemPayloadSchema),
              upstreamClient.getJson('/5', ItemPayloadSchema),
            ],
            { concurrency: 'unbounded' },
          ),
        );
        yield* TestClock.adjust('100 millis');
        yield* TestClock.adjust('100 millis');
        yield* TestClock.adjust('100 millis');
        const responses = yield* Fiber.join(fiber);
        const maxObserved = yield* Ref.get(maxObservedRef);
        return { count: responses.length, maxObserved };
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(result.count).toBe(5);
    expect(result.maxObserved).toBe(2);
  });

  it('refuses calls queued behind maxConcurrency once the breaker opens', async () => {
    let transportCalls = 0;
    const transport = makeMockHttpClient(() =>
      Effect.gen(function* () {
        transportCalls += 1;
        yield* Effect.sleep('10 millis');
        return new Response('busy', { status: 503 });
      }),
    );
    const exits = await Effect.runPromise(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({
          upstream: 'test:bulkhead-breaker',
          baseUrl: 'https://api.example.com',
          maxConcurrency: 1,
          retry: { maxRetries: 0 },
          circuitBreaker: {},
        });
        const fiber = yield* Effect.forkChild(
          Effect.all(
            Array.from({ length: 5 }, () => Effect.exit(api.getJson('/item', ItemPayloadSchema))),
            { concurrency: 'unbounded' },
          ),
        );
        yield* TestClock.adjust('1 second');
        return yield* Fiber.join(fiber);
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            makeHttpResilienceLayer({ consecutiveFailures: 1 }),
            Layer.succeed(HttpClient.HttpClient, transport),
            TestClock.layer(),
          ),
        ),
      ),
    );
    expect(transportCalls).toBe(1);
    expect(exits.slice(1).map(exitOutcome)).toEqual(Array(4).fill('circuit_open'));
  });

  it('records the success counter, one retry, and the latency histogram', async () => {
    const mockHttp = makeMockHttpClient((_req, _url, attempt) =>
      Effect.gen(function* () {
        yield* Effect.sleep('25 millis');
        if (attempt === 1) {
          return new Response('Server Error', { status: 502 });
        }

        return Response.json(
          { id: 10, title: 'Metrics Test' },
          {
            status: 200,
            headers: { 'content-type': 'application/json' },
          },
        );
      }),
    );
    const metricsSnapshot = await Effect.runPromise(
      Effect.gen(function* () {
        const httpFactory = yield* ResilientHttp;
        const upstreamClient = yield* httpFactory.client({
          upstream: 'test:metrics-src',
          baseUrl: 'https://api.example.com',
        });
        const fiber = yield* Effect.forkChild(
          upstreamClient.getJson('/metrics-item', ItemPayloadSchema),
        );
        yield* TestClock.adjust('25 millis');
        yield* TestClock.adjust('1 second');
        yield* TestClock.adjust('25 millis');
        const payload = yield* Fiber.join(fiber);
        const reqAttributes = {
          upstream: 'test:metrics-src',
          method: 'GET',
          outcome: 'success',
        };
        const reqCount = yield* Metric.value(
          Metric.withAttributes(httpRequestsTotal, reqAttributes),
        );
        const durationHist = yield* Metric.value(
          Metric.withAttributes(httpRequestDurationMs, reqAttributes),
        );
        const retryCount = yield* Metric.value(
          Metric.withAttributes(httpRetriesTotal, {
            upstream: 'test:metrics-src',
            reason: 'server_error',
          }),
        );
        return {
          payload,
          reqCount: reqCount.count,
          durationCount: durationHist.count,
          durationSum: durationHist.sum,
          retryCount: retryCount.count,
        };
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            ResilientHttp.layer.pipe(
              Layer.provide(Layer.succeed(HttpClient.HttpClient, mockHttp)),
              Layer.provide(HttpResilienceLive),
            ),
            TestClock.layer(),
          ),
        ),
      ),
    );
    expect(metricsSnapshot.payload).toEqual({ id: 10, title: 'Metrics Test' });
    expect(metricsSnapshot.reqCount).toBe(1);
    expect(metricsSnapshot.retryCount).toBe(1);
    expect(metricsSnapshot.durationCount).toBe(1);
    expect(metricsSnapshot.durationSum).toBeGreaterThanOrEqual(50);
  });

  it('passes a 404 to withWebResponse and execute when accepted', async () => {
    const http = makeMockHttpClient(() =>
      Effect.succeed(new Response('nothing here', { status: 404 })),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const upstreamClient = yield* makeClientOver(http, {
          upstream: 'test:web-404',
          baseUrl: 'https://api.example.com',
        });
        const webStatus = yield* upstreamClient.withWebResponse(
          '/missing',
          (response) =>
            Effect.map(
              Effect.promise(() => response.text()),
              (text) => [response.status, text],
            ),
          { acceptNotFound: true },
        );
        const executed = yield* upstreamClient.execute(HttpClientRequest.get('/missing'), {
          acceptNotFound: true,
        });
        return { webStatus, status: executed.status };
      }),
    );
    expect(result.webStatus).toEqual([404, 'nothing here']);
    expect(result.status).toBe(404);
  });
});

describe('makeHttpResilienceLayer', () => {
  it.each([
    ['consecutiveFailures 0', { consecutiveFailures: 0 }, 'consecutiveFailures'],
    ['openDuration NaN', { openDuration: Number.NaN }, 'openDuration'],
    ['openDuration 1e3 millis', { openDuration: '1e3 millis' }, 'openDuration'],
    ['openBackoffLimit 0', { openBackoffLimit: 0 }, 'openBackoffLimit'],
  ] as const)('returns a layer for %s whose build dies', async (_label, options, name) => {
    const layer = makeHttpResilienceLayer(options);
    const exit = await Effect.runPromiseExit(Effect.provide(Effect.void, layer));
    expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
    expect(String(Exit.isFailure(exit) ? Cause.squash(exit.cause) : null)).toContain(name);
  });

  it('builds one breaker when the same layer is provided twice in one build', async () => {
    const seen: Array<CircuitBreaker.CircuitBreaker> = [];
    const resilience = makeHttpResilienceLayer({ consecutiveFailures: 1 });
    const recordBreaker = Layer.effectDiscard(
      Effect.map(CircuitBreaker.CircuitBreaker, (breaker) => seen.push(breaker)),
    ).pipe(Layer.provide(resilience));
    await Effect.runPromise(
      Effect.map(CircuitBreaker.CircuitBreaker, (breaker) => seen.push(breaker)).pipe(
        Effect.provide(Layer.mergeAll(resilience, recordBreaker)),
      ),
    );
    expect(seen).toHaveLength(2);
    expect(new Set(seen).size).toBe(1);
  });
});

describe('postJson body encoding', () => {
  it.each([
    ['a custom toJSON result', { toJSON: () => undefined }],
    ['a function body', () => 1],
  ])('refuses %s when serialization produces no JSON', async (_name, body) => {
    let sent = 0;
    const transport = makeMockHttpClient(() =>
      Effect.sync(() => {
        sent += 1;
        return Response.json({ id: 1, title: 'x' });
      }),
    );
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, { upstream: 'test:post-empty-json' });
        return yield* http.postJson('https://api.example.com/items', body, ItemPayloadSchema);
      }),
    );
    expect(sent).toBe(0);
    expect(exitOutcome(exit)).toBe('blocked');
  });

  it('keeps the caller’s Content-Type when sending a JSON body', async () => {
    const transport = makeMockHttpClient((request) =>
      Effect.succeed(Response.json({ id: 1, title: request.headers['content-type'] })),
    );
    const data = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, { upstream: 'test:post-content-type' });
        return yield* http.postJson(
          'https://api.example.com/items',
          { title: 'x' },
          ItemPayloadSchema,
          { headers: { 'content-type': 'application/vnd.api+json' } },
        );
      }),
    );
    expect(data.title).toBe('application/vnd.api+json');
  });

  it('refuses an undefined body before sending instead of posting an empty JSON body', async () => {
    let sent = 0;
    const transport = makeMockHttpClient(() =>
      Effect.sync(() => {
        sent += 1;
        return Response.json({ id: 1, title: 'x' });
      }),
    );
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, { upstream: 'test:post-undefined' });
        return yield* http.postJson('https://api.example.com/items', undefined, ItemPayloadSchema);
      }),
    );
    expect(sent).toBe(0);
    expect(exitOutcome(exit)).toBe('blocked');
    expect(exitErrorMeta(exit)).toMatchObject({ retryable: false, circuit: 'ignore' });
  });

  it('tags a body that cannot be encoded as a non-retryable refusal and gives the reason', async () => {
    const transport = makeMockHttpClient(() => Effect.succeed(Response.json({})));
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const http = yield* makeClientOver(transport, { upstream: 'test:post-bigint' });
        return yield* http.postJson('https://api.example.com/items', { n: 1n }, ItemPayloadSchema);
      }),
    );
    expect(exitOutcome(exit)).toBe('blocked');
    expect(exitMessage(exit)).toMatch(/Failed to encode test:post-bigint request JSON body: .+/);
    expect(exitMessage(exit)).toContain('BigInt');
  });
});

describe('half-open circuit probes', () => {
  it('closes the circuit when the probe gets an answer that is not a failure, such as a 404', async () => {
    let healthy = false;
    const mockHttp = makeMockHttpClient(() =>
      Effect.sync(() =>
        healthy ? new Response('missing', { status: 404 }) : new Response('down', { status: 503 }),
      ),
    );
    const breakerLayer = CircuitBreaker.layer({
      tripPolicy: CircuitBreaker.consecutive(1),
      openDuration: '10 seconds',
    }).pipe(Layer.provide(CircuitBreaker.layerStoreMemory()));
    const layer = Layer.mergeAll(
      Layer.succeed(HttpClient.HttpClient, mockHttp),
      breakerLayer,
      TestClock.layer(),
    );
    const state = await Effect.runPromise(
      Effect.gen(function* () {
        const upstreamClient = yield* makeResilientHttpClient({
          upstream: 'test:half-open-404',
          baseUrl: 'https://api.example.com',
          retry: { maxRetries: 0 },
          circuitBreaker: { key: 'test:half-open-404' },
        });
        yield* Effect.exit(upstreamClient.getText('/item'));
        healthy = true;
        yield* TestClock.adjust('11 seconds');
        yield* Effect.exit(upstreamClient.getText('/item'));
        const breaker = yield* CircuitBreaker.CircuitBreaker;
        return yield* breaker.state('test:half-open-404');
      }).pipe(Effect.provide(layer)),
    );
    expect(state._tag).toBe('Closed');
  });
});
