import { Deferred, Effect, Exit, Fiber, Layer, Metric, Option, Redacted, Schema } from 'effect';
import { HttpClient, HttpClientError, HttpClientRequest } from 'effect/http';
import { describe, expect, it } from 'vite-plus/test';

import type { ResilientHttpClient } from './client.ts';
import type { EtagCacheEntry, EtagCacheShape } from './conditional.ts';
import type { HttpError } from './errors.ts';
import type { HttpPolicy } from './policy.ts';
import type { HttpRequestOptions } from './request-options.ts';

import { makeResilientHttpClient, ResilientHttp } from './client.ts';
import { EtagCache, makeEtagCache } from './conditional.ts';
import {
  exitOutcome,
  ItemPayloadSchema,
  makeClientOver,
  makeMockHttpClient,
  makeTrackedStream,
  runPastTimeouts,
  squashExitFailure,
} from './http.fixture.ts';
import { httpActiveRequests, httpRequestsTotal } from './metrics.ts';

/**
 * Second schema over the same payload as `ItemPayloadSchema`, reading a different field.
 */
const SubtitlePayloadSchema = Schema.Struct({
  id: Schema.Int,
  subtitle: Schema.String,
});

/**
 * Builds a JSON 200 response, with optional extra headers.
 */
const jsonResponse = (body: unknown, headers: Record<string, string> = {}): Response =>
  new Response(JSON.stringify(body), {
    status: 200,
    headers: { 'content-type': 'application/json', ...headers },
  });

/**
 * One request as the mock transport saw it.
 */
type SeenRequest = {
  /**
   * `If-None-Match` header sent, or `null`.
   */
  readonly ifNoneMatch: string | null;

  /**
   * `If-Modified-Since` header sent, or `null`.
   */
  readonly ifModifiedSince: string | null;

  /**
   * `Authorization` header sent, or `null`.
   */
  readonly authorization: string | null;
};

/**
 * Runs `program` against a upstream client over `respond`, sharing one `EtagCache` that the program
 * can also read. `respond` gets the request headers and the 1-based attempt number.
 */
const runCached = <A>(params: {
  readonly respond: (seen: SeenRequest, attempt: number) => Effect.Effect<Response>;
  readonly staleIfError?: '1 hour' | undefined;
  readonly cacheMaxBytes?: number | undefined;
  readonly program: (context: {
    readonly client: ResilientHttpClient;
    readonly cache: EtagCacheShape;
    readonly seen: Array<SeenRequest>;
  }) => Effect.Effect<A, HttpError>;
}): Promise<A> => {
  const seen: Array<SeenRequest> = [];
  const mockHttp = makeMockHttpClient((request, _url, attempt) => {
    const entry: SeenRequest = {
      ifNoneMatch: request.headers['if-none-match'] ?? null,
      ifModifiedSince: request.headers['if-modified-since'] ?? null,
      authorization: request.headers['authorization'] ?? null,
    };
    seen.push(entry);
    return params.respond(entry, attempt);
  });
  return Effect.runPromise(
    Effect.gen(function* () {
      const cache = yield* makeEtagCache(10, params.cacheMaxBytes);
      const layer = ResilientHttp.layer.pipe(
        Layer.provide(Layer.succeed(HttpClient.HttpClient, mockHttp)),
        Layer.provide(Layer.succeed(EtagCache, cache)),
      );
      return yield* Effect.gen(function* () {
        const httpFactory = yield* ResilientHttp;
        const client = yield* httpFactory.client({
          upstream: 'test:client-cache',
          baseUrl: 'https://api.example.com',
          retry: { maxRetries: 0 },
          staleIfError: params.staleIfError,
        });
        return yield* params.program({ client, cache, seen });
      }).pipe(Effect.provide(layer));
    }),
  );
};

/**
 * Answers 304 to any request that carries a validator and a 200 with `body` and an ETag otherwise.
 */
const conditionalResponder =
  (body: unknown) =>
  (seen: SeenRequest): Effect.Effect<Response> =>
    Effect.sync(() =>
      seen.ifNoneMatch === null && seen.ifModifiedSince === null
        ? jsonResponse(body, { etag: '"v1"' })
        : new Response(null, { status: 304, headers: { etag: '"v1"' } }),
    );

describe('getCachedJson byte budget', () => {
  it('does not keep a body larger than the byte budget', async () => {
    const body = { id: 1, title: 'x'.repeat(100) };
    const result = await runCached({
      cacheMaxBytes: 100,
      respond: conditionalResponder(body),
      program: ({ client, cache, seen }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/story', ItemPayloadSchema);
          yield* client.getCachedJson('/story', ItemPayloadSchema);
          return { size: yield* cache.size, seen };
        }),
    });
    expect(result.size).toBe(0);
    expect(result.seen.map((request) => request.ifNoneMatch)).toEqual([null, null]);
  });

  it('keeps counting a revalidated entry against the budget after a 304', async () => {
    const body = { id: 1, title: 'x'.repeat(40) };
    const result = await runCached({
      cacheMaxBytes: 100,
      respond: conditionalResponder(body),
      program: ({ client, seen }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/a', ItemPayloadSchema);
          yield* client.getCachedJson('/a', ItemPayloadSchema); // 304, stored again
          yield* client.getCachedJson('/b', ItemPayloadSchema); // must evict /a to fit
          yield* client.getCachedJson('/a', ItemPayloadSchema);
          return seen.map((request) => request.ifNoneMatch);
        }),
    });
    expect(result).toEqual([null, '"v1"', null, null]);
  });
});

describe('getCachedJson cache partitioning', () => {
  it('does not decode a cached entry with a different schema', async () => {
    const body = { id: 1, title: 'Title', subtitle: 'Sub' };
    const result = await runCached({
      respond: conditionalResponder(body),
      program: ({ client, seen }) =>
        Effect.gen(function* () {
          const first = yield* Effect.exit(client.getCachedJson('/story', ItemPayloadSchema));
          const other = yield* Effect.exit(client.getCachedJson('/story', SubtitlePayloadSchema));
          const again = yield* Effect.exit(client.getCachedJson('/story', ItemPayloadSchema));
          return { first, other, again, seen };
        }),
    });
    expect(Exit.isSuccess(result.first) ? result.first.value : null).toEqual({
      id: 1,
      title: 'Title',
    });
    expect(Exit.isSuccess(result.other) ? result.other.value : null).toEqual({
      id: 1,
      subtitle: 'Sub',
    });
    expect(result.seen.map((entry) => entry.ifNoneMatch)).toEqual([null, null, '"v1"']);
    expect(Exit.isSuccess(result.again) ? result.again.value : null).toEqual({
      id: 1,
      title: 'Title',
    });
  });

  it('keeps entries of different credentials apart', async () => {
    const result = await runCached({
      respond: conditionalResponder({ id: 1, title: 'Title' }),
      program: ({ client, seen }) =>
        Effect.gen(function* () {
          const asAlice: HttpRequestOptions = { bearerToken: 'alice-token' };
          const asBob: HttpRequestOptions = { bearerToken: Redacted.make('bob-token') };
          yield* client.getCachedJson('/me', ItemPayloadSchema, asAlice);
          yield* client.getCachedJson('/me', ItemPayloadSchema, asBob);
          yield* client.getCachedJson('/me', ItemPayloadSchema, asAlice);
          yield* client.getCachedJson('/me', ItemPayloadSchema);
          return seen;
        }),
    });
    expect(result.map((entry) => entry.ifNoneMatch)).toEqual([null, null, '"v1"', null]);
  });

  it('does not serve another credential payload as a stale fallback', async () => {
    const result = await runCached({
      staleIfError: '1 hour',
      respond: (_seen, attempt) =>
        Effect.sync(() =>
          attempt === 1
            ? jsonResponse({ id: 1, title: 'Private' }, { etag: '"v1"' })
            : new Response('down', { status: 503 }),
        ),
      program: ({ client }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/me', ItemPayloadSchema, { bearerToken: 'alice-token' });
          return yield* Effect.exit(
            client.getCachedJson('/me', ItemPayloadSchema, { bearerToken: 'bob-token' }),
          );
        }),
    });
    expect(Exit.isFailure(result)).toBe(true);
    expect(squashExitFailure(result)).toMatchObject({ status: 503 });
  });
});

/**
 * Upstream that answers 304 to any validator and otherwise a 200 naming the caller by its
 * `Authorization` header, as a per-tenant resource behind one URL would.
 */
const tenantUpstream = makeMockHttpClient((request) =>
  Effect.sync(() =>
    request.headers['if-none-match'] === undefined
      ? jsonResponse(
          { id: 1, title: request.headers['authorization'] ?? 'anonymous' },
          { etag: '"v1"' },
        )
      : new Response(null, { status: 304, headers: { etag: '"v1"' } }),
  ),
);

/**
 * Builds a client for one upstream over `httpClient`, sharing `cache` with every other client.
 */
const tenantClient = (httpClient: HttpClient.HttpClient, cache: EtagCacheShape) =>
  makeResilientHttpClient({
    upstream: 'test:tenants',
    baseUrl: 'https://api.example.com',
    retry: { maxRetries: 0 },
  }).pipe(
    Effect.provideService(HttpClient.HttpClient, httpClient),
    Effect.provideService(EtagCache, cache),
  );

describe('getCachedJson with credentials added by the underlying HttpClient', () => {
  it('keeps apart tenants whose HttpClient adds their Authorization with mapRequest', async () => {
    const titles = await Effect.runPromise(
      Effect.gen(function* () {
        const cache = yield* makeEtagCache(10);
        const alice = yield* tenantClient(
          HttpClient.mapRequest(tenantUpstream, HttpClientRequest.bearerToken('alice')),
          cache,
        );
        const bob = yield* tenantClient(
          HttpClient.mapRequest(tenantUpstream, HttpClientRequest.bearerToken('bob')),
          cache,
        );
        const first = yield* alice.getCachedJson('/me', ItemPayloadSchema);
        const second = yield* bob.getCachedJson('/me', ItemPayloadSchema);
        return [first.title, second.title];
      }),
    );
    expect(titles).toEqual(['Bearer alice', 'Bearer bob']);
  });

  it('fails like an uncached request when the HttpClient request transform fails', async () => {
    const failing = HttpClient.mapRequestEffect(tenantUpstream, (request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({ request, description: 'no token' }),
        }),
      ),
    );
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const cache = yield* makeEtagCache(10);
        const client = yield* tenantClient(failing, cache);
        return yield* Effect.exit(client.getCachedJson('/me', ItemPayloadSchema));
      }),
    );
    expect(squashExitFailure(exit)).toMatchObject({ _tag: 'HttpRequestError', status: null });
  });
});

describe('getCachedJson caller validators', () => {
  it('does not return the cached payload for a 304 on validators the entry does not hold', async () => {
    const result = await runCached({
      respond: conditionalResponder({ id: 1, title: 'Title' }),
      program: ({ client, seen }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/story', ItemPayloadSchema);
          const exit = yield* Effect.exit(
            client.getCachedJson('/story', ItemPayloadSchema, { ifNoneMatch: '"newer"' }),
          );
          return { exit, seen };
        }),
    });
    expect(result.seen[1]?.ifNoneMatch).toBe('"newer"');
    expect(Exit.isFailure(result.exit)).toBe(true);
    expect(squashExitFailure(result.exit)).toMatchObject({ status: 304 });
  });

  it('still serves the cached payload when the caller sends the cached validator', async () => {
    const result = await runCached({
      respond: conditionalResponder({ id: 1, title: 'Title' }),
      program: ({ client }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/story', ItemPayloadSchema);
          return yield* client.getCachedJson('/story', ItemPayloadSchema, {
            ifNoneMatch: '"v1"',
          });
        }),
    });
    expect(result).toEqual({ id: 1, title: 'Title' });
  });
});

describe('getCachedJson cache bookkeeping', () => {
  it('does not let a late 304 overwrite a newer entry', async () => {
    const newer: EtagCacheEntry = {
      etag: '"v2"',
      lastModified: null,
      data: { id: 2, title: 'Newer' },
      updatedAtMs: 99,
    };
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        const started = yield* Deferred.make<void>();
        let attempt = 0;
        const respond = (): Effect.Effect<Response> =>
          Effect.gen(function* () {
            attempt += 1;
            if (attempt === 1) {
              return jsonResponse({ id: 1, title: 'Older' }, { etag: '"v1"' });
            }

            yield* Deferred.succeed(started, undefined);
            yield* Deferred.await(release);
            return new Response(null, { status: 304, headers: { etag: '"v1"' } });
          });
        const cache = yield* makeEtagCache(10);
        const keys: Array<string> = [];
        const recording: EtagCacheShape = {
          ...cache,
          get: (key) => {
            keys.push(key);
            return cache.get(key);
          },
        };
        const layer = ResilientHttp.layer.pipe(
          Layer.provide(Layer.succeed(HttpClient.HttpClient, makeMockHttpClient(respond))),
          Layer.provide(Layer.succeed(EtagCache, recording)),
        );
        return yield* Effect.gen(function* () {
          const httpFactory = yield* ResilientHttp;
          const client = yield* httpFactory.client({
            upstream: 'test:client-cache-cas',
            baseUrl: 'https://api.example.com',
          });
          yield* client.getCachedJson('/story', ItemPayloadSchema);
          const late = yield* Effect.forkChild(client.getCachedJson('/story', ItemPayloadSchema));
          yield* Deferred.await(started);
          // The entry the 304 will revalidate is replaced by a newer one before the 304 arrives.
          const key = keys.at(-1) ?? '';
          yield* cache.set(key, newer);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(late);
          const stored = yield* cache.get(key);
          return Option.isSome(stored) ? stored.value : null;
        }).pipe(Effect.provide(layer));
      }),
    );
    expect(result).toEqual(newer);
  });

  it('evicts the older entry when a newer success is not cached', async () => {
    const result = await runCached({
      respond: (_seen, attempt) =>
        Effect.sync(() => {
          if (attempt === 1) {
            return jsonResponse({ id: 1, title: 'Old' }, { etag: '"v1"' });
          }

          return attempt === 2
            ? jsonResponse({ id: 1, title: 'Newer' })
            : new Response('down', { status: 503 });
        }),
      program: ({ client, cache }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/story', ItemPayloadSchema);
          const sizeAfterFirst = yield* cache.size;
          const second = yield* client.getCachedJson('/story', ItemPayloadSchema);
          const sizeAfterSecond = yield* cache.size;
          const third = yield* Effect.exit(
            client.getCachedJson('/story', ItemPayloadSchema, { staleIfError: '1 hour' }),
          );
          return { sizeAfterFirst, second, sizeAfterSecond, third };
        }),
    });
    expect(result.sizeAfterFirst).toBe(1);
    expect(result.second).toEqual({ id: 1, title: 'Newer' });
    expect(result.sizeAfterSecond).toBe(0);
    expect(Exit.isFailure(result.third)).toBe(true);
    expect(squashExitFailure(result.third)).toMatchObject({ status: 503 });
  });
});

describe('withWebResponse active requests gauge', () => {
  it('stays raised while use runs and drops after it finishes', async () => {
    const upstream = 'test:client-cache-gauge';
    const mockHttp = makeMockHttpClient(() => Effect.succeed(jsonResponse({ id: 1, title: 'x' })));
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const layer = ResilientHttp.layer.pipe(
          Layer.provide(Layer.succeed(HttpClient.HttpClient, mockHttp)),
        );
        const gauge = Metric.withAttributes(httpActiveRequests, { upstream });
        return yield* Effect.gen(function* () {
          const httpFactory = yield* ResilientHttp;
          const client = yield* httpFactory.client({
            upstream,
            baseUrl: 'https://api.example.com',
          });
          let duringUse = -1;
          yield* client.withWebResponse('/page', () =>
            Effect.gen(function* () {
              duringUse = (yield* Metric.value(gauge)).value;
            }),
          );
          const after = (yield* Metric.value(gauge)).value;
          return { duringUse, after };
        }).pipe(Effect.provide(layer));
      }),
    );
    expect(result).toEqual({ duringUse: 1, after: 0 });
  });
});

/**
 * Runs `withWebResponse` with `use` and reads the request counters of `upstream` per outcome.
 */
const runMetric = (
  upstream: string,
  use: () => Effect.Effect<void>,
): Promise<{
  readonly success: number;
  readonly networkError: number;
  readonly duringUse: number;
}> => {
  const mockHttp = makeMockHttpClient(() => Effect.succeed(jsonResponse({ id: 1, title: 'x' })));
  const counter = (outcome: string) =>
    Metric.withAttributes(httpRequestsTotal, { upstream, method: 'GET', outcome });
  return Effect.runPromise(
    Effect.gen(function* () {
      const httpFactory = yield* ResilientHttp;
      const client = yield* httpFactory.client({
        upstream,
        baseUrl: 'https://api.example.com',
      });
      let duringUse = -1;
      yield* Effect.exit(
        client.withWebResponse('/page', () =>
          Effect.gen(function* () {
            duringUse = (yield* Metric.value(counter('success'))).count;
            yield* use();
          }),
        ),
      );
      return {
        success: (yield* Metric.value(counter('success'))).count,
        networkError: (yield* Metric.value(counter('network_error'))).count,
        duringUse,
      };
    }).pipe(
      Effect.provide(
        ResilientHttp.layer.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, mockHttp))),
      ),
    ),
  );
};

/**
 * Builds a client for `upstream` over a transport that counts how often it is reached.
 */
const withClient = <A>(
  policy: HttpPolicy,
  body: (client: ResilientHttpClient, sent: { count: number }) => Effect.Effect<A>,
): Promise<A> => {
  const sent = { count: 0 };
  const mockHttp = makeMockHttpClient(() =>
    Effect.sync(() => {
      sent.count += 1;
      return jsonResponse({ id: 1, title: 'x' });
    }),
  );
  return Effect.runPromise(
    Effect.gen(function* () {
      const httpFactory = yield* ResilientHttp;
      const client = yield* httpFactory.client(policy);
      return yield* body(client, sent);
    }).pipe(
      Effect.provide(
        ResilientHttp.layer.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, mockHttp))),
      ),
    ),
  );
};

describe('withWebResponse request metric', () => {
  it('counts a request once, after use has finished', async () => {
    const counts = await runMetric('test:client-metric-ok', () => Effect.void);
    expect(counts).toEqual({ success: 1, networkError: 0, duringUse: 0 });
  });

  it('records the outcome of a failing use instead of a success', async () => {
    const counts = await runMetric('test:client-metric-fail', () => Effect.die('boom'));
    expect(counts).toEqual({ success: 0, networkError: 1, duringUse: 0 });
  });
});

describe('invalid configuration through a upstream client', () => {
  it('fails a request with a typed blocked error when the policy is invalid, without sending', async () => {
    const result = await withClient(
      { upstream: 'test:bad-policy', baseUrl: 'https://api.example.com', maxConcurrency: 0 },
      (client, sent) =>
        Effect.gen(function* () {
          const exit = yield* Effect.exit(client.getJson('/item', ItemPayloadSchema));
          return { outcome: exitOutcome(exit), sent: sent.count };
        }),
    );
    expect(result).toEqual({ outcome: 'blocked', sent: 0 });
  });

  it('fails a request with a typed blocked error when its options are invalid, without sending', async () => {
    const result = await withClient(
      { upstream: 'test:bad-options', baseUrl: 'https://api.example.com' },
      (client, sent) =>
        Effect.gen(function* () {
          const exits = [
            yield* Effect.exit(client.getText('/a', { attemptTimeout: '1e3 millis' })),
            yield* Effect.exit(
              client.getJson('/b', ItemPayloadSchema, { maxBodyBytes: Number.NaN }),
            ),
            yield* Effect.exit(
              client.withWebResponse('/c', () => Effect.void, { operationTimeout: '-1 seconds' }),
            ),
            yield* Effect.exit(
              // @ts-expect-error -- untyped configuration (JSON, parsed environment) can hold null
              client.getText('/d', { attemptTimeout: null }),
            ),
          ];
          return { outcomes: exits.map(exitOutcome), sent: sent.count };
        }),
    );
    expect(result).toEqual({
      outcomes: ['blocked', 'blocked', 'blocked', 'blocked'],
      sent: 0,
    });
  });
});

/**
 * Returns the HTTP status of the `HttpRequestError` a failed `exit` carries, or null.
 */
const failedStatus = (exit: Exit.Exit<unknown, HttpError>): number | null => {
  const failure = squashExitFailure(exit);
  return failure?._tag === 'HttpRequestError' ? failure.status : null;
};

describe('getCachedJson cache safety', () => {
  it.each([true, false])(
    'fetches the new target after a redirect changes the resource (initial redirect: %s)',
    async (initialRedirect) => {
      let target = '/a';
      const transport = makeMockHttpClient((request, url) =>
        Effect.sync(() => {
          if (url.pathname === '/doc' && (initialRedirect || target === '/b')) {
            return new Response(null, { status: 302, headers: { location: target } });
          }

          if (request.headers['if-none-match'] === '"v1"') {
            return new Response(null, { status: 304, headers: { etag: '"v1"' } });
          }

          return jsonResponse(url.pathname, { etag: '"v1"' });
        }),
      );
      const values = await Effect.runPromise(
        Effect.gen(function* () {
          const client = yield* makeClientOver(transport, {
            upstream: 'test:redirect-cache',
            baseUrl: 'https://api.example.com',
            retry: { maxRetries: 0 },
          });
          const first = yield* client.getCachedJson('/doc', Schema.String);
          target = '/b';
          const second = yield* client.getCachedJson('/doc', Schema.String);
          return [first, second];
        }),
      );
      expect(values).toEqual([initialRedirect ? '/a' : '/doc', '/b']);
    },
  );

  it('evicts a cached item when its 404 body stalls past the operation deadline', async () => {
    const transport = makeMockHttpClient((_request, _url, attempt) =>
      Effect.sync(() => {
        if (attempt === 1) {
          return jsonResponse(7, { etag: '"v1"' });
        }

        if (attempt === 2) {
          const { stream } = makeTrackedStream({ totalBytes: 16, chunkBytes: 8, stall: true });
          return new Response(stream, { status: 404 });
        }

        return new Response('down', { status: 503 });
      }),
    );
    const result = await runPastTimeouts(
      Effect.gen(function* () {
        const client = yield* makeClientOver(transport, {
          upstream: 'test:cache-stalled-404',
          staleIfError: '1 hour',
          operationTimeout: '100 millis',
          attemptTimeout: '5 seconds',
          retry: { maxRetries: 0 },
        });
        const url = 'https://api.example.com/doc';
        yield* client.getCachedJson(url, Schema.Int);
        const gone = yield* Effect.exit(client.getCachedJson(url, Schema.Int));
        const outage = yield* Effect.exit(client.getCachedJson(url, Schema.Int));
        return [failedStatus(gone), failedStatus(outage)];
      }),
      '1 second',
    );
    expect(result).toEqual(Exit.succeed([404, 503]));
  });

  it('evicts a cached payload before decoding a no-store response', async () => {
    const result = await runCached({
      staleIfError: '1 hour',
      respond: (_seen, attempt) =>
        Effect.succeed(
          [
            jsonResponse({ id: 1, title: 'old' }),
            jsonResponse('invalid payload', { 'cache-control': 'no-store' }),
          ][attempt - 1] ?? new Response('down', { status: 503 }),
        ),
      program: ({ client }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/doc', ItemPayloadSchema);
          const invalid = yield* Effect.exit(client.getCachedJson('/doc', ItemPayloadSchema));
          const outage = yield* Effect.exit(client.getCachedJson('/doc', ItemPayloadSchema));
          return { invalid: exitOutcome(invalid), outage: failedStatus(outage) };
        }),
    });
    expect(result).toEqual({ invalid: 'decode_error', outage: 503 });
  });

  it.each([503, 429])('serves the cached payload after a no-store %i', async (status) => {
    const body = { id: 1, title: 'old' };
    const result = await runCached({
      staleIfError: '1 hour',
      respond: (_seen, attempt) =>
        Effect.succeed(
          attempt === 1
            ? jsonResponse(body, { etag: '"v1"' })
            : new Response('unavailable', { status, headers: { 'cache-control': 'no-store' } }),
        ),
      program: ({ client }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/doc', ItemPayloadSchema);
          return yield* client.getCachedJson('/doc', ItemPayloadSchema);
        }),
    });
    expect(result).toEqual(body);
  });

  it('serves the cached payload when a redirect target returns 503', async () => {
    const body = { id: 1, title: 'old' };
    const result = await runCached({
      staleIfError: '1 hour',
      respond: (_seen, attempt) =>
        Effect.succeed(
          [
            jsonResponse(body, { etag: '"v1"' }),
            new Response(null, { status: 302, headers: { location: '/target' } }),
          ][attempt - 1] ?? new Response('down', { status: 503 }),
        ),
      program: ({ client }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/doc', ItemPayloadSchema);
          return yield* client.getCachedJson('/doc', ItemPayloadSchema);
        }),
    });
    expect(result).toEqual(body);
  });

  it('never serves one tenant the stale payload of another tenant under a custom key header', async () => {
    const result = await runCached({
      staleIfError: '1 hour',
      respond: (_seen, attempt) =>
        Effect.succeed(
          attempt === 1
            ? jsonResponse({ id: 1, title: 'tenant-a private data' })
            : new Response('down', { status: 503 }),
        ),
      program: ({ client }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/me', ItemPayloadSchema, {
            headers: { 'ocp-apim-subscription-key': 'tenant-a' },
          });
          return yield* Effect.exit(
            client.getCachedJson('/me', ItemPayloadSchema, {
              headers: { 'ocp-apim-subscription-key': 'tenant-b' },
            }),
          );
        }),
    });
    expect(Exit.isFailure(result)).toBe(true);
  });

  it('does not store a no-store response and drops the entry it replaces', async () => {
    const result = await runCached({
      staleIfError: '1 hour',
      respond: (_seen, attempt) =>
        Effect.succeed(
          [
            jsonResponse({ id: 1, title: 'old' }),
            jsonResponse({ id: 1, title: 'secret' }, { 'cache-control': 'private, no-store' }),
          ][attempt - 1] ?? new Response('down', { status: 503 }),
        ),
      program: ({ client, cache }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/doc', ItemPayloadSchema);
          const fresh = yield* client.getCachedJson('/doc', ItemPayloadSchema);
          const size = yield* cache.size;
          const later = yield* Effect.exit(client.getCachedJson('/doc', ItemPayloadSchema));
          return { fresh, size, later: Exit.isFailure(later) };
        }),
    });
    expect(result).toEqual({ fresh: { id: 1, title: 'secret' }, size: 0, later: true });
  });

  it('evicts the entry a no-store 304 revalidates, so a later outage cannot serve it', async () => {
    const result = await runCached({
      staleIfError: '1 hour',
      respond: (_seen, attempt) =>
        Effect.succeed(
          [
            jsonResponse({ id: 1, title: 'private' }, { etag: '"v1"' }),
            new Response(null, { status: 304, headers: { 'cache-control': 'no-store' } }),
          ][attempt - 1] ?? new Response('down', { status: 503 }),
        ),
      program: ({ client, cache }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/doc', ItemPayloadSchema);
          const revalidated = yield* client.getCachedJson('/doc', ItemPayloadSchema);
          const size = yield* cache.size;
          const outage = yield* Effect.exit(client.getCachedJson('/doc', ItemPayloadSchema));
          return { revalidated, size, outage: Exit.isFailure(outage) };
        }),
    });
    expect(result).toEqual({ revalidated: { id: 1, title: 'private' }, size: 0, outage: true });
  });

  it.each([
    [
      'a no-store response',
      () => jsonResponse({ id: 2, title: 'new' }, { 'cache-control': 'no-store' }),
    ],
    ['a 404', () => new Response('gone', { status: 404 })],
  ])('fails an overlapping outage after %s evicts the stale entry', async (_name, evict) => {
    const releaseSlow = await Effect.runPromise(Deferred.make<void>());
    const slowStarted = await Effect.runPromise(Deferred.make<void>());
    const result = await runCached({
      staleIfError: '1 hour',
      respond: (_seen, attempt) => {
        if (attempt === 1) {
          return Effect.succeed(jsonResponse({ id: 1, title: 'old' }, { etag: '"v1"' }));
        }

        if (attempt === 2) {
          return Deferred.succeed(slowStarted, undefined).pipe(
            Effect.andThen(Deferred.await(releaseSlow)),
            Effect.as(new Response('down', { status: 503 })),
          );
        }

        return Effect.sync(evict);
      },
      program: ({ client }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/doc', ItemPayloadSchema);
          const slow = yield* Effect.forkChild(
            Effect.exit(client.getCachedJson('/doc', ItemPayloadSchema)),
          );
          yield* Deferred.await(slowStarted);
          yield* Effect.exit(client.getCachedJson('/doc', ItemPayloadSchema));
          yield* Deferred.succeed(releaseSlow, undefined);
          const outage = yield* Fiber.join(slow);
          return failedStatus(outage);
        }),
    });
    expect(result).toBe(503);
  });

  it.each([
    [304, { id: 1, title: 'old' }],
    [200, { id: 2, title: 'new' }],
  ] as const)(
    'serves the cached value after an overlapping %i refreshes it',
    async (status, expected) => {
      const releaseSlow = await Effect.runPromise(Deferred.make<void>());
      const slowStarted = await Effect.runPromise(Deferred.make<void>());
      const outage = await runCached({
        staleIfError: '1 hour',
        respond: (_seen, attempt) => {
          if (attempt === 1) {
            return Effect.succeed(jsonResponse({ id: 1, title: 'old' }, { etag: '"v1"' }));
          }

          if (attempt === 2) {
            return Deferred.succeed(slowStarted, undefined).pipe(
              Effect.andThen(Deferred.await(releaseSlow)),
              Effect.as(new Response('down', { status: 503 })),
            );
          }

          return Effect.succeed(
            status === 304
              ? new Response(null, { status })
              : jsonResponse(expected, { etag: '"v2"' }),
          );
        },
        program: ({ client }) =>
          Effect.gen(function* () {
            yield* client.getCachedJson('/doc', ItemPayloadSchema);
            const slow = yield* Effect.forkChild(
              Effect.exit(client.getCachedJson('/doc', ItemPayloadSchema)),
            );
            yield* Deferred.await(slowStarted);
            yield* client.getCachedJson('/doc', ItemPayloadSchema);
            yield* Deferred.succeed(releaseSlow, undefined);
            return yield* Fiber.join(slow);
          }),
      });
      expect(outage).toEqual(Exit.succeed(expected));
    },
  );

  it('evicts the cached payload when the upstream answers 403, so a later outage cannot serve it', async () => {
    const result = await runCached({
      staleIfError: '1 hour',
      respond: (_seen, attempt) =>
        Effect.succeed(
          attempt === 1
            ? jsonResponse({ id: 1, title: 'private' })
            : new Response('denied', { status: attempt === 2 ? 403 : 503 }),
        ),
      program: ({ client }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/doc', ItemPayloadSchema);
          const denied = yield* Effect.exit(client.getCachedJson('/doc', ItemPayloadSchema));
          const outage = yield* Effect.exit(client.getCachedJson('/doc', ItemPayloadSchema));
          return { denied: failedStatus(denied), outage: Exit.isFailure(outage) };
        }),
    });
    expect(result).toEqual({ denied: 403, outage: true });
  });

  it('ignores a cached entry whose validator differs from one passed as a raw header', async () => {
    const result = await runCached({
      respond: (seen, attempt) =>
        Effect.sync(() => {
          if (attempt === 1) {
            return jsonResponse({ id: 1, title: 'old' }, { 'last-modified': 'Mon' });
          }

          return seen.ifNoneMatch === '"other"'
            ? new Response(null, { status: 304, headers: { etag: '"other"' } })
            : jsonResponse({ id: 2, title: 'new' });
        }),
      program: ({ client }) =>
        Effect.gen(function* () {
          yield* client.getCachedJson('/doc', ItemPayloadSchema);
          return yield* Effect.exit(
            client.getCachedJson('/doc', ItemPayloadSchema, {
              headers: { 'if-none-match': '"other"' },
            }),
          );
        }),
    });
    expect(failedStatus(result)).toBe(304);
  });

  it('keeps the newer entry when an older overlapping response finishes last', async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const releaseSlow = yield* Deferred.make<void>();
        const slowStarted = yield* Deferred.make<void>();
        const mockHttp = makeMockHttpClient((_request, _url, attempt) =>
          attempt === 1
            ? Effect.andThen(
                Deferred.succeed(slowStarted, undefined),
                Effect.as(Deferred.await(releaseSlow), jsonResponse({ id: 1, title: 'older' })),
              )
            : Effect.succeed(
                attempt === 2
                  ? jsonResponse({ id: 2, title: 'newer' })
                  : new Response('down', { status: 503 }),
              ),
        );
        const cache = yield* makeEtagCache(10);
        const layer = ResilientHttp.layer.pipe(
          Layer.provide(Layer.succeed(HttpClient.HttpClient, mockHttp)),
          Layer.provide(Layer.succeed(EtagCache, cache)),
        );
        return yield* Effect.gen(function* () {
          const httpFactory = yield* ResilientHttp;
          const client = yield* httpFactory.client({
            upstream: 'test:client-cache-race',
            baseUrl: 'https://api.example.com',
            retry: { maxRetries: 0 },
            staleIfError: '1 hour',
          });
          const slow = yield* Effect.forkChild(client.getCachedJson('/doc', ItemPayloadSchema));
          yield* Deferred.await(slowStarted);
          yield* client.getCachedJson('/doc', ItemPayloadSchema);
          yield* Deferred.succeed(releaseSlow, undefined);
          yield* Fiber.join(slow);
          return yield* client.getCachedJson('/doc', ItemPayloadSchema);
        }).pipe(Effect.provide(layer));
      }),
    );
    expect(result).toEqual({ id: 2, title: 'newer' });
  });
});
