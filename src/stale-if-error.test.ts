import type { Duration } from 'effect';

import { captureLogs } from '@yieldkit/logger/testing';
import { Effect, Exit, Layer, Metric } from 'effect';
import { HttpClient, UrlParams } from 'effect/http';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import type { HttpRequestOptions } from './request-options.ts';

import { ResilientHttp } from './client.ts';
import { EtagCache, makeEtagCache } from './conditional.ts';
import {
  exitOutcome,
  ItemPayloadSchema,
  makeMockHttpClient,
  squashExitFailure,
} from './http.fixture.ts';
import { httpRequestsTotal, httpStaleServedTotal } from './metrics.ts';

/**
 * Payload returned by the first, successful response of every scenario.
 */
const FIRST_PAYLOAD = { id: 7, title: 'Fresh Story' };

/**
 * Path every scenario requests: `redactUrlForLogs` masks the `token` query value and drops the
 * fragment with the credential in it.
 */
const STORY_PATH = '/story?token=abc#access_token=frag-secret';

/**
 * Builds a JSON 200 response carrying `FIRST_PAYLOAD`, with optional validators.
 */
const okResponse = (headers: Record<string, string> = {}): Response =>
  new Response(JSON.stringify(FIRST_PAYLOAD), {
    status: 200,
    headers: { 'content-type': 'application/json', ...headers },
  });

/**
 * Describes one stale-if-error scenario: the policy setting, the per-request options, the responses
 * served in order, and how far the clock moves before the second call.
 */
type Scenario = {
  readonly upstream: string;
  readonly policyStaleIfError?: Duration.Input | undefined;
  readonly options?: HttpRequestOptions | undefined;
  readonly responses: ReadonlyArray<() => Response>;
  readonly advance?: Duration.Input | undefined;
  readonly calls?: number | undefined;
  /**
   * Path to request; `STORY_PATH` when absent.
   */
  readonly path?: string | undefined;
  /**
   * `UrlParams.CurrentRedactedNames` for the scenario; Effect's default when absent.
   */
  readonly redactedNames?: ReadonlyArray<string | RegExp> | undefined;
};

/**
 * Calls `getCachedJson` `calls` times (twice by default) against the scripted responses, moving the
 * clock by `advance` before each call after the first. Returns the first, second and last exits,
 * the captured warning logs, and the stale-served counter for the upstream.
 */
const runScenario = (scenario: Scenario) => {
  const mockHttp = makeMockHttpClient((_request, _url, attempt) =>
    Effect.sync(() => {
      const respond = scenario.responses[attempt - 1] ?? scenario.responses.at(-1);
      return respond?.() ?? new Response(null, { status: 500 });
    }),
  );
  const cache = Effect.runSync(makeEtagCache());
  const program = Effect.gen(function* () {
    const httpFactory = yield* ResilientHttp;
    const upstreamClient = yield* httpFactory.client({
      upstream: scenario.upstream,
      baseUrl: 'https://api.example.com',
      retry: { maxRetries: 0 },
      staleIfError: scenario.policyStaleIfError,
    });
    /**
     * Moves the clock by `advance`, when set, and then calls `getCachedJson` once.
     */
    const advanceAndCall = Effect.gen(function* () {
      if (scenario.advance !== undefined) {
        yield* TestClock.adjust(scenario.advance);
      }

      return yield* Effect.exit(
        upstreamClient.getCachedJson(
          scenario.path ?? STORY_PATH,
          ItemPayloadSchema,
          scenario.options,
        ),
      );
    });
    const first = yield* Effect.exit(
      upstreamClient.getCachedJson(
        scenario.path ?? STORY_PATH,
        ItemPayloadSchema,
        scenario.options,
      ),
    );
    const second = yield* advanceAndCall;
    let last = second;
    for (let call = 2; call < (scenario.calls ?? 2); call += 1) {
      last = yield* advanceAndCall;
    }

    return { first, second, last };
  });
  const layer = Layer.mergeAll(
    ResilientHttp.layer.pipe(
      Layer.provide(Layer.succeed(HttpClient.HttpClient, mockHttp)),
      Layer.provide(Layer.succeed(EtagCache, cache)),
    ),
    TestClock.layer(),
  );
  return Effect.runPromise(
    Effect.gen(function* () {
      const { value, logs } = yield* captureLogs(
        scenario.redactedNames === undefined
          ? program
          : program.pipe(
              Effect.provideService(UrlParams.CurrentRedactedNames, scenario.redactedNames),
            ),
      );
      const served = yield* Metric.value(
        Metric.withAttributes(httpStaleServedTotal, { upstream: scenario.upstream }),
      );
      const failed = yield* Metric.value(
        Metric.withAttributes(httpRequestsTotal, {
          upstream: scenario.upstream,
          method: 'GET',
          outcome: 'server_error',
        }),
      );
      const cacheSize = yield* cache.size;
      return { ...value, logs, served: served.count, serverErrors: failed.count, cacheSize };
    }).pipe(Effect.provide(layer)),
  );
};

describe('getCachedJson stale-if-error', () => {
  it('serves the cached payload after a 503, logs a warning and counts it', async () => {
    const result = await runScenario({
      upstream: 'test:stale-503',
      policyStaleIfError: '1 hour',
      responses: [() => okResponse({ etag: '"v1"' }), () => new Response('down', { status: 503 })],
    });
    expect(Exit.isSuccess(result.first)).toBe(true);
    expect(Exit.isSuccess(result.second) ? result.second.value : null).toEqual(FIRST_PAYLOAD);
    expect(result.served).toBe(1);
    expect(result.serverErrors).toBe(1);
    const warning = result.logs.find((entry) => entry.message === 'http.cache.stale_served');
    expect(warning?.level).toBe('WARN');
    expect(warning?.annotations).toMatchObject({
      upstream: 'test:stale-503',
      outcome: 'server_error',
      errorTag: 'HttpRequestError',
      ageMs: 0,
    });
    expect(warning?.annotations.url).toBe('https://api.example.com/story?token=<redacted>');
    expect(JSON.stringify(warning?.annotations)).not.toContain('abc');
    expect(JSON.stringify(warning?.annotations)).not.toContain('frag-secret');
  });

  it('masks query values named in UrlParams.CurrentRedactedNames in the stale-served log', async () => {
    const result = await runScenario({
      upstream: 'test:stale-redacted-names',
      policyStaleIfError: '1 hour',
      path: '/story?ticket=TICKET-SECRET-1&page=2',
      redactedNames: ['ticket'],
      responses: [() => okResponse({ etag: '"v1"' }), () => new Response('down', { status: 503 })],
    });
    const warning = result.logs.find((entry) => entry.message === 'http.cache.stale_served');
    expect(warning?.annotations.url).toBeDefined();
    expect(JSON.stringify(warning?.annotations)).not.toContain('TICKET-SECRET-1');
    expect(warning?.annotations.url).toContain('page=2');
  });

  it('serves stale when the upstream returns invalid JSON', async () => {
    const result = await runScenario({
      upstream: 'test:stale-decode',
      policyStaleIfError: '1 hour',
      responses: [
        () => okResponse(),
        () =>
          new Response('<html>oops', {
            status: 200,
            headers: { 'content-type': 'application/json' },
          }),
      ],
    });
    expect(Exit.isSuccess(result.second) ? result.second.value : null).toEqual(FIRST_PAYLOAD);
    expect(result.logs.find((entry) => entry.message === 'http.cache.stale_served')).toMatchObject({
      annotations: { outcome: 'decode_error' },
    });
  });

  it('serves a payload that has no validators when enabled', async () => {
    const result = await runScenario({
      upstream: 'test:stale-no-validators',
      policyStaleIfError: '1 hour',
      responses: [() => okResponse(), () => new Response('down', { status: 502 })],
    });
    expect(Exit.isSuccess(result.second) ? result.second.value : null).toEqual(FIRST_PAYLOAD);
    expect(result.served).toBe(1);
  });

  it('does not serve stale for a 404', async () => {
    const result = await runScenario({
      upstream: 'test:stale-404',
      policyStaleIfError: '1 hour',
      responses: [() => okResponse(), () => new Response('gone', { status: 404 })],
    });
    expect(Exit.isFailure(result.second)).toBe(true);
    expect(squashExitFailure(result.second)).toMatchObject({ status: 404 });
    expect(exitOutcome(result.second)).toBe('client_error');
    expect(result.served).toBe(0);
  });

  it('fails with the original error when the entry is older than staleIfError', async () => {
    const result = await runScenario({
      upstream: 'test:stale-expired',
      policyStaleIfError: '1 minute',
      advance: '2 minutes',
      responses: [() => okResponse(), () => new Response('down', { status: 503 })],
    });
    expect(Exit.isFailure(result.second)).toBe(true);
    expect(squashExitFailure(result.second)).toMatchObject({
      _tag: 'HttpRequestError',
      status: 503,
    });
    expect(result.served).toBe(0);
    expect(result.logs.some((entry) => entry.message === 'http.cache.stale_served')).toBe(false);
  });

  it('measures the stale age from the last 304 revalidation, not the last 200', async () => {
    const result = await runScenario({
      upstream: 'test:stale-after-304',
      policyStaleIfError: '1 minute',
      advance: '50 seconds',
      calls: 3,
      responses: [
        () => okResponse({ etag: '"v1"' }),
        () => new Response(null, { status: 304, headers: { etag: '"v1"' } }),
        () => new Response('down', { status: 503 }),
      ],
    });
    expect(Exit.isSuccess(result.second) ? result.second.value : null).toEqual(FIRST_PAYLOAD);
    expect(Exit.isSuccess(result.last) ? result.last.value : null).toEqual(FIRST_PAYLOAD);
    expect(result.served).toBe(1);
    expect(result.logs.find((entry) => entry.message === 'http.cache.stale_served')).toMatchObject({
      annotations: { ageMs: 50_000 },
    });
  });

  it('serves an entry exactly as old as staleIfError', async () => {
    const result = await runScenario({
      upstream: 'test:stale-boundary',
      policyStaleIfError: '1 minute',
      advance: '1 minute',
      responses: [() => okResponse(), () => new Response('down', { status: 503 })],
    });
    expect(Exit.isSuccess(result.second)).toBe(true);
    expect(result.served).toBe(1);
  });

  it('is disabled by default', async () => {
    const result = await runScenario({
      upstream: 'test:stale-default',
      responses: [() => okResponse({ etag: '"v1"' }), () => new Response('down', { status: 503 })],
    });
    expect(Exit.isFailure(result.second)).toBe(true);
    expect(squashExitFailure(result.second)).toMatchObject({ status: 503 });
    expect(result.served).toBe(0);
  });

  it('does not cache payloads without validators when disabled', async () => {
    const result = await runScenario({
      upstream: 'test:stale-disabled-no-validators',
      responses: [() => okResponse(), () => new Response('down', { status: 503 })],
    });
    expect(Exit.isFailure(result.second)).toBe(true);
    expect(result.served).toBe(0);
    expect(result.cacheSize).toBe(0);
  });

  it('lets a per-request false disable the policy setting', async () => {
    const result = await runScenario({
      upstream: 'test:stale-override-off',
      policyStaleIfError: '1 hour',
      options: { staleIfError: false },
      responses: [() => okResponse({ etag: '"v1"' }), () => new Response('down', { status: 503 })],
    });
    expect(Exit.isFailure(result.second)).toBe(true);
    expect(result.served).toBe(0);
  });

  it('lets a per-request duration enable it without a policy setting', async () => {
    const result = await runScenario({
      upstream: 'test:stale-override-on',
      options: { staleIfError: '1 hour' },
      responses: [() => okResponse(), () => new Response('down', { status: 503 })],
    });
    expect(Exit.isSuccess(result.second) ? result.second.value : null).toEqual(FIRST_PAYLOAD);
    expect(result.served).toBe(1);
  });
});
