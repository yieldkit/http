import { Effect, Fiber, Option, Redacted } from 'effect';
import { HttpClientRequest } from 'effect/http';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import type { HttpPolicy } from './policy.ts';
import type { HttpRequestOptions } from './request-options.ts';

import {
  exitMessage,
  exitOutcome,
  ItemPayloadSchema,
  makeClientOver,
  makeMockHttpClient,
} from './http.fixture.ts';
import { DEFAULT_MAX_BODY_BYTES, resolveHttpPolicy } from './policy.ts';
import {
  prepareRequestParams,
  requestOptionsError,
  validateRequestConfig,
} from './request-options.ts';

const HTTP_CLIENT = makeMockHttpClient(() => Effect.succeed(new Response('unused')));

/**
 * Prepares the parameters of a GET to `path` under `policy` with `options`.
 */
const prepare = (
  policy: Partial<HttpPolicy>,
  path: string,
  options?: HttpRequestOptions,
  defaultAcceptJson = false,
) =>
  prepareRequestParams(
    {
      httpClient: HTTP_CLIENT,
      policy: resolveHttpPolicy({ upstream: 'test:options', ...policy }),
    },
    HttpClientRequest.get(path),
    options,
    defaultAcceptJson,
  );

/**
 * Returns the absolute URL of `request` including its query string.
 */
const urlOf = (request: HttpClientRequest.HttpClientRequest): string =>
  Option.getOrThrow(HttpClientRequest.toUrl(request)).href;

describe('prepareRequestParams', () => {
  it('resolves a relative path against the base URL and keeps an absolute URL', () => {
    const policy = { baseUrl: 'https://api.example.com/v1/' };
    expect(urlOf(prepare(policy, '/items').request)).toBe('https://api.example.com/v1/items');
    expect(urlOf(prepare(policy, 'https://other.example/x').request)).toBe(
      'https://other.example/x',
    );
  });

  it('sends the policy user agent, and its default headers win over it', () => {
    expect(prepare({}, 'https://a.example/').request.headers['user-agent']).toBeUndefined();
    const custom = prepare({ userAgent: 'custom/1' }, 'https://a.example/');
    expect(custom.request.headers['user-agent']).toBe('custom/1');
    const overridden = prepare(
      { userAgent: 'custom/1', defaultHeaders: { 'User-Agent': 'from-defaults' } },
      'https://a.example/',
    );
    expect(overridden.request.headers['user-agent']).toBe('from-defaults');
  });

  it('merges per-request headers over the policy default headers', () => {
    const { request } = prepare(
      { defaultHeaders: { 'x-team': 'core', 'x-env': 'prod' } },
      'https://a.example/',
      { headers: { 'x-env': 'staging', 'x-extra': '1' } },
    );
    expect(request.headers).toMatchObject({ 'x-team': 'core', 'x-env': 'staging', 'x-extra': '1' });
  });

  it('adds Accept: application/json only when asked and no Accept is set', () => {
    expect(prepare({}, 'https://a.example/', undefined, true).request.headers.accept).toBe(
      'application/json',
    );
    expect(prepare({}, 'https://a.example/').request.headers.accept).toBeUndefined();
    const withAccept = prepare(
      { defaultHeaders: {} },
      'https://a.example/',
      { headers: { accept: 'text/csv' } },
      true,
    );
    expect(withAccept.request.headers.accept).toBe('text/csv');
  });

  it('keeps an Accept the policy default headers set, when the request had none', () => {
    const { request } = prepare(
      { defaultHeaders: { accept: 'application/vnd.api+json' } },
      'https://a.example/',
      undefined,
      true,
    );
    expect(request.headers.accept).toBe('application/vnd.api+json');
  });

  it('keeps the headers of a request the caller built over the policy defaults', () => {
    const { request } = prepareRequestParams(
      {
        httpClient: HTTP_CLIENT,
        policy: resolveHttpPolicy({
          upstream: 'test:options',
          userAgent: 'policy-agent',
          defaultHeaders: { authorization: 'Bearer app-token', accept: 'application/vnd.default' },
        }),
      },
      HttpClientRequest.get('https://a.example/x', {
        headers: { authorization: 'Bearer user-token', accept: 'text/csv', 'user-agent': 'mine' },
      }),
    );
    expect(request.headers).toMatchObject({
      authorization: 'Bearer user-token',
      accept: 'text/csv',
      'user-agent': 'mine',
    });
  });

  it('sends the default headers only to the base URL origin', () => {
    const policy = {
      baseUrl: 'https://api.example.com/v1',
      userAgent: 'agent/1',
      defaultHeaders: { 'x-api-key': 'SECRET-KEY' },
    };
    expect(prepare(policy, '/items').request.headers['x-api-key']).toBe('SECRET-KEY');
    expect(prepare(policy, 'https://api.example.com/v1/next').request.headers['x-api-key']).toBe(
      'SECRET-KEY',
    );
    const elsewhere = prepare(policy, 'https://attacker.example/next').request.headers;
    expect(elsewhere['x-api-key']).toBeUndefined();
    expect(elsewhere['user-agent']).toBe('agent/1');
    const optedIn = prepare(policy, 'https://cdn.example.net/file', {
      headers: { 'x-api-key': 'SECRET-KEY' },
    });
    expect(optedIn.request.headers['x-api-key']).toBe('SECRET-KEY');
  });

  it('sends no default headers to an absolute URL when the base URL does not parse', () => {
    const policy = { baseUrl: 'api.example.com/v1', defaultHeaders: { 'x-api-key': 'K-SECRET' } };
    const headers = prepare(policy, 'https://evil.example.org/page2').request.headers;
    expect(headers['x-api-key']).toBeUndefined();
  });

  it('appends query parameters and skips the undefined ones', () => {
    const { request } = prepare({}, 'https://a.example/items?keep=1', {
      urlParams: { page: '2', skipped: undefined, q: 'a b' },
    });
    const url = new URL(urlOf(request));
    expect(url.searchParams.get('keep')).toBe('1');
    expect(url.searchParams.get('page')).toBe('2');
    expect(url.searchParams.get('q')).toBe('a b');
    expect(url.searchParams.has('skipped')).toBe(false);
  });

  it('appends a query value to an existing value on a URL object', () => {
    const request = HttpClientRequest.get(new URL('https://a.example/items?tag=one'));
    const params = prepareRequestParams(
      { httpClient: HTTP_CLIENT, policy: resolveHttpPolicy({ upstream: 'test:url-object' }) },
      request,
      { urlParams: { tag: 'two' } },
    );
    expect(new URL(urlOf(params.request)).searchParams.getAll('tag')).toEqual(['one', 'two']);
  });

  it('sets a Bearer token from a string or a Redacted value', () => {
    const fromString = prepare({}, 'https://a.example/', { bearerToken: 'abc' });
    const fromRedacted = prepare({}, 'https://a.example/', { bearerToken: Redacted.make('xyz') });
    expect(fromString.request.headers.authorization).toBe('Bearer abc');
    expect(fromRedacted.request.headers.authorization).toBe('Bearer xyz');
    expect(prepare({}, 'https://a.example/').request.headers.authorization).toBeUndefined();
  });

  it('sets the conditional validators and skips null and undefined ones', () => {
    const both = prepare({}, 'https://a.example/', {
      ifNoneMatch: '"v1"',
      ifModifiedSince: 'Wed, 21 Oct 2015 07:28:00 GMT',
    });
    expect(both.request.headers).toMatchObject({
      'if-none-match': '"v1"',
      'if-modified-since': 'Wed, 21 Oct 2015 07:28:00 GMT',
    });
    const none = prepare({}, 'https://a.example/', { ifNoneMatch: null, ifModifiedSince: null });
    expect(none.request.headers).not.toHaveProperty('if-none-match');
    expect(none.request.headers).not.toHaveProperty('if-modified-since');
    const missing = prepare({}, 'https://a.example/', {});
    expect(missing.request.headers).not.toHaveProperty('if-none-match');
  });

  it('takes the timeouts and the body cap from the policy', () => {
    const params = prepare(
      { attemptTimeout: '3 seconds', operationTimeout: '20 seconds', maxBodyBytes: 4096 },
      'https://a.example/',
    );
    expect(params).toMatchObject({
      attemptTimeout: '3 seconds',
      operationTimeout: '20 seconds',
      maxBodyBytes: 4096,
    });
  });

  it('lets per-request options win over the policy limits', () => {
    const params = prepare(
      { attemptTimeout: '3 seconds', operationTimeout: '20 seconds', maxBodyBytes: 4096 },
      'https://a.example/',
      { attemptTimeout: '1 second', operationTimeout: '5 seconds', maxBodyBytes: 64 },
    );
    expect(params).toMatchObject({
      attemptTimeout: '1 second',
      operationTimeout: '5 seconds',
      maxBodyBytes: 64,
    });
  });

  it('leaves idempotent and acceptNotFound to the per-request options', () => {
    const plain = prepare({}, 'https://a.example/');
    expect(plain.idempotent).toBeUndefined();
    expect(plain.acceptNotFound).toBeUndefined();
    const flagged = prepare({}, 'https://a.example/', { idempotent: true, acceptNotFound: true });
    expect(flagged).toMatchObject({ idempotent: true, acceptNotFound: true });
  });

  it('passes the upstream, transport and policy flags on to the executor', () => {
    const params = prepare(
      {
        treatQuotaExhausted403AsRateLimit: true,
        treat403AsRateLimit: true,
        detectHtmlEdgeBlocks: true,
        publicAddressesOnly: true,
        maxRedirects: 2,
        retry: { maxRetries: 7 },
        rateLimit: { limit: 5, window: '1 minute' },
        circuitBreaker: {},
      },
      'https://a.example/',
    );
    expect(params).toMatchObject({
      client: HTTP_CLIENT,
      upstream: 'test:options',
      treatQuotaExhausted403AsRateLimit: true,
      treat403AsRateLimit: true,
      detectHtmlEdgeBlocks: true,
      publicAddressesOnly: true,
      maxRedirects: 2,
      retry: { maxRetries: 7 },
      rateLimit: { limit: 5, window: '1 minute' },
      circuitBreaker: { key: 'circuit:test:options' },
    });
  });

  it('gives no rate limit or circuit breaker when the policy has none', () => {
    const params = prepare({}, 'https://a.example/');
    expect(params.rateLimit).toBeUndefined();
    expect(params.circuitBreaker).toBeUndefined();
    expect(params.publicAddressesOnly).toBe(false);
  });

  it('does not change the request it was given', () => {
    const base = HttpClientRequest.get('/items');
    prepareRequestParams(
      {
        httpClient: HTTP_CLIENT,
        policy: resolveHttpPolicy({ upstream: 'test:options', baseUrl: 'https://a.example' }),
      },
      base,
      { headers: { 'x-extra': '1' }, bearerToken: 'abc' },
      true,
    );
    expect(base.url).toBe('/items');
    expect(base.headers).toEqual({});
  });
});

describe('per-request overrides through a upstream client', () => {
  it('lets a per-request operationTimeout win over the policy', async () => {
    const counter = { attempts: 0 };
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const hung = makeMockHttpClient((_req, _url, attempt) =>
          Effect.suspend(() => {
            counter.attempts = attempt;
            return Effect.never;
          }),
        );
        const upstreamClient = yield* makeClientOver(hung, {
          upstream: 'test:deadline-override',
          baseUrl: 'https://api.example.com',
          operationTimeout: '1 hour',
          retry: { maxRetries: 10, baseDelay: '4 seconds', maxBackoffSeconds: 5 },
        });
        const fiber = yield* Effect.forkChild(
          Effect.exit(
            upstreamClient.getJson('/item', ItemPayloadSchema, { operationTimeout: '5 seconds' }),
          ),
        );
        for (let step = 0; step < 7; step += 1) {
          yield* TestClock.adjust('1 second');
        }

        return yield* Fiber.join(fiber);
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(exitOutcome(exit)).toBe('timeout');
    expect(exitMessage(exit)).toContain('operation timed out after 5 seconds');
    expect(counter.attempts).toBeLessThanOrEqual(2);
  });
});

describe('per-request option validation', () => {
  it.each([0, -1, Number.NaN, Infinity, 1.5])(
    'keeps the policy byte cap when maxBodyBytes is %s, so the cap is never off',
    (maxBodyBytes) => {
      const params = prepare({ maxBodyBytes: 1024 }, 'https://a.example/', { maxBodyBytes });
      expect(params.maxBodyBytes).toBe(1024);
      expect(prepare({}, 'https://a.example/', { maxBodyBytes }).maxBodyBytes).toBe(
        DEFAULT_MAX_BODY_BYTES,
      );
    },
  );

  it('falls back to the policy timeouts for a duration that does not parse', () => {
    const params = prepare({ attemptTimeout: '2 seconds', operationTimeout: '9 seconds' }, '/', {
      attemptTimeout: '1e3 millis',
      operationTimeout: Number.NaN,
    });
    expect(params.attemptTimeout).toBe('2 seconds');
    expect(params.operationTimeout).toBe('9 seconds');
  });

  it('still honours valid per-request values', () => {
    const params = prepare({}, 'https://a.example/', {
      maxBodyBytes: 10,
      attemptTimeout: '1 second',
    });
    expect(params.maxBodyBytes).toBe(10);
    expect(params.attemptTimeout).toBe('1 second');
  });

  it('describes invalid options as a typed, non-retryable refusal', () => {
    const error = requestOptionsError('test:options', {
      maxBodyBytes: Infinity,
      attemptTimeout: '1e3 millis',
      staleIfError: Number.NaN,
    });
    expect(error?._tag).toBe('HttpRequestError');
    expect(error?.status).toBeNull();
    expect(error?.message).toContain('maxBodyBytes must be a positive safe integer');
    expect(error?.message).toContain('attemptTimeout must be a finite, non-negative duration');
    expect(error?.message).toContain('staleIfError');
    expect(
      requestOptionsError('test:options', { staleIfError: false, maxBodyBytes: 5 }),
    ).toBeNull();
    expect(requestOptionsError('test:options', undefined)).toBeNull();
  });

  it('fails validateRequestConfig with a blocked HttpRequestError for options, then policy', async () => {
    const policy = resolveHttpPolicy({ upstream: 'test:options' });
    const optionExit = await Effect.runPromiseExit(
      validateRequestConfig(policy, { maxBodyBytes: Number.NaN }),
    );
    expect(exitOutcome(optionExit)).toBe('blocked');
    expect(exitMessage(optionExit)).toContain('maxBodyBytes');
    const okExit = await Effect.runPromiseExit(validateRequestConfig(policy, { maxBodyBytes: 5 }));
    expect(okExit._tag).toBe('Success');
    const badPolicy = resolveHttpPolicy({ upstream: 'test:options', maxConcurrency: 0 });
    const policyExit = await Effect.runPromiseExit(validateRequestConfig(badPolicy, undefined));
    expect(exitMessage(policyExit)).toContain('maxConcurrency');
  });
});
