import { Cause, Data, Deferred, Effect, Exit, Fiber, Schema } from 'effect';
import { HttpClient, HttpClientResponse } from 'effect/http';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import { makeResilientHttpClient } from './client.ts';
import { HttpRequestError } from './errors.ts';
import {
  exitMessage,
  exitOutcome,
  forkUntilSuspended,
  isFiberDone,
  makeMockHttpClient,
  makeTrackedStream,
  runPastTimeouts,
  squashExitFailure,
  withResponseUrl,
} from './http.fixture.ts';

/**
 * Raised when reading a Web response body fails. `message` repeats the read error's message so
 * tests can assert on it, and `cause` keeps the original value.
 */
class BodyReadError extends Data.TaggedError('BodyReadError')<{
  readonly message: string;
  readonly cause: unknown;
}> {}

/**
 * Wraps a body read failure as a `BodyReadError` that carries its message.
 */
const toBodyReadError = (cause: unknown): BodyReadError =>
  new BodyReadError({ message: cause instanceof Error ? cause.message : String(cause), cause });

/**
 * Reads a Web response body as text inside Effect, keeping the read error as the failure.
 */
const readText = (response: Response): Effect.Effect<string, BodyReadError> =>
  Effect.tryPromise({
    try: () => response.text(),
    catch: toBodyReadError,
  });

/**
 * Declared and actual size of the oversized body in the prefix test: far above its 1 KiB cap.
 */
const DECLARED_BYTES = 10 * 1024 * 1024;

/**
 * Chunk size of the oversized body, below the cap so the first chunk passes the limiter.
 */
const PREFIX_CHUNK_BYTES = 512;

/**
 * Reads only the first chunk of a Web response body and decodes it as text.
 */
const readFirstChunk = (response: Response): Effect.Effect<string, BodyReadError> =>
  Effect.tryPromise({
    try: async () => {
      const first = await response.body?.getReader().read();
      const value: unknown = first?.value;
      return new TextDecoder().decode(value instanceof Uint8Array ? value : undefined);
    },
    catch: toBodyReadError,
  });

/**
 * Key under which a fetch-backed response exposes its Web upstream; hiding it makes the response
 * look like it came from a non-fetch transport.
 */
const WEB_SOURCE_KEY = '~effect/http/HttpClientResponse/WebSource';

/**
 * Builds a transport whose responses have no Web upstream, like a non-fetch `HttpClient`.
 */
const makeNonWebClient = (respond: () => Response): HttpClient.HttpClient =>
  HttpClient.make((request) =>
    Effect.sync(() => {
      const response = HttpClientResponse.fromWeb(request, respond());
      // An own `undefined` shadows the prototype's Web source accessor, so `toWeb` sees none.
      Object.defineProperty(response, WEB_SOURCE_KEY, { value: undefined });
      return response;
    }),
  );

describe('withWebResponse with a non-fetch transport', () => {
  it('fails a stalled body at attemptTimeout instead of hanging in an uninterruptible acquire', async () => {
    const { stream } = makeTrackedStream({ totalBytes: 10, chunkBytes: 10, stall: true });
    const client = makeNonWebClient(() => new Response(stream));
    const exit = await runPastTimeouts(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({
          upstream: 'test:web-nonweb-stall',
          baseUrl: 'https://www.example.com',
          attemptTimeout: '50 millis',
        });
        return yield* http.withWebResponse('/slow', (response) => readText(response));
      }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
      '50 millis',
    );
    expect(exitOutcome(exit)).toBe('timeout');
  });

  it.each([204, 205])(
    'hands use a bodyless Response for a %i instead of throwing',
    async (status) => {
      const client = makeNonWebClient(() => new Response(null, { status }));
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const http = yield* makeResilientHttpClient({
            upstream: 'test:web-nonweb-204',
            baseUrl: 'https://www.example.com',
          });
          return yield* http.withWebResponse('/none', (response) =>
            Effect.map(readText(response), (text) => ({ status: response.status, text })),
          );
        }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
      );
      expect(result).toEqual({ status, text: '' });
    },
  );

  it('still buffers a normal body within the cap', async () => {
    const client = makeNonWebClient(() => new Response('hello'));
    const text = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({
          upstream: 'test:web-nonweb-ok',
          baseUrl: 'https://www.example.com',
        });
        return yield* http.withWebResponse('/x', (response) => readText(response));
      }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );
    expect(text).toBe('hello');
  });
});

describe('withWebResponse', () => {
  it('keeps a body cap failure in the error channel when reading with tryPromise', async () => {
    const client = makeMockHttpClient(() => Effect.succeed(new Response('ab')));
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({ upstream: 'test:web-body-cap-example' });
        return yield* api.withWebResponse(
          'https://example.com/',
          (response, finalUrl) => {
            const text = Effect.tryPromise({
              try: () => response.text(),
              catch: (error) =>
                Schema.is(HttpRequestError)(error)
                  ? error
                  : new HttpRequestError({
                      upstream: api.policy.upstream,
                      status: response.status,
                      message: 'Failed to read response body',
                    }),
            });
            return Effect.map(text, (html) => ({ finalUrl, size: html.length }));
          },
          { maxBodyBytes: 1 },
        );
      }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );
    expect(Exit.isFailure(exit) && Cause.hasFails(exit.cause)).toBe(true);
    expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(false);
    expect(squashExitFailure(exit)).toBeInstanceOf(HttpRequestError);
    expect(exitOutcome(exit)).toBe('body_too_large');
  });

  it('holds the bulkhead permit until use completes', async () => {
    let transportCalls = 0;
    const client = makeMockHttpClient(() =>
      Effect.sync(() => {
        transportCalls += 1;
        return new Response('page');
      }),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({
          upstream: 'test:web-bulkhead',
          baseUrl: 'https://www.example.com',
          maxConcurrency: 1,
        });
        const firstEntered = yield* Deferred.make<void>();
        const releaseFirst = yield* Deferred.make<void>();
        const first = yield* Effect.forkChild(
          http.withWebResponse('/one', (response) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(firstEntered, undefined);
              yield* Deferred.await(releaseFirst);
              return yield* readText(response);
            }),
          ),
        );
        yield* Deferred.await(firstEntered);
        // On return the second request has reached the transport or is parked on the bulkhead.
        const second = yield* forkUntilSuspended(
          http.withWebResponse('/two', (response) => readText(response)),
        );
        const callsWhileFirstRuns = transportCalls;
        yield* Deferred.succeed(releaseFirst, undefined);
        const bodies = yield* Fiber.joinAll([first, second]);
        return { callsWhileFirstRuns, bodies, finalCalls: transportCalls };
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provide(TestClock.layer()),
      ),
    );
    expect(result.callsWhileFirstRuns).toBe(1);
    expect(result.bodies).toEqual(['page', 'page']);
    expect(result.finalCalls).toBe(2);
  });

  it('fails use that runs past attemptTimeout and cancels the body', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 10, chunkBytes: 10, stall: true });
    const client = makeMockHttpClient(() => Effect.succeed(new Response(stream)));
    const exit = await runPastTimeouts(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({
          upstream: 'test:web-timeout',
          baseUrl: 'https://www.example.com',
          attemptTimeout: '50 millis',
        });
        return yield* http.withWebResponse('/slow', (response) => readText(response));
      }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
      '50 millis',
    );
    expect(squashExitFailure(exit)).toMatchObject({
      _tag: 'HttpRequestError',
      upstream: 'test:web-timeout',
      status: null,
    });
    expect(exitOutcome(exit)).toBe('timeout');
    expect(tracker.cancelled).toBe(true);
  });

  it('errors the body stream once more than maxBodyBytes are read', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 1024 * 1024, chunkBytes: 1024 });
    let attempts = 0;
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.sync(() => {
        attempts = attempt;
        return new Response(stream);
      }),
    );
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({
          upstream: 'test:web-cap',
          baseUrl: 'https://www.example.com',
          maxBodyBytes: 4096,
        });
        return yield* http.withWebResponse('/big', (response) => readText(response));
      }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );
    expect(exitMessage(exit)).toContain('exceeded maxBytes (4096 bytes)');
    expect(attempts).toBe(1);
    expect(tracker.pulledBytes).toBeLessThanOrEqual(8 * 1024);
    expect(tracker.cancelled).toBe(true);
  });

  it('lets use read a prefix of a body whose Content-Length is over the cap', async () => {
    const { stream, tracker } = makeTrackedStream({
      totalBytes: DECLARED_BYTES,
      chunkBytes: PREFIX_CHUNK_BYTES,
      prefix: '<head><title>t</title></head>',
    });
    const client = makeMockHttpClient(() =>
      Effect.succeed(
        new Response(stream, { headers: { 'content-length': String(DECLARED_BYTES) } }),
      ),
    );
    const prefix = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({
          upstream: 'test:web-declared',
          baseUrl: 'https://www.example.com',
          maxBodyBytes: 1024,
        });
        return yield* http.withWebResponse('/big', (response) => readFirstChunk(response));
      }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );
    expect(prefix).toContain('<title>t</title>');
    expect(tracker.pulledBytes).toBeLessThan(DECLARED_BYTES);
    expect(tracker.cancelled).toBe(true);
  });

  it('passes the post-redirect response URL as finalUrl', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(withResponseUrl(new Response('page'), 'https://www.example.com/landed')),
    );
    const finalUrl = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({
          upstream: 'test:web-url',
          baseUrl: 'https://www.example.com',
        });
        return yield* http.withWebResponse('/start', (response, url) =>
          Effect.succeed(`${url} ${response.url}`),
        );
      }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );
    expect(finalUrl).toBe('https://www.example.com/landed https://www.example.com/landed');
  });
});

describe('withWebResponse body release', () => {
  it('releases the request permit while the transport retains a response clone', async () => {
    const { stream } = makeTrackedStream({ totalBytes: 100, chunkBytes: 1 });
    const response = new Response(stream);
    const retained = response.clone();
    const transport = makeMockHttpClient((_request, _url, attempt) =>
      Effect.succeed(attempt === 1 ? response : new Response('next')),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({
          upstream: 'test:web-retained-clone',
          maxConcurrency: 1,
        });
        const first = yield* forkUntilSuspended(
          api.withWebResponse('https://example.com/doc', () => Effect.succeed(1), {
            attemptTimeout: '50 millis',
          }),
        );
        const next = yield* forkUntilSuspended(api.getText('https://example.com/next'));
        yield* TestClock.adjust('2 seconds');
        const done = yield* isFiberDone(first);
        const permitReleased = yield* isFiberDone(next);
        yield* Effect.promise(() => retained.body?.cancel() ?? Promise.resolve());
        return { done, permitReleased };
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, transport),
        Effect.provide(TestClock.layer()),
      ),
    );
    expect(result).toEqual({ done: true, permitReleased: true });
  });

  it('cancels the body at once when use only looks at the headers', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 4096, chunkBytes: 256 });
    const client = makeMockHttpClient(() =>
      Effect.succeed(new Response(stream, { headers: { 'content-type': 'application/pdf' } })),
    );
    const contentType = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({ upstream: 'test:web-headers-only' });
        return yield* http.withWebResponse('https://www.example.com/doc', (response) =>
          Effect.succeed(response.headers.get('content-type')),
        );
      }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );
    expect(contentType).toBe('application/pdf');
    expect(tracker.cancelled).toBe(true);
    expect(tracker.pulledBytes).toBe(0);
  });

  it('drops the wire encoding headers, because the body it hands over is already decoded', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(
        new Response('x'.repeat(100), {
          headers: { 'content-encoding': 'gzip', 'content-length': '40', 'x-kept': 'yes' },
        }),
      ),
    );
    const headers = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({ upstream: 'test:web-wire-headers' });
        return yield* http.withWebResponse('https://www.example.com/page', (response) =>
          Effect.succeed({
            encoding: response.headers.get('content-encoding'),
            length: response.headers.get('content-length'),
            kept: response.headers.get('x-kept'),
          }),
        );
      }).pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );
    expect(headers).toEqual({ encoding: null, length: null, kept: 'yes' });
  });
});
