import { Deferred, Effect, Exit, Fiber, Stream } from 'effect';
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/http';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import { readBodyPrefix, readBoundedBytes, readBoundedText, readSharedBodyPrefix } from './body.ts';
import { executeBytesRequest, executeTextRequest } from './execute.ts';
import {
  exitErrorMeta,
  exitMessage,
  exitOutcome,
  forkUntilSuspended,
  isFiberDone,
  makeMockHttpClient,
  makeTrackedStream,
  squashExitFailure,
} from './http.fixture.ts';
import { DEFAULT_MAX_BODY_BYTES } from './policy.ts';

const responseFor = (body: string, init?: ResponseInit): HttpClientResponse.HttpClientResponse =>
  HttpClientResponse.fromWeb(
    HttpClientRequest.get('https://example.com/file'),
    new Response(body, init),
  );

describe('fetch response cancellation', () => {
  it.each([
    ['an oversized Content-Length', 200, 'body_too_large'],
    ['a rate limit', 429, 'rate_limited'],
  ])('stops a streamed upload when the response reports %s', async (_name, status, outcome) => {
    let signal: AbortSignal | undefined;
    const fetch: typeof globalThis.fetch = async (_url, init) => {
      signal = init?.signal ?? undefined;
      if (init?.body instanceof ReadableStream) {
        const reader = init.body.getReader();
        await reader.read();
        reader.releaseLock();
      }

      return new Response('data', { status, headers: { 'content-length': '4' } });
    };
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const finalized = yield* Deferred.make<void>();
        const upload = Stream.concat(Stream.make(Uint8Array.of(1)), Stream.never).pipe(
          Stream.ensuring(Deferred.succeed(finalized, undefined)),
        );
        const request = HttpClientRequest.bodyStream(
          HttpClientRequest.post('https://example.com/upload'),
          upload,
        );
        const exit = yield* Effect.exit(
          executeTextRequest({
            client,
            request,
            upstream: 'test:upload-cancellation',
            maxBodyBytes: 1,
            retry: { maxRetries: 0 },
          }),
        );
        const aborted = signal?.aborted;
        const finalization = yield* Effect.exit(
          Deferred.await(finalized).pipe(Effect.timeout('1 second')),
        );
        return { outcome: exitOutcome(exit), aborted, finalization };
      }).pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
      ),
    );
    expect(result).toEqual({ outcome, aborted: true, finalization: Exit.succeed(undefined) });
  });
});

describe('readBoundedBytes', () => {
  it('returns the body when it is within the cap', async () => {
    const bytes = await Effect.runPromise(
      readBoundedBytes({
        response: responseFor('hello', { status: 200 }),
        upstream: 'test:body',
        maxBytes: 16,
      }),
    );
    expect(new TextDecoder().decode(bytes)).toBe('hello');
  });

  it('fails when Content-Length is above the cap', async () => {
    const exit = await Effect.runPromiseExit(
      readBoundedBytes({
        response: responseFor('tiny', {
          status: 200,
          headers: { 'content-length': '99999' },
        }),
        upstream: 'test:body',
        maxBytes: 8,
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(squashExitFailure(exit)).toMatchObject({
      _tag: 'HttpRequestError',
      upstream: 'test:body',
      status: 200,
    });
  });

  it('fails when the streamed bytes exceed the cap', async () => {
    const exit = await Effect.runPromiseExit(
      readBoundedText({
        response: responseFor('x'.repeat(32), { status: 200 }),
        upstream: 'test:body',
        maxBytes: 8,
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(squashExitFailure(exit)).toMatchObject({
      _tag: 'HttpRequestError',
      status: 200,
    });
  });
});

describe('readBoundedBytes cancellation', () => {
  it('refuses an oversized Content-Length without pulling and cancels the body', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 64, chunkBytes: 8 });
    const exit = await Effect.runPromiseExit(
      readBoundedBytes({
        response: HttpClientResponse.fromWeb(
          HttpClientRequest.get('https://example.com/file'),
          new Response(stream, { headers: { 'content-length': '64' } }),
        ),
        upstream: 'test:body',
        maxBytes: 16,
      }),
    );
    expect(squashExitFailure(exit)).toMatchObject({ _tag: 'HttpRequestError', status: 200 });
    expect(exitOutcome(exit)).toBe('body_too_large');
    expect(tracker.pulledBytes).toBe(0);
    expect(tracker.cancelled).toBe(true);
  });

  it('accepts a body of exactly maxBytes and cancels one a byte over it', async () => {
    const exact = makeTrackedStream({ totalBytes: 4, chunkBytes: 1, prefix: '  {}' });
    const text = await Effect.runPromise(
      readBoundedText({
        response: HttpClientResponse.fromWeb(
          HttpClientRequest.get('https://example.com/file'),
          new Response(exact.stream),
        ),
        upstream: 'test:body',
        maxBytes: 4,
      }),
    );
    expect(text).toBe('  {}');

    const over = makeTrackedStream({ totalBytes: 5, chunkBytes: 1, prefix: '   {}' });
    const exit = await Effect.runPromiseExit(
      readBoundedText({
        response: HttpClientResponse.fromWeb(
          HttpClientRequest.get('https://example.com/file'),
          new Response(over.stream),
        ),
        upstream: 'test:body',
        maxBytes: 4,
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(over.tracker.cancelled).toBe(true);
  });
});

describe('readBoundedBytes with an invalid cap', () => {
  it.each([Number.NaN, Number.POSITIVE_INFINITY, 0])(
    'falls back to DEFAULT_MAX_BODY_BYTES for maxBytes %s',
    async (maxBytes) => {
      const { stream, tracker } = makeTrackedStream({
        totalBytes: DEFAULT_MAX_BODY_BYTES + 1024 * 1024,
        chunkBytes: 256 * 1024,
      });
      const exit = await Effect.runPromiseExit(
        readBoundedBytes({
          response: HttpClientResponse.fromWeb(
            HttpClientRequest.get('https://example.com/file'),
            new Response(stream),
          ),
          upstream: 'test:body',
          maxBytes,
        }),
      );
      expect(exitOutcome(exit)).toBe('body_too_large');
      expect(exitMessage(exit)).toContain(`(${String(DEFAULT_MAX_BODY_BYTES)} bytes)`);
      expect(tracker.cancelled).toBe(true);
    },
  );
});

describe('readBoundedText streaming', () => {
  it('decodes multibyte characters split across single-byte chunks', async () => {
    const text = 'é€😀';
    const bytes = new TextEncoder().encode(text);
    expect(bytes.byteLength).toBe(9);
    const { stream } = makeTrackedStream({ totalBytes: 9, chunkBytes: 1, prefix: text });
    const decoded = await Effect.runPromise(
      readBoundedText({
        response: HttpClientResponse.fromWeb(
          HttpClientRequest.get('https://example.com/file'),
          new Response(stream),
        ),
        upstream: 'test:body',
        maxBytes: 9,
      }),
    );
    expect(decoded).toBe(text);
  });

  it('fails a body longer than its declared Content-Length once it crosses the cap', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 4096, chunkBytes: 1024 });
    const exit = await Effect.runPromiseExit(
      readBoundedBytes({
        response: HttpClientResponse.fromWeb(
          HttpClientRequest.get('https://example.com/file'),
          new Response(stream, { headers: { 'content-length': '1024' } }),
        ),
        upstream: 'test:body',
        maxBytes: 3072,
      }),
    );
    expect(exitOutcome(exit)).toBe('body_too_large');
    expect(tracker.pulledBytes).toBe(4096);
    expect(tracker.cancelled).toBe(true);
  });

  it('cancels the body stream when the read is interrupted', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 8, chunkBytes: 8, stall: true });
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const read = yield* forkUntilSuspended(
          readBoundedBytes({
            response: HttpClientResponse.fromWeb(
              HttpClientRequest.get('https://example.com/file'),
              new Response(stream),
            ),
            upstream: 'test:body',
          }),
        );
        return yield* Fiber.interrupt(read).pipe(Effect.andThen(Fiber.await(read)));
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(Exit.hasInterrupts(exit)).toBe(true);
    expect(tracker.cancelled).toBe(true);
  });
});

describe('readBodyPrefix', () => {
  it('reads at most maxBytes and cancels the rest of the body', async () => {
    const { stream, tracker } = makeTrackedStream({
      totalBytes: 1024 * 1024,
      chunkBytes: 1024,
      prefix: 'hello',
    });
    const prefix = await Effect.runPromise(
      readBodyPrefix({
        response: HttpClientResponse.fromWeb(
          HttpClientRequest.get('https://example.com/file'),
          new Response(stream),
        ),
        maxBytes: 1500,
      }),
    );
    expect(prefix.text).toHaveLength(1500);
    expect(prefix.text.startsWith('hello')).toBe(true);
    expect(prefix.truncated).toBe(true);
    expect(tracker.pulledBytes).toBeLessThanOrEqual(2048);
    expect(tracker.cancelled).toBe(true);
  });

  it('returns a short body whole and an already consumed body as empty text', async () => {
    const response = responseFor('short', { status: 200 });
    const first = await Effect.runPromise(readBodyPrefix({ response, maxBytes: 64 }));
    expect(first).toEqual({ text: 'short', truncated: false });

    const second = await Effect.runPromise(readBodyPrefix({ response, maxBytes: 64 }));
    expect(second.text).toBe('');
  });
});

describe('readBoundedText', () => {
  it('decodes a UTF-8 body', async () => {
    const text = await Effect.runPromise(
      readBoundedText({
        response: responseFor('café', { status: 200 }),
        upstream: 'test:body',
        maxBytes: 16,
      }),
    );
    expect(text).toBe('café');
  });
});

/**
 * Responses that carry no body, each with the request that produced it.
 */
const BODYLESS: ReadonlyArray<
  readonly [label: string, make: () => HttpClientResponse.HttpClientResponse]
> = [
  [
    '204',
    () =>
      HttpClientResponse.fromWeb(
        HttpClientRequest.get('https://example.com/file'),
        new Response(null, { status: 204 }),
      ),
  ],
  [
    '205',
    () =>
      HttpClientResponse.fromWeb(
        HttpClientRequest.get('https://example.com/file'),
        new Response(null, { status: 205 }),
      ),
  ],
  [
    '304 with a large Content-Length',
    () =>
      HttpClientResponse.fromWeb(
        HttpClientRequest.get('https://example.com/file'),
        new Response(null, { status: 304, headers: { 'content-length': '99999999' } }),
      ),
  ],
  [
    'HEAD with a large Content-Length',
    () =>
      HttpClientResponse.fromWeb(
        HttpClientRequest.head('https://example.com/file'),
        new Response(null, { status: 200, headers: { 'content-length': '99999999' } }),
      ),
  ],
  [
    '200 with a null body',
    () =>
      HttpClientResponse.fromWeb(
        HttpClientRequest.get('https://example.com/file'),
        new Response(null, { status: 200 }),
      ),
  ],
];

describe('bodyless responses', () => {
  it.each(BODYLESS)('reads a %s as zero bytes and empty text', async (_label, make) => {
    const bytes = await Effect.runPromise(
      readBoundedBytes({ response: make(), upstream: 'test:bodyless', maxBytes: 16 }),
    );
    expect(bytes.byteLength).toBe(0);
    const text = await Effect.runPromise(
      readBoundedText({ response: make(), upstream: 'test:bodyless', maxBytes: 16 }),
    );
    expect(text).toBe('');
    const prefix = await Effect.runPromise(readBodyPrefix({ response: make(), maxBytes: 16 }));
    expect(prefix).toEqual({ text: '', truncated: false });
  });

  it('answers a 204 with empty text and bytes after one attempt', async () => {
    let attempts = 0;
    const client = makeMockHttpClient(() =>
      Effect.sync(() => {
        attempts += 1;
        return new Response(null, { status: 204 });
      }),
    );
    const request = HttpClientRequest.get('https://api.example.com/item');
    const text = await Effect.runPromise(
      executeTextRequest({
        client,
        request,
        upstream: 'test:bodyless-204',
        retry: { maxRetries: 2 },
      }),
    );
    expect(text).toBe('');
    expect(attempts).toBe(1);

    const bytes = await Effect.runPromise(
      executeBytesRequest({
        client,
        request,
        upstream: 'test:bodyless-204',
        retry: { maxRetries: 2 },
      }),
    );
    expect(bytes.byteLength).toBe(0);
    expect(attempts).toBe(2);
  });
});

describe('body read failures', () => {
  it('names only the method and origin of a body that fails partway, never its query', async () => {
    const request = HttpClientRequest.get(
      'https://user:PW@h.example/share?token=SECRET_T&api_key=SECRET_K&page=2',
    );
    const response = HttpClientResponse.fromWeb(
      request,
      new Response(
        new ReadableStream<Uint8Array>({
          pull: (controller) => {
            controller.error(new Error('connection reset'));
          },
        }),
      ),
    );
    const exit = await Effect.runPromiseExit(
      readBoundedBytes({ response, upstream: 'test:body-reset' }),
    );
    const message = exitMessage(exit) ?? '';
    expect(message).toContain('test:body-reset failed reading response body (HTTP 200)');
    expect(message).toContain('connection reset');
    expect(message).toContain('GET https://h.example');
    for (const secret of ['SECRET_T', 'SECRET_K', 'page=2', '/share', 'PW']) {
      expect(message).not.toContain(secret);
    }

    expect(exitOutcome(exit)).toBe('network_error');
  });

  it('cuts a URL quoted by the stream error down to its origin', async () => {
    const response = HttpClientResponse.fromWeb(
      HttpClientRequest.get('https://h.example/share'),
      new Response(
        new ReadableStream<Uint8Array>({
          pull: (controller) => {
            controller.error(new Error('reset by https://h.example/next?session_hint=SECRET'));
          },
        }),
      ),
    );
    const exit = await Effect.runPromiseExit(
      readBoundedBytes({ response, upstream: 'test:body-reset' }),
    );
    expect(exitMessage(exit)).not.toContain('SECRET');
    expect(exitMessage(exit)).toContain(
      'reset by https://h.example (HTTP 200, GET https://h.example)',
    );
    expect(exitErrorMeta(exit)).toMatchObject({ outcome: 'network_error' });
  });

  it('masks a sent credential that the body-read error quotes', async () => {
    const response = HttpClientResponse.fromWeb(
      HttpClientRequest.get('https://h.example/share').pipe(
        HttpClientRequest.bearerToken('SECRET_TOKEN_1'),
      ),
      new Response(
        new ReadableStream<Uint8Array>({
          pull: (controller) => {
            controller.error(new Error('stream reset for Bearer SECRET_TOKEN_1'));
          },
        }),
      ),
    );
    const exit = await Effect.runPromiseExit(
      readBoundedBytes({ response, upstream: 'test:body-reset' }),
    );
    expect(exitMessage(exit)).toContain('stream reset for <redacted>');
    expect(exitMessage(exit)).not.toContain('SECRET_TOKEN_1');
  });
});

describe('readSharedBodyPrefix', () => {
  it.each([Number.NaN, -1, Number.POSITIVE_INFINITY])(
    'reads nothing for a maxBytes of %s instead of failing or lifting the cap',
    async (maxBytes) => {
      const response = HttpClientResponse.fromWeb(
        HttpClientRequest.get('https://example.com/file'),
        new Response('hello world'),
      );
      const prefix = await Effect.runPromise(readSharedBodyPrefix({ response, maxBytes }));
      expect(prefix.text).toBe('');
    },
  );

  it('reads a body once and gives later callers the same bytes, cut to their limit', async () => {
    const { stream, tracker } = makeTrackedStream({
      totalBytes: 10_000,
      chunkBytes: 1000,
      prefix: 'hello world',
    });
    const response = HttpClientResponse.fromWeb(
      HttpClientRequest.get('https://example.com/file'),
      new Response(stream),
    );
    const first = await Effect.runPromise(readSharedBodyPrefix({ response, maxBytes: 4000 }));
    expect(first.text.startsWith('hello world')).toBe(true);
    expect(first.text).toHaveLength(4000);
    expect(first.truncated).toBe(true);

    const second = await Effect.runPromise(readSharedBodyPrefix({ response, maxBytes: 5 }));
    expect(second).toEqual({ text: 'hello', truncated: true });
    expect(tracker.pulledBytes).toBeLessThanOrEqual(5000);
    expect(tracker.cancelled).toBe(true);
  });

  it('gives up on a stalled body after its 2 second budget and remembers the empty result', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 8, chunkBytes: 8, stall: true });
    const response = HttpClientResponse.fromWeb(
      HttpClientRequest.get('https://example.com/file'),
      new Response(stream),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const first = yield* forkUntilSuspended(readSharedBodyPrefix({ response, maxBytes: 64 }));
        yield* TestClock.adjust('1999 millis');
        const doneBeforeBudget = yield* isFiberDone(first);
        yield* TestClock.adjust('1 millis');
        const doneAtBudget = yield* isFiberDone(first);
        const firstPrefix = yield* Fiber.join(first);
        const second = yield* forkUntilSuspended(readSharedBodyPrefix({ response, maxBytes: 64 }));
        const secondDoneWithoutTime = yield* isFiberDone(second);
        const secondPrefix = yield* Fiber.join(second);
        return { doneBeforeBudget, doneAtBudget, firstPrefix, secondDoneWithoutTime, secondPrefix };
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(result).toEqual({
      doneBeforeBudget: false,
      doneAtBudget: true,
      firstPrefix: { text: '', truncated: true },
      secondDoneWithoutTime: true,
      secondPrefix: { text: '', truncated: true },
    });
    expect(tracker.cancelled).toBe(true);
  });
});
