import { Effect, Exit } from 'effect';
import { HttpClientRequest } from 'effect/http';
import { describe, expect, it } from 'vite-plus/test';

import { bufferResponse, rebuildWebResponse } from './buffered-response.ts';
import { executeResponseRequest } from './execute.ts';
import {
  exitMessage,
  exitOutcome,
  FAST_RETRY,
  makeClientResponse,
  makeMockHttpClient,
  makeTrackedStream,
  runPastTimeouts,
  squashExitFailure,
  withResponseUrl,
} from './http.fixture.ts';
import { DEFAULT_MAX_BODY_BYTES } from './policy.ts';

const UPSTREAM = 'test:buffer';

/**
 * Builds a body that delivers one chunk and then fails, like a connection reset partway through.
 */
const makeResettingStream = (): ReadableStream<Uint8Array> => {
  let pulls = 0;
  return new ReadableStream<Uint8Array>({
    pull: (controller) => {
      pulls += 1;
      if (pulls === 1) {
        controller.enqueue(new TextEncoder().encode('partial'));
        return;
      }

      controller.error(new Error('connection reset'));
    },
  });
};

describe('bufferResponse', () => {
  it('keeps the headers of a HEAD response but cancels any body a transport sent', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 64, chunkBytes: 16 });
    const response = makeClientResponse(
      new Response(stream, { headers: { 'content-length': '64' } }),
      HttpClientRequest.head('https://example.com/file'),
    );
    const buffered = await Effect.runPromise(bufferResponse(response, UPSTREAM, 8));
    expect(buffered.headers['content-length']).toBe('64');
    expect(await Effect.runPromise(buffered.text)).toBe('');
    expect(tracker.pulledBytes).toBe(0);
    expect(tracker.cancelled).toBe(true);
  });

  it.each([204, 205, 304])('keeps the headers of a %i response and no body', async (status) => {
    const response = makeClientResponse(
      new Response(null, { status, headers: { etag: '"v1"', 'content-length': '0' } }),
    );
    const buffered = await Effect.runPromise(bufferResponse(response, UPSTREAM, 8));
    expect(buffered.status).toBe(status);
    expect(buffered.headers).toMatchObject({ etag: '"v1"', 'content-length': '0' });
    expect(await Effect.runPromise(buffered.text)).toBe('');
  });

  it('rebuilds a response whose buffered body can be read as text', async () => {
    const response = makeClientResponse(new Response('héllo'));
    const buffered = await Effect.runPromise(bufferResponse(response, UPSTREAM, 1024));
    expect(buffered).not.toBe(response);
    expect(await Effect.runPromise(buffered.text)).toBe('héllo');
  });

  it('drops the headers that describe the wire encoding and keeps the others', async () => {
    const response = makeClientResponse(
      new Response('payload', {
        headers: {
          'content-length': '7',
          'content-encoding': 'identity',
          'transfer-encoding': 'chunked',
          'content-type': 'text/plain',
          'x-extra': 'kept',
        },
      }),
    );
    const buffered = await Effect.runPromise(bufferResponse(response, UPSTREAM, 1024));
    expect(buffered.headers['content-length']).toBeUndefined();
    expect(buffered.headers['content-encoding']).toBeUndefined();
    expect(buffered.headers['transfer-encoding']).toBeUndefined();
    expect(buffered.headers['content-type']).toBe('text/plain');
    expect(buffered.headers['x-extra']).toBe('kept');
  });

  it('keeps the status and the post-redirect url', async () => {
    const response = makeClientResponse(
      withResponseUrl(new Response('made', { status: 201 }), 'https://example.com/created/1'),
    );
    const buffered = await Effect.runPromise(bufferResponse(response, UPSTREAM, 1024));
    expect(buffered.status).toBe(201);
    expect(buffered.url).toBe('https://example.com/created/1');
    expect(buffered.request).toBe(response.request);
  });

  it('reports the request url when the response carries none', async () => {
    const buffered = await Effect.runPromise(
      bufferResponse(makeClientResponse(new Response('x')), UPSTREAM, 1024),
    );
    expect(buffered.url).toBe('https://example.com/file');
  });

  it('buffers an empty body of a 200 as an empty text', async () => {
    const buffered = await Effect.runPromise(
      bufferResponse(makeClientResponse(new Response('')), UPSTREAM, 1024),
    );
    expect(await Effect.runPromise(buffered.text)).toBe('');
  });

  it('accepts a body of exactly maxBodyBytes', async () => {
    const buffered = await Effect.runPromise(
      bufferResponse(makeClientResponse(new Response('x'.repeat(128))), UPSTREAM, 128),
    );
    expect(await Effect.runPromise(buffered.text)).toBe('x'.repeat(128));
  });

  it('fails a streamed body over the cap as body_too_large with the response status', async () => {
    const exit = await Effect.runPromiseExit(
      bufferResponse(makeClientResponse(new Response('x'.repeat(129))), UPSTREAM, 128),
    );
    expect(exitOutcome(exit)).toBe('body_too_large');
    expect(squashExitFailure(exit)).toMatchObject({
      _tag: 'HttpRequestError',
      upstream: UPSTREAM,
      status: 200,
    });
  });

  it('refuses a declared Content-Length over the cap without pulling the body', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 4096, chunkBytes: 256 });
    const exit = await Effect.runPromiseExit(
      bufferResponse(
        makeClientResponse(new Response(stream, { headers: { 'content-length': '4096' } })),
        UPSTREAM,
        1024,
      ),
    );
    expect(exitOutcome(exit)).toBe('body_too_large');
    expect(tracker.pulledBytes).toBe(0);
    expect(tracker.cancelled).toBe(true);
  });

  it('applies the default cap when none is given', async () => {
    const exit = await Effect.runPromiseExit(
      bufferResponse(
        makeClientResponse(new Response('x'.repeat(DEFAULT_MAX_BODY_BYTES + 1))),
        UPSTREAM,
        undefined,
      ),
    );
    expect(exitOutcome(exit)).toBe('body_too_large');
  });
});

describe('executeResponseRequest', () => {
  it('buffers the body and keeps the post-redirect URL', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(
        withResponseUrl(
          new Response('payload', { headers: { 'content-length': '7', 'x-extra': 'kept' } }),
          'https://www.example.com/final',
        ),
      ),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const response = yield* executeResponseRequest({
          client,
          request: HttpClientRequest.get('https://www.example.com/start'),
          upstream: 'test:buffered',
        });
        const text = yield* response.text;
        return { text, url: response.url, headers: response.headers };
      }),
    );
    expect(result.text).toBe('payload');
    expect(result.url).toBe('https://www.example.com/final');
    expect(result.headers['x-extra']).toBe('kept');
    expect(result.headers['content-length']).toBeUndefined();
  });

  it('fails a body over the cap', async () => {
    const client = makeMockHttpClient(() => Effect.succeed(new Response('x'.repeat(512))));
    const exit = await Effect.runPromiseExit(
      executeResponseRequest({
        client,
        request: HttpClientRequest.get('https://www.example.com/big'),
        upstream: 'test:buffered-cap',
        maxBodyBytes: 128,
      }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(exitOutcome(exit)).toBe('body_too_large');
  });

  it('retries a GET whose body fails partway and does not retry a POST', async () => {
    let attempts = 0;
    const makeClient = () =>
      makeMockHttpClient((_req, _url, attempt) =>
        Effect.sync(() => {
          attempts = attempt;
          return attempt === 1 ? new Response(makeResettingStream()) : new Response('recovered');
        }),
      );
    const text = await Effect.runPromise(
      Effect.flatMap(
        executeResponseRequest({
          client: makeClient(),
          request: HttpClientRequest.get('https://www.example.com/reset'),
          upstream: 'test:buffered-reset',
          retry: FAST_RETRY,
        }),
        (response) => response.text,
      ),
    );
    expect(text).toBe('recovered');
    expect(attempts).toBe(2);

    attempts = 0;
    const postExit = await Effect.runPromiseExit(
      executeResponseRequest({
        client: makeClient(),
        request: HttpClientRequest.post('https://www.example.com/reset'),
        upstream: 'test:buffered-reset-post',
        retry: FAST_RETRY,
      }),
    );
    expect(attempts).toBe(1);
    expect(squashExitFailure(postExit)).toMatchObject({ status: null });
    expect(exitOutcome(postExit)).toBe('network_error');
    expect(exitMessage(postExit)).toContain('failed reading response body (HTTP 200)');
  });

  it('times out a stalled body inside the attempt and retries it', async () => {
    const stalled = makeTrackedStream({ totalBytes: 10, chunkBytes: 10, stall: true });
    const client = makeMockHttpClient((_req, _url, attempt) =>
      Effect.succeed(attempt === 1 ? new Response(stalled.stream) : new Response('recovered')),
    );
    const exit = await runPastTimeouts(
      Effect.flatMap(
        executeResponseRequest({
          client,
          request: HttpClientRequest.get('https://www.example.com/slow'),
          upstream: 'test:buffered-stall',
          attemptTimeout: '50 millis',
          retry: FAST_RETRY,
        }),
        (response) => response.text,
      ),
      '1 second',
    );
    expect(Exit.isSuccess(exit) ? exit.value : exit).toBe('recovered');
    expect(stalled.tracker.cancelled).toBe(true);
  });
});

describe('rebuildWebResponse', () => {
  it.each([204, 205, 304])('builds a null-body Response for a %i instead of throwing', (status) => {
    const response = makeClientResponse(new Response(null, { status }));
    const rebuilt = rebuildWebResponse(response, new Uint8Array());
    expect(rebuilt.status).toBe(status);
    expect(rebuilt.body).toBeNull();
  });

  it('keeps the bytes of a normal response', async () => {
    const response = makeClientResponse(new Response('x'));
    const rebuilt = rebuildWebResponse(response, new TextEncoder().encode('hello'));
    expect(await rebuilt.text()).toBe('hello');
  });
});
