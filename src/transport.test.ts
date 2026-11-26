import * as NodeHttpClient from '@effect/platform-node/NodeHttpClient';
import { errors, MockAgent } from '@effect/platform-node/Undici';
import { Cause, Effect, Exit, Layer, Redacted, Tracer } from 'effect';
import {
  FetchHttpClient,
  HttpClient,
  HttpClientError,
  HttpClientRequest,
  HttpClientResponse,
  UrlParams,
} from 'effect/http';
import { describe, expect, it, vi } from 'vite-plus/test';

import { HttpRequestError } from './errors.ts';
import {
  exitErrorMeta,
  exitMessage,
  exitOutcome,
  makeMockHttpClient,
  squashExitFailure,
} from './http.fixture.ts';
import { sendRequest } from './transport.ts';

/**
 * Request whose URL carries credentials in its userinfo, path-adjacent query and fragment.
 */
const SECRET_REQUEST = HttpClientRequest.get(
  'https://user:PW@h.example/share?token=SECRET_T&client_secret=SECRET_C&api_key=SECRET_K&q=private#frag',
);

/**
 * Text that must never appear in an error built for `SECRET_REQUEST`.
 */
const SECRET_PARTS = ['SECRET_T', 'SECRET_C', 'SECRET_K', 'private', '/share', 'PW', 'frag'];

const REQUEST = HttpClientRequest.get('https://api.example.com/item');

/**
 * A `fetch` that rejects the way a refused connection does, quoting the full URL in its message.
 */
const failingFetch: typeof globalThis.fetch = () =>
  Promise.reject(new Error('connect ECONNREFUSED for https://h.example/share?token=SECRET_T'));

describe('sendRequest', () => {
  // Bun replaces Undici with a stub, so `MockAgent` has no dispatcher to stub there.
  it.skipIf(process.versions.bun !== undefined)(
    'classifies an Undici connect timeout as a timeout',
    async () => {
      const dispatcher = new MockAgent();
      dispatcher.disableNetConnect();
      vi.spyOn(dispatcher, 'request').mockRejectedValue(new errors.ConnectTimeoutError());
      const layer = NodeHttpClient.layerUndiciNoDispatcher.pipe(
        Layer.provide(Layer.succeed(NodeHttpClient.Dispatcher, dispatcher)),
      );
      try {
        const exit = await Effect.runPromiseExit(
          Effect.gen(function* () {
            const client = yield* HttpClient.HttpClient;
            return yield* sendRequest(client, REQUEST, 'test:undici-timeout');
          }).pipe(Effect.provide(layer)),
        );
        expect(exitMessage(exit)).toContain('UND_ERR_CONNECT_TIMEOUT');
        expect(exitOutcome(exit)).toBe('timeout');
      } finally {
        await dispatcher.close();
      }
    },
  );

  it('returns the response of a successful request, whatever its status', async () => {
    const client = makeMockHttpClient(() => Effect.succeed(new Response('gone', { status: 404 })));
    const response = await Effect.runPromise(sendRequest(client, REQUEST, 'test:transport'));
    expect(response.status).toBe(404);
  });

  it('maps a transport failure to an HttpRequestError without a status', async () => {
    const client = HttpClient.make((request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({ request, description: 'connection reset' }),
        }),
      ),
    );
    const exit = await Effect.runPromiseExit(sendRequest(client, REQUEST, 'test:transport'));
    const failure = squashExitFailure(exit);
    expect(failure).toBeInstanceOf(HttpRequestError);
    expect(failure).toMatchObject({ upstream: 'test:transport', status: null });
    expect(exitMessage(exit)).toContain('connection reset');
  });

  it('keeps the response status of a failure that carries a response', async () => {
    const client = HttpClient.make((request) => {
      const response = HttpClientResponse.fromWeb(request, new Response('bad', { status: 502 }));
      return Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.StatusCodeError({ request, response }),
        }),
      );
    });
    const exit = await Effect.runPromiseExit(sendRequest(client, REQUEST, 'test:transport'));
    expect(squashExitFailure(exit)).toMatchObject({
      _tag: 'HttpRequestError',
      upstream: 'test:transport',
      status: 502,
    });
  });

  it('sends no traceparent header even inside an active span', async () => {
    const seen: Array<Record<string, string>> = [];
    const client = makeMockHttpClient((request) =>
      Effect.sync(() => {
        seen.push({ ...request.headers });
        return new Response('ok');
      }),
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        yield* client.execute(REQUEST);
        yield* sendRequest(client, REQUEST, 'test:transport');
      }).pipe(Effect.withSpan('parent')),
    );
    expect(seen).toHaveLength(2);
    expect(seen[0]).toHaveProperty('traceparent');
    expect(seen[1]).not.toHaveProperty('traceparent');
    expect(seen[1]).not.toHaveProperty('b3');
  });

  it('names only the method and origin of a failed request, never its query', async () => {
    const client = HttpClient.make((request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({ request, description: 'connection reset' }),
        }),
      ),
    );
    const exit = await Effect.runPromiseExit(sendRequest(client, SECRET_REQUEST, 'test:transport'));
    expect(exitMessage(exit)).toBe(
      'test:transport request failed: Transport error: connection reset (GET https://h.example)',
    );
    for (const secret of SECRET_PARTS) {
      expect(exitMessage(exit)).not.toContain(secret);
    }
  });

  it('cuts a URL of any scheme quoted by the transport down to its origin', async () => {
    const client = HttpClient.make((request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({
            request,
            description: 'proxy refused ftp://user:FTP_PASSWORD@files.example/private/dir',
          }),
        }),
      ),
    );
    const exit = await Effect.runPromiseExit(sendRequest(client, REQUEST, 'test:transport'));
    expect(exitMessage(exit)).toBe(
      'test:transport request failed: Transport error: proxy refused ftp://files.example (GET https://api.example.com)',
    );
  });

  it('masks a sent credential that the transport detail quotes', async () => {
    const client = HttpClient.make((request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({
            request,
            description: 'proxy rejected credential PROXYKEY0123456789abcdefXYZ12',
          }),
        }),
      ),
    );
    const request = HttpClientRequest.get('https://api.example.com/x', {
      headers: { 'x-api-key': 'PROXYKEY0123456789abcdefXYZ12' },
    });
    const exit = await Effect.runPromiseExit(sendRequest(client, request, 'test:transport'));
    expect(exitMessage(exit)).not.toContain('PROXYKEY0123456789abcdefXYZ12');
    expect(exitMessage(exit)).toContain('proxy rejected credential <redacted>');
  });

  it('keeps the query out of a real fetch failure and labels it a network error', async () => {
    const exit = await Effect.runPromiseExit(
      Effect.flatMap(HttpClient.HttpClient, (client) =>
        sendRequest(client, SECRET_REQUEST, 'test:transport'),
      ).pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.provideService(FetchHttpClient.Fetch, failingFetch),
      ),
    );
    const message = exitMessage(exit) ?? '';
    expect(message).toContain('test:transport request failed: Transport error');
    expect(message).toContain('ECONNREFUSED');
    expect(message).toContain('GET https://h.example');
    for (const secret of SECRET_PARTS) {
      expect(message).not.toContain(secret);
    }

    expect(exitOutcome(exit)).toBe('network_error');
    expect(exitErrorMeta(exit)).toMatchObject({ outcome: 'network_error' });
  });

  it('labels a transport timeout as a timeout', async () => {
    const client = HttpClient.make((request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({
            request,
            cause: new Cause.TimeoutError('Operation timed out after 1 second'),
            description: 'timed out after 1 second',
          }),
        }),
      ),
    );
    const exit = await Effect.runPromiseExit(sendRequest(client, REQUEST, 'test:transport'));
    expect(exitErrorMeta(exit)).toMatchObject({ outcome: 'timeout' });
  });

  it('never retries a request whose URL is invalid and keeps it off the circuit breaker', async () => {
    const client = HttpClient.make((request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.InvalidUrlError({ request, description: 'bad url' }),
        }),
      ),
    );
    const exit = await Effect.runPromiseExit(sendRequest(client, REQUEST, 'test:transport'));
    expect(exitErrorMeta(exit)).toEqual({
      outcome: 'blocked',
      retryable: false,
      circuit: 'ignore',
    });
  });

  it('leaves a failure that carries a response to be classified by its status', async () => {
    const client = HttpClient.make((request) => {
      const response = HttpClientResponse.fromWeb(request, new Response('bad', { status: 502 }));
      return Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.StatusCodeError({ request, response }),
        }),
      );
    });
    const exit = await Effect.runPromiseExit(sendRequest(client, SECRET_REQUEST, 'test:transport'));
    expect(exitOutcome(exit)).toBe('server_error');
    expect(exitMessage(exit)).toBe(
      'test:transport request failed: StatusCode error (HTTP 502, GET https://h.example)',
    );
  });

  it('adds credential names to the redacted URL parameters and keeps the existing ones', async () => {
    const seen: Array<ReadonlyArray<string | RegExp>> = [];
    const client = HttpClient.make((request, _url, _signal, fiber) =>
      Effect.sync(() => {
        seen.push(fiber.getRef(UrlParams.CurrentRedactedNames));
        return HttpClientResponse.fromWeb(request, new Response('ok'));
      }),
    );
    const request = HttpClientRequest.get(
      'https://api.example.com/item?client_secret=S1&api_key=S2&code=S3&page=2',
    );
    await Effect.runPromise(
      sendRequest(client, request, 'test:transport').pipe(
        Effect.provideService(UrlParams.CurrentRedactedNames, ['X-Amz-Signature', 'custom']),
      ),
    );
    expect(seen).toHaveLength(1);
    expect(seen[0]).toEqual(
      expect.arrayContaining(['X-Amz-Signature', 'custom', 'client_secret', 'api_key', 'code']),
    );
    expect(seen[0]).not.toContain('page');
  });
});

/**
 * Runs `effect` with a tracer that records every span it starts, and returns the spans.
 */
const recordSpans = async <A, E>(
  effect: Effect.Effect<A, E>,
): Promise<Array<Tracer.NativeSpan>> => {
  const spans: Array<Tracer.NativeSpan> = [];
  const tracer = Tracer.make({
    span: (options) => {
      const span = new Tracer.NativeSpan(options);
      spans.push(span);
      return span;
    },
  });
  await Effect.runPromiseExit(Effect.provideService(effect, Tracer.Tracer, tracer));
  return spans;
};

describe('sendRequest client span', () => {
  it('masks every credential query value and the fragment like the logs do', async () => {
    const client = makeMockHttpClient(() => Effect.succeed(new Response('ok')));
    const request = HttpClientRequest.get(
      'https://a.test/cb?sessionId=SESS_SECRET&x-api-key=XK_SECRET&access-token=AT_SECRET&token=T_SECRET&page=2#access_token=FRAG_SECRET',
    );
    const spans = await recordSpans(sendRequest(client, request, 'test:span'));
    expect(spans).toHaveLength(1);
    const attributes = Object.fromEntries(spans[0]?.attributes ?? []);
    expect(attributes).toMatchObject({
      'http.request.method': 'GET',
      'url.scheme': 'https',
      'server.address': 'a.test',
      'server.port': 443,
      'http.response.status_code': 200,
    });
    const exported = JSON.stringify(attributes);
    for (const secret of ['SESS_SECRET', 'XK_SECRET', 'AT_SECRET', 'T_SECRET', 'FRAG_SECRET']) {
      expect(exported).not.toContain(secret);
    }

    expect(attributes['url.query']).toContain('page=2');
  });

  it('masks Redacted query values and UrlParams.CurrentRedactedNames like Effect’s own span', async () => {
    const client = makeMockHttpClient(() => Effect.succeed(new Response('ok')));
    const request = HttpClientRequest.get('https://a.test/x?ticket=NAMED_SECRET&page=2', {
      urlParams: { pass: Redacted.make('WRAPPED_SECRET') },
    });
    const spans = await recordSpans(
      Effect.provideService(
        sendRequest(client, request, 'test:span'),
        UrlParams.CurrentRedactedNames,
        ['ticket'],
      ),
    );
    const attributes = Object.fromEntries(spans[0]?.attributes ?? []);
    const exported = JSON.stringify(attributes);
    expect(exported).not.toContain('NAMED_SECRET');
    expect(exported).not.toContain('WRAPPED_SECRET');
    expect(attributes['url.full']).toContain('page=2');
    expect(attributes['url.query']).toContain('page=2');
  });

  it('ends a failed span with the sanitised error, never the raw transport cause', async () => {
    const client = HttpClient.make((request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({
            request,
            description: 'proxy said Authorization: token SECRET_PROXY_TOKEN',
          }),
        }),
      ),
    );
    const request = HttpClientRequest.get('https://h.example/p', {
      headers: { authorization: 'token SECRET_PROXY_TOKEN' },
    });
    const spans = await recordSpans(sendRequest(client, request, 'test:span-fail'));
    expect(spans).toHaveLength(1);
    const status = spans[0]?.status;
    const exit = status?._tag === 'Ended' ? status.exit : null;
    expect(exit !== null && Exit.isFailure(exit)).toBe(true);
    const failure = exit !== null && Exit.isFailure(exit) ? Cause.squash(exit.cause) : null;
    expect(failure).toBeInstanceOf(HttpRequestError);
    expect(exit !== null && Exit.isFailure(exit) ? Cause.pretty(exit.cause) : '').not.toContain(
      'SECRET_PROXY_TOKEN',
    );
  });

  it.each([404, 503])(
    'ends the span of an HTTP %i response as failed with error.type and no secret',
    async (status) => {
      const client = makeMockHttpClient(() => Effect.succeed(new Response('no', { status })));
      const request = HttpClientRequest.get('https://a.test/p?token=T_SECRET&page=2');
      const spans = await recordSpans(sendRequest(client, request, 'test:span-status'));
      expect(spans).toHaveLength(1);
      const span = spans[0];
      const attributes = Object.fromEntries(span?.attributes ?? []);
      expect(attributes).toMatchObject({
        'http.response.status_code': status,
        'error.type': String(status),
      });
      const exit = span?.status._tag === 'Ended' ? span.status.exit : null;
      expect(exit !== null && Exit.isFailure(exit)).toBe(true);
      const failure = exit !== null && Exit.isFailure(exit) ? Cause.squash(exit.cause) : null;
      expect(failure).toBeInstanceOf(HttpRequestError);
      const exported = `${JSON.stringify(attributes)} ${exit === null || Exit.isSuccess(exit) ? '' : Cause.pretty(exit.cause)}`;
      expect(exported).not.toContain('T_SECRET');
    },
  );

  it.each([200, 304])('leaves the span of an HTTP %i response successful', async (status) => {
    const client = makeMockHttpClient(() => Effect.succeed(new Response(null, { status })));
    const spans = await recordSpans(sendRequest(client, REQUEST, 'test:span-ok'));
    const span = spans[0];
    expect(Object.fromEntries(span?.attributes ?? [])).not.toHaveProperty('error.type');
    const exit = span?.status._tag === 'Ended' ? span.status.exit : null;
    expect(exit !== null && Exit.isSuccess(exit)).toBe(true);
  });

  it('records the status and error.type of a failure that carries a response', async () => {
    const client = HttpClient.make((request) => {
      const response = HttpClientResponse.fromWeb(request, new Response('bad', { status: 502 }));
      return Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.StatusCodeError({ request, response }),
        }),
      );
    });
    const spans = await recordSpans(sendRequest(client, REQUEST, 'test:span-status-error'));
    expect(Object.fromEntries(spans[0]?.attributes ?? [])).toMatchObject({
      'http.response.status_code': 502,
      'error.type': '502',
    });
  });

  it('sets error.type to the reason tag when no response arrived', async () => {
    const client = HttpClient.make((request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({ request, description: 'reset' }),
        }),
      ),
    );
    const spans = await recordSpans(sendRequest(client, REQUEST, 'test:span-type'));
    expect(Object.fromEntries(spans[0]?.attributes ?? [])).toMatchObject({
      'error.type': 'TransportError',
    });
  });

  it('starts no span when HttpClient.TracerDisabledWhen matches the request', async () => {
    const client = makeMockHttpClient(() => Effect.succeed(new Response('ok')));
    const spans = await recordSpans(
      sendRequest(client, REQUEST, 'test:span-off').pipe(
        Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
      ),
    );
    expect(spans).toEqual([]);
  });
});
