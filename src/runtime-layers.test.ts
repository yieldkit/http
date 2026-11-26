import type { Layer } from 'effect';
import type { IncomingMessage, Server, ServerResponse } from 'node:http';
import type { Socket } from 'node:net';

import { Effect, Exit, Schedule, Schema, Stream } from 'effect';
import { HttpClient, HttpClientRequest } from 'effect/http';
import { createServer } from 'node:http';
import { createServer as createNetServer } from 'node:net';
import { gzipSync } from 'node:zlib';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import { cancelResponseBody } from './body.ts';
import { ResilientHttpLive as BunResilientHttpLive } from './bun.ts';
import { makeResilientHttpClient } from './client.ts';
import { exitMessage, exitOutcome, FAST_RETRY, ItemPayloadSchema } from './http.fixture.ts';
import { ResilientHttpLive as NodeResilientHttpLive } from './node.ts';

/**
 * A request a local server received: its path and the credential headers it carried.
 */
type ReceivedRequest = {
  readonly path: string;
  readonly authorization: string | undefined;
  readonly apiKey: string | undefined;
};

/**
 * A local HTTP server and the requests it has received.
 */
type LocalServer = {
  readonly server: Server;
  readonly received: Array<ReceivedRequest>;
  port: number;
};

/**
 * Answers requests on the first server: a JSON item, a 503 then a 200 per `/flaky` URL, a
 * cross-origin redirect to the second server, a body that never ends, an oversized body and a 500
 * that echoes nothing.
 */
const makeHandler =
  (received: Array<ReceivedRequest>, redirectTarget: () => string) =>
  (request: IncomingMessage, response: ServerResponse): void => {
    const path = request.url ?? '/';
    received.push({
      path,
      authorization: request.headers.authorization,
      apiKey: request.headers['x-api-key']?.toString(),
    });
    if (path === '/item') {
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ id: 42, title: 'first' }));
      return;
    }

    if (path.startsWith('/flaky')) {
      const attempts = received.filter((entry) => entry.path === path).length;
      response.writeHead(attempts === 1 ? 503 : 200, { 'content-type': 'text/plain' });
      response.end(attempts === 1 ? 'busy' : 'ok');
      return;
    }

    if (path === '/start') {
      response.writeHead(302, { location: redirectTarget() });
      response.end();
      return;
    }

    if (path === '/final') {
      response.writeHead(200, { 'content-type': 'text/plain' });
      response.end('landed');
      return;
    }

    if (path === '/hang') {
      response.writeHead(200, { 'content-type': 'text/plain' });
      response.write('partial');
      return;
    }

    if (path === '/large') {
      response.writeHead(200, { 'content-type': 'application/octet-stream' });
      response.write(new Uint8Array(4096));
      response.end(new Uint8Array(4096));
      return;
    }

    response.writeHead(500, { 'content-type': 'text/plain' });
    response.end('internal error');
  };

/**
 * Starts `server` on an ephemeral loopback port and records the port.
 */
const listen = (local: LocalServer): Promise<void> =>
  new Promise((resolve) => {
    local.server.listen(0, '127.0.0.1', () => {
      const address = local.server.address();
      local.port = typeof address === 'object' && address !== null ? address.port : 0;
      resolve();
    });
  });

/**
 * Stops `server`, dropping idle keep-alive connections first.
 */
const close = (local: LocalServer): Promise<void> =>
  new Promise((resolve) => {
    local.server.closeAllConnections();
    local.server.close(() => resolve());
  });

const origin: LocalServer = { server: createServer(), received: [], port: 0 };
const target: LocalServer = { server: createServer(), received: [], port: 0 };

/**
 * The redirect target is another origin: the same loopback address on another port.
 */
const redirectTarget = (): string => `http://127.0.0.1:${String(target.port)}/final`;

origin.server.on('request', makeHandler(origin.received, redirectTarget));
target.server.on('request', makeHandler(target.received, redirectTarget));

beforeAll(async () => {
  await Promise.all([listen(origin), listen(target)]);
});

afterAll(async () => {
  await Promise.all([close(origin), close(target)]);
});

/**
 * A client for the first server, with credentials that must stay on its origin.
 */
const localClient = (options: { readonly publicAddressesOnly?: boolean } = {}) =>
  makeResilientHttpClient({
    upstream: 'test:local',
    baseUrl: `http://127.0.0.1:${String(origin.port)}`,
    defaultHeaders: { authorization: 'Bearer SECRET_AUTH', 'x-api-key': 'SECRET_KEY' },
    retry: FAST_RETRY,
    maxBodyBytes: 1024,
    publicAddressesOnly: options.publicAddressesOnly ?? false,
  });

const RUNTIME_LAYERS = [
  ['node', NodeResilientHttpLive],
  ['bun', BunResilientHttpLive],
] as const;

describe.each(RUNTIME_LAYERS)('ResilientHttpLive (%s) over a real socket', (name, live) => {
  const run = <A, E>(effect: Effect.Effect<A, E, Layer.Success<typeof live>>) =>
    Effect.runPromiseExit(Effect.provide(effect, live));

  it('decodes a JSON body and retries a 503', async () => {
    const exit = await run(
      Effect.gen(function* () {
        const api = yield* localClient();
        const item = yield* api.getJson('/item', ItemPayloadSchema);
        const text = yield* api.getText(`/flaky?runtime=${name}`);
        return { item, text };
      }),
    );
    expect(exit).toStrictEqual(Exit.succeed({ item: { id: 42, title: 'first' }, text: 'ok' }));
    expect(origin.received.filter((entry) => entry.path === `/flaky?runtime=${name}`)).toHaveLength(
      2,
    );
  });

  it('follows a cross-origin redirect itself and drops the credentials on the way', async () => {
    target.received.length = 0;
    const exit = await run(
      Effect.gen(function* () {
        const api = yield* localClient();
        return yield* api.withWebResponse('/start', (response, finalUrl) =>
          Effect.promise(async () => ({ body: await response.text(), finalUrl })),
        );
      }),
    );
    expect(exit).toStrictEqual(Exit.succeed({ body: 'landed', finalUrl: redirectTarget() }));
    expect(target.received).toStrictEqual([
      { path: '/final', authorization: undefined, apiKey: undefined },
    ]);
  });

  it('keeps a token a client transform adds off a cross-origin hop', async () => {
    target.received.length = 0;
    const exit = await run(
      Effect.gen(function* () {
        const transport = yield* HttpClient.HttpClient;
        const api = yield* makeResilientHttpClient({
          upstream: 'test:local-transform',
          baseUrl: `http://127.0.0.1:${String(origin.port)}`,
        }).pipe(
          Effect.provideService(
            HttpClient.HttpClient,
            HttpClient.mapRequest(transport, HttpClientRequest.bearerToken('TENANT_TOKEN')),
          ),
        );
        return yield* api.getText('/start');
      }),
    );
    expect(exit).toStrictEqual(Exit.succeed('landed'));
    expect(origin.received.at(-1)).toStrictEqual({
      path: '/start',
      authorization: 'Bearer TENANT_TOKEN',
      apiKey: undefined,
    });
    expect(target.received).toStrictEqual([
      { path: '/final', authorization: undefined, apiKey: undefined },
    ]);
  });

  it('fails a streamed body past maxBodyBytes', async () => {
    const exit = await run(
      Effect.gen(function* () {
        const api = yield* localClient();
        return yield* Stream.runDrain(api.stream('/large'));
      }),
    );
    expect(exitOutcome(exit)).toBe('body_too_large');
  });

  it('refuses a loopback address with publicAddressesOnly', async () => {
    const before = origin.received.length;
    const exit = await run(
      Effect.gen(function* () {
        const api = yield* localClient({ publicAddressesOnly: true });
        return yield* api.getText('/item');
      }),
    );
    expect(exitOutcome(exit)).toBe('blocked');
    expect(origin.received.length).toBe(before);
  });

  it('aborts an attempt whose body never ends', async () => {
    const exit = await run(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({
          upstream: 'test:local-hang',
          baseUrl: `http://127.0.0.1:${String(origin.port)}`,
          attemptTimeout: '200 millis',
          retry: { maxRetries: 0 },
        });
        return yield* api.getText('/hang');
      }),
    );
    expect(exitOutcome(exit)).toBe('timeout');
  });

  it('keeps credentials in the query out of an error message', async () => {
    const exit = await run(
      Effect.gen(function* () {
        const api = yield* localClient();
        return yield* api.getText('/broken?token=SECRET_QUERY');
      }),
    );
    const message = exitMessage(exit) ?? '';
    expect(message).toContain('500');
    expect(message).not.toContain('SECRET_QUERY');
  });
});

/**
 * A local server started for one test, with the sockets it still holds open.
 */
type TrackedServer = {
  readonly url: string;
  readonly sockets: ReadonlySet<Socket>;
  readonly close: () => Promise<void>;
};

/**
 * Starts an HTTP server on an ephemeral loopback port that tracks its open sockets.
 */
const startTrackedServer = (
  handler: (request: IncomingMessage, response: ServerResponse) => void,
): Promise<TrackedServer> =>
  new Promise((resolve) => {
    const server = createServer(handler);
    const sockets = new Set<Socket>();
    server.on('connection', (socket) => {
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
    });
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      const port = typeof address === 'object' && address !== null ? address.port : 0;
      resolve({
        url: `http://127.0.0.1:${String(port)}`,
        sockets,
        close: () =>
          new Promise((done) => {
            server.closeAllConnections();
            server.close(() => done());
          }),
      });
    });
  });

/**
 * Waits until `condition` holds, checking every 10 ms for at most 2 seconds.
 */
const waitUntil = (condition: () => boolean): Effect.Effect<void> =>
  Effect.sync(condition).pipe(
    Effect.repeat({ schedule: Schedule.spaced('10 millis'), until: (done) => done }),
    Effect.timeoutOrElse({ duration: '2 seconds', orElse: () => Effect.succeed(false) }),
    Effect.asVoid,
  );

/**
 * A 4 MiB body, larger than any socket buffer, so an unread body keeps its connection busy.
 */
const LARGE_BODY = Buffer.alloc(4 * 1024 * 1024, 97);

/**
 * Answers a 404, a redirect and an over-cap 200 with `LARGE_BODY`, `/ok` with a small JSON body,
 * `/cookies` with two `Set-Cookie` fields, `/gzip` with a gzip-encoded JSON body and `/endless`
 * with a body that never ends, counting the responses the client closed.
 */
const bodyHandler =
  (closed: { count: number }) =>
  (request: IncomingMessage, response: ServerResponse): void => {
    const path = request.url ?? '/';
    if (path === '/missing' || path === '/redirect' || path === '/huge') {
      const status = { '/missing': 404, '/redirect': 302, '/huge': 200 }[path];
      response.writeHead(status, { location: '/ok', 'content-length': LARGE_BODY.length });
      response.end(LARGE_BODY);
      return;
    }

    if (path === '/odd-cookie') {
      response.setHeader('set-cookie', ['a=1; Path=/münchen', 'b=2; Path=/']);
      response.end('ok');
      return;
    }

    if (path === '/cookies') {
      response.setHeader('set-cookie', ['a=1; Path=/', 'b=2; Path=/']);
      response.end('ok');
      return;
    }

    if (path === '/gzip') {
      const body = gzipSync(JSON.stringify({ id: 1 }));
      response.writeHead(200, {
        'content-type': 'application/json',
        'content-encoding': 'gzip',
        'content-length': body.length,
      });
      response.end(body);
      return;
    }

    if (path === '/endless') {
      response.writeHead(200, { 'content-type': 'text/plain' });
      const timer = setInterval(() => response.write('x'.repeat(16 * 1024)), 5);
      response.on('close', () => {
        clearInterval(timer);
        closed.count += 1;
      });
      return;
    }

    response.end('{"ok":true}');
  };

describe.each(RUNTIME_LAYERS)('ResilientHttpLive (%s) response bodies', (_name, live) => {
  const run = <A, E>(effect: Effect.Effect<A, E, Layer.Success<typeof live>>) =>
    Effect.runPromiseExit(Effect.provide(effect, live));
  const closed = { count: 0 };
  let server: TrackedServer;

  beforeAll(async () => {
    server = await startTrackedServer(bodyHandler(closed));
  });

  afterAll(async () => {
    await server.close();
  });

  it('releases the connection of every body it discards', async () => {
    const exit = await run(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({
          upstream: 'test:local-discard',
          baseUrl: server.url,
          maxBodyBytes: 1024,
          retry: { maxRetries: 0 },
        });
        for (let call = 0; call < 30; call += 1) {
          yield* api.getOptionalJson('/missing', Schema.Unknown);
          yield* api.getText('/redirect');
          yield* Effect.exit(api.getText('/huge'));
        }

        // Every discarded body closes its connection; only the keep-alive one of `/ok` may stay.
        yield* waitUntil(() => server.sockets.size <= 1);
        return server.sockets.size <= 1;
      }),
    );
    expect(exit).toStrictEqual(Exit.succeed(true));
  });

  it('cancels a body after a full, a partial or a text read without failing', async () => {
    const exit = await run(
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        const get = client.execute(HttpClientRequest.get(`${server.url}/huge`));
        const drained = yield* get;
        yield* Stream.runDrain(drained.stream);
        yield* cancelResponseBody(drained);
        const partial = yield* get;
        yield* Stream.runHead(partial.stream);
        yield* cancelResponseBody(partial);
        const text = yield* get;
        yield* text.text;
        yield* cancelResponseBody(text);
      }),
    );
    expect(exit).toStrictEqual(Exit.void);
  });

  it('closes the connection when use returns with a reader still locked', async () => {
    closed.count = 0;
    const exit = await run(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({
          upstream: 'test:local-locked',
          baseUrl: server.url,
          maxBodyBytes: 1_000_000_000,
        });
        for (let call = 0; call < 3; call += 1) {
          yield* api.withWebResponse('/endless', (response) =>
            Effect.andThen(
              Effect.promise(async () => response.body?.getReader().read()),
              Effect.sleep('50 millis'),
            ),
          );
        }

        yield* waitUntil(() => closed.count === 3);
        return closed.count;
      }),
    );
    expect(exit).toStrictEqual(Exit.succeed(3));
  });

  it('keeps every Set-Cookie field through getResponse and withWebResponse', async () => {
    const exit = await run(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({
          upstream: 'test:local-cookies',
          baseUrl: server.url,
        });
        const buffered = yield* api.getResponse('/cookies');
        const streamed = yield* api.withWebResponse('/cookies', (response) =>
          Effect.succeed(response.headers.getSetCookie()),
        );
        return { buffered: Object.keys(buffered.cookies.cookies), streamed };
      }),
    );
    expect(exit).toStrictEqual(
      Exit.succeed({ buffered: ['a', 'b'], streamed: ['a=1; Path=/', 'b=2; Path=/'] }),
    );
  });
});

/**
 * Runs `effect` with the `@yieldkit/http/node` layer.
 */
const runOnNode = <A, E>(
  effect: Effect.Effect<A, E, Layer.Success<typeof NodeResilientHttpLive>>,
) => Effect.runPromiseExit(Effect.provide(effect, NodeResilientHttpLive));

describe('ResilientHttpLive (node) response bodies', () => {
  it('keeps a Set-Cookie header Effect cannot write back instead of failing', async () => {
    const server = await startTrackedServer(bodyHandler({ count: 0 }));
    const exit = await runOnNode(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({
          upstream: 'test:local-odd-cookie',
          baseUrl: server.url,
        });
        const response = yield* api.getResponse('/odd-cookie');
        return response.status;
      }),
    );
    await server.close();
    expect(exit).toStrictEqual(Exit.succeed(200));
  });

  it('keeps Content-Encoding on a body Undici returns undecoded', async () => {
    const server = await startTrackedServer(bodyHandler({ count: 0 }));
    const exit = await runOnNode(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({
          upstream: 'test:local-gzip',
          baseUrl: server.url,
          defaultHeaders: { 'accept-encoding': 'gzip' },
        });
        const response = yield* api.getResponse('/gzip');
        const bytes = new Uint8Array(yield* response.arrayBuffer);
        return { encoding: response.headers['content-encoding'], magic: [...bytes.subarray(0, 2)] };
      }),
    );
    await server.close();
    expect(exit).toStrictEqual(Exit.succeed({ encoding: 'gzip', magic: [0x1f, 0x8b] }));
  });

  it('gives a 205 that carries a body anyway an empty body, whatever maxBodyBytes', async () => {
    const payload = 'X'.repeat(5000);
    const raw = createNetServer((socket) => {
      socket.once('data', () => {
        socket.end(
          `HTTP/1.1 205 Reset Content\r\ncontent-length: ${String(payload.length)}\r\nconnection: close\r\n\r\n${payload}`,
        );
      });
    });
    await new Promise<void>((resolve) => {
      raw.listen(0, '127.0.0.1', () => resolve());
    });
    const address = raw.address();
    const port = typeof address === 'object' && address !== null ? address.port : 0;
    const exit = await runOnNode(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({
          upstream: 'test:local-205',
          baseUrl: `http://127.0.0.1:${String(port)}`,
          maxBodyBytes: 1,
          retry: { maxRetries: 0 },
        });
        const response = yield* api.getResponse('/reset');
        return { status: response.status, body: yield* response.text };
      }),
    );
    await new Promise<void>((resolve) => {
      raw.close(() => resolve());
    });
    expect(exit).toStrictEqual(Exit.succeed({ status: 205, body: '' }));
  });
});
