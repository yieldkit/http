import type { HttpClientResponse } from 'effect/http';

import { Effect, Exit, Layer, Metric, Schema } from 'effect';
import { FetchHttpClient, HttpClient, HttpClientError, HttpClientRequest } from 'effect/http';
import { CircuitBreaker } from 'effect/persistence';
import { describe, expect, it } from 'vite-plus/test';

import type { ResolveAddresses } from './address-guard.ts';
import type { HttpPolicy } from './policy.ts';

import { AddressResolver, guardedExecute } from './address-guard.ts';
import { classifyCircuitExit } from './circuit.ts';
import { ResilientHttp, makeHttpResilienceLayer, makeResilientHttpClient } from './client.ts';
import { HttpRequestError } from './errors.ts';
import { executeTextRequest } from './execute.ts';
import { exitMessage, exitOutcome, makeMockHttpClient, squashExitFailure } from './http.fixture.ts';
import { httpRequestsTotal } from './metrics.ts';

/**
 * Retry policy with near-zero backoff so retry tests run on the real clock.
 */
const FAST_RETRY = { maxRetries: 2, baseDelay: '1 millis' } as const;

/**
 * Public addresses the stub resolver answers for the test hostnames.
 */
const PUBLIC_DNS = {
  'www.example.com': ['93.184.215.14'],
  'cdn.example.net': ['2606:4700::6810:84e5'],
  'mixed.example.com': ['93.184.215.14', '10.0.0.5'],
  'rebind.example.com': ['127.0.0.1'],
} satisfies Readonly<Record<string, ReadonlyArray<string>>>;

/**
 * One request seen by the recording transport.
 */
type SeenRequest = {
  readonly url: string;
  readonly method: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly bodyTag: string;
};

/**
 * Builds a stub resolver over `answers` that records every hostname it is asked for. An unknown
 * hostname fails like `getaddrinfo` does with `ENOTFOUND`.
 */
const makeResolver = (
  answers: Readonly<Record<string, ReadonlyArray<string>>> = PUBLIC_DNS,
  lookups: Array<string> = [],
): ResolveAddresses => {
  return (hostname) => {
    lookups.push(hostname);
    const found = answers[hostname];
    if (found === undefined) {
      return Promise.reject(
        Object.assign(new Error(`getaddrinfo ${hostname}`), { code: 'ENOTFOUND' }),
      );
    }

    return Promise.resolve(found);
  };
};

/**
 * Builds a transport that records each request and answers it with `respond`.
 */
const makeRecordingClient = (respond: (url: URL, attempt: number) => Response) => {
  const seen: Array<SeenRequest> = [];
  const client = makeMockHttpClient((request, url, attempt) =>
    Effect.sync(() => {
      seen.push({
        url: url.href,
        method: request.method,
        headers: { ...request.headers },
        bodyTag: request.body._tag,
      });
      return respond(url, attempt);
    }),
  );
  return { client, seen };
};

/**
 * Builds a redirect response to `location`.
 */
const redirectTo = (location: string, status = 302): Response =>
  new Response(null, { status, headers: { location } });

/**
 * Runs `getText(url)` through a guarded upstream client with the stub resolver.
 */
const guardedGetText = (params: {
  readonly client: HttpClient.HttpClient;
  readonly url: string;
  readonly resolver?: ResolveAddresses | undefined;
  readonly policy?: Partial<HttpPolicy> | undefined;
  readonly headers?: Readonly<Record<string, string>> | undefined;
}) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const http = yield* makeResilientHttpClient({
        upstream: 'test:guard',
        publicAddressesOnly: true,
        retry: FAST_RETRY,
        ...params.policy,
      });
      return yield* Effect.exit(http.getText(params.url, { headers: params.headers }));
    }).pipe(
      Effect.provideService(HttpClient.HttpClient, params.client),
      Effect.provideService(AddressResolver, params.resolver ?? makeResolver()),
    ),
  );

/**
 * Asserts that `url` is refused with outcome `blocked` before any request is sent.
 */
const expectRefusedWithoutRequest = async (url: string) => {
  const { client, seen } = makeRecordingClient(() => new Response('secret'));
  const lookups: Array<string> = [];
  const exit = await guardedGetText({ client, url, resolver: makeResolver(PUBLIC_DNS, lookups) });
  expect({ url, outcome: exitOutcome(exit) }).toEqual({ url, outcome: 'blocked' });
  expect({ url, requests: seen.length }).toEqual({ url, requests: 0 });
  expect(exitMessage(exit)).toContain('test:guard refused ');
  expect(exitMessage(exit)).not.toContain('/latest');
  return { exit, lookups };
};

/**
 * Sends a JSON POST through the guard to a transport that redirects it with `status`.
 */
const runPostRedirect = async (status: number) => {
  const recording = makeRecordingClient((url) =>
    url.pathname === '/submit' ? redirectTo('/result', status) : new Response('done'),
  );
  const request = HttpClientRequest.post('https://www.example.com/submit').pipe(
    HttpClientRequest.bodyText('{"a":1}', 'application/json'),
    HttpClientRequest.setHeader('content-language', 'en'),
  );
  const result = await Effect.runPromise(
    Effect.exit(
      executeTextRequest({
        client: recording.client,
        request,
        upstream: 'test:guard-post',
        publicAddressesOnly: true,
      }),
    ).pipe(Effect.provideService(AddressResolver, makeResolver())),
  );
  return { result, seen: recording.seen };
};

describe('public-address guard', () => {
  it.each(['127.0.0.1', '10.1.2.3', '169.254.169.254', '100.64.0.1', '0.0.0.0'])(
    'refuses the IPv4 literal %s without sending a request or resolving',
    async (host) => {
      const { exit, lookups } = await expectRefusedWithoutRequest(
        `http://${host}/latest/meta-data?token=abc`,
      );
      expect(exitMessage(exit)).toBe(`test:guard refused http://${host}: not a public address`);
      expect(lookups).toEqual([]);
    },
  );

  it.each(['http://2130706433/latest', 'http://0x7f.1/latest', 'http://127.0.0.1./latest'])(
    'refuses %s, which URL parsing normalizes to 127.0.0.1',
    async (url) => {
      const { exit } = await expectRefusedWithoutRequest(url);
      expect(exitMessage(exit)).toBe('test:guard refused http://127.0.0.1: not a public address');
    },
  );

  it.each([
    '[::1]',
    '[fe80::1]',
    '[fc00::1]',
    '[::ffff:127.0.0.1]',
    '[64:ff9b::7f00:1]',
    '[::127.0.0.1]',
  ])('refuses the IPv6 literal %s', async (host) => {
    const { exit } = await expectRefusedWithoutRequest(`http://${host}/latest`);
    expect(exitMessage(exit)).toContain('not a public address');
  });

  it.each([
    ['http://localhost/latest', 'test:guard refused http://localhost: not a public hostname'],
    ['http://localhost./latest', 'test:guard refused http://localhost.: not a public hostname'],
    [
      'http://api.localhost/latest',
      'test:guard refused http://api.localhost: not a public hostname',
    ],
    ['http://foo.internal/latest', 'test:guard refused http://foo.internal: not a public hostname'],
    [
      'http://printer.local/latest',
      'test:guard refused http://printer.local: not a public hostname',
    ],
    [
      'http://nas.home.arpa/latest',
      'test:guard refused http://nas.home.arpa: not a public hostname',
    ],
    ['http://router.lan/latest', 'test:guard refused http://router.lan: not a public hostname'],
    ['http://intranet/latest', 'test:guard refused http://intranet: not a public hostname'],
    [
      'http://user:pass@www.example.com/latest',
      'test:guard refused http://www.example.com: credentials in the URL are not allowed',
    ],
    [
      'ftp://www.example.com/latest',
      'test:guard refused ftp://www.example.com: protocol ftp: is not allowed',
    ],
  ])('refuses %s before resolving', async (url, message) => {
    const { exit, lookups } = await expectRefusedWithoutRequest(url);
    expect(exitMessage(exit)).toBe(message);
    expect(lookups).toEqual([]);
  });

  it('refuses a name when any resolved address is not public', async () => {
    const { exit } = await expectRefusedWithoutRequest('https://mixed.example.com/latest');
    expect(exitMessage(exit)).toBe(
      'test:guard refused https://mixed.example.com: resolves to a non-public address',
    );
    await expectRefusedWithoutRequest('https://rebind.example.com/latest');
  });

  it('fails closed on a resolver error or an empty answer, and retries the lookup', async () => {
    const { client, seen } = makeRecordingClient(() => new Response('ok'));
    const lookups: Array<string> = [];
    const exit = await guardedGetText({
      client,
      url: 'https://unknown.example.org/latest',
      resolver: makeResolver(PUBLIC_DNS, lookups),
    });
    expect(exitOutcome(exit)).toBe('network_error');
    expect(exitMessage(exit)).toBe(
      'test:guard could not resolve https://unknown.example.org: ENOTFOUND',
    );
    expect(lookups).toHaveLength(FAST_RETRY.maxRetries + 1);
    expect(seen).toHaveLength(0);

    const empty = await guardedGetText({
      client,
      url: 'https://empty.example.org/latest',
      resolver: makeResolver({ 'empty.example.org': [] }),
    });
    expect(exitOutcome(empty)).toBe('network_error');
    expect(exitMessage(empty)).toContain('no addresses');
    expect(seen).toHaveLength(0);
  });

  it('sends the request only after a retried lookup succeeds', async () => {
    const { client, seen } = makeRecordingClient(() => new Response('ok'));
    let lookups = 0;
    const flaky: ResolveAddresses = () => {
      lookups += 1;
      return lookups === 1
        ? Promise.reject(Object.assign(new Error('temporary'), { code: 'EAI_AGAIN' }))
        : Promise.resolve(['93.184.215.14']);
    };
    const exit = await guardedGetText({
      client,
      url: 'https://www.example.com/latest',
      resolver: flaky,
    });
    expect(exit).toEqual(Exit.succeed('ok'));
    expect(lookups).toBe(2);
    expect(seen).toHaveLength(1);
  });

  it('lets a public host through', async () => {
    const { client, seen } = makeRecordingClient(() => new Response('hello'));
    const lookups: Array<string> = [];
    const exit = await guardedGetText({
      client,
      url: 'https://www.example.com/latest',
      resolver: makeResolver(PUBLIC_DNS, lookups),
    });
    expect(exit).toEqual(Exit.succeed('hello'));
    expect(lookups).toEqual(['www.example.com']);
    expect(seen.map((request) => request.url)).toEqual(['https://www.example.com/latest']);
    const literal = await guardedGetText({ client, url: 'http://93.184.215.14/' });
    expect(literal).toEqual(Exit.succeed('hello'));
  });

  it('records a refusal once as blocked, without retries or a circuit failure', async () => {
    const upstream = 'test:guard-metrics';
    const { client, seen } = makeRecordingClient(() => redirectTo('http://169.254.169.254/'));
    const exit = await guardedGetText({
      client,
      url: 'https://www.example.com/start',
      policy: { upstream },
    });
    const blocked = await Effect.runPromise(
      Metric.value(
        Metric.withAttributes(httpRequestsTotal, { upstream, method: 'GET', outcome: 'blocked' }),
      ),
    );
    expect(seen).toHaveLength(1);
    expect(exitMessage(exit)).toBe(
      `${upstream} refused http://169.254.169.254: not a public address`,
    );
    expect(blocked.count).toBe(1);
    const failure = squashExitFailure(exit);
    expect(failure).toBeInstanceOf(HttpRequestError);
    const circuit = Schema.is(HttpRequestError)(failure)
      ? classifyCircuitExit(Exit.fail(failure))
      : null;
    expect(circuit).toBe(CircuitBreaker.ignored);
  });

  it('leaves policies without the opt-in unchecked', async () => {
    const { client, seen } = makeRecordingClient(() => new Response('local'));
    const lookups: Array<string> = [];
    const exit = await guardedGetText({
      client,
      url: 'http://localhost:8080/health',
      resolver: makeResolver(PUBLIC_DNS, lookups),
      policy: { publicAddressesOnly: false },
    });
    expect(exit).toEqual(Exit.succeed('local'));
    expect(lookups).toEqual([]);
    expect(seen).toHaveLength(1);
  });
});

describe('guarded redirects', () => {
  it('drops credentials on a cross-origin hop and keeps them dropped on the way back', async () => {
    const { client, seen } = makeRecordingClient((url) => {
      if (url.pathname === '/start') {
        return redirectTo('https://cdn.example.net/hop');
      }

      if (url.pathname === '/hop') {
        return redirectTo('https://www.example.com/final');
      }

      return new Response('done');
    });
    const exit = await guardedGetText({
      client,
      url: 'https://www.example.com/start',
      headers: {
        authorization: 'Bearer s3cret',
        cookie: 'session=1',
        'x-api-key': 'k',
        accept: 'text/html',
      },
    });
    expect(exit).toEqual(Exit.succeed('done'));
    expect(seen.map((request) => request.url)).toEqual([
      'https://www.example.com/start',
      'https://cdn.example.net/hop',
      'https://www.example.com/final',
    ]);
    expect(seen[0]?.headers).toMatchObject({ authorization: 'Bearer s3cret', cookie: 'session=1' });
    for (const hop of seen.slice(1)) {
      expect(Object.keys(hop.headers).toSorted()).toEqual(['accept']);
    }
  });

  it('keeps credentials on a same-origin hop', async () => {
    const { client, seen } = makeRecordingClient((url) =>
      url.pathname === '/start' ? redirectTo('/final', 301) : new Response('done'),
    );
    const exit = await guardedGetText({
      client,
      url: 'https://www.example.com/start',
      headers: { authorization: 'Bearer s3cret', cookie: 'session=1' },
    });
    expect(exit).toEqual(Exit.succeed('done'));
    expect(seen[1]?.url).toBe('https://www.example.com/final');
    expect(seen[1]?.headers).toMatchObject({ authorization: 'Bearer s3cret', cookie: 'session=1' });
  });

  it('takes the next query from the Location only', async () => {
    const { client, seen } = makeRecordingClient((url) =>
      url.pathname === '/start' ? redirectTo('/final?page=2') : new Response('done'),
    );
    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({
          upstream: 'test:guard',
          publicAddressesOnly: true,
        });
        return yield* Effect.exit(
          http.getText('https://www.example.com/start', { urlParams: { token: 'abc' } }),
        );
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provideService(AddressResolver, makeResolver()),
      ),
    );
    expect(exit).toEqual(Exit.succeed('done'));
    expect(seen.map((request) => request.url)).toEqual([
      'https://www.example.com/start?token=abc',
      'https://www.example.com/final?page=2',
    ]);
  });

  it('refuses a redirect to the metadata address after exactly one request', async () => {
    const { client, seen } = makeRecordingClient(() => redirectTo('http://169.254.169.254/latest'));
    const exit = await guardedGetText({ client, url: 'https://www.example.com/start' });
    expect(exitOutcome(exit)).toBe('blocked');
    expect(seen).toHaveLength(1);
  });

  it('turns a POST into a bodyless GET after a 303 and keeps the method after a 307', async () => {
    const seeOther = await runPostRedirect(303);
    expect(seeOther.result).toEqual(Exit.succeed('done'));
    expect(seeOther.seen[0]).toMatchObject({ method: 'POST', bodyTag: 'Uint8Array' });
    expect(seeOther.seen[1]).toMatchObject({ method: 'GET', bodyTag: 'Empty' });
    expect(seeOther.seen[1]?.headers['content-type']).toBeUndefined();
    expect(seeOther.seen[1]?.headers['content-language']).toBeUndefined();

    const temporary = await runPostRedirect(307);
    expect(temporary.seen[1]).toMatchObject({ method: 'POST', bodyTag: 'Uint8Array' });
    expect(temporary.seen[1]?.headers['content-type']).toBe('application/json');
  });

  it.each([307, 308])(
    'refuses a cross-origin %i that would forward the POST body, after one request',
    async (status) => {
      const recording = makeRecordingClient(() =>
        redirectTo('https://cdn.example.net/collect', status),
      );
      const request = HttpClientRequest.post('https://www.example.com/submit').pipe(
        HttpClientRequest.bodyText('{"secret":1}', 'application/json'),
      );
      const exit = await Effect.runPromise(
        Effect.exit(
          executeTextRequest({
            client: recording.client,
            request,
            upstream: 'test:guard-post',
            publicAddressesOnly: true,
          }),
        ).pipe(Effect.provideService(AddressResolver, makeResolver())),
      );
      expect(exitOutcome(exit)).toBe('blocked');
      expect(exitMessage(exit)).toBe(
        `test:guard-post refused https://www.example.com: a ${String(status)} redirect to another origin would forward the request body`,
      );
      expect(recording.seen).toHaveLength(1);
    },
  );

  it('refuses a cross-origin 307 that would forward the body of a GET, after one request', async () => {
    const recording = makeRecordingClient(() => redirectTo('https://cdn.example.net/search', 307));
    const request = HttpClientRequest.get('https://www.example.com/search').pipe(
      HttpClientRequest.bodyText('{"secret":1}', 'application/json'),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({ client: recording.client, request, upstream: 'test:guard-get' }),
    );
    expect(exitMessage(exit)).toBe(
      'test:guard-get refused https://www.example.com: a 307 redirect to another origin would forward the request body',
    );
    expect(recording.seen).toHaveLength(1);
  });

  it('still follows a cross-origin 307 for a request without a body', async () => {
    const { client, seen } = makeRecordingClient((url) =>
      url.hostname === 'www.example.com'
        ? redirectTo('https://cdn.example.net/data', 307)
        : new Response('done'),
    );
    const exit = await guardedGetText({ client, url: 'https://www.example.com/start' });
    expect(exit).toEqual(Exit.succeed('done'));
    expect(seen).toHaveLength(2);
  });

  it('refuses the hop past maxRedirects', async () => {
    const { client, seen } = makeRecordingClient((url) =>
      redirectTo(`/loop-${String(Number(url.pathname.slice('/loop-'.length) || '0') + 1)}`),
    );
    const exit = await guardedGetText({
      client,
      url: 'https://www.example.com/loop-0',
      policy: { maxRedirects: 2 },
    });
    expect(exitOutcome(exit)).toBe('blocked');
    expect(exitMessage(exit)).toBe(
      'test:guard refused https://www.example.com: more than 2 redirects',
    );
    expect(seen).toHaveLength(3);

    const defaults = makeRecordingClient(() => redirectTo('/again'));
    await guardedGetText({ client: defaults.client, url: 'https://www.example.com/again' });
    expect(defaults.seen).toHaveLength(6);
  });

  it('refuses an invalid Location without retrying', async () => {
    const { client, seen } = makeRecordingClient(() => redirectTo('http://[::1'));
    const exit = await guardedGetText({ client, url: 'https://www.example.com/start' });
    expect(exitOutcome(exit)).toBe('blocked');
    expect(exitMessage(exit)).toBe(
      'test:guard refused https://www.example.com: invalid redirect location',
    );
    expect(seen).toHaveLength(1);
  });

  it('returns a 3xx without Location as it is', async () => {
    const { client, seen } = makeRecordingClient(() => new Response(null, { status: 302 }));
    const response = await Effect.runPromise(
      guardedExecute(client, HttpClientRequest.get('https://www.example.com/start'), {
        upstream: 'test:guard',
      }).pipe(Effect.provideService(AddressResolver, makeResolver())),
    );
    expect(response.status).toBe(302);
    expect(seen).toHaveLength(1);
  });

  it('reports the last hop as finalUrl and response url', async () => {
    const { client } = makeRecordingClient((url) =>
      url.hostname === 'www.example.com'
        ? redirectTo('https://cdn.example.net/article')
        : new Response('<title>x</title>'),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({
          upstream: 'test:guard',
          publicAddressesOnly: true,
        });
        const finalUrl = yield* http.withWebResponse(
          'https://www.example.com/post',
          (_response, url) => Effect.succeed(url),
        );
        const response: HttpClientResponse.HttpClientResponse = yield* http.getResponse(
          'https://www.example.com/post',
        );
        return { finalUrl, responseUrl: response.url };
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provideService(AddressResolver, makeResolver()),
      ),
    );
    expect(result).toEqual({
      finalUrl: 'https://cdn.example.net/article',
      responseUrl: 'https://cdn.example.net/article',
    });
  });
});

/**
 * One call seen by the stub `fetch`.
 */
type FetchCall = {
  readonly url: string;
  readonly redirect: RequestInit['redirect'];
  readonly cache: RequestInit['cache'];
  readonly authorization: string | null;
  readonly apiKey: string | null;
  readonly ambientHeader: string | null;
};

/**
 * Returns the URL string `fetch` was called with.
 */
const requestTarget = (input: string | URL | Request): string => {
  if (typeof input === 'string') {
    return input;
  }

  return input instanceof URL ? input.href : input.url;
};

/**
 * Builds a stub `fetch` that answers `/start` with a 302 to another origin and everything else with
 * a page, recording the options each call received. It never touches the network.
 */
const makeStubFetch = (location = 'https://cdn.example.net/page') => {
  const calls: Array<FetchCall> = [];
  const stub = (input: string | URL | Request, init?: RequestInit): Promise<Response> => {
    const url = requestTarget(input);
    const headers = new Headers(init?.headers);
    calls.push({
      url,
      redirect: init?.redirect,
      cache: init?.cache,
      authorization: headers.get('authorization'),
      apiKey: headers.get('x-api-key'),
      ambientHeader: headers.get('x-ambient-token'),
    });
    const response = new URL(url).pathname.endsWith('/start')
      ? new Response(null, { status: 302, headers: { location } })
      : new Response('page');
    return Promise.resolve(response);
  };
  const fetch: typeof globalThis.fetch = stub;
  return { fetch, calls };
};

/**
 * Runs `getText('/start')` over the stub `fetch` through a client whose `prependUrl` transform
 * makes the URL absolute, with a custom credential header, and returns the exit and the calls
 * seen.
 */
const getTextThroughPrependUrl = async (maxRedirects: number) => {
  const stub = makeStubFetch();
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const transport = yield* HttpClient.HttpClient;
      const http = yield* makeResilientHttpClient({
        upstream: 'test:fetch-relative',
        defaultHeaders: { 'x-api-key': 'SECRET-KEY' },
        maxRedirects,
      }).pipe(
        Effect.provideService(
          HttpClient.HttpClient,
          HttpClient.mapRequest(transport, HttpClientRequest.prependUrl('https://www.example.com')),
        ),
      );
      return yield* Effect.exit(http.getText('/start'));
    }).pipe(
      Effect.provide(FetchHttpClient.layer),
      Effect.provideService(FetchHttpClient.Fetch, stub.fetch),
    ),
  );
  return {
    result,
    calls: stub.calls.map(({ url, redirect, apiKey }) => ({ url, redirect, apiKey })),
  };
};

describe('guarded requests over FetchHttpClient', () => {
  it('sends every hop with redirect manual and follows the redirect itself', async () => {
    const { fetch, calls } = makeStubFetch();
    const layer = ResilientHttp.layer.pipe(Layer.provide(FetchHttpClient.layer));
    const body = await Effect.runPromise(
      Effect.gen(function* () {
        const httpFactory = yield* ResilientHttp;
        const http = yield* httpFactory.client({
          upstream: 'test:fetch',
          publicAddressesOnly: true,
        });
        return yield* http.getText('https://www.example.com/start', {
          headers: { authorization: 'Bearer s3cret' },
        });
      }).pipe(
        Effect.provide(layer),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
        Effect.provideService(FetchHttpClient.RequestInit, {
          cache: 'no-store',
          headers: { 'x-ambient-token': 'ambient' },
        }),
        Effect.provideService(AddressResolver, (hostname) =>
          Promise.resolve(hostname === 'www.example.com' ? ['93.184.215.14'] : ['2606:4700::1']),
        ),
      ),
    );
    expect(body).toBe('page');
    expect(calls).toEqual([
      {
        url: 'https://www.example.com/start',
        redirect: 'manual',
        cache: 'no-store',
        authorization: 'Bearer s3cret',
        apiKey: null,
        ambientHeader: 'ambient',
      },
      {
        url: 'https://cdn.example.net/page',
        redirect: 'manual',
        cache: 'no-store',
        authorization: null,
        apiKey: null,
        ambientHeader: null,
      },
    ]);
  });

  it('follows redirects itself when the guard is off, so custom credentials stay on their origin', async () => {
    const { fetch, calls } = makeStubFetch();
    const body = await Effect.runPromise(
      Effect.gen(function* () {
        const httpFactory = yield* ResilientHttp;
        const http = yield* httpFactory.client({
          upstream: 'test:fetch-plain',
          baseUrl: 'https://www.example.com',
          defaultHeaders: { 'x-api-key': 'SECRET-KEY' },
        });
        return yield* http.getText('/start');
      }).pipe(
        Effect.provide(ResilientHttp.layer.pipe(Layer.provide(FetchHttpClient.layer))),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
        Effect.provideService(FetchHttpClient.RequestInit, {
          headers: { 'x-ambient-token': 'ambient' },
        }),
      ),
    );
    expect(body).toBe('page');
    expect(
      calls.map(({ url, redirect, apiKey, ambientHeader }) => ({
        url,
        redirect,
        apiKey,
        ambientHeader,
      })),
    ).toEqual([
      {
        url: 'https://www.example.com/start',
        redirect: 'manual',
        apiKey: 'SECRET-KEY',
        ambientHeader: 'ambient',
      },
      {
        url: 'https://cdn.example.net/page',
        redirect: 'manual',
        apiKey: null,
        ambientHeader: null,
      },
    ]);
  });

  it.each([
    ['a file: URL', 'file:///etc/passwd'],
    ['a data: URL', 'data:text/plain,secret'],
    ['a URL with credentials', 'https://user:pass@cdn.example.net/page'],
  ])('refuses a redirect to %s even when the guard is off', async (_label, location) => {
    const { fetch, calls } = makeStubFetch(location);
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const httpFactory = yield* ResilientHttp;
        const http = yield* httpFactory.client({
          upstream: 'test:fetch-plain',
          baseUrl: 'https://www.example.com',
        });
        return yield* http.getText('/start');
      }).pipe(
        Effect.provide(ResilientHttp.layer.pipe(Layer.provide(FetchHttpClient.layer))),
        Effect.provideService(FetchHttpClient.Fetch, fetch),
      ),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(calls.map(({ url }) => url)).toEqual(['https://www.example.com/start']);
  });

  it('follows the redirects of a URL a transform made absolute, within maxRedirects', async () => {
    const followed = await getTextThroughPrependUrl(1);
    expect(followed.result).toEqual(Exit.succeed('page'));
    expect(followed.calls).toEqual([
      { url: 'https://www.example.com/start', redirect: 'manual', apiKey: 'SECRET-KEY' },
      { url: 'https://cdn.example.net/page', redirect: 'manual', apiKey: null },
    ]);
    const limited = await getTextThroughPrependUrl(0);
    expect(exitMessage(limited.result)).toBe(
      'test:fetch-relative refused https://www.example.com: more than 0 redirects',
    );
    expect(limited.calls).toHaveLength(1);
  });

  it('refuses a guarded response that the transport redirected on its own', async () => {
    const lookups: Array<string> = [];
    const followingTransport = HttpClient.followRedirects(
      makeMockHttpClient((request) =>
        Effect.succeed(
          request.url.includes('169.254.169.254')
            ? new Response('METADATA')
            : redirectTo('http://169.254.169.254/latest/meta-data'),
        ),
      ),
    );
    const exit = await guardedGetText({
      client: followingTransport,
      url: 'https://www.example.com/x',
      resolver: makeResolver(PUBLIC_DNS, lookups),
    });
    expect(exitOutcome(exit)).toBe('blocked');
    expect(exitMessage(exit)).toContain('the transport followed a redirect the guard never saw');
  });
});

describe('guarded requests through client transforms', () => {
  it('checks the URL a transform rewrote, before any request', async () => {
    const { client, seen } = makeRecordingClient(() => new Response('internal'));
    const rewriting = HttpClient.mapRequest(
      client,
      HttpClientRequest.setUrl('http://127.0.0.1:1/internal'),
    );
    const exit = await guardedGetText({ client: rewriting, url: 'https://www.example.com/x' });
    expect(exitOutcome(exit)).toBe('blocked');
    expect(exitMessage(exit)).toBe('test:guard refused http://127.0.0.1:1: not a public address');
    expect(seen).toHaveLength(0);
  });

  it('keeps a token a transform added off a cross-origin hop', async () => {
    const { client, seen } = makeRecordingClient((url) =>
      url.hostname === 'www.example.com'
        ? redirectTo('https://cdn.example.net/page')
        : new Response('page'),
    );
    const exit = await guardedGetText({
      client: HttpClient.mapRequest(client, HttpClientRequest.bearerToken('TENANT_TOKEN')),
      url: 'https://www.example.com/start',
    });
    expect(exit).toEqual(Exit.succeed('page'));
    expect(seen.map((request) => request.headers.authorization)).toEqual([
      'Bearer TENANT_TOKEN',
      undefined,
    ]);
  });

  it('maps a failing request transform to a sanitised error, before any request', async () => {
    const { client, seen } = makeRecordingClient(() => new Response('ok'));
    const failing = HttpClient.mapRequestEffect(client, (request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({
            request,
            description: 'token service rejected Bearer SECRET_TOKEN_1',
          }),
        }),
      ),
    );
    const exit = await guardedGetText({
      client: failing,
      url: 'https://www.example.com/x?token=SECRET_QUERY',
      headers: { authorization: 'Bearer SECRET_TOKEN_1' },
    });
    expect(squashExitFailure(exit)).toBeInstanceOf(HttpRequestError);
    expect(exitOutcome(exit)).toBe('network_error');
    expect(exitMessage(exit)).toBe(
      'test:guard request failed: Transport error: token service rejected <redacted> (GET https://www.example.com)',
    );
    expect(seen).toHaveLength(0);
  });

  it('masks a token an earlier transform added when a later transform fails', async () => {
    const { client, seen } = makeRecordingClient(() => new Response('ok'));
    const withToken = HttpClient.mapRequest(
      client,
      HttpClientRequest.bearerToken('ADDED_TOKEN_77'),
    );
    const failing = HttpClient.mapRequestEffect(withToken, (request) =>
      Effect.fail(
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({
            request,
            description: `signer rejected ${request.headers.authorization ?? ''}`,
          }),
        }),
      ),
    );
    const exit = await guardedGetText({ client: failing, url: 'https://www.example.com/x' });
    expect(exitMessage(exit)).not.toContain('ADDED_TOKEN_77');
    expect(seen).toHaveLength(0);
  });

  it('refuses a header value with a line break before sending, without opening the breaker', async () => {
    const { client, seen } = makeRecordingClient(() => new Response('ok'));
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const http = yield* makeResilientHttpClient({
          upstream: 'test:guard-header',
          retry: FAST_RETRY,
          circuitBreaker: {},
        });
        const refused = yield* Effect.forEach(Array.from({ length: 5 }), () =>
          Effect.exit(
            http.getText('https://www.example.com/x', {
              headers: { 'x-api-key': 'SECRET_KEY\n' },
            }),
          ),
        );
        const healthy = yield* Effect.exit(http.getText('https://www.example.com/x'));
        return { refused, healthy };
      }).pipe(
        Effect.provide(makeHttpResilienceLayer({ consecutiveFailures: 2 })),
        Effect.provideService(HttpClient.HttpClient, client),
      ),
    );
    expect(result.refused.map(exitOutcome)).toEqual(Array.from({ length: 5 }, () => 'blocked'));
    expect(exitMessage(result.refused[0] ?? Exit.void)).toBe(
      'test:guard-header refused: header x-api-key contains a line break or NUL character',
    );
    expect(result.healthy).toEqual(Exit.succeed('ok'));
    expect(seen).toHaveLength(1);
  });
});
