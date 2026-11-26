import { Option, Stream } from 'effect';
import { HttpBody, HttpClientRequest } from 'effect/http';
import { Readable } from 'node:stream';
import { describe, expect, it } from 'vite-plus/test';

import { makeClientResponse } from './http.fixture.ts';
import {
  blocksBodyReplay,
  isCrossOrigin,
  isOneShotBody,
  redirectLocation,
  redirectRequest,
  replaysStreamBody,
  safelistedHeaderInit,
} from './redirects.ts';

/**
 * Builds a redirect response with `status` and an optional `Location`.
 */
const redirectResponse = (status: number, location?: string) =>
  makeClientResponse(
    new Response(null, { status, headers: location === undefined ? {} : { location } }),
  );

/**
 * Builds a POST with a JSON body and a mix of body, credential and safelisted headers.
 */
const makePost = (method: 'POST' | 'PUT' | 'PATCH' | 'DELETE' = 'POST') =>
  HttpClientRequest.make(method)('https://www.example.com/submit?old=1', {
    headers: {
      accept: 'application/json',
      authorization: 'Bearer s3cret',
      'content-language': 'en',
      'content-location': '/x',
      'x-trace': 'abc',
    },
  }).pipe(HttpClientRequest.bodyJsonUnsafe({ a: 1 }));

const FROM = new URL('https://www.example.com/submit?old=1');
const SAME_ORIGIN = new URL('https://www.example.com/next?new=2');
const OTHER_ORIGIN = new URL('https://cdn.example.net/next?new=2');

describe('redirectLocation', () => {
  it.each([301, 302, 303, 307, 308])('returns the Location of a %i', (status) => {
    expect(redirectLocation(redirectResponse(status, '/next'))).toBe('/next');
  });

  it.each([200, 204, 300, 304, 305, 404])('ignores the Location of a %i', (status) => {
    expect(redirectLocation(redirectResponse(status, '/next'))).toBeNull();
  });

  it('returns null for a redirect status without a Location header', () => {
    expect(redirectLocation(redirectResponse(302))).toBeNull();
  });

  it('returns the raw Location without resolving it', () => {
    expect(redirectLocation(redirectResponse(301, '../a b?x=1'))).toBe('../a b?x=1');
  });
});

describe('redirectRequest', () => {
  it.each([301, 302, 303])('turns a POST into a bodyless GET after a %i', (status) => {
    const next = redirectRequest({
      request: makePost(),
      status,
      from: FROM,
      to: SAME_ORIGIN,
    });
    expect(next.method).toBe('GET');
    expect(next.body._tag).toBe('Empty');
    expect(next.headers).not.toHaveProperty('content-type');
    expect(next.headers).not.toHaveProperty('content-length');
    expect(next.headers).not.toHaveProperty('content-language');
    expect(next.headers).not.toHaveProperty('content-location');
    expect(next.headers).toMatchObject({
      accept: 'application/json',
      authorization: 'Bearer s3cret',
      'x-trace': 'abc',
    });
  });

  it.each(['PUT', 'PATCH', 'DELETE'] as const)('turns a %s into a GET after a 303', (method) => {
    const next = redirectRequest({
      request: makePost(method),
      status: 303,
      from: FROM,
      to: SAME_ORIGIN,
    });
    expect(next.method).toBe('GET');
    expect(next.body._tag).toBe('Empty');
  });

  it.each([
    ['PUT', 301],
    ['DELETE', 302],
    ['PATCH', 301],
  ] as const)('keeps a %s with its method and body after a %i, as fetch does', (method, status) => {
    const request = makePost(method);
    const next = redirectRequest({ request, status, from: FROM, to: SAME_ORIGIN });
    expect(next.method).toBe(method);
    expect(next.body).toBe(request.body);
  });

  it('sends the target query byte for byte, so a signed URL stays valid', () => {
    const next = redirectRequest({
      request: HttpClientRequest.get('https://www.example.com/start', {
        urlParams: { page: '2' },
      }),
      status: 302,
      from: FROM,
      to: new URL('https://www.example.com/signed?q=a%20b&flag&x=~#part'),
    });
    expect(Option.getOrThrow(HttpClientRequest.toUrl(next)).href).toBe(
      'https://www.example.com/signed?q=a%20b&flag&x=~',
    );
  });

  it.each([307, 308])('keeps the method, body and body headers after a %i', (status) => {
    const request = makePost();
    const next = redirectRequest({ request, status, from: FROM, to: SAME_ORIGIN });
    expect(next.method).toBe('POST');
    expect(next.body).toBe(request.body);
    expect(next.headers['content-type']).toBe('application/json');
  });

  it('keeps a GET and a HEAD as they are after a 303', () => {
    for (const method of ['GET', 'HEAD'] as const) {
      const request = HttpClientRequest.make(method)('https://www.example.com/page', {
        headers: { accept: 'text/html' },
      });
      const next = redirectRequest({
        request,
        status: 303,
        from: FROM,
        to: SAME_ORIGIN,
      });
      expect(next.method).toBe(method);
      expect(next.headers).toEqual(request.headers);
    }
  });

  it('takes the URL and the query from the target alone', () => {
    const next = redirectRequest({
      request: HttpClientRequest.get('https://www.example.com/start', {
        urlParams: { page: '2', token: 'abc' },
      }),
      status: 302,
      from: FROM,
      to: new URL('https://www.example.com/final?only=this'),
    });
    expect(Option.getOrThrow(HttpClientRequest.toUrl(next)).href).toBe(
      'https://www.example.com/final?only=this',
    );
  });

  it('keeps every header on a same-origin hop', () => {
    const request = makePost();
    const next = redirectRequest({ request, status: 307, from: FROM, to: SAME_ORIGIN });
    expect(next.headers).toEqual(request.headers);
  });

  it.each([
    ['host', 'https://cdn.example.net/next'],
    ['scheme', 'http://www.example.com/next'],
    ['port', 'https://www.example.com:8443/next'],
  ])('keeps only safelisted headers when the %s changes', (_change, target) => {
    const request = HttpClientRequest.get('https://www.example.com/start', {
      headers: {
        Accept: 'text/html',
        'Accept-Language': 'en',
        'accept-encoding': 'gzip',
        'User-Agent': 'example-agent',
        Authorization: 'Bearer s3cret',
        Cookie: 'sid=1',
        'Proxy-Authorization': 'Basic abc',
        'X-Api-Key': 'k',
      },
    });
    const next = redirectRequest({
      request,
      status: 302,
      from: new URL('https://www.example.com/start'),
      to: new URL(target),
    });
    expect(Object.keys(next.headers).toSorted()).toEqual([
      'accept',
      'accept-encoding',
      'accept-language',
      'user-agent',
    ]);
  });

  it('drops body headers and credentials together on a cross-origin 303 from a POST', () => {
    const next = redirectRequest({
      request: makePost(),
      status: 303,
      from: FROM,
      to: OTHER_ORIGIN,
    });
    expect(next.method).toBe('GET');
    expect(Object.keys(next.headers)).toEqual(['accept']);
  });

  it('keeps a POST body across a same-origin 307', () => {
    const request = makePost();
    const next = redirectRequest({ request, status: 307, from: FROM, to: SAME_ORIGIN });
    expect(next.method).toBe('POST');
    expect(next.body).toBe(request.body);
  });
});

describe('blocksBodyReplay', () => {
  it.each([307, 308])('blocks a cross-origin %i that would replay a POST body', (status) => {
    expect(blocksBodyReplay({ request: makePost(), status, from: FROM, to: OTHER_ORIGIN })).toBe(
      true,
    );
  });

  it('blocks every method that carries a body, not only POST', () => {
    for (const method of ['PUT', 'PATCH', 'DELETE'] as const) {
      expect(
        blocksBodyReplay({ request: makePost(method), status: 308, from: FROM, to: OTHER_ORIGIN }),
      ).toBe(true);
    }
  });

  it('allows a same-origin 307, a body-dropping 301, 302 or 303, and a bodyless request', () => {
    expect(
      blocksBodyReplay({ request: makePost(), status: 307, from: FROM, to: SAME_ORIGIN }),
    ).toBe(false);
    for (const status of [301, 302, 303]) {
      expect(blocksBodyReplay({ request: makePost(), status, from: FROM, to: OTHER_ORIGIN })).toBe(
        false,
      );
    }

    expect(
      blocksBodyReplay({
        request: HttpClientRequest.get('https://www.example.com/x'),
        status: 307,
        from: FROM,
        to: OTHER_ORIGIN,
      }),
    ).toBe(false);
    expect(
      blocksBodyReplay({
        request: HttpClientRequest.post('https://www.example.com/x'),
        status: 308,
        from: FROM,
        to: OTHER_ORIGIN,
      }),
    ).toBe(false);
  });

  it('blocks a GET or HEAD that carries a body, on every hop that keeps it', () => {
    for (const method of ['GET', 'HEAD'] as const) {
      const request = HttpClientRequest.make(method)('https://www.example.com/search').pipe(
        HttpClientRequest.bodyText('{"q":"private"}', 'application/json'),
      );
      for (const status of [301, 302, 303, 307, 308]) {
        expect(blocksBodyReplay({ request, status, from: FROM, to: OTHER_ORIGIN })).toBe(true);
      }
    }
  });

  it('blocks a cross-origin 301 or 302 of a PUT, which keeps its body', () => {
    for (const status of [301, 302]) {
      expect(
        blocksBodyReplay({ request: makePost('PUT'), status, from: FROM, to: OTHER_ORIGIN }),
      ).toBe(true);
    }
  });
});

describe('replaysStreamBody', () => {
  const streamPut = HttpClientRequest.put('https://www.example.com/upload').pipe(
    HttpClientRequest.setBody(HttpBody.stream(Stream.make(new Uint8Array([1])))),
  );

  it('reports a hop that would send a stream body again', () => {
    expect(replaysStreamBody(streamPut, 307)).toBe(true);
    expect(replaysStreamBody(streamPut, 301)).toBe(true);
  });

  it('allows a hop that switches to GET and a request without a stream body', () => {
    expect(replaysStreamBody(streamPut, 303)).toBe(false);
    expect(replaysStreamBody(makePost('PUT'), 307)).toBe(false);
  });
});

describe('isOneShotBody', () => {
  it('recognizes a synchronous iterator without calling its iterator method', () => {
    const iterator = {
      next: () => ({ done: true, value: undefined }),
      [Symbol.iterator]: () => {
        throw new Error('The iterator method must not run during classification');
      },
    };
    expect(isOneShotBody(HttpBody.raw(iterator))).toBe(true);
  });

  it('reports a stream, a Web ReadableStream and an async iterable such as a Node Readable', () => {
    expect(isOneShotBody(HttpBody.stream(Stream.make(new Uint8Array([1]))))).toBe(true);
    expect(isOneShotBody(HttpBody.raw(new ReadableStream()))).toBe(true);
    expect(isOneShotBody(HttpBody.raw(Readable.from(['a'])))).toBe(true);
  });

  it('allows bodies that can be sent again', () => {
    expect(isOneShotBody(HttpBody.empty)).toBe(false);
    expect(isOneShotBody(HttpBody.text('a'))).toBe(false);
    expect(isOneShotBody(HttpBody.raw('a'))).toBe(false);
    expect(isOneShotBody(HttpBody.raw(null))).toBe(false);
    expect(isOneShotBody(HttpBody.raw(new Uint8Array([1])))).toBe(false);
  });
});

describe('isCrossOrigin', () => {
  it('is false for another path or query on the same origin', () => {
    expect(isCrossOrigin(new URL('https://a.example/x'), new URL('https://a.example/y?z=1'))).toBe(
      false,
    );
  });

  it('treats an explicit default port as the same origin', () => {
    expect(isCrossOrigin(new URL('https://a.example/x'), new URL('https://a.example:443/x'))).toBe(
      false,
    );
  });

  it('is true when the scheme, the host or the port differs', () => {
    const base = new URL('https://a.example/x');
    expect(isCrossOrigin(base, new URL('http://a.example/x'))).toBe(true);
    expect(isCrossOrigin(base, new URL('https://b.example/x'))).toBe(true);
    expect(isCrossOrigin(base, new URL('https://a.example:8443/x'))).toBe(true);
    expect(isCrossOrigin(base, new URL('https://sub.a.example/x'))).toBe(true);
  });
});

describe('safelistedHeaderInit', () => {
  it('returns undefined when there are no default headers', () => {
    expect(safelistedHeaderInit(undefined)).toBeUndefined();
  });

  it('drops credentials and custom headers from a record', () => {
    const kept = safelistedHeaderInit({
      Authorization: 'Bearer s3cret',
      Cookie: 'sid=1',
      'X-Ambient-Token': 'ambient',
      Accept: 'text/html',
      'User-Agent': 'example-agent',
    });
    expect(Object.fromEntries(new Headers(kept))).toEqual({
      accept: 'text/html',
      'user-agent': 'example-agent',
    });
  });

  it('reads tuple lists and Headers instances the same way', () => {
    const fromTuples = safelistedHeaderInit([
      ['authorization', 'Bearer s3cret'],
      ['accept-language', 'en'],
    ]);
    const fromHeaders = safelistedHeaderInit(
      new Headers({ 'proxy-authorization': 'Basic abc', 'accept-encoding': 'gzip' }),
    );
    expect(Object.fromEntries(new Headers(fromTuples))).toEqual({ 'accept-language': 'en' });
    expect(Object.fromEntries(new Headers(fromHeaders))).toEqual({ 'accept-encoding': 'gzip' });
  });

  it('returns an empty set when nothing is safelisted', () => {
    expect([...new Headers(safelistedHeaderInit({ authorization: 'x' }))]).toEqual([]);
  });
});
