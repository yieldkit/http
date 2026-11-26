import { Effect, Metric } from 'effect';
import { FetchHttpClient, HttpClientRequest } from 'effect/http';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import { executeJsonRequest, executeTextRequest } from './execute.ts';
import {
  exitErrorMeta,
  exitOutcome,
  ItemPayloadSchema,
  makeClientResponse,
  makeMockHttpClient,
  squashExitFailure,
} from './http.fixture.ts';
import { httpRequestsTotal, httpRetriesTotal } from './metrics.ts';
import { makeSentValueMask } from './sent-values.ts';
import {
  bodyExcerpt,
  ensureSuccessStatus,
  errorBodySnippet,
  issueExcerpt,
} from './status-error.ts';

describe('bodyExcerpt', () => {
  it('masks JSON values whose key is a sensitive name, with or without a prefix', () => {
    const excerpt = bodyExcerpt(
      String.raw`{"access_token": "a1", "refresh_token":"r\"2", "password":123, "id":4}`,
    );
    expect(excerpt).toBe(
      '{"access_token":"<redacted>","refresh_token":"<redacted>","password":"<redacted>","id":4}',
    );
  });

  it('masks camelCase and header-style credential names in JSON', () => {
    const excerpt = bodyExcerpt(
      JSON.stringify({
        id: 7,
        accessToken: 'AAA',
        apiKey: 'BBB',
        clientSecret: 'CCC',
        authorization: 'DDD',
        cookie: 'sid=EEE',
        'x-api-key': 'FFF',
        private_key: 'GGG',
        sessionId: 'HHH',
        RefreshToken: 'III',
      }),
    );
    for (const secret of ['AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF', 'GGG', 'HHH', 'III']) {
      expect(excerpt).not.toContain(secret);
    }

    expect(excerpt).toContain('{"id":7,');
  });

  it('masks sensitive keys at any depth and whole values under them', () => {
    expect(bodyExcerpt('{"error":{"access_token":"AAA"}}')).toBe(
      '{"error":{"access_token":"<redacted>"}}',
    );
    expect(bodyExcerpt('{"items":[{"password":"AAA"}]}')).toBe(
      '{"items":[{"password":"<redacted>"}]}',
    );
    expect(bodyExcerpt('{"access_token":["aaa","bbb"],"id":1}')).toBe(
      '{"access_token":"<redacted>","id":1}',
    );
  });

  it('leaves names that only contain a sensitive word, and null values, alone', () => {
    expect(bodyExcerpt('{"monkey":"banana","token":null}')).toBe(
      '{"monkey":"banana","token":null}',
    );
    expect(bodyExcerpt('{"code":404,"message":"missing"}')).toBe(
      '{"code":404,"message":"missing"}',
    );
  });

  it('writes a body that is not complete JSON as given, without scanning its text', () => {
    expect(bodyExcerpt('rejected Bearer abc.def\nat https://x.test/cb?token=t1')).toBe(
      'rejected Bearer abc.def at https://x.test/cb?token=t1',
    );
    expect(bodyExcerpt('{"id":1,"access_token":"aaa","page":')).toBe(
      '{"id":1,"access_token":"aaa","page":',
    );
  });

  it('masks a JSON body nested deeper than it walks instead of overflowing the stack', () => {
    const depth = 50_000;
    const excerpt = bodyExcerpt(`${'['.repeat(depth)}"x"${']'.repeat(depth)}`);
    expect(excerpt).toMatch(/^\[+"<redacted>"\]+$/);
  });

  it("masks only the request's own credentials in a schema issue", () => {
    const request = HttpClientRequest.get('https://api.example.com/x', {
      headers: { 'x-api-key': 'ISSUEKEY-0123456789' },
    });
    expect(
      issueExcerpt(
        'Expected string, got "ISSUEKEY-0123456789"\n  at ["id"]',
        makeSentValueMask(request),
      ),
    ).toBe('Expected string, got "<redacted>" at ["id"]');
  });
});

describe('errorBodySnippet', () => {
  it('masks credentials in a non-2xx JSON body', () => {
    expect(
      errorBodySnippet({
        contentType: 'application/json',
        text: '{"error":"bad","client_secret":"cs-1"}',
        truncated: false,
      }),
    ).toBe('{"error":"bad","client_secret":"<redacted>"}');
  });

  it('masks a long JSON body by key name before the cut', () => {
    const body = JSON.stringify({ access_token: 'opaqueValue', detail: 'd'.repeat(2100) });
    expect(bodyExcerpt(body)).toMatch(/^\{"access_token":"<redacted>","detail":"d+$/);
    expect(
      errorBodySnippet({ contentType: 'application/json', text: body, truncated: false }),
    ).toMatch(/^\{"access_token":"<redacted>","detail":"d+…$/);
  });

  it('masks a sent value a long JSON body echoes in an escaped form', () => {
    const request = HttpClientRequest.get('https://api.example.com/x', {
      headers: { 'x-api-key': 'AbC+dEf/0123456789QwErTyZx==' },
    });
    const body = `{"error":"bad key AbC\\u002BdEf/0123456789QwErTyZx==","detail":"${'d'.repeat(2100)}"}`;
    const snippet = errorBodySnippet({
      contentType: 'application/json',
      text: body,
      truncated: false,
      mask: makeSentValueMask(request),
    });
    expect(snippet).toMatch(/^\{"error":"bad key <redacted>","detail":"d+…$/);
    expect(bodyExcerpt(body, makeSentValueMask(request))).not.toContain('0123456789QwErTyZx');
  });
});

describe('errorBodySnippet for HTML pages', () => {
  it('quotes only the title of an XHTML page or an HTML page sent without a content type', () => {
    const page =
      '<!DOCTYPE html><html><head><title>Server Error</title></head><body>secret page text</body></html>';
    expect(
      errorBodySnippet({ contentType: 'application/xhtml+xml', text: page, truncated: false }),
    ).toBe('Server Error');
    expect(errorBodySnippet({ contentType: null, text: page, truncated: false })).toBe(
      'Server Error',
    );
  });
});

describe('error text that echoes the request credentials', () => {
  it('masks an API key the upstream echoes back in prose and keeps the body out of the message', async () => {
    const request = HttpClientRequest.get(
      'https://api.example.com/v1/models?api_key=qk-live-123456789',
      {
        headers: { authorization: 'Bearer sk-live-ABCDEF123456', 'x-tenant-id': '42' },
      },
    );
    const body = JSON.stringify({
      error: {
        message:
          'Incorrect API key provided: sk-live-ABCDEF123456 (query key qk-live-123456789, tenant 42)',
      },
    });
    const response = makeClientResponse(
      new Response(body, { status: 401, headers: { 'content-type': 'application/json' } }),
      request,
    );
    const exit = await Effect.runPromiseExit(ensureSuccessStatus(response, 'test:echo'));
    const error = squashExitFailure(exit);
    expect(error?.message).toBe('test:echo responded with HTTP 401');
    const snippet = error?._tag === 'HttpRequestError' ? (error.bodySnippet ?? '') : '';
    expect(snippet).toContain('Incorrect API key provided: <redacted>');
    expect(snippet).not.toContain('sk-live-ABCDEF123456');
    expect(snippet).not.toContain('qk-live-123456789');
    expect(snippet).toContain('tenant 42');
  });

  it('masks a credential echoed in a body that fails to decode', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(Response.json({ id: 'Invalid API key sk-live-ABCDEF123456', title: 1 })),
    );
    const exit = await Effect.runPromiseExit(
      executeJsonRequest({
        client,
        request: HttpClientRequest.get('https://api.example.com/item', {
          headers: { 'x-api-key': 'sk-live-ABCDEF123456' },
        }),
        upstream: 'test:echo-decode',
        schema: ItemPayloadSchema,
      }),
    );
    const error = squashExitFailure(exit);
    expect(error?.message).toBe('Failed to decode test:echo-decode response: schema mismatch');
    expect(JSON.stringify(error)).not.toContain('sk-live-ABCDEF123456');
    expect(error).toMatchObject({
      bodySnippet: expect.stringContaining('Invalid API key <redacted>'),
    });
  });
});

/**
 * Runs `ensureSuccessStatus` on a 401 JSON `body` answering `request` and returns the error's
 * `bodySnippet`, or `''` when it has none.
 */
const statusSnippet = async (
  request: HttpClientRequest.HttpClientRequest,
  body: string,
  headers: Record<string, string> = {},
): Promise<string> => {
  const response = makeClientResponse(
    new Response(body, {
      status: 401,
      headers: { 'content-type': 'application/json', ...headers },
    }),
    request,
  );
  const exit = await Effect.runPromiseExit(ensureSuccessStatus(response, 'test:echo'));
  const error = squashExitFailure(exit);
  return error?._tag === 'HttpRequestError' ? (error.bodySnippet ?? '') : '';
};

describe('error text that echoes the request credentials in other forms', () => {
  it.each([
    ['.NET', String.raw`{"error":"invalid API key AbC\u002BdEf/0123456789QwErTyZx=="}`],
    ['PHP', String.raw`{"error":"invalid API key AbC+dEf\/0123456789QwErTyZx=="}`],
  ])('masks a sent key the %s JSON encoder escaped', async (_encoder, body) => {
    const request = HttpClientRequest.get('https://api.example.com/x', {
      headers: { 'x-api-key': 'AbC+dEf/0123456789QwErTyZx==' },
    });
    const snippet = await statusSnippet(request, body);
    expect(snippet).not.toContain('0123456789QwErTyZx');
    expect(snippet).toContain('invalid API key <redacted>');
  });

  it('masks a sent value holding a quote that JSON writes back escaped', async () => {
    const request = HttpClientRequest.get('https://api.example.com/x', {
      headers: { authorization: 'Bearer q"uoted-SECRET-1' },
    });
    const snippet = await statusSnippet(
      request,
      JSON.stringify({ error: 'rejected q"uoted-SECRET-1 here' }),
    );
    expect(snippet).toContain('rejected');
    expect(snippet).not.toContain('uoted-SECRET-1');
  });

  it('masks a rotated value under a JSON key named like a sent cookie', async () => {
    const request = HttpClientRequest.get('https://api.example.com/x', {
      headers: { cookie: 'session_id=oldSessionValue1' },
    });
    const snippet = await statusSnippet(
      request,
      JSON.stringify({
        session_id: 'newSessionValue2',
        error: 'expired',
        echo: 'oldSessionValue1',
      }),
    );
    expect(snippet).toBe('{"session_id":"<redacted>","error":"expired","echo":"<redacted>"}');
  });

  it('masks a key sent through an ambient FetchHttpClient.RequestInit', async () => {
    const request = HttpClientRequest.get('https://api.example.com/x');
    const response = makeClientResponse(
      new Response('Invalid API key AMBIENTKEY0123456789abcdefXY for this account', {
        status: 401,
      }),
      request,
    );
    const exit = await Effect.runPromiseExit(
      ensureSuccessStatus(response, 'test:echo').pipe(
        Effect.provideService(FetchHttpClient.RequestInit, {
          headers: { 'x-api-key': 'AMBIENTKEY0123456789abcdefXY' },
        }),
      ),
    );
    expect(squashExitFailure(exit)).toMatchObject({
      bodySnippet: 'Invalid API key <redacted> for this account',
    });
  });

  it('leaves a value sent under an ordinary name alone, whatever it looks like', async () => {
    const request = HttpClientRequest.get('https://api.example.com/x', {
      headers: { 'x-tenant-id': 'tenant0123456789abcdefXYZ' },
    });
    expect(await statusSnippet(request, '{"error":"unknown tenant0123456789abcdefXYZ"}')).toBe(
      '{"error":"unknown tenant0123456789abcdefXYZ"}',
    );
  });

  it('drops a request id that holds a credential the request sent', async () => {
    const request = HttpClientRequest.get('https://api.example.com/x', {
      headers: { 'x-api-key': 'sk_live_0123456789abcdefABCDEF' },
    });
    const response = makeClientResponse(
      new Response('', {
        status: 401,
        headers: { 'x-request-id': 'sk_live_0123456789abcdefABCDEF' },
      }),
      request,
    );
    const exit = await Effect.runPromiseExit(ensureSuccessStatus(response, 'test:echo'));
    expect(squashExitFailure(exit)?.message).toBe('test:echo responded with HTTP 401');
  });

  it('masks a sent value that a schema issue quotes across the cut at 500 characters', () => {
    const request = HttpClientRequest.get('https://api.example.com/x', {
      headers: { 'x-api-key': 'SPLITKEY0123456789abcdefXYZ' },
    });
    const excerpt = issueExcerpt(
      `${'x'.repeat(490)} SPLITKEY0123456789abcdefXYZ`,
      makeSentValueMask(request),
    );
    expect(excerpt).not.toContain('SPLITKEY0');
  });
});

describe('non-2xx errors', () => {
  it('puts the body in bodySnippet, the request id in the message, and labels a 503 a server error', async () => {
    const upstream = 'test:snippet-503';
    const client = makeMockHttpClient(() =>
      Effect.succeed(
        new Response(JSON.stringify({ error: 'decode timeout' }), {
          status: 503,
          headers: { 'content-type': 'application/json', 'x-request-id': 'req_123' },
        }),
      ),
    );
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          executeJsonRequest({
            client,
            request: HttpClientRequest.get('https://api.example.com/item'),
            schema: ItemPayloadSchema,
            upstream,
            retry: { maxRetries: 1, baseDelay: '1 millis' },
          }),
        );
        const counts = yield* Effect.forEach(
          ['server_error', 'timeout', 'decode_error'] as const,
          (outcome) =>
            Metric.value(
              Metric.withAttributes(httpRequestsTotal, { upstream, method: 'GET', outcome }),
            ),
        );
        const retries = yield* Effect.forEach(['server_error', 'timeout'] as const, (reason) =>
          Metric.value(Metric.withAttributes(httpRetriesTotal, { upstream, reason })),
        );
        return { exit, counts: counts.map((c) => c.count), retries: retries.map((c) => c.count) };
      }),
    );
    expect(squashExitFailure(result.exit)).toMatchObject({
      _tag: 'HttpRequestError',
      status: 503,
      message: `${upstream} responded with HTTP 503 (request id req_123)`,
      bodySnippet: '{"error":"decode timeout"}',
    });
    expect(exitOutcome(result.exit)).toBe('server_error');
    expect(result.counts).toEqual([1, 0, 0]);
    expect(result.retries).toEqual([1, 0]);
  });

  it('quotes only the title of an HTML error page and drops a malformed request id', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(
        new Response('<html><head><title>  Bad\nGateway </title></head><body>secret</body>', {
          status: 502,
          headers: { 'content-type': 'text/html', 'x-request-id': 'bad id with spaces' },
        }),
      ),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({
        client,
        request: HttpClientRequest.get('https://www.example.com/page'),
        upstream: 'test:snippet-html',
        retry: { maxRetries: 0 },
      }),
    );
    expect(squashExitFailure(exit)).toMatchObject({
      message: 'test:snippet-html responded with HTTP 502',
      bodySnippet: 'Bad Gateway',
    });
  });

  it('cuts a long snippet at 256 characters', async () => {
    const client = makeMockHttpClient(() =>
      Effect.succeed(
        new Response('e'.repeat(1000), { status: 400, headers: { 'cf-ray': 'abc-1' } }),
      ),
    );
    const exit = await Effect.runPromiseExit(
      executeTextRequest({
        client,
        request: HttpClientRequest.get('https://www.example.com/page'),
        upstream: 'test:snippet-long',
      }),
    );
    expect(squashExitFailure(exit)).toMatchObject({
      message: 'test:snippet-long responded with HTTP 400 (request id abc-1)',
      bodySnippet: `${'e'.repeat(256)}…`,
    });
    expect(exitOutcome(exit)).toBe('client_error');
  });

  it('leaves bodySnippet out for an empty body', async () => {
    const exit = await Effect.runPromiseExit(
      ensureSuccessStatus(
        makeClientResponse(new Response('', { status: 404 })),
        'test:snippet-empty',
      ),
    );
    const error = squashExitFailure(exit);
    expect(error?.message).toBe('test:snippet-empty responded with HTTP 404');
    expect(error?.bodySnippet).toBeUndefined();
  });
});

describe('ensureSuccessStatus Retry-After', () => {
  it.each([
    [503, '120', 120],
    [408, '3', 3],
    [425, '1', 1],
    [500, '4', 4],
  ])('tags a %i with its Retry-After of %s seconds', async (status, header, seconds) => {
    const exit = await Effect.runPromiseExit(
      ensureSuccessStatus(
        makeClientResponse(new Response('down', { status, headers: { 'retry-after': header } })),
        'test:status-retry-after',
      ),
    );
    expect(exitErrorMeta(exit)?.retryAfterSeconds).toBe(seconds);
  });

  it('reads an HTTP-date Retry-After against the clock', async () => {
    const nowMs = Date.UTC(2026, 0, 15, 12);
    const date = new Date(nowMs + 30_000).toUTCString();
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        yield* TestClock.setTime(nowMs);
        yield* TestClock.adjust('6 seconds');
        return yield* ensureSuccessStatus(
          makeClientResponse(
            new Response('down', { status: 503, headers: { 'retry-after': date } }),
          ),
          'test:status-retry-date',
        );
      }).pipe(Effect.provide(TestClock.layer())),
    );
    expect(exitErrorMeta(exit)?.retryAfterSeconds).toBe(24);
  });

  it('leaves a status that is not retried, or a 503 without the header, untagged', async () => {
    const notFound = await Effect.runPromiseExit(
      ensureSuccessStatus(
        makeClientResponse(new Response('gone', { status: 404, headers: { 'retry-after': '5' } })),
        'test:status-retry-404',
      ),
    );
    const bare = await Effect.runPromiseExit(
      ensureSuccessStatus(
        makeClientResponse(new Response('down', { status: 503 })),
        'test:status-retry-bare',
      ),
    );
    expect(exitErrorMeta(notFound)?.retryAfterSeconds).toBeUndefined();
    expect(exitErrorMeta(bare)?.retryAfterSeconds).toBeUndefined();
    expect(exitOutcome(bare)).toBe('server_error');
  });
});
