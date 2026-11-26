# Requests

A client talks to one upstream. Build it with `makeResilientHttpClient(policy)` or `ResilientHttp.client(policy)`.

```ts
import { makeResilientHttpClient } from '@yieldkit/http';
import { Effect, Option, Schema } from 'effect';

const Item = Schema.Struct({ id: Schema.String, name: Schema.String });

const program = Effect.gen(function* () {
  const api = yield* makeResilientHttpClient({
    upstream: 'example-api',
    baseUrl: 'https://api.example.com/v1',
  });
  const item = yield* api.getJson('/items/42', Item);
  const maybe = yield* api.getOptionalJson('/items/43', Item);
  return Option.isSome(maybe) ? [item, maybe.value] : [item];
});
```

Each method takes a path, joined to `baseUrl`, or an absolute URL, and fails with `HttpError`. `stream` returns a `Stream` with the same errors.

The samples on this page use `api`, a client like the one above, and its `Item` schema. `etag`, `lastModified`, `token` and `response` stand for values you already have.

Build each client once and reuse it: `maxConcurrency` and the local ETag cache belong to the instance, while the rate limiter and breaker are shared by key.

## Methods

<!-- prettier-ignore -->
```ts
import { HttpClientRequest } from 'effect/http';

api.getJson('/items/42', Item);                                          // decoded body
api.postJson('/search', { query: 'effect' }, Item);                      // JSON in, decoded body out
api.getOptionalJson('/items/missing', Item);                             // Option.none() on a 404
api.getCachedJson('/items/42', Item);                                    // cached value on a 304
api.getConditionalJson('/items/42', Item, { ifNoneMatch: etag });        // Modified or NotModified
api.getText('/robots.txt');                                              // UTF-8 string
api.getBytes('/logo.png');                                               // Uint8Array
api.getResponse('/x');                                                   // HttpClientResponse, body already buffered
api.execute(HttpClientRequest.get('https://api.example.com/x'));         // your own request, same pipeline
api.stream('/archive.tar');                                              // Stream<Uint8Array>, body never buffered
api.withWebResponse('/page', (response) => Effect.succeed(response.ok)); // streaming Web Response
api.policy;                                                              // ResolvedHttpPolicy
```

JSON methods send `Accept: application/json` unless you set `accept`. For the cached methods see [Caching](caching.md).

## Send JSON

A `POST` is retried only on a rate limit with `retryOnRateLimit`, unless you mark it `idempotent` (see [What is retried](retries-and-timeouts.md#what-is-retried)):

```ts
const created = api.postJson(
  '/orders',
  { sku: 'abc', quantity: 1 },
  Schema.Struct({ id: Schema.String }),
  { idempotent: true, headers: { 'idempotency-key': 'order-7f3a' } },
);
```

## Per-request options

`HttpRequestOptions` is the last argument of every method:

<!-- prettier-ignore -->
```ts
import { Redacted } from 'effect';

api.getJson('/search', Item, {
  headers: { 'x-api-version': '2024-01-01' },  // merged over defaultHeaders
  urlParams: { q: 'effect', page: undefined }, // undefined values are skipped
  bearerToken: Redacted.make(token),           // Authorization: Bearer ...
  attemptTimeout: '5 seconds',
  operationTimeout: '30 seconds',
  idleTimeout: '10 seconds',                   // stream and withWebResponse: longest gap between chunks
  maxBodyBytes: 512 * 1024,
  idempotent: true,                            // lets a POST or PATCH retry on any transient failure
  acceptNotFound: true,                        // 404 is a success (not for the JSON methods)
  staleIfError: false,                         // getCachedJson only: a duration, or false to turn it off
  ifNoneMatch: etag,                           // conditional headers, string or null
  ifModifiedSince: lastModified,
});
```

An invalid option fails the request before sending with `<upstream> has invalid request options: <fields>`.

## Headers

Headers are layered, weakest first: `userAgent`, `defaultHeaders`, the headers on a request passed to `execute`, `options.headers`, `bearerToken`, then the validators. JSON methods add `Accept: application/json` when no layer sets `Accept`. Without `userAgent` the transport's default `User-Agent` goes out.

With a `baseUrl`, `defaultHeaders` go only to its origin. Another origin, such as a pagination link, gets only the `userAgent`:

<!-- prettier-ignore -->
```ts
const files = Effect.gen(function* () {
  const api = yield* makeResilientHttpClient({
    upstream: 'example-api',
    baseUrl: 'https://api.example.com',
    defaultHeaders: { 'x-api-key': 'k-123' },
  });
  yield* api.getText('/files');                           // sends x-api-key
  yield* api.getText('https://cdn.example.net/file.txt'); // does not
});
```

To send it there anyway, pass it in `options.headers`.

## Send your own request

`execute` sends an `HttpClientRequest` you built, joined to `baseUrl`, and returns the response with its body buffered. Use it for other methods, or for a `Redacted` query value (see [Credential masking](errors.md#credential-masking)):

```ts
const deleted = api.execute(
  HttpClientRequest.delete('/items/42', { urlParams: { ticket: Redacted.make('t-9f2c41d7') } }),
);
```

## Stream a body

`stream` sends a GET and returns the body as a `Stream`, so a large download never sits in memory:

<!-- prettier-ignore -->
```ts
import { NodeFileSystem } from '@effect/platform-node';
import { Effect, FileSystem, Stream } from 'effect';

const download = (url: string, targetPath: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const partPath = `${targetPath}.part`;
    yield* api
      .stream(url, {
        maxBodyBytes: 2 * 1024 ** 3,        // the cap still applies; raise it for big files
        operationTimeout: '20 minutes',     // covers the request and the whole body
        idleTimeout: '60 seconds',          // longest gap between chunks (default 30 seconds)
      })
      .pipe(Stream.run(fs.sink(partPath)));
    yield* fs.rename(partPath, targetPath); // only a complete download gets the real name
  }).pipe(Effect.provide(NodeFileSystem.layer));

const lines = api.stream('/events.ndjson').pipe(Stream.decodeText(), Stream.splitLines);
```

If the download fails, the `.part` file is left behind.

The stream is lazy. Until the 2xx arrives, it goes through the same pipeline as `getBytes`. After that nothing is retried, because part of the body may already have reached you.

## Read a streaming Web `Response`

`withWebResponse` hands `use` a Web `Response` over the same body stream, plus the URL after redirects. Use it for APIs that take a `Response`, such as `HTMLRewriter`:

```ts
import { HttpRequestError } from '@yieldkit/http';
import { Effect, Schema } from 'effect';

const page = api.withWebResponse('https://example.com/', (response, finalUrl) => {
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
  return text.pipe(Effect.map((html) => ({ finalUrl, size: html.length })));
});
```

`use` holds a `maxConcurrency` permit, gets its own `attemptTimeout` and is never retried. The body errors past `maxBodyBytes` or after `idleTimeout` without data.

## Request transforms

The underlying `HttpClient`'s request transforms (`HttpClient.mapRequest`, `mapRequestEffect`, such as `prependUrl` or a per-tenant `bearerToken`) run once per attempt, before the first hop:

```ts
import { HttpClient } from 'effect/http';

const tenantApi = (tenantToken: string) =>
  Effect.gen(function* () {
    const base = yield* HttpClient.HttpClient;
    return yield* makeResilientHttpClient({ upstream: 'example-api' }).pipe(
      Effect.provideService(
        HttpClient.HttpClient,
        HttpClient.mapRequest(base, HttpClientRequest.bearerToken(tenantToken)),
      ),
    );
  });
```

- Redirect hops run only the response side (`postprocess`). A credential a transform adds is not re-added on a hop, and a signed URL is valid for the first hop only.
- The [redirect rules](redirects-and-ssrf.md#redirects) and the SSRF guard apply to the request the transforms produce.
- A service a response transform (`HttpClient.transformResponse`) provides is not visible to a request transform. Provide it to the caller.
- Retries follow the request as given to the client. A transform that changes the method or body is not taken into account: a GET it turns into a POST is retried like a GET.
- A transform that changes the method, or rewrites an absolute URL, makes every response count as a redirect hop's, so a rate-limited `POST` or `PATCH` is not replayed.
- `getCachedJson` runs the transforms once more to build its cache key (see [Caching](caching.md#limits)).

## Lower-level exports

The executors run one request through the same pipeline without a client.

<!-- prettier-ignore -->
```ts
import { readBoundedText, redactUrlForLogs } from '@yieldkit/http';

redactUrlForLogs('https://u:p@h.test/a?api_key=k&page=2#x');            //=> 'https://h.test/a?api_key=<redacted>&page=2'
redactUrlForLogs('not a url');                                          //=> '<unparseable url>'
readBoundedText({ response, upstream: 'example-api', maxBytes: 1024 }); // HttpRequestError above the cap
```

| Group     | Exports                                                                                                                                                                  |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Executors | `executeJsonRequest`, `executeOptionalJsonRequest`, `executeConditionalJsonRequest`, `executeTextRequest`, `executeBytesRequest`, `executeResponseRequest`               |
| Bodies    | `readBoundedBytes`, `readBoundedText`, `streamBoundedBytes`, `withStreamTimeouts`, `readSharedBodyPrefix`, `hasNoBody`                                                   |
| Headers   | `parseRetryAfterSeconds`, `parseRateLimitResetSeconds`, `parseRateLimitRemaining`, `extractEtag`, `extractLastModified`, `isHtmlEdgeBlock`, `detectRateLimitError`       |
| Retry     | `createRetrySchedule`, `createBackoffSchedule`, `isTransientHttpStatus`, `isTransientHttpError`, `isRetryableForMethod`, `readRetryAfterSeconds`, `isRedirectedResponse` |
| URLs      | `resolveRequestUrl(baseUrl, pathOrUrl)`, `redactUrlForLogs`                                                                                                              |

`redactUrlForLogs` drops userinfo and the fragment, and masks query values under a [credential name](errors.md#credential-masking).

## Limits

- A `204`, `205`, `304` or `HEAD` response, or one without a body, reads as empty whatever its `Content-Length`. An empty body is `null` for `getJson` with a schema that accepts `null`, and a `decode_error` otherwise.
- `getResponse` and `execute` return a `HEAD` response with its headers and no body; a body the transport sent anyway is cancelled. A 204, 205 or 304 always comes back without a body.
- `getResponse`, `execute` and `withWebResponse` drop `content-length` and `transfer-encoding`, and `content-encoding` when the transport decoded the body (fetch and Bun do, Undici does not).
- `Set-Cookie`: fetch and Bun keep every field as sent. Undici folds them, so on `./node` they are rebuilt from Effect's parsed cookies, one field per name, attributes as Effect writes them; a cookie that cannot be written back keeps the folded header. `response.cookies` works on every transport.
- `postJson` refuses a body when `JSON.stringify` returns `undefined` or throws (for example, for a `BigInt` or a cycle) before sending, outcome `blocked`.
- `stream`: a failure partway through the body is a `network_error`. Retry the whole download yourself if that is safe.
- `stream`: a declared `Content-Length` over `maxBodyBytes` fails before reading; otherwise `body_too_large` fails it once the cap is passed. No data for `idleTimeout`, or passing `operationTimeout` (counted from the `maxConcurrency` permit), fails with outcome `timeout`.
- `stream` holds a `maxConcurrency` permit and raises the active-requests gauge while it runs. Its request metric is recorded once, when it ends (`not_found` for a 404 accepted with `acceptNotFound`). Ending, failing or interrupting it, `Stream.take` included, cancels the body.
- `withWebResponse` does not refuse a large `Content-Length` up front, so `use` can read a prefix. The body is cancelled when `use` ends, even if it holds a reader or never touched the body.
