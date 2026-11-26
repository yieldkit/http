# Caching

`getCachedJson` is for resources you poll: it revalidates with `ETag` and `Last-Modified` and returns the cached value on a `304`. Define the schema once, at module level, because the cache is partitioned by schema identity.

```ts
import { makeResilientHttpClient } from '@yieldkit/http';
import { Effect, Schema } from 'effect';

const Items = Schema.Array(Schema.Struct({ id: Schema.String, name: Schema.String }));

const twoPolls = Effect.gen(function* () {
  const api = yield* makeResilientHttpClient({
    upstream: 'example-api',
    baseUrl: 'https://api.example.com',
  });
  const first = yield* api.getCachedJson('/items', Items); // 200, stored with its ETag
  const second = yield* api.getCachedJson('/items', Items); // If-None-Match; the stored value on a 304
  return [first, second];
});
```

## ETag cache

The cached entry's `ETag` and `Last-Modified` go out as `If-None-Match` and `If-Modified-Since`.

Without `EtagCache` in context each client gets its own 250-entry cache. `EtagCache.layer` (in `HttpResilienceLive` and `ResilientHttpLive`) shares one 500-entry LRU between clients; `makeEtagCache(maxEntries, maxBytes)` builds one directly.

Each cache also holds at most `maxBytes` of response bodies, 32 MiB by default. Storing an entry evicts the least recently used ones until it fits, and a body larger than the budget is not cached, so it is fetched in full every time. The budget counts body bytes as read, not heap: a decoded value took about 1.15 to 1.3 times its JSON size in memory (measured on Node, string arrays and arrays of small objects).

The key is the upstream, the URL, the schema instance and a hash of every request header except `User-Agent`, `Accept-Encoding` and the validators, ambient `FetchHttpClient.RequestInit` headers included. Headers and URL are read after the request transforms (see [Request transforms](requests.md#request-transforms)), so a per-tenant client that adds its own `Authorization` gets its own entries. Entries are never shared across schemas, credentials (whatever the header is called: `Authorization`, `Ocp-Apim-Subscription-Key`, `X-Tenant-Id`, `bearerToken`) or representations (`Accept`, `Accept-Language`).

The decoded value is what is cached. Responses that follow a redirect bypass the cache.

## Manage validators yourself

`getConditionalJson` leaves the cache to you. It sends `ifNoneMatch` or `ifModifiedSince` and returns `Modified` or `NotModified`:

```ts
import type { ResilientHttpClient } from '@yieldkit/http';

const refresh = (api: ResilientHttpClient, etag: string | null) =>
  Effect.map(api.getConditionalJson('/items', Items, { ifNoneMatch: etag }), (result) =>
    result._tag === 'Modified' ? { items: result.data, etag: result.etag } : null,
  );
```

`Modified` has `data`, `bodyBytes` (the body's size as read), `etag`, `lastModified`, `status` and `cacheControl`; `NotModified` has `etag`, `lastModified` and `cacheControl`. `cacheControl` is the `Cache-Control` header, or `null`.

## Stale-if-error

With `staleIfError`, `getCachedJson` serves its last good payload when the upstream fails:

<!-- prettier-ignore -->
```ts
const meta = Effect.gen(function* () {
  const registry = yield* makeResilientHttpClient({
    upstream: 'registry',
    baseUrl: 'https://registry.example.com',
    staleIfError: '1 hour',
  });
  const latest = yield* registry.getCachedJson('/packages/example', Items);                          // stale payload if the registry is down
  const strict = yield* registry.getCachedJson('/packages/example', Items, { staleIfError: false }); // fail as usual
  return { latest, strict };
});
```

- With it on, a cacheable 2xx is stored even without `ETag` or `Last-Modified`.
- It serves for outcomes `rate_limited`, `server_error`, `timeout`, `network_error`, `decode_error`, `circuit_open` and `body_too_large`, when a 200 or 304 confirmed the entry within the limit. A `client_error` such as a 404 fails as usual.
- Each use logs `http.cache.stale_served` and increments `resilient_http_stale_served_total`.

## Limits

- The transforms run once more per call to build the key, outside `attemptTimeout` and `operationTimeout`; bound a slow `mapRequestEffect` yourself. If they fail, the call goes out without the cache.
- A transform that adds a header that changes on every request (a signature date, a request id) makes every key unique, so nothing is served from the cache.
- Headers added past the transforms are not seen: inside a custom `HttpClient.make` function, a `transformResponse`/`postprocess` wrapper, or by fetch itself. Give such clients their own `upstream`, or put the credential in a request header.
- `User-Agent` does not partition the cache and `Vary` is not honoured, so a JSON variant by `User-Agent` can be served for another on a `304` or as a stale fallback.
- A schema built on every call (`Schema.Array(Item)` inside the polling function) gets a fresh partition each time: no entry, no `If-None-Match`, nothing to serve stale.
- A schema whose decoding reads a per-caller service returns the first caller's result to every caller.
- A caller-supplied `If-None-Match` or `If-Modified-Since` (option or raw header) that the entry does not hold drops the entry for that request; a `304` then fails instead of returning the cached payload.
- A late `304` or a slow older `200` never overwrites a newer entry, and a newer success that isn't cached evicts the older one.
- A `2xx` or `304` marked `Cache-Control: no-store`, or one reached through a redirect, evicts the entry when its headers arrive, even if its body fails decoding. A `401`, `403`, `404` or `410` also evicts the entry.
- A `403` the policy treats as a rate limit (`treat403AsRateLimit`, `treatQuotaExhausted403AsRateLimit`) does not evict the entry, and `staleIfError` may serve it.
- Cached values are shared by reference between callers. Treat them as immutable.
- The cache is a bounded in-memory LRU, so stale-if-error is best effort.
- Each client without an `EtagCache` service has its own 32 MiB budget. Many such clients multiply it; provide `EtagCache.layer` to bound the process.
