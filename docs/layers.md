# Layers and runtimes

`makeResilientHttpClient` needs an `HttpClient`, and picks up a `RateLimiter`, `CircuitBreaker` and `EtagCache` from context when present. On Node, one layer provides all of them:

```ts
import { NodeRuntime } from '@effect/platform-node';
import { makeResilientHttpClient } from '@yieldkit/http';
import { ResilientHttpLive } from '@yieldkit/http/node';
import { Effect } from 'effect';

const program = Effect.gen(function* () {
  const api = yield* makeResilientHttpClient({ upstream: 'example-api' });
  yield* Effect.log(yield* api.getText('https://api.example.com/status'));
});

program.pipe(Effect.provide(ResilientHttpLive), NodeRuntime.runMain);
```

Provide it once per process, at the entry point: providing it per call gives each call a fresh limiter, breaker and cache.

| Export                                      | Provides                                                                                                                          |
| ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `ResilientHttpLive` (`@yieldkit/http/node`) | `HttpClient` (Node, Undici), `RateLimiter`, `CircuitBreaker`, `EtagCache` and `ResilientHttp`                                     |
| `ResilientHttpLive` (`@yieldkit/http/bun`)  | `HttpClient` (Bun), `RateLimiter`, `CircuitBreaker`, `EtagCache` and `ResilientHttp`                                              |
| `HttpResilienceLive`                        | `RateLimiter`, `CircuitBreaker` and `EtagCache` only. Pair it with your own `HttpClient`                                          |
| `makeHttpResilienceLayer(options)`          | The same with other [breaker settings](rate-limiting-and-circuit-breaker.md#circuit-breaker)                                      |
| `ResilientHttp.layer`                       | `ResilientHttp`. Requires `HttpClient`                                                                                            |
| `EtagCache.layer`                           | A 500-entry, 32 MiB LRU. `makeEtagCache(maxEntries, maxBytes)` builds one directly (a non-finite bound falls back to its default) |

The rate limiter, breaker and cache in these layers live in memory, so each process has its own.

## Node, Bun and other runtimes

The package root needs only `effect`, `@yieldkit/logger` and the `node:dns/promises` and `node:crypto` built-ins, which Node and Bun both provide. Each runtime layer has its own entry point and optional peer:

| Entry point           | Transport                    | Install                          |
| --------------------- | ---------------------------- | -------------------------------- |
| `@yieldkit/http/node` | `NodeHttpClient.layerUndici` | `pnpm add @effect/platform-node` |
| `@yieldkit/http/bun`  | `BunHttpClient.layer`        | `pnpm add @effect/platform-bun`  |

Both transports return each 3xx as it is, so redirects follow this package's rules (see [Redirects and the SSRF guard](redirects-and-ssrf.md)).

On any runtime with a global `fetch`, `FetchHttpClient.layer` replaces the platform packages:

```ts
import { HttpResilienceLive, ResilientHttp } from '@yieldkit/http';
import { Layer } from 'effect';
import { FetchHttpClient } from 'effect/http';

const HttpLive = Layer.mergeAll(
  FetchHttpClient.layer,
  HttpResilienceLive,
  ResilientHttp.layer.pipe(Layer.provide(FetchHttpClient.layer), Layer.provide(HttpResilienceLive)),
);
```

The SSRF guard resolves names with `node:dns/promises` unless you provide an `AddressResolver`. See [Redirects and the SSRF guard](redirects-and-ssrf.md#ssrf-guard).

## The `ResilientHttp` service

`ResilientHttp` lets code depend on a service instead of `HttpClient`. Its `client(policy)` builds a `ResilientHttpClient`:

```ts
import { ResilientHttp } from '@yieldkit/http';
import { Effect } from 'effect';

const status = Effect.gen(function* () {
  const http = yield* ResilientHttp;
  const api = yield* http.client({
    upstream: 'example-api',
    baseUrl: 'https://api.example.com/v1',
  });
  return yield* api.getText('/status');
});
```

Build each client once and reuse it (see [Requests](requests.md)). Without an `EtagCache` in context, each client keeps its own local ETag cache.

## Testing

Provide an `HttpClient` built from a function. Add `HttpResilienceLive` when the test covers rate limits or the breaker.

```ts
import { ResilientHttp } from '@yieldkit/http';
import { Effect } from 'effect';
import { HttpClient, HttpClientResponse } from 'effect/http';

const notFound = HttpClient.make((request) =>
  Effect.succeed(HttpClientResponse.fromWeb(request, new Response('nope', { status: 404 }))),
);

const result = status.pipe(
  Effect.provide(ResilientHttp.layer),
  Effect.provideService(HttpClient.HttpClient, notFound),
);
```

## Performance

Measured on localhost (Node 26, Apple M4 Pro) against a plain `HttpClient` on the same transport:

- A call costs about 60 to 140 µs more CPU, with Undici or fetch (two runs on a busy machine). A rate limiter and circuit breaker add about 25 to 40 µs.
- `stream` keeps memory at about 1 to 1.7 times the body per call in flight; `getBytes` buffers the whole body.
- A body over `maxBodyBytes` destroys its connection, so each refused call opens a new one.
- Heap, sockets and handles returned to baseline after hung upstreams, 503 storms, large bodies and a 2-minute soak.

## Limits

- Use each entry point on its own runtime. Bun replaces Undici with a stub, so `@yieldkit/http/node` fails there with a transport error.
- Headers set through `BunHttpClient.layerOptions` reach every redirect hop, other origins included (see [Redirects](redirects-and-ssrf.md#limits)).
- The Undici transport does not read `FetchHttpClient.RequestInit`. Use `FetchHttpClient.layer` if you set fetch options that way.
- Fetch and Bun send an `Accept-Encoding` and decode the answer. Undici does neither, so a compressed answer to an `accept-encoding` header you set keeps its `content-encoding` and fails `getJson`.
