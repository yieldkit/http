# @yieldkit/http

[![npm](https://img.shields.io/npm/v/@yieldkit/http)](https://www.npmjs.com/package/@yieldkit/http)
[![CI](https://github.com/yieldkit/http/actions/workflows/ci.yml/badge.svg)](https://github.com/yieldkit/http/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/@yieldkit/http)](LICENSE)

Resilient outbound HTTP on Effect's `HttpClient`: one client per upstream, with timeouts, retries, rate limiting, circuit breaking, ETag caching, a body cap and an SSRF guard.

Every failure is an `HttpRateLimitedError` or an `HttpRequestError` (their union is `HttpError`), with a short message that never quotes the response body.

## Install

```bash
pnpm add @yieldkit/http effect
pnpm add @effect/platform-node   # for @yieldkit/http/node
pnpm add @effect/platform-bun    # for @yieldkit/http/bun
```

`effect` 4.1.6 or a later 4.1 patch is a peer dependency. The client builds on Effect's unstable `HttpClient`, `RateLimiter` and `CircuitBreaker` modules, so a new Effect minor version needs a release of this package. The platform packages are optional peers: install the one for the entry point you import, or neither if you bring your own `HttpClient`. Runs on Node 22.19+, 24.11+ or 26+, and on Bun.

| Import                | Contents                                                                     |
| --------------------- | ---------------------------------------------------------------------------- |
| `@yieldkit/http`      | The client, its layers, errors, policy, metrics and header helpers           |
| `@yieldkit/http/node` | `ResilientHttpLive` over Node's Undici client; needs `@effect/platform-node` |
| `@yieldkit/http/bun`  | `ResilientHttpLive` over Bun's fetch client; needs `@effect/platform-bun`    |

The root entry point loads neither platform package.

## Quick start

```ts
import { makeResilientHttpClient } from '@yieldkit/http';
import { ResilientHttpLive } from '@yieldkit/http/node';
import { Effect, Schema } from 'effect';

const Item = Schema.Struct({ id: Schema.String, name: Schema.String });

const fetchItem = Effect.gen(function* () {
  const api = yield* makeResilientHttpClient({
    upstream: 'example-api',
    baseUrl: 'https://api.example.com',
    userAgent: 'my-app/1.0',
    rateLimit: { limit: 30, window: '1 minute' },
    circuitBreaker: {},
  });
  return yield* api.getJson('/items/42', Item);
});

const program = fetchItem.pipe(
  Effect.catchTags({
    HttpRateLimitedError: (error) => Effect.succeed(`retry in ${error.retryAfterSeconds}s`),
    HttpRequestError: (error) => Effect.succeed(`failed: ${error.status} ${error.message}`),
  }),
  Effect.provide(ResilientHttpLive),
);
```

`ResilientHttpLive` provides an `HttpClient` plus an in-memory rate limiter, circuit breaker and ETag cache. Provide it once per process, at the program's entry point. Providing it per call, as this snippet does if you copy it into a request handler, gives each call a fresh limiter, breaker and cache. On Bun, import it from `@yieldkit/http/bun`. Elsewhere, provide any `HttpClient` (such as `FetchHttpClient.layer`) with `HttpResilienceLive`. See [Layers and runtimes](docs/layers.md).

Only `upstream` is required. Every option and its default is in [Policy](docs/policy.md).

## Common tasks

In these samples, `api` is a client built as in the quick start and `Item` is its schema.

### Poll a resource with ETags

`getCachedJson` sends `If-None-Match` and returns the cached value on a `304`. `staleIfError` serves the last good payload while the upstream is down. See [Caching](docs/caching.md).

```ts
import { Schedule } from 'effect';

const Items = Schema.Array(Item); // module level: the cache is keyed by schema identity

const watchItems = Effect.gen(function* () {
  const registry = yield* makeResilientHttpClient({
    upstream: 'registry',
    baseUrl: 'https://registry.example.com',
    staleIfError: '1 hour',
  });
  const poll = registry.getCachedJson('/items', Items);
  return yield* poll.pipe(Effect.repeat(Schedule.spaced('1 minute')));
});
```

### Retry a POST

`POST` and `PATCH` are retried only on a rate limit with `retryOnRateLimit`, unless you mark the request `idempotent`. See [Retries and timeouts](docs/retries-and-timeouts.md).

```ts
const order = api.postJson('/orders', { sku: 'abc' }, Schema.Struct({ id: Schema.String }), {
  idempotent: true,
  headers: { 'idempotency-key': 'order-7f3a' },
  operationTimeout: '10 seconds',
});
```

### Stream a large download

`stream` never buffers the body. The `maxBodyBytes` cap and an idle timeout still apply. See [Requests](docs/requests.md#stream-a-body).

```ts
import { Stream } from 'effect';

const lines = api
  .stream('/exports/events.ndjson', { maxBodyBytes: 1024 ** 3, idleTimeout: '60 seconds' })
  .pipe(Stream.decodeText(), Stream.splitLines);
```

### Fetch URLs nobody vetted

`publicAddressesOnly` refuses private, loopback and link-local targets on the first URL and every redirect hop. See [Redirects and the SSRF guard](docs/redirects-and-ssrf.md).

```ts
const makePreview = Effect.gen(function* () {
  const scraper = yield* makeResilientHttpClient({
    upstream: 'link-preview',
    publicAddressesOnly: true,
    maxRedirects: 3,
  });
  return (userSuppliedUrl: string) => scraper.getText(userSuppliedUrl);
});
```

### Log a failure without leaking the body

The message carries the status and request id. The body goes in `bodySnippet`, with the credentials the request sent masked. See [Errors](docs/errors.md).

```ts
const item = Effect.tapError(api.getJson('/items/42', Item), (error) =>
  error._tag === 'HttpRequestError'
    ? Effect.logWarning(error.message).pipe(
        Effect.annotateLogs({ bodySnippet: error.bodySnippet ?? '' }),
      )
    : Effect.void,
);
```

### Test against a stub

Provide an `HttpClient` built from a function. See [Layers and runtimes](docs/layers.md#testing).

```ts
import { HttpClient, HttpClientResponse } from 'effect/http';

const notFound = HttpClient.make((request) =>
  Effect.succeed(HttpClientResponse.fromWeb(request, new Response('nope', { status: 404 }))),
);

const result = fetchItem.pipe(Effect.provideService(HttpClient.HttpClient, notFound));
```

## Documentation

| Page                                                                               | Covers                                                                    |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| [Requests](docs/requests.md)                                                       | Every method, per-request options, headers, streaming, request transforms |
| [Retries and timeouts](docs/retries-and-timeouts.md)                               | What is retried, backoff, `attemptTimeout` and `operationTimeout`         |
| [Rate limiting and the circuit breaker](docs/rate-limiting-and-circuit-breaker.md) | `rateLimit`, `circuitBreaker` and `makeHttpResilienceLayer`               |
| [Caching](docs/caching.md)                                                         | `getCachedJson`, `getConditionalJson` and `staleIfError`                  |
| [Redirects and the SSRF guard](docs/redirects-and-ssrf.md)                         | Redirect rules and `publicAddressesOnly`                                  |
| [Errors](docs/errors.md)                                                           | Error fields, body snippets and credential masking                        |
| [Metrics and tracing](docs/metrics.md)                                             | `resilient_http_*` metrics, outcomes and client spans                     |
| [Policy](docs/policy.md)                                                           | Every option with its default, validation and 403 handling                |
| [Layers and runtimes](docs/layers.md)                                              | Node, Bun and fetch layers, the `ResilientHttp` service, testing          |

## Development

```bash
pnpm install
pnpm check   # format, lint and types
pnpm test
```

See [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request, and [SECURITY.md](SECURITY.md) to report a vulnerability.

## License

[MIT](LICENSE)
