# Retries and timeouts

`attemptTimeout` bounds each attempt and `operationTimeout` the whole call. Transient failures are retried with jittered exponential backoff.

<!-- prettier-ignore -->
```ts
import { makeResilientHttpClient } from '@yieldkit/http';
import { Effect } from 'effect';

const makeApi = makeResilientHttpClient({
  upstream: 'example-api',
  attemptTimeout: '5 seconds',    // each attempt, body included
  operationTimeout: '20 seconds', // limiter wait, attempts and backoff sleeps
  retry: {
    maxRetries: 2,
    baseDelay: '500 millis',
    maxBackoffSeconds: 4,
    retryOnRateLimit: true,       // also retry a 429 inline
  },
});
```

Both can be set per request (see [Per-request options](requests.md#per-request-options)):

```ts
const report = Effect.flatMap(makeApi, (api) =>
  api.getBytes('/reports/2026.csv', { operationTimeout: '2 minutes' }),
);
```

## One call, step by step

1. With `rateLimit` and a `RateLimiter` in context, the call takes a token from the `rateLimit.key` bucket. See [Rate limiting and the circuit breaker](rate-limiting-and-circuit-breaker.md).
2. Each attempt runs under `attemptTimeout`, body included. A transient failure is retried up to `retry.maxRetries` times.
3. Limiter wait, attempts and backoff sleeps run under `operationTimeout`. Running out fails with `<upstream> operation timed out after <duration> (<n> attempts)`, outcome `timeout`, unless an error status's body was being read.
4. With `circuitBreaker` and a `CircuitBreaker` in context, all of that runs through the `circuit:<upstream>` breaker.

## What is retried

Transient means `408`, `425`, `500`–`504`, `520`–`524`, no response, or a timeout. A rate limit (`429`, or a 403 the policy treats as one) is transient only with `retry.retryOnRateLimit: true`; by default it fails at once.

Only idempotent methods are retried on every transient failure:

| Method                                             | Retried on                                                                                      |
| -------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `GET`, `HEAD`, `OPTIONS`, `PUT`, `DELETE`, `TRACE` | Any transient failure, including a body that fails partway through                              |
| `POST`, `PATCH`                                    | Only `HttpRateLimitedError` with `retryOnRateLimit`, unless the request sets `idempotent: true` |

`IDEMPOTENT_METHODS` and `isRetryableForMethod({ error, method, idempotent, retryOnRateLimit })` expose the rule:

<!-- prettier-ignore -->
```ts
import { HttpRequestError, isRetryableForMethod } from '@yieldkit/http';

const error = new HttpRequestError({ upstream: 'example-api', status: 503, message: 'down' });

isRetryableForMethod({ error, method: 'GET' });                    // true
isRetryableForMethod({ error, method: 'POST' });                   // false
isRetryableForMethod({ error, method: 'POST', idempotent: true }); // true
```

Invalid JSON and schema mismatches are never retried.

## Backoff

The delay is jittered exponential from `retry.baseDelay`, cut to `retry.maxBackoffSeconds`. A `Retry-After` on a transient status is the minimum delay; one over `retry.maxInlineRetryAfterSeconds` fails at once. A retry whose delay would end at or after the operation deadline is not started: the last error is returned instead.

Each retry logs `http.request.retry` at debug level, credential query values masked. `resilient_http_retries_total` counts it when the next attempt goes out, so a retry the deadline or the limiter cut short is not counted.

## Defaults

| Setting                            | Default        | Constant                                 |
| ---------------------------------- | -------------- | ---------------------------------------- |
| `attemptTimeout`                   | `'15 seconds'` | `REQUEST_TIMEOUT`                        |
| `operationTimeout`                 | `'60 seconds'` | `DEFAULT_OPERATION_TIMEOUT`              |
| `idleTimeout` (per request)        | `'30 seconds'` | `DEFAULT_IDLE_TIMEOUT`                   |
| `retry.maxRetries`                 | `3`            | `DEFAULT_MAX_RETRIES`                    |
| `retry.baseDelay`                  | `'250 millis'` | `DEFAULT_BASE_DELAY`                     |
| `retry.maxBackoffSeconds`          | `5`            | `DEFAULT_MAX_BACKOFF_SECONDS`            |
| `retry.maxInlineRetryAfterSeconds` | `10`           | `DEFAULT_MAX_INLINE_RETRY_AFTER_SECONDS` |
| `retry.retryOnRateLimit`           | `false`        | `DEFAULT_RETRY_ON_RATE_LIMIT`            |

`MAX_RATE_LIMIT_DELAY_SECONDS` caps any `Retry-After` or rate-limit reset this package reads.

## Limits

- `operationTimeout` is not an end-to-end deadline. It starts once the call holds its `maxConcurrency` permit, and does not cover `withWebResponse`'s `use` or `getCachedJson`'s cache-key transform run (see [Caching](caching.md#limits)). For `stream` it also covers the body.
- The wait for a `maxConcurrency` permit has no bound and is outside `operationTimeout`: with a hung upstream, `maxConcurrency` 100 and 1,000 callers, the last calls waited ~58 s despite a 10 s `operationTimeout` (measured on localhost). Bound the caller yourself, e.g. `Effect.timeout` around the call, if queue time matters.
- When `attemptTimeout` or `operationTimeout` runs out while an error status's body is being read, the request fails with that status error, without a snippet.
- A rate limit on a redirect target is not replayed for a `POST` or `PATCH`, since the replay would resend the original request.
- A request whose body is one-shot (`HttpBody.stream`, or `HttpBody.raw` around a `ReadableStream` or a Node `Readable`) is never retried, whatever its method, because the first attempt consumed it.
- Retries are decided from the request as given to the client, not as a transform rewrote it. See [Request transforms](requests.md#request-transforms).
- Once a `stream` has a 2xx, nothing is retried.
