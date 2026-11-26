# Rate limiting and the circuit breaker

`rateLimit` keeps a client under an upstream's quota, and `circuitBreaker` stops calling an upstream that keeps failing. They need a `RateLimiter` and a `CircuitBreaker` from `effect/persistence` in context, or they do nothing. `HttpResilienceLive` and `ResilientHttpLive` provide in-memory ones.

```ts
import { HttpResilienceLive, makeResilientHttpClient } from '@yieldkit/http';
import { Effect } from 'effect';

const program = Effect.gen(function* () {
  const api = yield* makeResilientHttpClient({
    upstream: 'example-api',
    rateLimit: { limit: 30, window: '1 minute' },
    circuitBreaker: {},
  });
  return yield* api.getText('https://api.example.com/status');
}).pipe(Effect.provide(HttpResilienceLive));
```

## Rate limiting

<!-- prettier-ignore -->
```ts
makeResilientHttpClient({
  upstream: 'example-api',
  rateLimit: {
    limit: 30,
    window: '1 minute',
    key: 'ratelimit:example-api', // default: 'ratelimit:<upstream>'
    algorithm: 'token-bucket',    // or 'fixed-window'
    onExceeded: 'delay',          // or 'fail'
  },
});
```

Every attempt, retries included, takes one token from the `key` bucket. Clients that share a key share the bucket:

```ts
const quota = { limit: 100, window: '1 minute', key: 'ratelimit:example-org' } as const;

const clients = Effect.all([
  makeResilientHttpClient({ upstream: 'example-search', rateLimit: quota }),
  makeResilientHttpClient({ upstream: 'example-items', rateLimit: quota }),
]);
```

- `onExceeded: 'delay'` waits for a token. The call polls the limiter, and a refusal never reserves a slot. The wait counts against `operationTimeout`.
- `onExceeded: 'fail'` fails at once with `HttpRateLimitedError`, also when a retry finds the bucket empty. That error is not retried and not counted by the breaker.

Every upstream rate limit (a `429`, or a 403 the policy treats as one) and every `'fail'` refusal increments `resilient_http_rate_limits_total`.

## Circuit breaker

`circuitBreaker: {}` turns the breaker on for the `circuit:<upstream>` key; set `key` to share one between clients. The whole call (limiter wait, attempts, backoff) runs through it.

- Transient failures, rate limits and deadline overruns count against the breaker.
- Other 4xx, accepted 404s and SSRF refusals don't.
- An open breaker fails with an `HttpRequestError` with `status: 503` and outcome `circuit_open`.

When a circuit opens, and for how long, is set on the `CircuitBreaker` service, because one breaker serves every key. `HttpResilienceLive` opens a key after 5 consecutive failures, with a backoff of 5, 10, 20, 40, 80, then 160 seconds. If any rate-limited response (`HttpRateLimitedError`, including a 403 the policy treats as a rate limit) opens or reopens the circuit, its `Retry-After` can lengthen the opening up to one hour. A 503's `Retry-After` applies only to retries within a call. `makeHttpResilienceLayer` takes other settings:

<!-- prettier-ignore -->
```ts
import { makeHttpResilienceLayer } from '@yieldkit/http';

const resilience = makeHttpResilienceLayer({
  consecutiveFailures: 3,        // DEFAULT_CIRCUIT_CONSECUTIVE_FAILURES is 5
  openDuration: '10 seconds',    // DEFAULT_CIRCUIT_OPEN_DURATION is '5 seconds'
  openBackoffLimit: '2 minutes', // DEFAULT_CIRCUIT_OPEN_BACKOFF_LIMIT is '5 minutes'
});
```

`consecutiveFailures` must be a positive safe integer, and both durations finite and above zero. Calling `makeHttpResilienceLayer` never throws; building the layer dies with a `RangeError` naming the setting. A `NaN` from an unset environment variable stops the program at startup instead of giving a breaker that never opens.

## Limits

- A call keeps its `maxConcurrency` permit for its whole run: while it waits on a token with `onExceeded: 'delay'`, and through its retries, backoff and `Retry-After` sleeps. Size `maxConcurrency` for the calls waiting or sleeping as well as those in flight. With `maxConcurrency` 100 and 20% of responses 503, throughput fell from 3,606 to 907 calls per second (measured on localhost, Undici, 20 ms upstream). In exchange the breaker sees each queued call when it gets its permit, so once a circuit opens the calls still queued are refused without reaching the upstream.
- A half-open probe answered with a 4xx or an accepted 404 closes the circuit, since the upstream answered.
