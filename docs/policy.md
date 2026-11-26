# Policy

An `HttpPolicy` configures one client. Only `upstream` is required. Every other field, with its default:

<!-- prettier-ignore -->
```ts
import { makeResilientHttpClient } from '@yieldkit/http';

makeResilientHttpClient({
  upstream: 'example-api',                  // errors, logs, metric labels, limiter/breaker/cache keys
  baseUrl: undefined,                       // prefix for relative paths; an absolute http(s) URL
  userAgent: undefined,                     // none added: the transport's default goes out
  defaultHeaders: {},                       // sent to the baseUrl origin (every request without one)
  attemptTimeout: '15 seconds',             // REQUEST_TIMEOUT; per attempt, body included
  operationTimeout: '60 seconds',           // DEFAULT_OPERATION_TIMEOUT; from the maxConcurrency permit on
  maxBodyBytes: 2 * 1024 * 1024,            // DEFAULT_MAX_BODY_BYTES; every body, JSON included
  staleIfError: undefined,                  // e.g. '1 hour'; see caching.md
  publicAddressesOnly: false,               // SSRF guard; see redirects-and-ssrf.md
  maxRedirects: 5,                          // DEFAULT_MAX_REDIRECTS
  maxConcurrency: undefined,                // unbounded; a positive integer
  treatQuotaExhausted403AsRateLimit: false, // 403 with x-ratelimit-remaining: 0 is a rate limit
  treat403AsRateLimit: false,               // every 403 is a rate limit
  detectHtmlEdgeBlocks: false,              // a 403 page a CDN or WAF generated is a rate limit
  retry: {
    maxRetries: 3,                          // DEFAULT_MAX_RETRIES
    baseDelay: '250 millis',                // DEFAULT_BASE_DELAY, jittered exponential
    maxBackoffSeconds: 5,                   // DEFAULT_MAX_BACKOFF_SECONDS
    maxInlineRetryAfterSeconds: 10,         // a longer Retry-After fails at once
    retryOnRateLimit: false,                // true retries a 429 or rate-limit 403 inline
  },
  rateLimit: {                              // off unless set
    limit: 30,
    window: '1 minute',
    key: 'ratelimit:example-api',           // default
    algorithm: 'token-bucket',              // or 'fixed-window'
    onExceeded: 'delay',                    // waits for a token; 'fail' raises HttpRateLimitedError
  },
  circuitBreaker: {},                       // off unless set; key defaults to 'circuit:<upstream>'
});
```

The topic pages explain each group: [requests](requests.md), [retries](retries-and-timeouts.md), [rate limits and the breaker](rate-limiting-and-circuit-breaker.md), [caching](caching.md) and [redirects](redirects-and-ssrf.md).

## Invalid values

An invalid value never hangs or dies. `resolveHttpPolicy(policy)` fills in the defaults and returns the `ResolvedHttpPolicy` a client exposes as `api.policy`. It never throws: an invalid value is replaced by its default and listed in `configError`.

<!-- prettier-ignore -->
```ts
import { resolveHttpPolicy } from '@yieldkit/http';

const resolved = resolveHttpPolicy({
  upstream: 'example-api',
  maxConcurrency: Number(process.env['HTTP_CONCURRENCY']), // NaN when unset
});

resolved.maxConcurrency; // null: unbounded
resolved.configError;    // HttpRequestError: example-api has invalid policy: maxConcurrency must be a positive safe integer
```

A client with that policy fails every request before sending with that error (`<upstream> has invalid policy: <fields>`, outcome `blocked`), not retried and ignored by the breaker. An invalid `rateLimit` is dropped.

Valid means:

- positive safe integers for `maxConcurrency`, `maxBodyBytes` and `rateLimit.limit`;
- non-negative integers for `maxRetries` and `maxRedirects`;
- finite non-negative numbers for the retry seconds;
- durations Effect can parse that are not negative, infinite or `NaN`, also inside a `[seconds, nanos]` tuple or a duration object (`'1e3 millis'` and `{ seconds: NaN }` are not valid).

Per-request options (`<upstream> has invalid request options: <fields>`) and limits passed straight to the `execute*Request` functions (`<upstream> has invalid request parameters: <fields>`) are checked the same way, before sending.

## 403 responses as rate limits

By default a 403 is a `client_error`. Three switches turn some of them into `HttpRateLimitedError`:

| Option                              | A 403 is a rate limit when                                                          |
| ----------------------------------- | ----------------------------------------------------------------------------------- |
| `treatQuotaExhausted403AsRateLimit` | it carries `x-ratelimit-remaining: 0`                                               |
| `treat403AsRateLimit`               | always                                                                              |
| `detectHtmlEdgeBlocks`              | its body or headers show a CDN or WAF block page (its `Retry-After`, or 60 seconds) |

```ts
const scraper = makeResilientHttpClient({
  upstream: 'news-site',
  detectHtmlEdgeBlocks: true,
  retry: { retryOnRateLimit: true, maxInlineRetryAfterSeconds: 60 },
});
```

`detectHtmlEdgeBlocks` looks for markers only block and challenge pages carry (Cloudflare `cf-chl` and `Attention Required!`, Akamai `Reference #`, CloudFront and AWS WAF block pages, Imperva, Sucuri, DDoS-Guard, PerimeterX, DataDome, CAPTCHA widgets) or a `cf-mitigated`, `x-amzn-waf-action` or `x-datadome` header. An ordinary `403 Forbidden` page stays a `client_error`.

## Limits

- An invalid `baseUrl` is listed in `configError`. A base URL must be an absolute `http(s)` URL without a query or fragment.
- `detectHtmlEdgeBlocks` is off in the `execute*Request` functions unless you pass it.
- The edge-block check and the error snippet share one body read with a 2 second budget. A 403 that is not a block page keeps its snippet; a stalled body gives a plain status error.
