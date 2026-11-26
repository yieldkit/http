# Metrics and tracing

Every client records `resilient_http_*` metrics and a client span per request, with URLs redacted. Export them with any Effect metrics and tracing setup, such as `@yieldkit/logger/otlp`.

The samples on this page use `fetchItem` and `program` from the [README quick start](../README.md#quick-start). `classifyErrorOutcome` and `classifyRetryReason` give an error the same labels the metrics use:

```ts
import { classifyErrorOutcome, classifyRetryReason } from '@yieldkit/http';
import { Effect } from 'effect';

const logged = fetchItem.pipe(
  Effect.tapError((error) =>
    Effect.logWarning('item fetch failed').pipe(
      Effect.annotateLogs({
        outcome: classifyErrorOutcome(error),
        reason: classifyRetryReason(error),
      }),
    ),
  ),
);
```

The `record*` helpers record them.

## Metrics

| Metric                               | Kind      | Labels                          | Export                  |
| ------------------------------------ | --------- | ------------------------------- | ----------------------- |
| `resilient_http_requests_total`      | counter   | `upstream`, `method`, `outcome` | `httpRequestsTotal`     |
| `resilient_http_request_duration_ms` | histogram | `upstream`, `method`, `outcome` | `httpRequestDurationMs` |
| `resilient_http_retries_total`       | counter   | `upstream`, `reason`            | `httpRetriesTotal`      |
| `resilient_http_rate_limits_total`   | counter   | `upstream`                      | `httpRateLimitsTotal`   |
| `resilient_http_stale_served_total`  | counter   | `upstream`                      | `httpStaleServedTotal`  |
| `resilient_http_active_requests`     | gauge     | `upstream`                      | `httpActiveRequests`    |

`outcome` is one of:

| Outcome             | Meaning                                                                  |
| ------------------- | ------------------------------------------------------------------------ |
| `success`           | A 2xx                                                                    |
| `not_modified`      | A 304 on a conditional request                                           |
| `not_found`         | A 404 accepted with `acceptNotFound` or by `getOptionalJson`             |
| `rate_limited`      | A 429, a 403 treated as a rate limit, or a local limiter refusal         |
| `client_error`      | Any other 4xx                                                            |
| `server_error`      | A 5xx                                                                    |
| `unexpected_status` | A non-2xx below 400, such as a 3xx that was not followed                 |
| `timeout`           | An attempt, operation, idle or transport timeout                         |
| `network_error`     | No response, a body that failed partway, or an interruption from outside |
| `decode_error`      | Invalid JSON or a schema mismatch                                        |
| `circuit_open`      | The breaker refused the call                                             |
| `body_too_large`    | A body over `maxBodyBytes`                                               |
| `blocked`           | A refusal during execution, such as an SSRF check or too many redirects  |

An invalid policy, invalid request options, or a `postJson` serialization failure can return `blocked` before request metrics start.

`reason` is `rate_limit`, `server_error`, `timeout` or `network_error`.

## Tracing

Each attempt and each redirect hop runs in its own client span. `HttpClient.SpanNameGenerator` names it, and `HttpClient.TracerDisabledWhen` turns it off:

```ts
import { HttpClient } from 'effect/http';

const traced = program.pipe(
  Effect.provideService(HttpClient.SpanNameGenerator, (request) => `example-api ${request.method}`),
  Effect.provideService(HttpClient.TracerDisabledWhen, (request) =>
    request.url.endsWith('/health'),
  ),
);
```

Spans follow the OpenTelemetry HTTP client conventions:

- `http.request.method`, `url.full`, `url.path`, `url.query`, `url.scheme`, `server.address` and `server.port`, from the redacted URL (see below);
- `http.response.status_code` when a response arrived;
- for a 4xx or 5xx, `error.type` set to the status code and the span failed, while the response still reaches your code;
- for a failure without a response, `error.type` set to the reason (`TransportError`, `InvalidUrlError`, `EncodeError`).

The URL is masked as Effect's own client span masks it (names in `UrlParams.CurrentRedactedNames`, `Redacted` query values), then as [`redactUrlForLogs`](requests.md#lower-level-exports) returns it: no userinfo, no fragment, credential query values masked.

A failed span holds the sanitised `HttpRequestError`, never the raw transport cause.

## Limits

- A failure without a status and without an outcome tag is a `network_error`. The message is never read.
- `stream` and `withWebResponse` record their request metric, and keep the gauge raised, until the body is done: the stream ends or `use` finishes.
- The package never adds trace propagation headers to outgoing requests.
