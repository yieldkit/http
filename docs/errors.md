# Errors

Every method fails with `HttpError`, the union of two tagged errors. The message carries the status and request id, never the response body.

The samples on this page use `fetchItem` from the [README quick start](../README.md#quick-start) and `api`, a client from `makeResilientHttpClient`.

```ts
import { Effect } from 'effect';

const itemOrReason = fetchItem.pipe(
  Effect.catchTags({
    HttpRateLimitedError: (error) => Effect.succeed(`retry in ${error.retryAfterSeconds}s`),
    HttpRequestError: (error) => Effect.succeed(`failed: ${error.status} ${error.message}`),
  }),
);
```

| Error                  | When                                                                                                                                                               | Fields                                                                                    |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------- |
| `HttpRateLimitedError` | A 429, a 403 the policy treats as a rate limit, or the client-side limiter with `onExceeded: 'fail'` (not retried, ignored by the breaker)                         | `upstream`, `retryAfterSeconds` or `null`, `message`                                      |
| `HttpRequestError`     | Any other non-2xx, a transport error, a timeout, a body over the cap, invalid JSON, a schema mismatch, an open circuit (`503`), an SSRF refusal or invalid options | `upstream`, `status` or `null`, `message`, and optionally `bodySnippet` and `decodeIssue` |

What `itemOrReason` gives for a few responses:

| Response                                                            | Result                                                                 |
| ------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| 403, `x-ratelimit-remaining: 0`, `retry-after: 120`                 | `'retry in 120s'` (with `treatQuotaExhausted403AsRateLimit`)           |
| 404, empty body                                                     | `'failed: 404 example-api responded with HTTP 404'`                    |
| 422, `x-request-id: req-1`, `{"message":"Not Found","token":"abc"}` | `'failed: 422 example-api responded with HTTP 422 (request id req-1)'` |
| 200, `{"id":1}`                                                     | `'failed: 200 Failed to decode example-api response: schema mismatch'` |

The request id in the message comes from `x-request-id`, `x-amzn-requestid` or `cf-ray`.

Each failure carries its outcome apart from the message, so metrics and retries never parse it. `classifyErrorOutcome` reads the outcome:

```ts
import { classifyErrorOutcome } from '@yieldkit/http';

const quietTimeouts = fetchItem.pipe(
  Effect.catchTag('HttpRequestError', (error) =>
    classifyErrorOutcome(error) === 'timeout' ? Effect.succeed(null) : Effect.fail(error),
  ),
);
```

The outcomes are listed in [Metrics](metrics.md#metrics).

## Body snippets

The body goes in its own fields, so you decide where it is written:

```ts
const logged = fetchItem.pipe(
  Effect.tapError((error) =>
    error._tag === 'HttpRequestError' && error.bodySnippet !== undefined
      ? Effect.logWarning(error.message).pipe(
          Effect.annotateLogs({ bodySnippet: error.bodySnippet }),
        )
      : Effect.void,
  ),
);
```

- `bodySnippet`: for a non-2xx, the first 4 KiB of the body cut at 256 characters (`…` marks a cut). A non-2xx HTML or XHTML body (by content type, or a leading doctype or `<html>` tag) contributes only its `<title>`. A decode failure quotes up to 200 characters of cleaned body text, which can include markup. Snippets are absent when there is nothing to quote. JSON is written back compact, and control characters and whitespace runs are collapsed.
- `decodeIssue`: for a schema mismatch, the schema issue cut at 500 characters. It quotes the values that failed to decode.

## Credential masking

Credentials are kept out by key name and by what the request sent, not by scanning text:

- In a JSON body that parses, the value under every key `@yieldkit/logger`'s `isSensitiveLogKey` matches (`access_token`, `password`, `apiKey`, `Authorization`, ...) becomes `<redacted>`, arrays and objects included.
- Sent values become `<redacted>` wherever the body, schema issue, request id or transport detail echoes them ("Incorrect API key provided: sk-live-…"), JSON-escaped forms (`\u002B`, `\/`) included. A request id holding one is dropped.

The sent values are:

- headers under a credential name, including those from an ambient `FetchHttpClient.RequestInit`;
- query parameters under a credential name: an `isSensitiveLogKey` name, or `key`, `apikey`, `auth`, `code`, `sig`, `signature`, `token` and the AWS and Google signed-URL names, in any case and percent-decoded;
- the parts of a cookie or `Bearer <token>` of 8 or more characters;
- every query parameter passed as `Redacted`.

`HttpRequestOptions.urlParams` takes strings, so send a `Redacted` query value with `execute`:

```ts
import { Redacted } from 'effect';
import { HttpClientRequest } from 'effect/http';

const search = api.execute(
  HttpClientRequest.get('/search', {
    urlParams: { q: 'effect', ref: Redacted.make('r-8c1f5a0e') },
  }),
);
```

URLs in logs and spans are masked by query parameter: only `&`-separated parameters with a credential name, a name in `UrlParams.CurrentRedactedNames` or a `Redacted` value.

## Transport failures

A transport failure, or a failing `HttpClient.mapRequestEffect`, reads `<upstream> request failed: <Reason> error[: <detail>] (<METHOD> <origin>)`, outcome `network_error` (`timeout` for a transport timeout). A body that fails partway reads `<upstream> failed reading response body (HTTP <n>): <detail>`.

Only the URL's origin appears, never its path or query. In the detail every `scheme://` URL (`https://…`, `ftp://…`) is cut to its origin, and sent values are masked as above, in both shapes.

A header value with a carriage return, line feed or NUL is refused before sending, on every transport: `<upstream> refused: header <name> contains a line break or NUL character`, outcome `blocked`, not retried, not counted by the breaker. Trim values read from files or environment variables.

## Limits

- Everything else is written as the upstream sent it. Treat `bodySnippet` and `decodeIssue` as untrusted text.
- A body that is not complete JSON (plain text, or JSON cut by the 4 KiB read) is masked only for sent values.
- A value sent under an ordinary name (an `x-tenant-id` header, a `ref` query parameter) is not masked. Pass such a query value as `Redacted`.
- A sent value shorter than 8 characters is never masked by value, `Redacted` included, so a short code can appear in `bodySnippet`.
- URL masking keeps tokens in the path, `;`-separated parameters and other names. Pass them as `Redacted` or add their names to `UrlParams.CurrentRedactedNames`.
- Other URL forms a transport quotes, such as a `data:` URL, are not cut to their origin.
- The 4 KiB snippet read and the 64 KiB `detectHtmlEdgeBlocks` read of a 403 have their own caps, independent of a smaller `maxBodyBytes`.
