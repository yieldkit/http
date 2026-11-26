# Redirects and the SSRF guard

Every client follows redirects itself, so credentials stay with their origin. `publicAddressesOnly` adds an SSRF guard for clients that fetch URLs nobody vetted, such as a link-preview scraper.

```ts
import { classifyErrorOutcome, makeResilientHttpClient } from '@yieldkit/http';
import { Effect } from 'effect';

const makePreview = Effect.gen(function* () {
  const scraper = yield* makeResilientHttpClient({
    upstream: 'link-preview',
    publicAddressesOnly: true,
    maxRedirects: 3,
    maxBodyBytes: 512 * 1024,
  });
  return (url: string) =>
    scraper
      .getText(url)
      .pipe(
        Effect.catchTag('HttpRequestError', (error) =>
          classifyErrorOutcome(error) === 'blocked' ? Effect.succeed(null) : Effect.fail(error),
        ),
      );
});
```

## Redirects

Clients follow up to `maxRedirects` hops (default 5), guard or not; one more fails with outcome `blocked`. `redirect: 'manual'` is merged into any ambient `FetchHttpClient.RequestInit`, so the transport returns each 3xx as it is.

- Methods follow the Fetch standard: a 303 turns any method but GET and HEAD into a body-less GET, a 301 or 302 does that only for a POST, and every other hop keeps its method and body.
- The target URL is sent as written, so a signed query (`?q=a%20b&flag&x=~`) reaches the next hop byte for byte.
- A hop to anything but `http:` or `https:`, or to a URL with credentials, is refused (outcome `blocked`), guard or not. Some transports serve other schemes, such as Bun's `fetch` reading `file:` URLs.
- A hop that would replay a non-empty body to another origin is refused, whatever the method, a GET with a body included (`<upstream> refused <origin>: a 307 redirect to another origin would forward the request body`, outcome `blocked`). So is any hop that would replay a one-shot body: `HttpBody.stream`, or `HttpBody.raw` around a `ReadableStream` or a Node `Readable`.
- Once a hop crosses origins, only `accept`, `accept-encoding`, `accept-language` and `user-agent` are kept, from the request and an ambient `RequestInit`, so credentials never reach another origin.

The rules apply to the request after the `HttpClient`'s [request transforms](requests.md#request-transforms), which run before the first hop only. A header a transform added is stripped on a cross-origin hop like any other, and the guard checks the URL a transform rewrote. A URL still relative after the transforms fails as `<upstream> refused: request URL is not absolute`.

## SSRF guard

`publicAddressesOnly: true` checks the first URL and every redirect hop. Each must:

1. use `http:` or `https:`, with no username or password;
2. have a public hostname: not `localhost`, `*.localhost`, `*.local`, `*.internal`, `*.home.arpa`, `*.lan` or a single label;
3. resolve only to public addresses. IP literals are normalised first (`http://2130706433/` is `127.0.0.1`), and a lookup error fails closed.

A refusal fails with `<upstream> refused <origin>: <reason>`, outcome `blocked`, not retried, not counted by the breaker. A DNS failure is a retried `network_error`.

Names resolve through the `AddressResolver` service, by default `lookup` from `node:dns/promises`. Provide your own to stub DNS in tests or use another resolver:

<!-- prettier-ignore -->
```ts
import { AddressResolver, isPublicIpAddress } from '@yieldkit/http';

isPublicIpAddress('93.184.215.14');    // true
isPublicIpAddress('10.0.0.1');         // false
isPublicIpAddress('::ffff:127.0.0.1'); // false: mapped addresses are judged as IPv4

const tested = makePreview.pipe(
  Effect.flatMap((preview) => preview('https://example.com/')),
  Effect.provideService(AddressResolver, () => Promise.resolve(['93.184.215.14'])),
);
```

## Limits

- **The guard does not stop DNS rebinding.** The transport resolves the name again after the check, so network egress rules are still the backstop.
- Headers added after the redirect rules, through `BunHttpClient.layerOptions` or a wrapper around the client's `postprocess` (such as `HttpClient.transformResponse`), reach every hop. Put credentials on the request or in a `mapRequest` transform.
- The rules hold for a transport that returns each 3xx as it is. `FetchHttpClient` and `BunHttpClient` do with `redirect: 'manual'`, and `NodeHttpClient.layerUndici` (the `@yieldkit/http/node` transport) never follows redirects.
- A transport that follows redirects on its own (a client wrapped in `HttpClient.followRedirects`, or a custom one that ignores the `RequestInit`) applies its own rules instead. With `publicAddressesOnly` its redirected responses are refused; without it they are used as they come.
