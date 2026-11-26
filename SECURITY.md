# Security policy

## Supported versions

Security fixes go into the latest published version of `@yieldkit/http`. Upgrade to the newest release before reporting an issue that may already be fixed.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use [GitHub private vulnerability reporting](https://github.com/yieldkit/http/security/advisories/new) to share the details with the maintainer.

Include the affected version, the `effect` version, the runtime and `HttpClient` layer, reproduction steps and the expected impact. Do not include real secrets, hosts or personal data in the report; use made-up values.

## Scope

Most useful are reports where the client breaks a rule its [redirect and SSRF guard](docs/redirects-and-ssrf.md) or [error](docs/errors.md) documentation promises:

- With `publicAddressesOnly`, a request or redirect hop reaches a private, loopback or link-local address.
- A redirect carries `Authorization`, cookies, default headers or a request body to another origin, or reaches a non-http(s) scheme or a URL with credentials.
- A credential the request sent (a header, a query parameter or a bearer token) appears in an error message, `bodySnippet`, log line, span attribute or metric label.

Also in scope: a response that makes the client buffer past `maxBodyBytes`, ignore its timeouts or hang. Out of scope: DNS rebinding between the guard's lookup and the transport's connect, which the docs describe as a limit of a guard that runs before the socket opens, and behaviour of a custom `HttpClient` that follows redirects on its own.
