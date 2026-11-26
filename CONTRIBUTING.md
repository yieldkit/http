# Contributing

Thanks for helping improve `@yieldkit/http`.

## Setup

Use Node.js 22.19+, 24.11+ or 26+ and the pnpm version declared in `package.json` (`devEngines`). Install [Bun](https://bun.sh) too if you want `test:package` to run the Bun checks locally; CI always runs them.

```bash
pnpm install
pnpm check
pnpm test
```

`pnpm check` formats, lints and type-checks through `effect-tsgo`, which adds Effect's type-aware rules to `vp check`. The lint preset is [`@yieldkit/oxlint-config`](https://github.com/yieldkit/oxlint-config).

## Layout

- `src/index.ts`: the public API of the root entry point. Anything not exported here, from `src/node.ts` or from `src/bun.ts` is internal.
- `src/node.ts` and `src/bun.ts`: the `@yieldkit/http/node` and `@yieldkit/http/bun` entry points. They are the only modules that may import `@effect/platform-node` or `@effect/platform-bun`.
- `src/<module>.ts`: one concern per file, with `<module>.test.ts` next to it. `src/http.fixture.ts` holds shared test helpers.
- `docs/`: one page per topic, linked from `README.md`.

## Tests

Most tests drive the client through a stub `HttpClient` (`makeMockHttpClient`) and `TestClock`, so they never touch the network. `src/runtime-layers.test.ts` runs both runtime layers against local servers on `127.0.0.1`.

## Changing redirects, the SSRF guard or redaction

`docs/redirects-and-ssrf.md` and `docs/errors.md` are the contract. A change to which headers cross an origin, which addresses the guard accepts, or what an error message, `bodySnippet` or span may contain needs a test that fails without it, and the pull request should say how it changes what reaches an upstream or a log.

## Before opening a pull request

```bash
pnpm check
pnpm test
pnpm pack --out /tmp/http.tgz
pnpm test:package /tmp/http.tgz
```

`test:package` installs the packed tarball into a temporary project. It runs the root entry point before the platform packages are installed, so loading either one fails the test. It also type-checks a consumer against all three entry points, and runs each runtime layer against a local server on Node and, when Bun is installed, on Bun.

## Commits and releases

Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`).

To release, bump `version` in `package.json` on `main`, wait for CI to pass, then publish a GitHub release tagged `v<version>`. The publish workflow checks the tag matches `package.json` and CI passed for that commit, then publishes to npm with trusted publishing and provenance.
