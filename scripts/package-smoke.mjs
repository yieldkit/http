import assert from 'node:assert/strict';
import { execFileSync, spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

/**
 * How long `npm install` of the tarball may take: it downloads every dependency on a cold cache.
 */
const INSTALL_TIMEOUT_MS = 300_000;

/**
 * How long one type-check or script run in the consumer may take.
 */
const COMMAND_TIMEOUT_MS = 120_000;

/**
 * TypeScript sources other than declarations, which must not ship: consumers load the built modules
 * from `node_modules`, where Node does not strip types.
 */
const TYPESCRIPT_SOURCE_PATTERN = /(?<!\.d)\.(ts|tsx)$/;

/**
 * The optional platform peers, each of which only its own entry point may load.
 */
const PLATFORM_PACKAGES = ['@effect/platform-node', '@effect/platform-bun'];

/**
 * The repository the tarball was packed from.
 */
const root = resolve(import.meta.dirname, '..');

/**
 * The tarball supplied by CI or a local `pnpm pack` command.
 */
const tarball = process.argv[2];
assert.ok(tarball, 'Usage: pnpm test:package /absolute/path/to/http.tgz');

/**
 * The manifest of the checkout, which the packed tarball must match.
 */
const sourceManifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));

/**
 * Reads the version of a dependency installed in this checkout, so the consumer runs the same
 * versions CI tested against.
 */
const installedVersion = (name) =>
  JSON.parse(readFileSync(join(root, 'node_modules', name, 'package.json'), 'utf8')).version;

/**
 * An isolated consumer project outside the checkout and its development dependencies, removed when
 * the script exits.
 */
const consumer = mkdtempSync(join(tmpdir(), 'http-package-'));
process.on('exit', () => rmSync(consumer, { recursive: true, force: true }));

writeFileSync(
  join(consumer, 'package.json'),
  JSON.stringify({ name: 'consumer', private: true, type: 'module' }, null, 2),
);

/**
 * Installs `specs` into the consumer. Dependencies resolve outside the lockfile here, so install
 * scripts stay off.
 */
const npmInstall = (specs) =>
  execFileSync('npm', ['install', '--no-audit', '--no-fund', '--ignore-scripts', ...specs], {
    cwd: consumer,
    stdio: 'inherit',
    timeout: INSTALL_TIMEOUT_MS,
  });

// First without the optional platform peers: the root entry point must work without them.
npmInstall([
  resolve(tarball),
  `effect@${installedVersion('effect')}`,
  `typescript@${installedVersion('typescript')}`,
  `@types/node@${installedVersion('@types/node')}`,
]);

/**
 * Where npm installed the package under test inside the consumer.
 */
const installed = join(consumer, 'node_modules', sourceManifest.name);

/**
 * The manifest npm installed from the tarball.
 */
const manifest = JSON.parse(readFileSync(join(installed, 'package.json'), 'utf8'));
assert.equal(manifest.version, sourceManifest.version);

for (const section of ['dependencies', 'peerDependencies', 'optionalDependencies']) {
  for (const version of Object.values(manifest[section] ?? {})) {
    assert.ok(
      !version.startsWith('catalog:') && !version.startsWith('workspace:'),
      'pnpm must resolve catalog and workspace versions before publishing',
    );
  }
}

for (const name of PLATFORM_PACKAGES) {
  assert.equal(manifest.peerDependenciesMeta?.[name]?.optional, true, `${name} must be optional`);
}

/**
 * Every path inside the installed package.
 */
const shipped = readdirSync(installed, { recursive: true }).map(String);

/**
 * Shipped TypeScript sources, which must be empty.
 */
const sources = shipped.filter((name) => TYPESCRIPT_SOURCE_PATTERN.test(name));
assert.deepEqual(sources, [], 'The package must not ship TypeScript sources');
assert.ok(!shipped.some((name) => name.includes('.test.')), 'The package must not ship tests');
assert.ok(
  !shipped.some((name) => name.includes('.fixture.')),
  'The package must not ship fixtures',
);

/**
 * Relative module specifiers in a built file, which the walk below follows.
 */
const RELATIVE_IMPORT_PATTERN = /from "(\.{1,2}\/[^"]+)"/g;

/**
 * Returns every built module `entry` loads, itself included, by following relative imports.
 */
const moduleGraph = (entry) => {
  const seen = new Set();
  const pending = [entry];
  while (pending.length > 0) {
    const file = pending.pop();
    if (!seen.has(file)) {
      seen.add(file);
      for (const [, specifier] of readFileSync(file, 'utf8').matchAll(RELATIVE_IMPORT_PATTERN)) {
        pending.push(resolve(dirname(file), specifier));
      }
    }
  }

  return [...seen];
};

/**
 * Built files reachable from `entry` that import `packageName` or one of its subpaths.
 */
const filesImporting = (entry, packageName) =>
  moduleGraph(join(installed, entry)).filter((file) =>
    readFileSync(file, 'utf8').includes(`from "${packageName}`),
  );

for (const name of PLATFORM_PACKAGES) {
  assert.deepEqual(
    filesImporting('dist/index.js', name),
    [],
    `dist/index.js must not load ${name}`,
  );
}

assert.deepEqual(filesImporting('dist/node.js', '@effect/platform-bun'), []);
assert.deepEqual(filesImporting('dist/bun.js', '@effect/platform-node'), []);

// The root entry point against a stub transport: a 503 is retried, a credential in the query stays
// out of the error, a loopback target is refused, and a public name passes the SSRF guard through a
// stub resolver, so nothing leaves the machine. It runs before the platform packages are installed,
// so loading either would fail.
writeFileSync(
  join(consumer, 'base.ts'),
  `import { AddressResolver, makeResilientHttpClient, type HttpError } from '${sourceManifest.name}';
import { Effect, Exit } from 'effect';
import { HttpClient, HttpClientResponse } from 'effect/http';

let attempts = 0;
const transport = HttpClient.make((request) => {
  attempts += request.url.includes('/flaky') ? 1 : 0;
  const status = request.url.includes('/broken') ? 500 : attempts === 1 ? 503 : 200;
  return Effect.succeed(HttpClientResponse.fromWeb(request, new Response('ok', { status })));
});

const failure = (exit: Exit.Exit<unknown, HttpError>): HttpError => {
  if (Exit.isSuccess(exit)) {
    throw new Error('Expected the request to fail');
  }
  const reason = exit.cause.reasons.find((entry) => entry._tag === 'Fail');
  if (reason === undefined) {
    throw new Error('Expected an HttpError');
  }
  return reason.error;
};

const program = Effect.gen(function* () {
  const api = yield* makeResilientHttpClient({
    upstream: 'smoke',
    baseUrl: 'https://api.example.com',
    retry: { maxRetries: 2, baseDelay: '1 millis' },
  });
  const text = yield* api.getText('/flaky');
  if (text !== 'ok' || attempts !== 2) {
    throw new Error(\`Expected one retry, got \${String(attempts)} attempts\`);
  }

  const broken = failure(yield* Effect.exit(api.getText('/broken?token=SECRET_TOKEN')));
  if (!broken.message.includes('500') || JSON.stringify(broken).includes('SECRET_TOKEN')) {
    throw new Error(\`The error leaks the credential or lost the status: \${broken.message}\`);
  }

  const guarded = yield* makeResilientHttpClient({ upstream: 'guarded', publicAddressesOnly: true });
  const loopback = failure(yield* Effect.exit(guarded.getText('http://127.0.0.1:9/')));
  if (!loopback.message.includes('refused')) {
    throw new Error(\`A loopback target was not refused: \${loopback.message}\`);
  }
  // A public name goes through the resolver before the stub transport answers.
  return yield* guarded.getText('https://example.com/');
});

const lookups: Array<string> = [];
const result = await Effect.runPromise(
  program.pipe(
    Effect.provideService(HttpClient.HttpClient, transport),
    Effect.provideService(AddressResolver, (hostname) => {
      lookups.push(hostname);
      return Promise.resolve(['93.184.215.14']);
    }),
  ),
);
if (result !== 'ok' || lookups.join() !== 'example.com') {
  throw new Error(\`The guarded request did not pass the resolver: \${lookups.join()}\`);
}
`,
);

// Each runtime entry point over a real socket: a JSON body is decoded, and a redirect to another
// origin is followed by the package with the credentials dropped.
writeFileSync(
  join(consumer, 'scenario.ts'),
  `import { makeResilientHttpClient, type ResilientHttp } from '${sourceManifest.name}';
import { Effect, Layer, Schema } from 'effect';
import type { HttpClient } from 'effect/http';
import { createServer, type Server } from 'node:http';

const Item = Schema.Struct({ id: Schema.Int, title: Schema.String });

const listen = (server: Server): Promise<void> =>
  new Promise((done) => {
    server.listen(0, '127.0.0.1', () => done());
  });

const portOf = (server: Server): number => {
  const address = server.address();
  return typeof address === 'object' && address !== null ? address.port : 0;
};

export const runScenario = async (
  live: Layer.Layer<HttpClient.HttpClient | ResilientHttp>,
): Promise<void> => {
  const leaked: Array<string> = [];
  const target = createServer((request, response) => {
    if (request.headers.authorization !== undefined) {
      leaked.push(request.headers.authorization);
    }
    response.end('landed');
  });
  const origin = createServer((request, response) => {
    if (request.url === '/start') {
      response.writeHead(302, { location: \`http://127.0.0.1:\${String(portOf(target))}/final\` });
      response.end();
      return;
    }
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ id: 1, title: 'first' }));
  });
  await Promise.all([listen(origin), listen(target)]);
  try {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const api = yield* makeResilientHttpClient({
          upstream: 'local',
          baseUrl: \`http://127.0.0.1:\${String(portOf(origin))}\`,
          defaultHeaders: { authorization: 'Bearer SECRET_AUTH' },
        });
        const item = yield* api.getJson('/item', Item);
        const landed = yield* api.getText('/start');
        return { item, landed };
      }).pipe(Effect.provide(live)),
    );
    if (result.item.id !== 1 || result.landed !== 'landed' || leaked.length > 0) {
      throw new Error(\`Unexpected result: \${JSON.stringify({ result, leaked })}\`);
    }
  } finally {
    origin.closeAllConnections();
    target.closeAllConnections();
    origin.close();
    target.close();
  }
};
`,
);

for (const runtime of ['node', 'bun']) {
  writeFileSync(
    join(consumer, `${runtime}.ts`),
    `import { ResilientHttpLive } from '${sourceManifest.name}/${runtime}';
import { runScenario } from './scenario.ts';

await runScenario(ResilientHttpLive);
`,
  );
}

/**
 * Process options shared by the consumer's type-check and runs.
 */
const options = {
  /**
   * Run inside the consumer project, so the installed tarball resolves.
   */
  cwd: consumer,

  /**
   * Show compiler and runtime output in the CI log.
   */
  stdio: 'inherit',

  /**
   * Fail instead of hanging on a stuck process.
   */
  timeout: COMMAND_TIMEOUT_MS,
};

/**
 * Whether Bun is on the `PATH`. CI installs it; a local run without it skips the Bun runs.
 */
const hasBun = spawnSync('bun', ['--version'], { stdio: 'ignore' }).status === 0;

// Node strips the types of a file outside `node_modules`, so the fixtures run as written.
execFileSync(process.execPath, ['base.ts'], options);
if (hasBun) {
  execFileSync('bun', ['base.ts'], options);
}

npmInstall(PLATFORM_PACKAGES.map((name) => `${name}@${installedVersion(name)}`));

execFileSync(
  join(consumer, 'node_modules/.bin/tsc'),
  [
    '--ignoreConfig',
    '--noEmit',
    '--strict',
    '--exactOptionalPropertyTypes',
    '--skipLibCheck',
    '--allowImportingTsExtensions',
    '--target',
    'ES2025',
    '--module',
    'nodenext',
    '--moduleResolution',
    'nodenext',
    '--types',
    'node',
    'base.ts',
    'node.ts',
    'bun.ts',
  ],
  options,
);

execFileSync(process.execPath, ['node.ts'], options);
// `node` runs on Node only: Bun swaps Undici for a stub without a dispatcher.
if (hasBun) {
  execFileSync('bun', ['bun.ts'], options);
} else {
  console.log('Bun is not on the PATH; skipped the Bun runs.');
}

console.log(`Package smoke test passed for ${manifest.name}@${manifest.version}.`);
