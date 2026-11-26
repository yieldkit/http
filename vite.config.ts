import { fmtConfig } from '@yieldkit/oxlint-config';
import oxlintConfig from '@yieldkit/oxlint-config/effect';
import { defineConfig } from 'vite-plus';

/**
 * Paths neither the formatter nor the linter touches: build output, packed tarballs and coverage.
 */
const ignoredPatterns = ['coverage/**', '*.tgz', 'pnpm-lock.yaml'];

export default defineConfig({
  pack: {
    entry: ['src/index.ts', 'src/bun.ts', 'src/node.ts'],
    platform: 'neutral',
    // Node built-ins (`node:dns/promises`, `node:crypto`) stay imports; Node and Bun both provide them.
    deps: { neverBundle: [/^node:/] },
    format: ['esm'],
    dts: true,
    unbundle: true,
    fixedExtension: false,
  },
  staged: {
    '*': 'effect-tsgo vp check --fix',
  },
  fmt: {
    ...fmtConfig,
    ignorePatterns: [...fmtConfig.ignorePatterns, ...ignoredPatterns],
  },
  lint: {
    ...oxlintConfig,
    ignorePatterns: [...oxlintConfig.ignorePatterns, ...ignoredPatterns],
    rules: {
      ...oxlintConfig.rules,
      // `HttpClient`, `RateLimiter` and `CircuitBreaker` live in Effect's unstable and experimental
      // modules; this package builds on them by design, so its `effect` peer range (`~4.1.6`) only
      // admits patch releases of the minor version it is tested against.
      'effecttsgo/unstable-api-usage': 'off',
      'effecttsgo/experimental-api-usage': 'off',
    },
    overrides: [
      ...oxlintConfig.overrides,
      {
        // Tests and fixtures run effects directly.
        files: ['src/**/*.test.ts', 'src/**/*.fixture.ts'],
        rules: {
          'slop/no-effect-run-outside-entry': 'off',
        },
      },
      {
        files: ['src/index.ts'],
        rules: {
          'oxc/no-barrel-file': 'off',
        },
      },
      {
        files: ['scripts/**/*.mjs'],
        rules: {
          'no-console': 'off',
          // Plain Node scripts without Effect: JSON comes from files this repository controls.
          'slop/no-raw-json-parse': 'off',
        },
      },
    ],
  },
  test: {
    include: ['src/**/*.test.ts'],
  },
  run: {
    cache: { scripts: false, tasks: true },
  },
});
