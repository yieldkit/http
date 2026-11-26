import { Duration, Exit } from 'effect';
import { CircuitBreaker } from 'effect/persistence';
import { describe, expect, it } from 'vite-plus/test';

import { classifyCircuitExit } from './circuit.ts';
import { tagHttpError } from './error-meta.ts';
import { HttpRateLimitedError, HttpRequestError } from './errors.ts';

describe('classifyCircuitExit', () => {
  it('counts a 503 and ignores the same error tagged circuit: ignore', () => {
    const counted = new HttpRequestError({ upstream: 'api', status: 503, message: 'down' });
    const ignored = tagHttpError(
      new HttpRequestError({ upstream: 'api', status: null, message: 'blocked' }),
      { circuit: 'ignore' },
    );
    expect(classifyCircuitExit(Exit.fail(counted))).toEqual(CircuitBreaker.failure());
    expect(classifyCircuitExit(Exit.fail(ignored))).toBe(CircuitBreaker.ignored);
  });

  it('counts 429 and HttpRateLimitedError as failures even when retryOnRateLimit is false', () => {
    const rateLimited = new HttpRateLimitedError({
      upstream: 'api',
      retryAfterSeconds: 12,
      message: 'limited',
    });
    const status429 = new HttpRequestError({ upstream: 'api', status: 429, message: 'limited' });
    expect(classifyCircuitExit(Exit.fail(rateLimited))).toEqual(
      CircuitBreaker.failure({ retryAfter: Duration.seconds(12) }),
    );
    expect(classifyCircuitExit(Exit.fail(status429))).toEqual(CircuitBreaker.failure());
  });

  it('closes a half-open probe that got a non-transient answer, and ignores it otherwise', () => {
    const notFound = new HttpRequestError({ upstream: 'api', status: 404, message: 'missing' });
    const accepted = { isIgnoredSuccess: (value: string) => value === 'accepted-404' };
    expect(classifyCircuitExit(Exit.fail(notFound))).toBe(CircuitBreaker.ignored);
    expect(classifyCircuitExit(Exit.fail(notFound), undefined, { probe: true })).toBe(
      CircuitBreaker.success,
    );
    expect(classifyCircuitExit(Exit.succeed('accepted-404'), accepted, { probe: false })).toBe(
      CircuitBreaker.ignored,
    );
    expect(classifyCircuitExit(Exit.succeed('accepted-404'), accepted, { probe: true })).toBe(
      CircuitBreaker.success,
    );
  });

  it('keeps ignoring a probe refused before anything was sent', () => {
    const refused = tagHttpError(
      new HttpRequestError({ upstream: 'api', status: null, message: 'blocked' }),
      { circuit: 'ignore' },
    );
    expect(classifyCircuitExit(Exit.fail(refused), undefined, { probe: true })).toBe(
      CircuitBreaker.ignored,
    );
  });
});
