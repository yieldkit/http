import { describe, expect, it } from 'vite-plus/test';

import { tagHttpError } from './error-meta.ts';
import { HttpRateLimitedError, HttpRequestError } from './errors.ts';
import { classifyErrorOutcome, classifyRetryReason } from './metrics.ts';

describe('classifyErrorOutcome', () => {
  it('labels rate limits, decode failures, and network errors', () => {
    expect(
      classifyErrorOutcome(
        new HttpRateLimitedError({ upstream: 'api', retryAfterSeconds: 1, message: 'limited' }),
      ),
    ).toBe('rate_limited');
    expect(
      classifyErrorOutcome(
        new HttpRequestError({ upstream: 'api', status: 200, message: 'Failed to decode api' }),
      ),
    ).toBe('decode_error');
    expect(
      classifyErrorOutcome(
        new HttpRequestError({ upstream: 'api', status: null, message: 'connect ECONNREFUSED' }),
      ),
    ).toBe('network_error');
  });
});

describe('classifyErrorOutcome for unusable statuses', () => {
  it('labels a 3xx that reached the status check unexpected_status, which is not stale-servable', () => {
    expect(
      classifyErrorOutcome(
        new HttpRequestError({ upstream: 'api', status: 302, message: 'moved' }),
      ),
    ).toBe('unexpected_status');
  });
});

describe('classifyRetryReason', () => {
  it('maps a 503 to a server error retry', () => {
    expect(
      classifyRetryReason(
        new HttpRequestError({ upstream: 'api', status: 503, message: 'unavailable' }),
      ),
    ).toBe('server_error');
  });
});

describe('classifyErrorOutcome precedence', () => {
  it('classifies by status before the message and by tagged outcome before both', () => {
    expect(
      classifyErrorOutcome(
        new HttpRequestError({
          upstream: 'api',
          status: 503,
          message: 'api responded with HTTP 503: {"error":"decode timeout"}',
        }),
      ),
    ).toBe('server_error');
    expect(
      classifyRetryReason(
        new HttpRequestError({ upstream: 'api', status: 503, message: 'timeout' }),
      ),
    ).toBe('server_error');
    expect(
      classifyErrorOutcome(
        tagHttpError(new HttpRequestError({ upstream: 'api', status: 200, message: 'x' }), {
          outcome: 'body_too_large',
        }),
      ),
    ).toBe('body_too_large');
    expect(
      classifyErrorOutcome(
        tagHttpError(new HttpRequestError({ upstream: 'api', status: 503, message: 'breaker' }), {
          outcome: 'circuit_open',
        }),
      ),
    ).toBe('circuit_open');
  });
});

describe('classifyErrorOutcome without a status or tag', () => {
  it.each([
    'api request failed: Transport error (GET https://timeout.example.com)',
    'connect ECONNREFUSED https://api.example.com/decode?parse=1&schema=2',
    'circuit breaker is open',
    'request timed out',
  ])('labels %j a network error, whatever its message says', (message) => {
    expect(
      classifyErrorOutcome(new HttpRequestError({ upstream: 'api', status: null, message })),
    ).toBe('network_error');
  });

  it('still uses the tagged outcome of a failure without a status', () => {
    expect(
      classifyErrorOutcome(
        tagHttpError(new HttpRequestError({ upstream: 'api', status: null, message: 'x' }), {
          outcome: 'timeout',
        }),
      ),
    ).toBe('timeout');
  });
});
