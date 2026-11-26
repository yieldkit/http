import type { Duration } from 'effect';

import { describe, expect, it } from 'vite-plus/test';

import {
  checkedDuration,
  DEFAULT_MAX_BODY_BYTES,
  DEFAULT_MAX_REDIRECTS,
  resolveHttpPolicy,
  resolveRequestUrl,
} from './policy.ts';

describe('resolveRequestUrl', () => {
  it('keeps an absolute URL and joins a relative path to the base', () => {
    expect(resolveRequestUrl('https://api.example.com/v1/', '/items')).toBe(
      'https://api.example.com/v1/items',
    );
    expect(resolveRequestUrl('https://api.example.com', 'items')).toBe(
      'https://api.example.com/items',
    );
    expect(resolveRequestUrl(null, 'https://other.example/x')).toBe('https://other.example/x');
  });

  it.each(['HTTPS://evil.test/a', 'Http://evil.test/a', 'hTTpS://evil.test/a'])(
    'treats %s as absolute whatever the scheme case, never joining it onto the base',
    (url) => {
      expect(resolveRequestUrl('https://api.example.com', url)).toBe(url);
    },
  );
});

describe('checkedDuration', () => {
  const accepted: ReadonlyArray<Duration.Input> = [
    '5 seconds',
    '0.5 seconds',
    250,
    0,
    10n,
    { seconds: 2 },
  ];
  const rejected: ReadonlyArray<Duration.Input> = [
    '1e3 millis',
    Number.NaN,
    Infinity,
    -5,
    '-1 seconds',
    'Infinity',
    '-Infinity',
    { seconds: Number.NaN },
  ];

  it.each(accepted)('accepts %s', (input) => {
    expect(checkedDuration(input)).not.toBeNull();
  });

  it.each(rejected)('rejects %s', (input) => {
    expect(checkedDuration(input)).toBeNull();
  });

  it('rejects a [seconds, nanos] tuple with a NaN part', () => {
    expect(checkedDuration([0, Number.NaN])).toBeNull();
    expect(checkedDuration([Number.NaN, 0])).toBeNull();
  });
});

describe('resolveHttpPolicy', () => {
  it('defaults the circuit key to circuit:upstream', () => {
    const resolved = resolveHttpPolicy({
      upstream: 'example-api',
      circuitBreaker: {},
    });
    expect(resolved.circuitBreaker?.key).toBe('circuit:example-api');
    expect(resolved.userAgent).toBeNull();
    expect(resolved.maxBodyBytes).toBe(2 * 1024 * 1024);
  });

  it('resolves the default operation deadline and attempt timeout', () => {
    const resolved = resolveHttpPolicy({ upstream: 'test:deadline-default' });
    expect(resolved.operationTimeout).toBe('60 seconds');
    expect(resolved.attemptTimeout).toBe('15 seconds');
    expect(resolved.retry.retryOnRateLimit).toBe(false);
  });

  it('preserves retryOnRateLimit when opted in', () => {
    const resolved = resolveHttpPolicy({
      upstream: 'test:rate-limit-opt-in',
      retry: { retryOnRateLimit: true },
    });
    expect(resolved.retry.retryOnRateLimit).toBe(true);
  });
});

describe('resolveHttpPolicy validation', () => {
  it.each(['?region=eu', '#docs', '?', '#'])('rejects a base URL ending in %s', (suffix) => {
    const resolved = resolveHttpPolicy({
      upstream: 'test:base-suffix',
      baseUrl: `https://api.example.com/v1${suffix}`,
    });
    expect(resolved.configError?.message).toContain('baseUrl must not contain a query or fragment');
  });

  it.each(['rateLimit', 'circuitBreaker', 'baseUrl'])(
    'reports a null %s in untyped configuration through configError',
    (field) => {
      const config = { upstream: 'test:null-config', [field]: null };
      const resolved = resolveHttpPolicy(config);
      expect(resolved.configError?.message).toContain(`${field} must`);
    },
  );

  it.each(['api.example.com/v1', 'ftp://files.example.com', 'https://'])(
    'rejects the baseUrl %s, which names no http(s) origin to bind the default headers to',
    (baseUrl) => {
      const resolved = resolveHttpPolicy({ upstream: 'test:base', baseUrl });
      expect(resolved.configError?.message).toContain(
        'baseUrl must be an absolute http or https URL',
      );
    },
  );

  it.each([undefined, '', 'https://api.example.com/v1', 'HTTP://api.example.com'])(
    'accepts the baseUrl %s',
    (baseUrl) => {
      expect(resolveHttpPolicy({ upstream: 'test:base', baseUrl }).configError).toBeNull();
    },
  );

  it('reports no configError for a valid policy', () => {
    const resolved = resolveHttpPolicy({
      upstream: 'test:valid',
      maxConcurrency: 2,
      maxBodyBytes: 1024,
      maxRedirects: 0,
      retry: { maxRetries: 0, baseDelay: '1 second' },
    });
    expect(resolved.configError).toBeNull();
    expect(resolved.maxConcurrency).toBe(2);
    expect(resolved.maxRedirects).toBe(0);
    expect(resolved.retry.maxRetries).toBe(0);
  });

  it.each([0, -1, Number.NaN, Infinity, 2.5])(
    'replaces maxConcurrency %s by unbounded instead of a semaphore that hangs',
    (maxConcurrency) => {
      const resolved = resolveHttpPolicy({ upstream: 'test:conc', maxConcurrency });
      expect(resolved.maxConcurrency).toBeNull();
      expect(resolved.configError?._tag).toBe('HttpRequestError');
      expect(resolved.configError?.message).toContain(
        'maxConcurrency must be a positive safe integer',
      );
    },
  );

  it.each([0, -1, Number.NaN, Infinity, 1.5])(
    'replaces maxBodyBytes %s by the default so the cap stays on',
    (maxBodyBytes) => {
      const resolved = resolveHttpPolicy({ upstream: 'test:cap', maxBodyBytes });
      expect(resolved.maxBodyBytes).toBe(DEFAULT_MAX_BODY_BYTES);
      expect(resolved.configError?.message).toContain('maxBodyBytes');
    },
  );

  it.each([-1, Number.NaN, Infinity])('replaces maxRedirects %s by the default', (maxRedirects) => {
    const resolved = resolveHttpPolicy({ upstream: 'test:redir', maxRedirects });
    expect(resolved.maxRedirects).toBe(DEFAULT_MAX_REDIRECTS);
    expect(resolved.configError?.message).toContain('maxRedirects');
  });

  it('replaces invalid retry numbers and durations by their defaults and lists each one', () => {
    const resolved = resolveHttpPolicy({
      upstream: 'test:retry',
      retry: {
        maxRetries: Infinity,
        baseDelay: '1e3 millis',
        maxBackoffSeconds: Number.NaN,
        maxInlineRetryAfterSeconds: -1,
      },
    });
    expect(resolved.retry).toEqual({
      maxRetries: 3,
      baseDelay: '250 millis',
      maxBackoffSeconds: 5,
      maxInlineRetryAfterSeconds: 10,
      retryOnRateLimit: false,
    });
    const message = resolved.configError?.message ?? '';
    for (const name of [
      'maxRetries',
      'baseDelay',
      'maxBackoffSeconds',
      'maxInlineRetryAfterSeconds',
    ]) {
      expect(message).toContain(`retry.${name}`);
    }
  });

  it('replaces unparseable timeouts, which Effect.timeout would turn into a defect', () => {
    const resolved = resolveHttpPolicy({
      upstream: 'test:durations',
      attemptTimeout: '1e3 millis',
      operationTimeout: Number.NaN,
      staleIfError: -1,
    });
    expect(resolved.attemptTimeout).toBe('15 seconds');
    expect(resolved.operationTimeout).toBe('60 seconds');
    expect(resolved.staleIfError).toBeNull();
    expect(resolved.configError).not.toBeNull();
  });

  it('reports a null duration from untyped configuration instead of throwing', () => {
    const resolved = resolveHttpPolicy({
      upstream: 'test:null',
      // @ts-expect-error -- untyped configuration (JSON, parsed environment) can hold null
      attemptTimeout: null,
    });
    expect(resolved.attemptTimeout).toBe('15 seconds');
    expect(resolved.configError?.message).toContain(
      'attemptTimeout must be a finite, non-negative duration',
    );
  });

  it('drops a rate limit whose limit or window cannot work', () => {
    expect(
      resolveHttpPolicy({ upstream: 'test:rl', rateLimit: { limit: 0, window: '1 second' } })
        .rateLimit,
    ).toBeNull();
    expect(
      resolveHttpPolicy({ upstream: 'test:rl', rateLimit: { limit: 1, window: '0 millis' } })
        .configError,
    ).not.toBeNull();
    expect(
      resolveHttpPolicy({ upstream: 'test:rl', rateLimit: { limit: 5, window: '1 second' } })
        .rateLimit,
    ).toEqual({ limit: 5, window: '1 second' });
  });
});
