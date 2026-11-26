import { describe, expect, it } from 'vite-plus/test';

import { readHttpErrorMeta, tagHttpError } from './error-meta.ts';
import { HttpRateLimitedError, HttpRequestError } from './errors.ts';

/**
 * Builds a fresh upstream error.
 */
const makeUpstreamError = (): HttpRequestError =>
  new HttpRequestError({ upstream: 'test:meta', status: 503, message: 'unavailable' });

describe('readHttpErrorMeta', () => {
  it('returns an empty object for an error that was never tagged', () => {
    expect(readHttpErrorMeta(makeUpstreamError())).toEqual({});
  });
});

describe('tagHttpError', () => {
  it('returns the same error instance it was given', () => {
    const error = makeUpstreamError();
    expect(tagHttpError(error, { outcome: 'timeout' })).toBe(error);
  });

  it('stores the metadata so it can be read back', () => {
    const error = tagHttpError(makeUpstreamError(), {
      outcome: 'blocked',
      retryable: false,
      circuit: 'ignore',
    });
    expect(readHttpErrorMeta(error)).toEqual({
      outcome: 'blocked',
      retryable: false,
      circuit: 'ignore',
    });
  });

  it('merges a later tag over an earlier one and keeps the fields it does not mention', () => {
    const error = tagHttpError(makeUpstreamError(), { outcome: 'timeout', retryable: true });
    tagHttpError(error, { outcome: 'network_error', circuit: 'ignore' });
    expect(readHttpErrorMeta(error)).toEqual({
      outcome: 'network_error',
      retryable: true,
      circuit: 'ignore',
    });
  });

  it('lets a field be set back to undefined', () => {
    const error = tagHttpError(makeUpstreamError(), { circuit: 'ignore' });
    tagHttpError(error, { circuit: undefined });
    expect(readHttpErrorMeta(error).circuit).toBeUndefined();
  });

  it('keeps the metadata of each error instance apart', () => {
    const tagged = tagHttpError(makeUpstreamError(), { retryable: false });
    const other = makeUpstreamError();
    expect(readHttpErrorMeta(other)).toEqual({});
    expect(readHttpErrorMeta(tagged)).toEqual({ retryable: false });
  });

  it('tags a rate limit error like an upstream error', () => {
    const error = tagHttpError(
      new HttpRateLimitedError({ upstream: 'test:meta', retryAfterSeconds: 3, message: 'slow' }),
      { outcome: 'rate_limited' },
    );
    expect(readHttpErrorMeta(error)).toEqual({ outcome: 'rate_limited' });
  });

  it('does not change the fields of the error', () => {
    const error = tagHttpError(makeUpstreamError(), { retryable: false });
    expect(error).toMatchObject({ upstream: 'test:meta', status: 503, message: 'unavailable' });
    expect(Object.keys(error)).not.toContain('retryable');
  });
});
