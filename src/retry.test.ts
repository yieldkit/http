import { Duration, Effect, Exit, Random, Schedule } from 'effect';
import { HttpClientRequest } from 'effect/http';
import { describe, expect, it } from 'vite-plus/test';

import type { HttpError } from './errors.ts';
import type { HttpRetryPolicy } from './policy.ts';
import type { RetryScheduleOptions } from './retry.ts';

import { tagHttpError } from './error-meta.ts';
import { HttpRateLimitedError, HttpRequestError } from './errors.ts';
import { makeClientResponse, withResponseUrl } from './http.fixture.ts';
import {
  createRetrySchedule,
  isRedirectedResponse,
  isRetryableForMethod,
  isTransientHttpStatus,
  isTransientHttpError,
  readRetryAfterSeconds,
} from './retry.ts';

/**
 * `Random` whose every draw is `draw`, so the schedule's jitter is fixed.
 */
const fixedRandom = (draw: number): Random.Random => ({
  nextIntUnsafe: () => 0,
  nextDoubleUnsafe: () => draw,
});

/**
 * Runs the first step of `createRetrySchedule(policy, options)` at `nowMs` for `input`, with jitter
 * drawn as `draw`. Returns the delay in milliseconds, or `null` when the schedule stops.
 */
const firstDelayMillis = (params: {
  readonly input: HttpError;
  readonly nowMs?: number | undefined;
  readonly draw?: number | undefined;
  readonly policy?: Partial<HttpRetryPolicy> | undefined;
  readonly options?: RetryScheduleOptions | undefined;
}): number | null =>
  Effect.runSync(
    Effect.gen(function* () {
      const step = yield* Schedule.toStep(createRetrySchedule(params.policy, params.options));
      const exit = yield* Effect.exit(step(params.nowMs ?? 0, params.input));
      if (Exit.isFailure(exit)) {
        return null;
      }

      return Duration.toMillis(exit.value[1]);
    }).pipe(Effect.provideService(Random.Random, fixedRandom(params.draw ?? 0.5))),
  );

/**
 * Builds a 503 tagged with the `Retry-After` wait `seconds`, as `ensureSuccessStatus` does.
 */
const make503WithRetryAfter = (seconds: number): HttpRequestError =>
  tagHttpError(new HttpRequestError({ upstream: 'api', status: 503, message: 'down' }), {
    retryAfterSeconds: seconds,
  });

describe('isTransientHttpStatus', () => {
  it('retries 408 and 5xx by default, and 429 only when retryOnRateLimit is true', () => {
    expect(isTransientHttpStatus(408)).toBe(true);
    expect(isTransientHttpStatus(429)).toBe(false);
    expect(isTransientHttpStatus(429, true)).toBe(true);
    expect(isTransientHttpStatus(503)).toBe(true);
    expect(isTransientHttpStatus(404)).toBe(false);
  });
});

describe('isTransientHttpError', () => {
  it('does not retry a rate limit by default or when Retry-After exceeds the cap', () => {
    const longWait = new HttpRateLimitedError({
      upstream: 'example-api',
      retryAfterSeconds: 45,
      message: 'limited',
    });
    const shortWait = new HttpRateLimitedError({
      upstream: 'example-api',
      retryAfterSeconds: 2,
      message: 'limited',
    });
    const notFound = new HttpRequestError({
      upstream: 'example-api',
      status: 404,
      message: 'missing',
    });
    expect(isTransientHttpError(shortWait, 10)).toBe(false);
    expect(isTransientHttpError(longWait, 10, true)).toBe(false);
    expect(isTransientHttpError(shortWait, 10, true)).toBe(true);
    expect(isTransientHttpError(notFound, 10, true)).toBe(false);
  });
});

describe('isTransientHttpError with metadata', () => {
  it('never retries an error tagged retryable: false', () => {
    const error = tagHttpError(
      new HttpRequestError({ upstream: 'api', status: 503, message: 'unavailable' }),
      { retryable: false },
    );
    expect(isTransientHttpError(error)).toBe(false);
  });
});

describe('isRetryableForMethod', () => {
  const serverError = new HttpRequestError({ upstream: 'api', status: 503, message: 'down' });
  const networkError = new HttpRequestError({ upstream: 'api', status: null, message: 'reset' });
  const notFound = new HttpRequestError({ upstream: 'api', status: 404, message: 'missing' });
  const shortLimit = new HttpRateLimitedError({
    upstream: 'api',
    retryAfterSeconds: 2,
    message: 'limited',
  });
  const longLimit = new HttpRateLimitedError({
    upstream: 'api',
    retryAfterSeconds: 45,
    message: 'limited',
  });

  it.each(['GET', 'HEAD', 'OPTIONS', 'PUT', 'DELETE', 'TRACE', 'get', 'put'])(
    'retries a transient failure of an idempotent %s',
    (method) => {
      expect(isRetryableForMethod({ error: serverError, method })).toBe(true);
      expect(isRetryableForMethod({ error: networkError, method })).toBe(true);
    },
  );

  it.each(['POST', 'PATCH', 'post'])('does not retry a transient failure of a %s', (method) => {
    expect(isRetryableForMethod({ error: serverError, method })).toBe(false);
    expect(isRetryableForMethod({ error: networkError, method })).toBe(false);
  });

  it.each(['POST', 'PATCH'])('retries a %s once the caller declares it idempotent', (method) => {
    expect(isRetryableForMethod({ error: serverError, method, idempotent: true })).toBe(true);
    expect(isRetryableForMethod({ error: serverError, method, idempotent: false })).toBe(false);
  });

  it('does not retry a short rate limit unless retryOnRateLimit is true', () => {
    expect(isRetryableForMethod({ error: shortLimit, method: 'GET' })).toBe(false);
    expect(isRetryableForMethod({ error: shortLimit, method: 'POST' })).toBe(false);
    expect(isRetryableForMethod({ error: shortLimit, method: 'PATCH' })).toBe(false);
    expect(isRetryableForMethod({ error: shortLimit, method: 'GET', retryOnRateLimit: true })).toBe(
      true,
    );
    expect(
      isRetryableForMethod({ error: shortLimit, method: 'POST', retryOnRateLimit: true }),
    ).toBe(true);
    expect(
      isRetryableForMethod({ error: shortLimit, method: 'PATCH', retryOnRateLimit: true }),
    ).toBe(true);
  });

  it('does not retry a rate limit whose wait is over the inline cap, whatever the method', () => {
    expect(
      isRetryableForMethod({
        error: longLimit,
        method: 'GET',
        maxInlineRetryAfterSeconds: 10,
        retryOnRateLimit: true,
      }),
    ).toBe(false);
    expect(
      isRetryableForMethod({
        error: longLimit,
        method: 'POST',
        maxInlineRetryAfterSeconds: 60,
        retryOnRateLimit: true,
      }),
    ).toBe(true);
  });

  it('never retries a permanent status, even when idempotent', () => {
    expect(isRetryableForMethod({ error: notFound, method: 'GET', idempotent: true })).toBe(false);
  });

  it('never retries an error tagged retryable: false', () => {
    const decodeError = tagHttpError(
      new HttpRequestError({ upstream: 'api', status: null, message: 'bad body' }),
      { retryable: false },
    );
    expect(isRetryableForMethod({ error: decodeError, method: 'GET', idempotent: true })).toBe(
      false,
    );
  });
});

describe('readRetryAfterSeconds', () => {
  it('reads a rate limit field, a tagged status error, and gives null otherwise', () => {
    expect(
      readRetryAfterSeconds(
        new HttpRateLimitedError({ upstream: 'api', retryAfterSeconds: 4, message: 'limited' }),
      ),
    ).toBe(4);
    expect(readRetryAfterSeconds(make503WithRetryAfter(9))).toBe(9);
    expect(
      readRetryAfterSeconds(new HttpRequestError({ upstream: 'api', status: 503, message: 'x' })),
    ).toBeNull();
  });
});

describe('Retry-After on a retryable status error', () => {
  it('retries a 503 whose Retry-After fits the inline cap and not one that is longer', () => {
    expect(isTransientHttpError(make503WithRetryAfter(10), 10)).toBe(true);
    expect(isTransientHttpError(make503WithRetryAfter(120), 10)).toBe(false);
    expect(
      isRetryableForMethod({
        error: make503WithRetryAfter(120),
        method: 'GET',
        maxInlineRetryAfterSeconds: 10,
      }),
    ).toBe(false);
  });

  it('waits at least the Retry-After of a 503 before the next attempt', () => {
    expect(firstDelayMillis({ input: make503WithRetryAfter(7) })).toBe(7000);
  });
});

describe('isRetryableForMethod after a redirect', () => {
  const redirectedLimit = tagHttpError(
    new HttpRateLimitedError({ upstream: 'api', retryAfterSeconds: 1, message: 'limited' }),
    { redirected: true },
  );

  it('does not replay a POST or PATCH for a rate limit that answered a redirect hop', () => {
    expect(
      isRetryableForMethod({ error: redirectedLimit, method: 'POST', retryOnRateLimit: true }),
    ).toBe(false);
    expect(
      isRetryableForMethod({ error: redirectedLimit, method: 'PATCH', retryOnRateLimit: true }),
    ).toBe(false);
  });

  it('still retries it for a GET or a POST declared idempotent when retryOnRateLimit is true', () => {
    expect(
      isRetryableForMethod({ error: redirectedLimit, method: 'GET', retryOnRateLimit: true }),
    ).toBe(true);
    expect(
      isRetryableForMethod({
        error: redirectedLimit,
        method: 'POST',
        idempotent: true,
        retryOnRateLimit: true,
      }),
    ).toBe(true);
  });
});

describe('createRetrySchedule', () => {
  const serverError = new HttpRequestError({ upstream: 'api', status: 503, message: 'down' });

  it('keeps a jittered delay within maxBackoffSeconds', () => {
    const delay = firstDelayMillis({
      input: serverError,
      draw: 0.999,
      policy: { baseDelay: '5 seconds', maxBackoffSeconds: 5 },
    });
    expect(delay).toBe(5000);
  });

  it('still jitters a delay below maxBackoffSeconds', () => {
    const delay = firstDelayMillis({
      input: serverError,
      draw: 0,
      policy: { baseDelay: '1 second', maxBackoffSeconds: 5 },
    });
    expect(delay).toBe(800);
  });

  it('lets a Retry-After above maxBackoffSeconds win over the cut', () => {
    const limited = new HttpRateLimitedError({
      upstream: 'api',
      retryAfterSeconds: 8,
      message: 'limited',
    });
    expect(
      firstDelayMillis({ input: limited, draw: 0.999, policy: { maxBackoffSeconds: 5 } }),
    ).toBe(8000);
  });

  it('stops instead of sleeping to or past the operation deadline', () => {
    const limited = new HttpRateLimitedError({
      upstream: 'api',
      retryAfterSeconds: 5,
      message: 'limited',
    });
    expect(
      firstDelayMillis({ input: limited, nowMs: 1000, options: { deadlineMs: 3000 } }),
    ).toBeNull();
    expect(
      firstDelayMillis({ input: limited, nowMs: 1000, options: { deadlineMs: 6000 } }),
    ).toBeNull();
    expect(firstDelayMillis({ input: limited, nowMs: 1000, options: { deadlineMs: 6001 } })).toBe(
      5000,
    );
  });

  it('has no deadline when none is given', () => {
    const limited = new HttpRateLimitedError({
      upstream: 'api',
      retryAfterSeconds: 5,
      message: 'limited',
    });
    expect(firstDelayMillis({ input: limited, nowMs: 1000 })).toBe(5000);
  });
});

describe('isRedirectedResponse', () => {
  it('treats a response the transport marked redirected as a hop, even back to the same URL', () => {
    const request = HttpClientRequest.post('https://api.example.com/jobs');
    const web = withResponseUrl(
      new Response('limited', { status: 429 }),
      'https://api.example.com/jobs',
    );
    Object.defineProperty(web, 'redirected', { value: true });
    expect(isRedirectedResponse(request, makeClientResponse(web, request))).toBe(true);
  });

  it('treats the URL fetch reports back for the request itself as the same request', () => {
    const request = HttpClientRequest.get('https://api.example.com/items?a=1#frag').pipe(
      HttpClientRequest.setUrlParam('b', '2'),
    );
    const response = makeClientResponse(
      withResponseUrl(new Response('ok'), 'https://api.example.com/items?a=1&b=2'),
      request,
    );
    expect(isRedirectedResponse(request, response)).toBe(false);
  });

  it('treats the trailing-slash form of a bare origin as the same request', () => {
    const request = HttpClientRequest.get('https://api.example.com');
    const response = makeClientResponse(
      withResponseUrl(new Response('ok'), 'https://api.example.com/'),
      request,
    );
    expect(isRedirectedResponse(request, response)).toBe(false);
  });

  it('compares a relative request with the URL a client transform completed it to', () => {
    const request = HttpClientRequest.post('/orders');
    const sent = HttpClientRequest.prependUrl(request, 'https://api.example.com');
    const response = makeClientResponse(
      withResponseUrl(new Response('limited', { status: 429 }), 'https://api.example.com/orders'),
      sent,
    );
    expect(isRedirectedResponse(request, response)).toBe(false);
  });

  it('treats a response without a reported URL as the answer to the request', () => {
    const request = HttpClientRequest.post('https://api.example.com/items');
    expect(isRedirectedResponse(request, makeClientResponse(new Response('ok'), request))).toBe(
      false,
    );
  });

  it('flags a response fetch reports for another URL', () => {
    const request = HttpClientRequest.post('https://api.example.com/items');
    const response = makeClientResponse(
      withResponseUrl(new Response('ok'), 'https://api.example.com/jobs/1'),
      request,
    );
    expect(isRedirectedResponse(request, response)).toBe(true);
  });

  it('flags a response to a hop the guard turned from a POST into a GET', () => {
    const request = HttpClientRequest.post('https://api.example.com/items');
    const response = makeClientResponse(
      new Response('ok'),
      HttpClientRequest.get('https://api.example.com/items'),
    );
    expect(isRedirectedResponse(request, response)).toBe(true);
  });
});
