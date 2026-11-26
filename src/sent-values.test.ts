import { Redacted } from 'effect';
import { HttpClientRequest } from 'effect/http';
import { describe, expect, it } from 'vite-plus/test';

import { maskSentValues } from './sent-values.ts';

describe('maskSentValues', () => {
  it('leaves echoed dates, versions, numbers, media types and short words alone', () => {
    const headers = {
      'x-api-version': '2024-01-01',
      'x-client-version': 'v1.2.3-beta.4',
      'x-timestamp': '20240101T120000Z',
      'x-page-size': '1000000000',
      accept: 'application/vnd.example.v2+json',
      'x-mode': 'production-strict',
      'user-agent': 'example-client/1.0 (+https://example.com)',
    };
    const request = HttpClientRequest.get(
      'https://api.example.com/items?page=12345678&lang=en-US',
      {
        headers,
      },
    );
    const text = `echo ${[...Object.values(headers), '12345678', 'en-US'].join(' | ')}`;
    expect(maskSentValues(text, request)).toBe(text);
  });

  it('masks values under credential names and Redacted query values, and nothing else', () => {
    const request = HttpClientRequest.get(
      'https://api.example.com/cb?access_token=QUERYTOKEN1&code=AUTHCODE1&state=s&ref=a8f3k2m9q7x1z5c4v6b0',
      {
        headers: {
          authorization: 'Bearer tok-SHORT1',
          'x-request-signature': 'abcdefghij',
          'x-trace': 'k9TzQ2mW8xYp4LrB7nVd',
        },
        urlParams: { opaque: Redacted.make('REDACTEDPARAM1') },
      },
    );
    expect(
      maskSentValues(
        'got tok-SHORT1, abcdefghij, QUERYTOKEN1, AUTHCODE1, REDACTEDPARAM1, a8f3k2m9q7x1z5c4v6b0, k9TzQ2mW8xYp4LrB7nVd, state s',
        request,
      ),
    ).toBe(
      'got <redacted>, <redacted>, <redacted>, <redacted>, <redacted>, a8f3k2m9q7x1z5c4v6b0, k9TzQ2mW8xYp4LrB7nVd, state s',
    );
  });

  it('masks cookie and parameter values but never their names', () => {
    const request = HttpClientRequest.get('https://api.example.com/x', {
      headers: {
        cookie: 'session_id=oldSessionValue1; preferences_v2=dark',
        authorization: 'Basic dXNlcjpwYXNzd29yZA==',
      },
    });
    expect(
      maskSentValues(
        'session_id oldSessionValue1 preferences_v2 dark dXNlcjpwYXNzd29yZA== dXNlcjpwYXNzd29yZA',
        request,
      ),
    ).toBe('session_id <redacted> preferences_v2 dark <redacted> <redacted>');
  });
});
