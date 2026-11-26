import { describe, expect, it } from 'vite-plus/test';

import { redactUrlForLogs, UNPARSEABLE_URL_LOG_VALUE } from './url-redaction.ts';

describe('redactUrlForLogs', () => {
  it('masks query values under credential names and keeps the rest as written', () => {
    expect(
      redactUrlForLogs(
        'https://api.example.com/v1/items?page=2&api_key=sec_12345&token=abc&q=effect&sort=new',
      ),
    ).toBe(
      'https://api.example.com/v1/items?page=2&api_key=<redacted>&token=<redacted>&q=effect&sort=new',
    );
  });

  it('masks short query names that carry a credential in URLs, in any case and percent-encoded', () => {
    expect(
      redactUrlForLogs(
        'https://h.example/f?key=K1&sig=S1&code=C1&X-Amz-Signature=A1&X-Amz-Credential=A2&KEY=K2&%73ig=S2&page=3',
      ),
    ).toBe(
      'https://h.example/f?key=<redacted>&sig=<redacted>&code=<redacted>&X-Amz-Signature=<redacted>&X-Amz-Credential=<redacted>&KEY=<redacted>&%73ig=<redacted>&page=3',
    );
  });

  it('matches OAuth, session and signature names in any case, and repeated names', () => {
    expect(
      redactUrlForLogs(
        'https://h.example/cb?client_secret=S1&refresh_token=S2&X-Amz-Signature=S4&sessionId=S5&token=S6&token=S7&page=2',
      ),
    ).toBe(
      'https://h.example/cb?client_secret=<redacted>&refresh_token=<redacted>&X-Amz-Signature=<redacted>&sessionId=<redacted>&token=<redacted>&token=<redacted>&page=2',
    );
  });

  it('checks decoded and structured parameter names', () => {
    expect(redactUrlForLogs('https://h.example/?api%5Fkey=S1&creds[token]=S2&q=a+b')).toBe(
      'https://h.example/?api%5Fkey=<redacted>&creds[token]=<redacted>&q=a+b',
    );
  });

  it('drops userinfo and the fragment', () => {
    const redacted = redactUrlForLogs('https://user:PASS@h.example/p?x=1#access_token=FRAG');
    expect(redacted).toBe('https://h.example/p?x=1');
  });

  it('keeps a relative URL relative and returns a placeholder for one that does not parse', () => {
    expect(redactUrlForLogs('/items?token=T1&page=2')).toBe('/items?token=<redacted>&page=2');
    expect(redactUrlForLogs('not a url ?token=T1')).toBe(UNPARSEABLE_URL_LOG_VALUE);
    expect(redactUrlForLogs('http://[bad/?token=T1')).toBe(UNPARSEABLE_URL_LOG_VALUE);
  });
});
