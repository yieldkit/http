import { Option, Schema } from 'effect';
import { Headers } from 'effect/http';
import { describe, expect, it } from 'vite-plus/test';

import type { RequestValidators } from './cache-key.ts';

import { buildCacheKey, requestValidators, selectUsableEntry } from './cache-key.ts';

/**
 * A request that carries no validators.
 */
const NO_VALIDATORS: RequestValidators = { ifNoneMatch: null, ifModifiedSince: null };

/**
 * Two distinct schemas, to check that identity partitions keys.
 */
const SchemaA = Schema.Struct({ id: Schema.Int });
const SchemaB = Schema.Struct({ id: Schema.Int });

/**
 * Builds a cache key for `schema` with `headers` and a fixed upstream and URL.
 */
const keyFor = (schema: Schema.Constraint, headers: Record<string, string> = {}): string =>
  buildCacheKey({
    upstream: 'src',
    schema,
    headers: Headers.fromInput(headers),
    url: 'https://a.example/x',
  });

/**
 * Builds a cache key for a request without headers sent with an ambient `x-tenant-id` of `tenant`.
 */
const withAmbient = (tenant: string): string =>
  buildCacheKey({
    upstream: 'src',
    schema: SchemaA,
    headers: Headers.fromInput({}),
    ambientHeaders: { 'x-tenant-id': tenant },
    url: 'https://a.example/x',
  });

describe('buildCacheKey', () => {
  it('is stable for the same schema, credentials and URL', () => {
    expect(keyFor(SchemaA, { authorization: 'Bearer one' })).toBe(
      keyFor(SchemaA, { Authorization: 'Bearer one' }),
    );
  });

  it('differs per schema identity', () => {
    expect(keyFor(SchemaA)).not.toBe(keyFor(SchemaB));
  });

  it('differs per credential and never contains it', () => {
    const one = keyFor(SchemaA, { authorization: 'Bearer secret-one' });
    const two = keyFor(SchemaA, { authorization: 'Bearer secret-two' });
    expect(one).not.toBe(two);
    expect(one).not.toBe(keyFor(SchemaA));
    expect(one).not.toContain('secret-one');
    expect(keyFor(SchemaA, { cookie: 'a=1' })).not.toBe(keyFor(SchemaA, { cookie: 'a=2' }));
    expect(keyFor(SchemaA, { 'x-api-key': 'k1' })).not.toBe(keyFor(SchemaA, { 'x-api-key': 'k2' }));
  });

  it('partitions on credential headers whatever they are called', () => {
    for (const name of [
      'ocp-apim-subscription-key',
      'x-auth-key',
      'x-functions-key',
      'x-tenant-id',
    ]) {
      expect(keyFor(SchemaA, { [name]: 'tenant-a' })).not.toBe(
        keyFor(SchemaA, { [name]: 'tenant-b' }),
      );
    }
  });

  it('partitions on the representation headers Accept and Accept-Language', () => {
    expect(keyFor(SchemaA, { accept: 'application/json' })).not.toBe(keyFor(SchemaA));
    expect(keyFor(SchemaA, { 'accept-language': 'fr' })).not.toBe(
      keyFor(SchemaA, { 'accept-language': 'de' }),
    );
  });

  it('ignores the user agent, accept-encoding and the validators', () => {
    expect(
      keyFor(SchemaA, {
        'user-agent': 'agent/1',
        'accept-encoding': 'gzip',
        'if-none-match': '"v1"',
        'if-modified-since': 'Mon',
      }),
    ).toBe(keyFor(SchemaA));
  });

  it('partitions on ambient fetch headers the request does not carry', () => {
    expect(withAmbient('a')).not.toBe(withAmbient('b'));
    expect(withAmbient('a')).not.toContain('x-tenant-id');
  });
});

describe('selectUsableEntry', () => {
  const entry = { etag: '"v1"', lastModified: 'Mon', data: 1, updatedAtMs: 1 };

  it('keeps the entry without caller validators or with matching ones', () => {
    expect(Option.isSome(selectUsableEntry(Option.some(entry), NO_VALIDATORS))).toBe(true);
    expect(
      Option.isSome(
        selectUsableEntry(Option.some(entry), { ifNoneMatch: '"v1"', ifModifiedSince: 'Mon' }),
      ),
    ).toBe(true);
  });

  it('drops the entry when a caller validator differs', () => {
    expect(
      Option.isNone(
        selectUsableEntry(Option.some(entry), { ...NO_VALIDATORS, ifNoneMatch: '"v2"' }),
      ),
    ).toBe(true);
    expect(
      Option.isNone(
        selectUsableEntry(Option.some(entry), { ...NO_VALIDATORS, ifModifiedSince: 'Tue' }),
      ),
    ).toBe(true);
  });
});

describe('requestValidators', () => {
  it('reads validators passed as raw headers in any case', () => {
    expect(
      requestValidators(Headers.fromInput({ 'If-None-Match': '"v2"', 'if-modified-since': 'Tue' })),
    ).toEqual({ ifNoneMatch: '"v2"', ifModifiedSince: 'Tue' });
    expect(requestValidators(Headers.fromInput({}))).toEqual(NO_VALIDATORS);
  });
});
