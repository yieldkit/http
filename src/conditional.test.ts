import { Effect, Option } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import type { EtagCacheEntry } from './conditional.ts';

import { makeEtagCache } from './conditional.ts';

/**
 * Builds a cache entry whose body was `bodyBytes` long.
 */
const sizedEntry = (bodyBytes: number): EtagCacheEntry<number> => ({
  etag: null,
  lastModified: null,
  data: 0,
  updatedAtMs: 1,
  bodyBytes,
});

describe('makeEtagCache', () => {
  it('keeps no entries at zero capacity', () => {
    const cache = Effect.runSync(makeEtagCache(0));
    Effect.runSync(cache.set('first', sizedEntry(1)));
    Effect.runSync(cache.set('second', sizedEntry(1)));
    expect(Effect.runSync(cache.size)).toBe(0);
  });

  it('returns a stored entry and none for a missing key', async () => {
    const cache = await Effect.runPromise(makeEtagCache(2));
    await Effect.runPromise(
      cache.set('story', {
        etag: '"v1"',
        lastModified: null,
        data: { id: 1 },
        updatedAtMs: 10,
      }),
    );
    const hit = await Effect.runPromise(cache.get<{ id: number }>('story'));
    const miss = await Effect.runPromise(cache.get('missing'));
    expect(Option.isSome(hit) && hit.value.data).toEqual({ id: 1 });
    expect(Option.isNone(miss)).toBe(true);
    expect(await Effect.runPromise(cache.size)).toBe(1);
  });

  it('evicts the least recently used entry when full', async () => {
    const cache = await Effect.runPromise(makeEtagCache(2));
    await Effect.runPromise(
      cache.set('first', { etag: '"a"', lastModified: null, data: 1, updatedAtMs: 1 }),
    );
    await Effect.runPromise(
      cache.set('second', { etag: '"b"', lastModified: null, data: 2, updatedAtMs: 2 }),
    );
    await Effect.runPromise(cache.get('first'));
    await Effect.runPromise(
      cache.set('third', { etag: '"c"', lastModified: null, data: 3, updatedAtMs: 3 }),
    );
    const first = await Effect.runPromise(cache.get('first'));
    const second = await Effect.runPromise(cache.get('second'));
    const third = await Effect.runPromise(cache.get('third'));
    expect(Option.isSome(first)).toBe(true);
    expect(Option.isNone(second)).toBe(true);
    expect(Option.isSome(third)).toBe(true);
  });

  it('evicts the oldest entries to stay within the byte budget', async () => {
    const cache = await Effect.runPromise(makeEtagCache(10, 100));
    const has = (key: string) => Effect.runSync(Effect.map(cache.get(key), Option.isSome));
    Effect.runSync(cache.set('a', sizedEntry(40)));
    Effect.runSync(cache.set('b', sizedEntry(40)));
    Effect.runSync(cache.set('c', sizedEntry(40)));
    const afterThird = { a: has('a'), b: has('b'), c: has('c') };
    Effect.runSync(cache.set('d', sizedEntry(90)));
    const afterLarge = { b: has('b'), c: has('c'), d: has('d') };
    expect(afterThird).toEqual({ a: false, b: true, c: true });
    expect(afterLarge).toEqual({ b: false, c: false, d: true });
    expect(Effect.runSync(cache.size)).toBe(1);
  });

  it('does not store an entry larger than the byte budget', async () => {
    const cache = await Effect.runPromise(makeEtagCache(10, 100));
    const small = { etag: '"a"', lastModified: null, data: 1, updatedAtMs: 1, bodyBytes: 40 };
    const huge = { etag: '"b"', lastModified: null, data: 2, updatedAtMs: 2, bodyBytes: 101 };
    Effect.runSync(cache.set('kept', small));
    Effect.runSync(cache.set('huge', huge));
    const afterSet = Effect.runSync(cache.size);
    Effect.runSync(cache.set('replaced', small));
    const replaced = Effect.runSync(cache.setIfCurrent('replaced', small, huge));
    expect(afterSet).toBe(1);
    expect(replaced).toBe(true);
    expect(Option.isNone(Effect.runSync(cache.get('huge')))).toBe(true);
    expect(Option.isNone(Effect.runSync(cache.get('replaced')))).toBe(true);
    expect(Option.isSome(Effect.runSync(cache.get('kept')))).toBe(true);
  });

  it.each([undefined, Number.NaN])(
    'defaults the byte budget to 32 MiB for %s',
    async (maxBytes) => {
      const cache = await Effect.runPromise(makeEtagCache(10, maxBytes));
      const entry = {
        etag: null,
        lastModified: null,
        data: 0,
        updatedAtMs: 1,
        bodyBytes: 20 * 2 ** 20,
      };
      Effect.runSync(cache.set('first', entry));
      Effect.runSync(cache.set('second', entry));
      expect(Option.isNone(Effect.runSync(cache.get('first')))).toBe(true);
      expect(Option.isSome(Effect.runSync(cache.get('second')))).toBe(true);
    },
  );

  it('clears every entry', async () => {
    const cache = await Effect.runPromise(makeEtagCache());
    await Effect.runPromise(
      cache.set('story', { etag: null, lastModified: null, data: 'x', updatedAtMs: 1 }),
    );
    await Effect.runPromise(cache.clear);
    expect(await Effect.runPromise(cache.size)).toBe(0);
  });

  it.each([Number.NaN, Number.POSITIVE_INFINITY, Number.NEGATIVE_INFINITY])(
    'stays bounded at the default capacity for a size of %s',
    async (maxEntries) => {
      const cache = await Effect.runPromise(makeEtagCache(maxEntries));
      await Effect.runPromise(
        Effect.forEach(
          Array.from({ length: 600 }, (_, index) => index),
          (index) =>
            cache.set(`key-${String(index)}`, {
              etag: null,
              lastModified: null,
              data: index,
              updatedAtMs: index,
            }),
        ),
      );

      expect(await Effect.runPromise(cache.size)).toBe(500);
    },
  );

  it('replaces an entry only while it is still the expected one', async () => {
    const cache = await Effect.runPromise(makeEtagCache(2));
    const older = { etag: '"a"', lastModified: null, data: 1, updatedAtMs: 1 };
    const newer = { etag: '"b"', lastModified: null, data: 2, updatedAtMs: 2 };
    const refreshed = { etag: '"a"', lastModified: null, data: 1, updatedAtMs: 3 };
    await Effect.runPromise(cache.set('story', older));
    await Effect.runPromise(cache.set('story', newer));
    expect(await Effect.runPromise(cache.setIfCurrent('story', older, refreshed))).toBe(false);
    const kept = await Effect.runPromise(cache.get('story'));
    expect(Option.isSome(kept) && kept.value).toBe(newer);
    expect(await Effect.runPromise(cache.setIfCurrent('story', newer, refreshed))).toBe(true);
    const replaced = await Effect.runPromise(cache.get('story'));
    expect(Option.isSome(replaced) && replaced.value).toBe(refreshed);
    expect(await Effect.runPromise(cache.setIfCurrent('missing', older, refreshed))).toBe(false);
  });

  it('deletes an entry only while it is still the expected one', async () => {
    const cache = await Effect.runPromise(makeEtagCache(2));
    const older = { etag: '"a"', lastModified: null, data: 1, updatedAtMs: 1 };
    const newer = { etag: '"b"', lastModified: null, data: 2, updatedAtMs: 2 };
    await Effect.runPromise(cache.set('story', newer));
    expect(await Effect.runPromise(cache.deleteIfCurrent('story', older))).toBe(false);
    expect(await Effect.runPromise(cache.size)).toBe(1);
    expect(await Effect.runPromise(cache.deleteIfCurrent('story', newer))).toBe(true);
    expect(await Effect.runPromise(cache.size)).toBe(0);
  });
});
