import { Context, Effect, Layer, Option } from 'effect';

/**
 * Result of a conditional HTTP GET request (`If-None-Match` / `If-Modified-Since`).
 */
export type ConditionalResponse<A> =
  | {
      /**
       * Discriminator indicating the upstream resource was modified (HTTP 2xx).
       */
      readonly _tag: 'Modified';

      /**
       * Decoded response payload.
       */
      readonly data: A;

      /**
       * Size in bytes of the response body `data` was decoded from, as read (after any transport
       * decompression). `getCachedJson` counts it against the ETag cache's byte budget.
       */
      readonly bodyBytes: number;

      /**
       * Response `ETag` header value when present.
       */
      readonly etag: string | null;

      /**
       * Response `Last-Modified` header value when present.
       */
      readonly lastModified: string | null;

      /**
       * HTTP 2xx status code returned by the server.
       */
      readonly status: number;

      /**
       * Response `Cache-Control` header value when present. `getCachedJson` never stores a
       * `no-store` response.
       */
      readonly cacheControl: string | null;
    }
  | {
      /**
       * Discriminator indicating the resource was not modified (HTTP 304).
       */
      readonly _tag: 'NotModified';

      /**
       * Retained `ETag` validator.
       */
      readonly etag: string | null;

      /**
       * Retained `Last-Modified` validator.
       */
      readonly lastModified: string | null;

      /**
       * `Cache-Control` header value of the 304 when present. `getCachedJson` evicts the entry it
       * revalidated when it holds `no-store`.
       */
      readonly cacheControl: string | null;
    };

/**
 * Cached conditional response entry stored in `EtagCache`.
 */
export interface EtagCacheEntry<A = unknown> {
  /**
   * Cached `ETag` header validator.
   */
  readonly etag: string | null;

  /**
   * Cached `Last-Modified` header validator.
   */
  readonly lastModified: string | null;

  /**
   * Cached decoded payload returned on `304 Not Modified`.
   */
  readonly data: A;

  /**
   * Epoch milliseconds when the cache entry was last stored.
   */
  readonly updatedAtMs: number;

  /**
   * Size in bytes of the response body `data` was decoded from. `makeEtagCache` counts it against
   * its byte budget; a missing value counts as zero.
   */
  readonly bodyBytes?: number | undefined;
}

/**
 * Service interface for bounded LRU conditional response caching.
 */
export interface EtagCacheShape {
  /**
   * Looks up a cached entry by key and promotes it as most-recently used.
   */
  readonly get: <A = unknown>(key: string) => Effect.Effect<Option.Option<EtagCacheEntry<A>>>;

  /**
   * Inserts or updates a cache entry, evicting least-recently used entries while over capacity.
   */
  readonly set: <A>(key: string, entry: EtagCacheEntry<A>) => Effect.Effect<void>;

  /**
   * Replaces the entry under `key` with `entry` only while the stored entry is still `expected`,
   * the object an earlier `get` returned. Returns whether it replaced it. A refresh that arrives
   * after a newer entry was stored is dropped instead of overwriting it.
   */
  readonly setIfCurrent: <A>(
    key: string,
    expected: EtagCacheEntry<A>,
    entry: EtagCacheEntry<A>,
  ) => Effect.Effect<boolean>;

  /**
   * Removes the entry under `key` only while it is still `expected`. Returns whether it removed it.
   */
  readonly deleteIfCurrent: <A>(key: string, expected: EtagCacheEntry<A>) => Effect.Effect<boolean>;

  /**
   * Removes all cached entries.
   */
  readonly clear: Effect.Effect<void>;

  /**
   * Returns the current number of cached entries.
   */
  readonly size: Effect.Effect<number>;
}

/**
 * Type guard narrowing a stored `EtagCacheEntry` to the caller's expected payload type.
 */
const isTypedCacheEntry = <A>(entry: EtagCacheEntry): entry is EtagCacheEntry<A> =>
  typeof entry.updatedAtMs === 'number';

/**
 * Capacity used when `makeEtagCache` is given a size that is not a finite number.
 */
const DEFAULT_ETAG_CACHE_ENTRIES = 500;

/**
 * Byte budget used when `makeEtagCache` is given none, or one that is not a finite number: 32 MiB.
 */
const DEFAULT_ETAG_CACHE_BYTES = 32 * 1024 * 1024;

/**
 * Returns the bytes `entry` counts against the budget: its `bodyBytes`, or zero when that is
 * missing or not a positive number.
 */
const entryBytes = (entry: EtagCacheEntry): number =>
  typeof entry.bodyBytes === 'number' && entry.bodyBytes > 0 ? entry.bodyBytes : 0;

/**
 * Creates an in-memory LRU `EtagCacheShape` bounded to `maxEntries` entries and `maxBytes` bytes of
 * `bodyBytes` (32 MiB by default). Storing an entry evicts least-recently used entries until both
 * bounds hold; an entry larger than `maxBytes` is not stored, and replacing an entry with one drops
 * the old entry. A bound that is not finite (`NaN`, `Infinity`) falls back to its default, so the
 * cache is never unbounded. A capacity of zero keeps no entries. Negative bounds are treated as
 * zero. The budget counts body bytes, not heap: a decoded value usually takes somewhat more memory
 * than its JSON.
 */
export const makeEtagCache = (
  maxEntries: number = DEFAULT_ETAG_CACHE_ENTRIES,
  maxBytes: number = DEFAULT_ETAG_CACHE_BYTES,
): Effect.Effect<EtagCacheShape> =>
  Effect.sync(() => {
    const capacity = Number.isFinite(maxEntries)
      ? Math.max(0, Math.floor(maxEntries))
      : DEFAULT_ETAG_CACHE_ENTRIES;
    const budget = Number.isFinite(maxBytes)
      ? Math.max(0, Math.floor(maxBytes))
      : DEFAULT_ETAG_CACHE_BYTES;
    const store = new Map<string, EtagCacheEntry>();
    let totalBytes = 0;
    /**
     * Removes the entry under `key`, if any, and its bytes from the total.
     */
    const remove = (key: string): void => {
      const existing = store.get(key);
      if (existing !== undefined) {
        store.delete(key);
        totalBytes -= entryBytes(existing);
      }
    };
    /**
     * Stores `entry` under `key` as most-recently used, evicting the oldest entries until it fits.
     * An entry over the budget only removes the one it replaces.
     */
    const insert = (key: string, entry: EtagCacheEntry): void => {
      remove(key);
      const size = entryBytes(entry);
      if (capacity === 0 || size > budget) {
        return;
      }

      const room = budget - size;
      // Map keys iterate oldest first, and deleting the current key keeps the iteration valid.
      for (const oldestKey of store.keys()) {
        if (store.size < capacity && totalBytes <= room) {
          break;
        }

        remove(oldestKey);
      }

      store.set(key, entry);
      totalBytes += size;
    };

    return {
      get: <A = unknown>(key: string): Effect.Effect<Option.Option<EtagCacheEntry<A>>> =>
        Effect.sync(() => {
          const existing = store.get(key);
          if (existing === undefined) {
            return Option.none();
          }

          store.delete(key);
          store.set(key, existing);
          return isTypedCacheEntry<A>(existing) ? Option.some(existing) : Option.none();
        }),
      set: <A>(key: string, entry: EtagCacheEntry<A>): Effect.Effect<void> =>
        Effect.sync(() => {
          insert(key, entry);
        }),
      setIfCurrent: <A>(
        key: string,
        expected: EtagCacheEntry<A>,
        entry: EtagCacheEntry<A>,
      ): Effect.Effect<boolean> =>
        Effect.sync(() => {
          if (store.get(key) !== expected) {
            return false;
          }

          insert(key, entry);
          return true;
        }),
      deleteIfCurrent: <A>(key: string, expected: EtagCacheEntry<A>): Effect.Effect<boolean> =>
        Effect.sync(() => {
          if (store.get(key) !== expected) {
            return false;
          }

          remove(key);
          return true;
        }),
      clear: Effect.sync(() => {
        store.clear();
        totalBytes = 0;
      }),
      size: Effect.sync(() => store.size),
    };
  });

/**
 * Effect service providing bounded in-memory conditional ETag caching across polls.
 */
export class EtagCache extends Context.Service<EtagCache, EtagCacheShape>()(
  '@yieldkit/http/EtagCache',
) {
  /**
   * Default in-memory `EtagCache` layer (500 entries, 32 MiB of response bodies).
   */
  static readonly layer: Layer.Layer<EtagCache> = Layer.effect(this, makeEtagCache());

  /**
   * Factory helper constructing a standalone `EtagCacheShape` instance.
   */
  static readonly make: (maxEntries?: number, maxBytes?: number) => Effect.Effect<EtagCacheShape> =
    makeEtagCache;
}
