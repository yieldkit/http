import type { Schema } from 'effect';

import { Option } from 'effect';
import { Headers } from 'effect/http';
import { createHash } from 'node:crypto';

import type { EtagCacheEntry } from './conditional.ts';

/**
 * Numeric ids handed to the schemas passed to `getCachedJson`, by identity, so one cache entry is
 * never decoded with a different schema than the one that decoded it. A schema rebuilt on every
 * call gets a new id each time and so never hits the cache; callers keep theirs at module level.
 */
const schemaPartitions = new WeakMap<Schema.Constraint, number>();

/**
 * Number of schemas given a partition id so far.
 */
let schemaPartitionCount = 0;

/**
 * Returns the stable partition id of `schema`, assigning the next one on first use.
 */
const schemaPartition = (schema: Schema.Constraint): number => {
  const existing = schemaPartitions.get(schema);
  if (existing !== undefined) {
    return existing;
  }

  schemaPartitionCount += 1;
  schemaPartitions.set(schema, schemaPartitionCount);
  return schemaPartitionCount;
};

/**
 * Request headers left out of the cache key: they do not select who is asking or which
 * representation comes back (`user-agent`, `accept-encoding`, which fetch decodes away), or they
 * are the validators the cache itself manages.
 */
const KEY_NEUTRAL_HEADERS: ReadonlySet<string> = new Set([
  'accept-encoding',
  'if-modified-since',
  'if-none-match',
  'user-agent',
]);

/**
 * Hex characters of the SHA-256 digest kept in the key.
 */
const FINGERPRINT_HEX_CHARS = 32;

/**
 * Lists `name:value` for every header of `headers` except `KEY_NEUTRAL_HEADERS`, lower-cased by
 * name and sorted, each prefixed with `origin` so request and ambient headers stay apart.
 */
const keyedHeaderLines = (
  origin: string,
  headers: Iterable<readonly [string, string]>,
): Array<string> =>
  Array.from(headers, ([name, value]) => ({ name: name.toLowerCase(), value }))
    .filter(({ name }) => !KEY_NEUTRAL_HEADERS.has(name))
    .map(({ name, value }) => `${origin}${name}:${value}`);

/**
 * Hashes every request header that can change who is asking or what comes back, so requests with
 * different credentials, tenants or `Accept`/`Accept-Language` never share a cache entry, whatever
 * the header is called (`Authorization`, `Ocp-Apim-Subscription-Key`, `X-Tenant-Id`). Only
 * `KEY_NEUTRAL_HEADERS` are left out. Headers from an ambient `FetchHttpClient.RequestInit`, which
 * fetch adds after the key is computed, count as well. The key holds a hash, never a header value.
 * Returns `null` when there is nothing to hash.
 */
const headerFingerprint = (
  headers: Headers.Headers,
  ambientHeaders: RequestInit['headers'],
): string | null => {
  const lines = [
    ...keyedHeaderLines('r:', Object.entries(headers)),
    ...keyedHeaderLines('a:', new globalThis.Headers(ambientHeaders)),
  ].toSorted();
  if (lines.length === 0) {
    return null;
  }

  return createHash('sha256')
    .update(lines.join('\n'))
    .digest('hex')
    .slice(0, FINGERPRINT_HEX_CHARS);
};

/**
 * Builds the `EtagCache` key of a cached JSON request from the upstream, the schema identity, a
 * hash of the request headers (see `headerFingerprint`) and the resolved URL.
 */
export const buildCacheKey = (params: {
  readonly upstream: string;
  readonly schema: Schema.Constraint;
  readonly headers: Headers.Headers;
  readonly ambientHeaders?: RequestInit['headers'];
  readonly url: string;
}): string => {
  const fingerprint = headerFingerprint(params.headers, params.ambientHeaders);
  const headerPart = fingerprint === null ? '' : `h${fingerprint}:`;
  return `${params.upstream}:s${String(schemaPartition(params.schema))}:${headerPart}${params.url}`;
};

/**
 * The conditional validators a request carries.
 */
export type RequestValidators = {
  /**
   * `If-None-Match` value, or null when absent.
   */
  readonly ifNoneMatch: string | null;

  /**
   * `If-Modified-Since` value, or null when absent.
   */
  readonly ifModifiedSince: string | null;
};

/**
 * Reads the validators from the prepared request headers, wherever the caller put them
 * (`ifNoneMatch` or a raw `If-None-Match` in `headers`).
 */
export const requestValidators = (headers: Headers.Headers): RequestValidators => ({
  ifNoneMatch: Option.getOrNull(Headers.get(headers, 'if-none-match')),
  ifModifiedSince: Option.getOrNull(Headers.get(headers, 'if-modified-since')),
});

/**
 * Tells whether a caller-supplied validator is absent or equal to the cached one. A validator that
 * differs means a `304` would confirm something other than the cached payload.
 */
const validatorAgrees = (supplied: string | null, cached: string | null): boolean =>
  supplied === null || supplied === cached;

/**
 * Returns the cached entry only when the validators the caller supplied are those of the entry.
 * With different caller validators the entry is ignored: its payload is neither served on a `304`
 * nor used as a stale fallback.
 */
export const selectUsableEntry = <A>(
  existing: Option.Option<EtagCacheEntry<A>>,
  supplied: RequestValidators,
): Option.Option<EtagCacheEntry<A>> =>
  Option.filter(
    existing,
    (entry) =>
      validatorAgrees(supplied.ifNoneMatch, entry.etag) &&
      validatorAgrees(supplied.ifModifiedSince, entry.lastModified),
  );
