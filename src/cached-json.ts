import type { Schema } from 'effect';

import { Clock, Effect, Option } from 'effect';
import { FetchHttpClient, HttpClient, HttpClientRequest, UrlParams } from 'effect/http';

import type { ConditionalResponse, EtagCacheEntry, EtagCacheShape } from './conditional.ts';
import type { HttpError } from './errors.ts';
import type { ResolvedHttpPolicy } from './policy.ts';
import type { HttpRequestOptions } from './request-options.ts';

import { buildCacheKey, requestValidators, selectUsableEntry } from './cache-key.ts';
import { HttpRequestError } from './errors.ts';
import { redirectLocation } from './redirects.ts';
import { prepareRequestParams } from './request-options.ts';
import { isRedirectedResponse } from './retry.ts';
import { recoverWithStaleEntry, resolveStaleIfError } from './stale-if-error.ts';
import { redactRequestUrlForLogs } from './url-redaction.ts';

/**
 * The parts of a client runtime that the cached JSON request needs.
 */
interface CachedJsonRuntime {
  /**
   * Underlying Effect `HttpClient` transport.
   */
  readonly httpClient: HttpClient.HttpClient;

  /**
   * Resolved policy for the upstream.
   */
  readonly policy: ResolvedHttpPolicy;

  /**
   * Conditional ETag cache instance.
   */
  readonly etagCache: EtagCacheShape;
}

/**
 * Statuses after which the cached payload of this key must not be served: the caller is not allowed
 * to see it (401, 403) or it is gone (404, 410).
 */
const EVICTING_STATUSES: ReadonlySet<number> = new Set([401, 403, 404, 410]);

/**
 * Responses below this bound count as answers for cache eviction.
 */
const FIRST_REDIRECT_STATUS = 300;

/**
 * A 304 also counts as an answer because it confirms the cached entry.
 */
const NOT_MODIFIED_STATUS = 304;

/**
 * `Cache-Control` directive that forbids storing a response.
 */
const NO_STORE_DIRECTIVE_RE = /(?:^|,)\s*no-store\s*(?:,|$)/i;

/**
 * Returns the absolute URL `request` resolves to, or its raw `url` when it cannot be built.
 */
const requestUrl = (request: HttpClientRequest.HttpClientRequest): string => {
  const url = HttpClientRequest.toUrl(request);
  return Option.isSome(url) ? url.value.toString() : request.url;
};

/**
 * Runs `conditional` without the cache, for a request whose cache key cannot be computed: the data
 * of a 2xx, or a 304 `HttpRequestError` when the upstream answers 304 to validators the caller
 * supplied, as when nothing is cached.
 */
const runUncached = <A, E, R>(
  runtime: CachedJsonRuntime,
  conditional: Effect.Effect<ConditionalResponse<A>, E, R>,
): Effect.Effect<A, E | HttpRequestError, R> =>
  Effect.flatMap(conditional, (result) =>
    result._tag === 'Modified'
      ? Effect.succeed(result.data)
      : revalidateCachedEntry({
          runtime,
          cacheKey: '',
          cached: Option.none(),
          result,
          cacheable: false,
        }),
  );

/**
 * Coordinates a conditional GET request with `EtagCache`, returning cached data on HTTP 304. With
 * stale-if-error enabled it caches every cacheable success and serves a payload no older than the
 * configured age when the upstream fails with a recoverable outcome. The cache key holds the schema
 * identity and a hash of the request headers (see `buildCacheKey`), so an entry is only ever used
 * for the schema, credentials and representation that produced it. The key is built from the
 * request as the underlying `HttpClient` sends it: its request transforms (`HttpClient.mapRequest`,
 * `mapRequestEffect`, which run in its `preprocess`) are applied to the prepared request first, so
 * credentials they add count too. Those transforms therefore run once more per call, and when they
 * fail the call goes out without the cache (see `runUncached`). Validators the caller supplied (as
 * `ifNoneMatch`/`ifModifiedSince` or as raw headers) that differ from the cached entry's make the
 * call ignore that entry. A 2xx or 304 evicts the entry read for this call if it is marked
 * `Cache-Control: no-store` or reached through a redirect. A 401, 403, 404 or 410 also evicts it
 * unless the policy treats the response as a rate limit. Cached values are shared between callers
 * by reference: treat them as immutable.
 */
export const runCachedJsonRequest = Effect.fnUntraced(function* <
  S extends Schema.Constraint,
>(params: {
  readonly runtime: CachedJsonRuntime;
  readonly getConditionalJson: <S extends Schema.Constraint>(
    pathOrUrl: string,
    schema: S,
    options?: HttpRequestOptions,
    client?: HttpClient.HttpClient,
  ) => Effect.Effect<ConditionalResponse<S['Type']>, HttpError, S['DecodingServices']>;
  readonly pathOrUrl: string;
  readonly schema: S;
  readonly options?: HttpRequestOptions | undefined;
}): Effect.fn.Return<S['Type'], HttpError, S['DecodingServices']> {
  const { runtime, getConditionalJson, pathOrUrl, schema, options } = params;
  const probe = prepareRequestParams(
    runtime,
    HttpClientRequest.get(pathOrUrl),
    options,
    true,
  ).request;
  const transportRequest = yield* Effect.option(runtime.httpClient.preprocess(probe));
  if (Option.isNone(transportRequest)) {
    return yield* runUncached(runtime, getConditionalJson(pathOrUrl, schema, options));
  }

  const ambientInit = yield* Effect.serviceOption(FetchHttpClient.RequestInit);
  const cacheKey = buildCacheKey({
    upstream: runtime.policy.upstream,
    schema,
    headers: transportRequest.value.headers,
    ambientHeaders: Option.isSome(ambientInit) ? ambientInit.value.headers : undefined,
    url: requestUrl(transportRequest.value),
  });
  const existingOpt = yield* runtime.etagCache.get<S['Type']>(cacheKey);
  const supplied = requestValidators(probe.headers);
  const cachedOpt = selectUsableEntry(existingOpt, supplied);
  const ifNoneMatch =
    supplied.ifNoneMatch ?? (Option.isSome(cachedOpt) ? cachedOpt.value.etag : null);
  const ifModifiedSince =
    supplied.ifModifiedSince ?? (Option.isSome(cachedOpt) ? cachedOpt.value.lastModified : null);
  const staleMaxAge = resolveStaleIfError(runtime.policy, options);
  const received = { redirected: false, cacheable: true };
  const client = HttpClient.tap(runtime.httpClient, (response) => {
    received.redirected ||=
      redirectLocation(response) !== null || isRedirectedResponse(response.request, response);
    const cacheControl = response.headers['cache-control'];
    const noStore = cacheControl !== undefined && NO_STORE_DIRECTIVE_RE.test(cacheControl);
    received.cacheable = !received.redirected && !noStore;
    const answered =
      response.status < FIRST_REDIRECT_STATUS || response.status === NOT_MODIFIED_STATUS;
    if (answered && !received.cacheable && Option.isSome(existingOpt)) {
      return Effect.asVoid(runtime.etagCache.deleteIfCurrent(cacheKey, existingOpt.value));
    }

    return Effect.void;
  });
  const conditional = getConditionalJson(
    pathOrUrl,
    schema,
    { ...options, ifNoneMatch, ifModifiedSince },
    client,
  ).pipe(
    Effect.tapError((error) => evictOnDefinitiveRefusal(runtime, cacheKey, existingOpt, error)),
  );
  const logUrl =
    redactRequestUrlForLogs(probe, yield* UrlParams.CurrentRedactedNames) ?? requestUrl(probe);
  const result = yield* staleMaxAge === null
    ? conditional
    : Effect.catch(conditional, (error) =>
        Effect.flatMap(runtime.etagCache.get<S['Type']>(cacheKey), (current) =>
          recoverWithStaleEntry({
            error,
            cached: selectUsableEntry(current, supplied),
            maxAge: staleMaxAge,
            upstream: runtime.policy.upstream,
            logUrl,
          }),
        ),
      );
  if (result._tag === 'StaleServed') {
    return result.data;
  }

  const { redirected, cacheable } = received;
  if (result._tag === 'NotModified') {
    // The redirect target cannot validate data from the original URL.
    if (redirected && Option.isSome(cachedOpt)) {
      return yield* runUncached(runtime, getConditionalJson(pathOrUrl, schema, options));
    }

    const revalidation = { runtime, cacheKey, cached: cachedOpt, result, cacheable };
    return yield* revalidateCachedEntry(revalidation);
  }

  if (cacheable) {
    yield* recordFreshResult({
      runtime,
      cacheKey,
      existing: existingOpt,
      result,
      staleEnabled: staleMaxAge !== null,
    });
  }

  return result.data;
});

/**
 * Returns `true` for a failure whose status is in `EVICTING_STATUSES`.
 */
const evictingStatus = (error: HttpError): boolean =>
  error._tag === 'HttpRequestError' && error.status !== null && EVICTING_STATUSES.has(error.status);

/**
 * Evicts the entry read for this call when the upstream refused the request definitively (see
 * `EVICTING_STATUSES`), so a later outage cannot serve the payload that was just refused.
 */
const evictOnDefinitiveRefusal = <A>(
  runtime: CachedJsonRuntime,
  cacheKey: string,
  existing: Option.Option<EtagCacheEntry<A>>,
  error: HttpError,
): Effect.Effect<void> =>
  evictingStatus(error) && Option.isSome(existing)
    ? Effect.asVoid(runtime.etagCache.deleteIfCurrent(cacheKey, existing.value))
    : Effect.void;

/**
 * Applies a fresh 2xx result to the cache. It is stored when it carries a validator or
 * stale-if-error is enabled. Otherwise the entry read for this call is evicted, so a later stale
 * fallback cannot serve data older than this success. A store only replaces the entry read for this
 * call (or fills a key that is still empty), so a slow response never overwrites a newer one stored
 * meanwhile.
 */
const recordFreshResult = <A>(params: {
  readonly runtime: CachedJsonRuntime;
  readonly cacheKey: string;
  readonly existing: Option.Option<EtagCacheEntry<A>>;
  readonly result: {
    readonly data: A;
    readonly bodyBytes: number;
    readonly etag: string | null;
    readonly lastModified: string | null;
  };
  readonly staleEnabled: boolean;
}): Effect.Effect<void> => {
  const { runtime, cacheKey, existing, result, staleEnabled } = params;
  const worthStoring = staleEnabled || result.etag !== null || result.lastModified !== null;
  if (worthStoring) {
    return storeCachedEntry({ runtime, key: cacheKey, existing, entry: result });
  }

  return Option.isSome(existing)
    ? Effect.asVoid(runtime.etagCache.deleteIfCurrent(cacheKey, existing.value))
    : Effect.void;
};
/**
 * Handles a real upstream 304: returns the cached payload and stores it again with the current time
 * and the returned validators, because the 304 confirms it is current and restarts its
 * stale-if-error age, when the response permits caching. Only the entry that was read is touched,
 * so a late 304 never overwrites a newer entry stored meanwhile. Fails with a 304
 * `HttpRequestError` when there is no usable cached entry.
 */
const revalidateCachedEntry = Effect.fnUntraced(function* <A>(params: {
  readonly runtime: CachedJsonRuntime;
  readonly cacheKey: string;
  readonly cached: Option.Option<EtagCacheEntry<A>>;
  readonly cacheable: boolean;
  readonly result: {
    readonly etag: string | null;
    readonly lastModified: string | null;
  };
}): Effect.fn.Return<A, HttpRequestError> {
  const { runtime, cacheKey, cached, cacheable, result } = params;
  if (Option.isNone(cached)) {
    return yield* new HttpRequestError({
      upstream: runtime.policy.upstream,
      status: NOT_MODIFIED_STATUS,
      message: `${runtime.policy.upstream} returned HTTP 304 without cached entry`,
    });
  }

  if (!cacheable) {
    return cached.value.data;
  }

  const updatedAtMs = yield* Clock.currentTimeMillis;
  yield* runtime.etagCache.setIfCurrent(cacheKey, cached.value, {
    etag: result.etag ?? cached.value.etag,
    lastModified: result.lastModified ?? cached.value.lastModified,
    data: cached.value.data,
    bodyBytes: cached.value.bodyBytes,
    updatedAtMs,
  });
  return cached.value.data;
});

/**
 * Stores `entry` under `key`, stamped with the current `Clock` time, when the key still holds the
 * entry read at the start of the call (`existing`), or is still empty when nothing was read.
 */
const storeCachedEntry = Effect.fnUntraced(function* <A>(params: {
  readonly runtime: CachedJsonRuntime;
  readonly key: string;
  readonly existing: Option.Option<EtagCacheEntry<A>>;
  readonly entry: {
    readonly etag: string | null;
    readonly lastModified: string | null;
    readonly data: A;
    readonly bodyBytes: number;
  };
}): Effect.fn.Return<void> {
  const { runtime, key, existing, entry } = params;
  const updatedAtMs = yield* Clock.currentTimeMillis;
  const next = {
    etag: entry.etag,
    lastModified: entry.lastModified,
    data: entry.data,
    bodyBytes: entry.bodyBytes,
    updatedAtMs,
  };
  if (Option.isSome(existing)) {
    yield* runtime.etagCache.setIfCurrent(key, existing.value, next);
    return;
  }

  const current = yield* runtime.etagCache.get(key);
  if (Option.isNone(current)) {
    yield* runtime.etagCache.set(key, next);
  }
});
