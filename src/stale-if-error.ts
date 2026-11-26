import { logWarning } from '@yieldkit/logger';
import { Clock, Duration, Effect, Option } from 'effect';

import type { EtagCacheEntry } from './conditional.ts';
import type { HttpOutcomeLabel } from './error-meta.ts';
import type { HttpError } from './errors.ts';
import type { ResolvedHttpPolicy } from './policy.ts';
import type { HttpRequestOptions } from './request-options.ts';

import { classifyErrorOutcome, recordHttpStaleServedMetric } from './metrics.ts';
import { redactUrlForLogs } from './url-redaction.ts';

/**
 * Outcomes after which a cached payload may be served. Any other or future label, such as
 * `client_error`, `not_found` or `blocked`, is rethrown, because the cached data may be exactly
 * what the upstream now refuses to return.
 */
const STALE_SERVABLE_OUTCOMES: ReadonlySet<HttpOutcomeLabel> = new Set<HttpOutcomeLabel>([
  'rate_limited',
  'server_error',
  'timeout',
  'network_error',
  'decode_error',
  'circuit_open',
  'body_too_large',
]);

/**
 * Result of a failed conditional request recovered from the cache. It is distinct from an upstream
 * `NotModified`, so the caller can refresh the entry's age only after a real 304.
 */
export type StaleServed<A> = {
  /**
   * Discriminator for a payload served from the cache after an upstream failure.
   */
  readonly _tag: 'StaleServed';

  /**
   * Cached payload being served.
   */
  readonly data: A;
};

/**
 * Resolves the maximum stale age for one request: `false` disables it, a duration overrides the
 * policy, and an omitted option falls back to the policy. `null` means disabled.
 */
export const resolveStaleIfError = (
  policy: ResolvedHttpPolicy,
  options: HttpRequestOptions | undefined,
): Duration.Input | null => {
  const override = options?.staleIfError;
  if (override === false) {
    return null;
  }

  return override ?? policy.staleIfError;
};

/**
 * Recovers a failed conditional request with the cached payload when stale-if-error allows it. The
 * payload is served only when the failure outcome is in `STALE_SERVABLE_OUTCOMES`, an entry exists,
 * and `now - updatedAtMs` is within `maxAge`. Otherwise it fails with the original error. Serving
 * logs `http.cache.stale_served` and increments `resilient_http_stale_served_total`, and the result
 * is reported as `StaleServed` so the caller returns the cached data without refreshing its age.
 */
export const recoverWithStaleEntry = Effect.fnUntraced(function* <A>(params: {
  readonly error: HttpError;
  readonly cached: Option.Option<EtagCacheEntry<A>>;
  readonly maxAge: Duration.Input;
  readonly upstream: string;
  readonly logUrl: string;
}): Effect.fn.Return<StaleServed<A>, HttpError> {
  const { error, cached, maxAge, upstream, logUrl } = params;
  const outcome = classifyErrorOutcome(error);
  if (Option.isNone(cached) || !STALE_SERVABLE_OUTCOMES.has(outcome)) {
    return yield* error;
  }

  const now = yield* Clock.currentTimeMillis;
  const ageMs = now - cached.value.updatedAtMs;
  if (ageMs > Duration.toMillis(maxAge)) {
    return yield* error;
  }

  yield* logWarning('http.cache.stale_served', {
    upstream,
    url: redactUrlForLogs(logUrl),
    ageMs,
    outcome,
    errorTag: error._tag,
  });
  yield* recordHttpStaleServedMetric(upstream);
  return { _tag: 'StaleServed', data: cached.value.data } satisfies StaleServed<A>;
});
