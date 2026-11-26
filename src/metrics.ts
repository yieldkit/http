import { Cause, Effect, Metric } from 'effect';

import type { HttpOutcomeLabel } from './error-meta.ts';
import type { HttpError } from './errors.ts';

import { readHttpErrorMeta } from './error-meta.ts';
import { isHttpError } from './errors.ts';

export type { HttpOutcomeLabel } from './error-meta.ts';

/**
 * Low-cardinality reason label recorded on `resilient_http_retries_total`.
 */
export type HttpRetryReasonLabel = 'rate_limit' | 'server_error' | 'timeout' | 'network_error';

/**
 * Cumulative counter of completed or failed outgoing HTTP requests by upstream, method, and
 * outcome.
 */
export const httpRequestsTotal: Metric.Counter<number> = Metric.counter(
  'resilient_http_requests_total',
  {
    description: 'Total outgoing HTTP requests by upstream, method, and outcome',
    incremental: true,
  },
);

/**
 * Histogram of outgoing HTTP request durations in milliseconds.
 */
export const httpRequestDurationMs: Metric.Histogram<number> = Metric.histogram(
  'resilient_http_request_duration_ms',
  {
    description: 'Outgoing HTTP request latency in milliseconds',
    boundaries: Metric.exponentialBoundaries({ start: 10, factor: 2, count: 12 }),
  },
);

/**
 * Cumulative counter of HTTP retry attempts by upstream and transient failure reason.
 */
export const httpRetriesTotal: Metric.Counter<number> = Metric.counter(
  'resilient_http_retries_total',
  {
    description: 'Total HTTP retry attempts by upstream and reason',
    incremental: true,
  },
);

/**
 * Cumulative counter of upstream HTTP rate-limit responses (429 / 403) encountered per upstream.
 */
export const httpRateLimitsTotal: Metric.Counter<number> = Metric.counter(
  'resilient_http_rate_limits_total',
  {
    description: 'Total upstream HTTP rate-limit events by upstream',
    incremental: true,
  },
);

/**
 * Cumulative counter of cached payloads served because the upstream request failed.
 */
export const httpStaleServedTotal: Metric.Counter<number> = Metric.counter(
  'resilient_http_stale_served_total',
  {
    description: 'Total cached payloads served after an upstream failure, by upstream',
    incremental: true,
  },
);

/**
 * Gauge tracking currently in-flight outgoing HTTP requests per upstream.
 */
export const httpActiveRequests: Metric.Gauge<number> = Metric.gauge(
  'resilient_http_active_requests',
  {
    description: 'Currently in-flight outgoing HTTP requests by upstream',
  },
);

/**
 * Maps the HTTP status of a failure into an outcome label. A failed 2xx means its body could not be
 * used (`decode_error`). Any other status below 400 is a response the request cannot use, such as a
 * 3xx that was not followed (no `Location`, or a 300 or 305): `unexpected_status`, which is not
 * stale-servable.
 */
const classifyStatusOutcome = (status: number): HttpOutcomeLabel => {
  if (status === 408) {
    return 'timeout';
  }

  if (status >= 500) {
    return 'server_error';
  }

  if (status >= 400) {
    return 'client_error';
  }

  return status >= 200 && status < 300 ? 'decode_error' : 'unexpected_status';
};

/**
 * Maps an HTTP failure into a bounded `HttpOutcomeLabel` for metric tagging. The outcome tagged by
 * the code that built the error wins, then rate limits, then the HTTP status. An untagged failure
 * without a status is a `network_error`. The message is never read, so a URL or upstream text in it
 * (`/timeout`, `?parse=1`) cannot change the label.
 */
export const classifyErrorOutcome = (error: HttpError): HttpOutcomeLabel => {
  const tagged = readHttpErrorMeta(error).outcome;
  if (tagged !== undefined) {
    return tagged;
  }

  if (error._tag === 'HttpRateLimitedError' || error.status === 429) {
    return 'rate_limited';
  }

  if (error.status !== null) {
    return classifyStatusOutcome(error.status);
  }

  return 'network_error';
};

/**
 * Maps the cause of a failed request into its outcome label: this package's errors through
 * `classifyErrorOutcome`, and any other failure, defect or interruption as a `network_error`.
 */
export const classifyFailureOutcome = (cause: Cause.Cause<unknown>): HttpOutcomeLabel => {
  const squashed = Cause.squash(cause);
  return isHttpError(squashed) ? classifyErrorOutcome(squashed) : 'network_error';
};

/**
 * Maps a transient HTTP failure into a bounded `HttpRetryReasonLabel` for retry telemetry, with the
 * same precedence as `classifyErrorOutcome`.
 */
export const classifyRetryReason = (error: HttpError): HttpRetryReasonLabel => {
  const outcome = classifyErrorOutcome(error);
  if (outcome === 'rate_limited') {
    return 'rate_limit';
  }

  if (outcome === 'timeout' || outcome === 'server_error') {
    return outcome;
  }

  return 'network_error';
};

/**
 * Records a completed HTTP request count and duration histogram observation.
 */
export const recordHttpRequestMetric = (params: {
  readonly upstream: string;
  readonly method: string;
  readonly outcome: HttpOutcomeLabel;
  readonly durationMs: number;
}): Effect.Effect<void> => {
  const attributes = {
    upstream: params.upstream,
    method: params.method,
    outcome: params.outcome,
  };
  return Effect.all(
    [
      Metric.update(Metric.withAttributes(httpRequestsTotal, attributes), 1),
      Metric.update(
        Metric.withAttributes(httpRequestDurationMs, attributes),
        Math.max(0, params.durationMs),
      ),
    ],
    { discard: true },
  );
};

/**
 * Increments `resilient_http_retries_total` for a transient retry attempt.
 */
export const recordHttpRetryMetric = (params: {
  readonly upstream: string;
  readonly reason: HttpRetryReasonLabel;
}): Effect.Effect<void> =>
  Metric.update(
    Metric.withAttributes(httpRetriesTotal, {
      upstream: params.upstream,
      reason: params.reason,
    }),
    1,
  );

/**
 * Increments `resilient_http_rate_limits_total` when an upstream hits an upstream rate limit.
 */
export const recordHttpRateLimitMetric = (upstream: string): Effect.Effect<void> =>
  Metric.update(Metric.withAttributes(httpRateLimitsTotal, { upstream }), 1);

/**
 * Increments `resilient_http_stale_served_total` when a stale cached payload is served.
 */
export const recordHttpStaleServedMetric = (upstream: string): Effect.Effect<void> =>
  Metric.update(Metric.withAttributes(httpStaleServedTotal, { upstream }), 1);
