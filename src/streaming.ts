import type { Scope } from 'effect';
import type { HttpClientResponse } from 'effect/http';

import { Clock, Duration, Effect, Exit, Metric, Stream } from 'effect';
import { HttpClientRequest } from 'effect/http';

import type { HttpOutcomeLabel } from './error-meta.ts';
import type { HttpError, HttpRequestError } from './errors.ts';
import type { HttpRequestOptions, RequestRuntime } from './request-options.ts';

import { cancelResponseBody, streamBoundedBytes } from './body.ts';
import * as Execute from './execute.ts';
import { classifyFailureOutcome, httpActiveRequests, recordHttpRequestMetric } from './metrics.ts';
import * as Policy from './policy.ts';
import { prepareRequestParams, validateRequestConfig } from './request-options.ts';
import { withStreamTimeouts } from './stream-body.ts';
import { formatTimeoutInput } from './timeouts.ts';

/**
 * Status of a response that only reaches the body phase when `acceptNotFound` accepted it.
 */
const NOT_FOUND_STATUS = 404;

/**
 * What the streaming methods need from a client: its transport and policy, plus the captured
 * resilience services and bulkhead.
 */
export type StreamingRuntime = RequestRuntime & {
  /**
   * Provides the captured `RateLimiter` and `CircuitBreaker` to `effect`, without a bulkhead
   * permit.
   */
  readonly provideResilience: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;

  /**
   * Takes one bulkhead permit until the surrounding scope closes. A no-op without `maxConcurrency`.
   */
  readonly holdPermit: Effect.Effect<void, never, Scope.Scope>;
};

/**
 * Parameters for opening a streamed GET.
 */
type OpenStreamParams = {
  /**
   * Client the request goes through.
   */
  readonly runtime: StreamingRuntime;

  /**
   * Path joined onto the policy's base URL, or an absolute URL.
   */
  readonly pathOrUrl: string;

  /**
   * Per-request options.
   */
  readonly options?: HttpRequestOptions | undefined;

  /**
   * `false` lets a body with a declared `Content-Length` over the cap start anyway, for a caller
   * that reads only a prefix. The cap still stops it once more than `maxBodyBytes` have passed.
   */
  readonly refuseDeclaredOverflow: boolean;

  /**
   * `true` makes `operationTimeout` cover the body as well as the request.
   */
  readonly bodyUnderOperationTimeout: boolean;
};

/**
 * A response whose status passed, with its body as a stream.
 */
export type OpenedStream = {
  /**
   * The 2xx (or accepted 404) response. Its body is unread; read it through `body`.
   */
  readonly response: HttpClientResponse.HttpClientResponse;

  /**
   * The post-redirect URL, or the requested URL when the transport does not report one.
   */
  readonly finalUrl: string;

  /**
   * The body, capped at `maxBodyBytes` and limited by `idleTimeout` (and by `operationTimeout` when
   * asked).
   */
  readonly body: Stream.Stream<Uint8Array, HttpRequestError>;
};

/**
 * Maps how the body phase ended to the outcome of the request metric. A completed body is a
 * `success`, or `not_found` for a 404 accepted with `acceptNotFound`. A package error keeps its own
 * label; any other failure or an interruption is a `network_error`, as in the request pipeline.
 */
const streamedOutcome = <A, E>(exit: Exit.Exit<A, E>, status: number): HttpOutcomeLabel => {
  if (Exit.isFailure(exit)) {
    return classifyFailureOutcome(exit.cause);
  }

  return status === NOT_FOUND_STATUS ? 'not_found' : 'success';
};

/**
 * Returns what is left of `operationTimeout` after `elapsedMs`, never less than zero.
 */
const remainingOperationBudget = (
  operationTimeout: Duration.Input,
  elapsedMs: number,
): Duration.Duration =>
  Duration.millis(
    Math.max(0, Duration.toMillis(Duration.fromInputUnsafe(operationTimeout)) - elapsedMs),
  );

/**
 * Sends a GET and opens its body as a stream, all within the caller's scope. The request phase
 * (validation, rate limiter, circuit breaker, SSRF guard, attempt timeout, retries on the status)
 * runs first, exactly as for the buffered methods. Once a status passes, nothing is retried. The
 * bulkhead permit and the active-requests gauge are held until the scope closes, and the request
 * metric (outcome and latency) is recorded once, when the scope closes, so it covers the body. The
 * response body is cancelled when the scope closes if nothing has read it, so a caller that only
 * looked at the status and headers still releases the connection at once.
 */
export const openStream = Effect.fnUntraced(function* (
  params: OpenStreamParams,
): Effect.fn.Return<OpenedStream, HttpError, Scope.Scope> {
  const { runtime, pathOrUrl, options } = params;
  const { upstream } = runtime.policy;
  yield* validateRequestConfig(runtime.policy, options);
  const requestParams = prepareRequestParams(runtime, HttpClientRequest.get(pathOrUrl), options);
  yield* runtime.holdPermit;
  const startMs = yield* Clock.currentTimeMillis;
  const response = yield* runtime.provideResilience(
    Execute.executeStreamingResponseRequest(requestParams),
  );
  yield* Effect.addFinalizer(() => cancelResponseBody(response));
  const gauge = Metric.withAttributes(httpActiveRequests, { upstream });
  yield* Effect.acquireRelease(Metric.modify(gauge, 1), () => Metric.modify(gauge, -1));
  yield* Effect.addFinalizer((exit) =>
    Effect.flatMap(Clock.currentTimeMillis, (endMs) =>
      recordHttpRequestMetric({
        upstream,
        method: 'GET',
        outcome: streamedOutcome(exit, response.status),
        durationMs: Math.max(0, endMs - startMs),
      }),
    ),
  );
  const operationTimeout = requestParams.operationTimeout ?? Policy.DEFAULT_OPERATION_TIMEOUT;
  const elapsedMs = Math.max(0, (yield* Clock.currentTimeMillis) - startMs);
  const body = withStreamTimeouts(
    streamBoundedBytes({
      response,
      upstream,
      maxBytes: requestParams.maxBodyBytes,
      refuseDeclaredOverflow: params.refuseDeclaredOverflow,
    }),
    {
      upstream,
      idleTimeout: options?.idleTimeout ?? Policy.DEFAULT_IDLE_TIMEOUT,
      totalTimeout: params.bodyUnderOperationTimeout
        ? remainingOperationBudget(operationTimeout, elapsedMs)
        : null,
      totalLabel: `operation timeout (${formatTimeoutInput(operationTimeout)})`,
    },
  );
  return {
    response,
    finalUrl:
      response.url.length > 0
        ? response.url
        : Policy.resolveRequestUrl(runtime.policy.baseUrl, pathOrUrl),
    body,
  };
});

/**
 * Streams the body of a GET to `pathOrUrl`. The stream is lazy: the request goes out when it is
 * first pulled. See `openStream` for the request phase. The body is capped at `maxBodyBytes` (a
 * declared `Content-Length` over it fails before reading), fails as stalled after `idleTimeout`
 * without data, and fails once the whole call passes `operationTimeout`. Ending, failing or
 * interrupting the stream cancels the body and releases the bulkhead permit.
 */
export const runStream = (params: {
  readonly runtime: StreamingRuntime;
  readonly pathOrUrl: string;
  readonly options?: HttpRequestOptions | undefined;
}): Stream.Stream<Uint8Array, HttpError> =>
  Stream.unwrap(
    Effect.map(
      openStream({ ...params, refuseDeclaredOverflow: true, bodyUnderOperationTimeout: true }),
      (opened) => opened.body,
    ),
  );
