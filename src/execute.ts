import type { Duration, Schema } from 'effect';
import type { HttpClientResponse } from 'effect/http';

import { logDebug } from '@yieldkit/logger';
import { Clock, Effect, Exit, Metric, Option, Scope } from 'effect';
import { Headers, HttpClient, HttpClientRequest, UrlParams } from 'effect/http';

import type { ConditionalResponse } from './conditional.ts';
import type { HttpError, HttpRequestError } from './errors.ts';
import type { HttpOutcomeLabel } from './metrics.ts';
import type { HttpCircuitBreakerPolicy, HttpRateLimitPolicy, HttpRetryPolicy } from './policy.ts';

import { guardedExecute } from './address-guard.ts';
import * as Body from './body.ts';
import { bufferResponse } from './buffered-response.ts';
import { withCircuitBreaker } from './circuit.ts';
import { tagHttpError } from './error-meta.ts';
import * as HeadersUtil from './headers.ts';
import { decodeJsonBody, decodeSizedJsonBody } from './json-body.ts';
import * as Metrics from './metrics.ts';
import * as Policy from './policy.ts';
import { consumeRateLimitToken } from './rate-limit.ts';
import { isOneShotBody } from './redirects.ts';
import * as Retry from './retry.ts';
import { ensureSuccessStatus, statusErrorWithoutBody } from './status-error.ts';
import { attemptTimeoutError, operationBudgetMillis, operationTimeoutError } from './timeouts.ts';
import { redactRequestUrlForLogs, redactUrlForLogs } from './url-redaction.ts';

/**
 * Lowest HTTP status that is an error.
 */
const FIRST_ERROR_STATUS = 400;

/**
 * HTTP status that `acceptNotFound` turns into a value.
 */
const NOT_FOUND_STATUS = 404;

/**
 * Common parameters for executing an outgoing HTTP request through the resilience pipeline.
 */
export interface ExecuteRequestBaseParams {
  /**
   * Underlying Effect `HttpClient` transport.
   */
  readonly client: HttpClient.HttpClient;

  /**
   * Immutable outgoing HTTP request description. A request with a one-shot body (`HttpBody.stream`,
   * or `HttpBody.raw` around a `ReadableStream` or a Node `Readable`) is sent once: it is never
   * retried, and a redirect that would replay the body is refused.
   */
  readonly request: HttpClientRequest.HttpClientRequest;

  /**
   * Logical upstream identifier for metrics, logs, and typed error attribution.
   */
  readonly upstream: string;

  /**
   * Optional per-attempt timeout override; defaults to `REQUEST_TIMEOUT`.
   */
  readonly attemptTimeout?: Duration.Input | undefined;

  /**
   * Optional deadline for the whole operation (rate-limiter waits, every attempt, backoff and
   * `Retry-After` sleeps); defaults to `DEFAULT_OPERATION_TIMEOUT`. It runs inside the circuit
   * breaker, so running out of it counts as a breaker failure, unless it ran out while the first
   * attempt was still waiting for a rate-limit token. A retry whose delay would end at or after the
   * deadline is not started; the last real error is returned instead.
   */
  readonly operationTimeout?: Duration.Input | undefined;

  /**
   * Declares a POST or PATCH request safe to repeat, so transient failures are retried like those
   * of an idempotent method.
   */
  readonly idempotent?: boolean | undefined;

  /**
   * Treats an HTTP 404 as a successful response: its body is read under the usual caps, it is not
   * retried, the circuit breaker does not count it, and the metric outcome is `not_found`.
   */
  readonly acceptNotFound?: boolean | undefined;

  /**
   * Optional maximum response body size in bytes; defaults to `DEFAULT_MAX_BODY_BYTES`.
   */
  readonly maxBodyBytes?: number | undefined;

  /**
   * Classifies HTTP 403 with `x-ratelimit-remaining: 0` as `HttpRateLimitedError`.
   */
  readonly treatQuotaExhausted403AsRateLimit?: boolean | undefined;

  /**
   * Classifies any HTTP 403 response as `HttpRateLimitedError`.
   */
  readonly treat403AsRateLimit?: boolean | undefined;

  /**
   * Scans HTTP 403 HTML bodies for CDN/WAF block signatures.
   */
  readonly detectHtmlEdgeBlocks?: boolean | undefined;

  /**
   * Optional retry schedule policy overrides.
   */
  readonly retry?: Partial<HttpRetryPolicy> | undefined;

  /**
   * Optional proactive rate-limiter policy.
   */
  readonly rateLimit?: HttpRateLimitPolicy | undefined;

  /**
   * Optional circuit breaker policy.
   */
  readonly circuitBreaker?: HttpCircuitBreakerPolicy | undefined;

  /**
   * Checks the first URL and every redirect hop with the public-address guard (`assertPublicUrl`).
   * Redirects are followed by this package (`guardedExecute`) either way.
   */
  readonly publicAddressesOnly?: boolean | undefined;

  /**
   * Redirect hops a request follows before it is refused; defaults to `DEFAULT_MAX_REDIRECTS`.
   */
  readonly maxRedirects?: number | undefined;
}

/**
 * Parameters for executing a JSON HTTP request and decoding the body with an Effect `Schema`.
 */
export interface ExecuteJsonRequestParams<
  S extends Schema.Constraint,
> extends ExecuteRequestBaseParams {
  /**
   * Effect `Schema` used to validate and decode the JSON response payload.
   */
  readonly schema: S;
}

/**
 * Parameters for executing a conditional JSON request (`If-None-Match` / `If-Modified-Since`).
 */
export interface ExecuteConditionalJsonRequestParams<
  S extends Schema.Constraint,
> extends ExecuteJsonRequestParams<S> {
  /**
   * Optional `ETag` validator attached as the `If-None-Match` request header.
   */
  readonly ifNoneMatch?: string | null | undefined;

  /**
   * Optional timestamp attached as the `If-Modified-Since` request header.
   */
  readonly ifModifiedSince?: string | null | undefined;
}

/**
 * Internal wrapper pairing a decoded response value with its metric outcome label.
 */
interface DecodedAttempt<A> {
  /**
   * Decoded value produced by the attempt.
   */
  readonly value: A;

  /**
   * Metric outcome classification (`success` or `not_modified`).
   */
  readonly outcome: HttpOutcomeLabel;
}

/**
 * Executes a single timed HTTP attempt including rate-limit inspection and payload decoding. A
 * rate-limited response has its body cancelled before the attempt fails, so the connection is
 * released at once, and is tagged `redirected` when it answered a redirect hop.
 *
 * Each request is attached to a scope closed at the end of the attempt. For streaming, `bodyScope`
 * supplies the caller's scope so the body stays open after the attempt returns.
 *
 * `pending` holds a 4xx or 5xx response (except an accepted 404) during the edge-block check or
 * error snippet read. If the attempt times out during that read, it fails with the stored status
 * and no snippet, so a stalled 403 body is still a non-retried `client_error`.
 */
const makeTimedAttempt = <A, R>(
  params: ExecuteRequestBaseParams,
  decode: (
    response: HttpClientResponse.HttpClientResponse,
  ) => Effect.Effect<DecodedAttempt<A>, HttpError, R>,
  pending: { response: Option.Option<HttpClientResponse.HttpClientResponse> },
  bodyScope: Scope.Scope | undefined,
): Effect.Effect<DecodedAttempt<A>, HttpError, R> => {
  const { client, request, upstream } = params;
  const timeout = params.attemptTimeout ?? Policy.REQUEST_TIMEOUT;
  const attempt = Effect.gen(function* () {
    const provideScope = Scope.provide(yield* Effect.scope);
    const scopedClient = HttpClient.transformResponse(HttpClient.withScope(client), provideScope);
    const response = yield* guardedExecute(scopedClient, request, {
      upstream,
      maxRedirects: params.maxRedirects,
      checkAddresses: params.publicAddressesOnly === true,
    });
    if (isErrorStatus(response.status, params.acceptNotFound === true)) {
      pending.response = Option.some(response);
    }

    const rateLimit = yield* HeadersUtil.detectRateLimitError({
      response,
      upstream,
      treatQuotaExhausted403AsRateLimit: params.treatQuotaExhausted403AsRateLimit,
      treat403AsRateLimit: params.treat403AsRateLimit,
      detectHtmlEdgeBlocks: params.detectHtmlEdgeBlocks,
    });
    if (Option.isSome(rateLimit)) {
      const rateLimitError = rateLimit.value;
      yield* Body.cancelResponseBody(response);
      yield* Metrics.recordHttpRateLimitMetric(upstream);
      if (Retry.isRedirectedResponse(request, response)) {
        tagHttpError(rateLimitError, { redirected: true });
      }

      return yield* rateLimitError;
    }

    return yield* decode(response);
  });
  const scoped = bodyScope === undefined ? Effect.scoped : Scope.provide(bodyScope);
  return scoped(attempt).pipe(
    Effect.timeoutOrElse({
      duration: timeout,
      orElse: () =>
        statusErrorWithoutBody(pending.response, upstream, () =>
          attemptTimeoutError(upstream, timeout),
        ),
    }),
  );
};

/**
 * Returns `true` for a status whose body is only read for an error message: a 4xx or 5xx, except a
 * 404 the request accepts as a value.
 */
const isErrorStatus = (status: number, acceptNotFound: boolean): boolean =>
  status >= FIRST_ERROR_STATUS && !(acceptNotFound && status === NOT_FOUND_STATUS);

/**
 * Orchestrates rate limiting, timed attempts, retries, circuit breaking, and metrics for a request.
 * Invalid limits (see `validateRequestLimits`) fail first, before the limiter, the breaker or any
 * metric, as `ResilientHttpClient` does for an invalid policy.
 *
 * Requests are attached to a scope closed at the end of each attempt, or to the caller's
 * `bodyScope` for streaming. The shared `pending` state lets the operation deadline report an error
 * response's status if its body is still being read.
 *
 * A retry is counted (`resilient_http_retries_total` and the `http.request.retry` debug line) when
 * the next attempt actually goes out, after its rate-limit permit, so a retry cut short by the
 * deadline or refused by the limiter is not counted. A retry whose delay would end after the
 * operation deadline is not started: the last real error is returned instead.
 */
const executeCoreRequest = Effect.fnUntraced(function* <A, R = never>(
  params: ExecuteRequestBaseParams,
  decode: (
    response: HttpClientResponse.HttpClientResponse,
  ) => Effect.Effect<DecodedAttempt<A>, HttpError, R>,
  bodyScope?: Scope.Scope,
): Effect.fn.Return<A, HttpError, R> {
  yield* Policy.validateRequestLimits(params.upstream, params);
  const { request, upstream } = params;
  const maxInline =
    params.retry?.maxInlineRetryAfterSeconds ?? Policy.DEFAULT_MAX_INLINE_RETRY_AFTER_SECONDS;
  const retryOnRateLimit = params.retry?.retryOnRateLimit ?? Policy.DEFAULT_RETRY_ON_RATE_LIMIT;
  const operationTimeout = params.operationTimeout ?? Policy.DEFAULT_OPERATION_TIMEOUT;
  /**
   * Applies the method rule: POST and PATCH retry only a refused rate limit unless idempotent. A
   * request with a one-shot body (`isOneShotBody`) is never retried, because the first attempt
   * consumed it and a retry would send an empty body.
   */
  const canRetry = (error: HttpError): boolean =>
    !isOneShotBody(request.body) &&
    Retry.isRetryableForMethod({
      error,
      method: request.method,
      idempotent: params.idempotent,
      maxInlineRetryAfterSeconds: maxInline,
      retryOnRateLimit,
    });
  const pending = { response: Option.none<HttpClientResponse.HttpClientResponse>() };
  const timedAttempt = makeTimedAttempt(params, decode, pending, bodyScope);
  const budgetMs = operationBudgetMillis(operationTimeout);
  let attemptCount = 0;
  let sent = false;
  let previousError: HttpError | null = null;
  /**
   * Records the retry of `error`, the failure of the attempt numbered `failedAttempt`. The logged
   * URL is masked as the client span masks it (`redactRequestUrlForLogs`).
   */
  const recordRetry = Effect.fnUntraced(function* (error: HttpError, failedAttempt: number) {
    const reason = Metrics.classifyRetryReason(error);
    const redactedNames = yield* UrlParams.CurrentRedactedNames;
    yield* Metrics.recordHttpRetryMetric({ upstream, reason });
    yield* logDebug('http.request.retry', {
      upstream,
      method: request.method,
      url: redactRequestUrlForLogs(request, redactedNames) ?? redactUrlForLogs(request.url),
      attempt: failedAttempt,
      reason,
      errorTag: error._tag,
    });
  });
  const attempt = Effect.gen(function* () {
    attemptCount += 1;
    yield* consumeRateLimitToken(upstream, params.rateLimit);
    sent = true;
    if (previousError !== null) {
      yield* recordRetry(previousError, attemptCount - 1);
    }

    const result = yield* timedAttempt;
    pending.response = Option.none();
    return result;
  }).pipe(
    Effect.tapError((error) =>
      Effect.sync(() => {
        previousError = error;
        pending.response = Option.none();
      }),
    ),
  );
  // The deadline is taken when the operation starts, inside the breaker, so the schedule can
  // refuse a retry that would sleep past it.
  const retried = Effect.flatMap(Clock.currentTimeMillis, (startedMs) =>
    Effect.retry(attempt, {
      schedule: Retry.createRetrySchedule(params.retry, {
        deadlineMs: budgetMs === null ? undefined : startedMs + budgetMs,
      }),
      while: canRetry,
    }),
  );
  // The deadline sits inside the breaker so a hung upstream that exhausts it counts as a
  // breaker failure; an interrupt from an outer timeout would be ignored by the breaker.
  const deadlined = retried.pipe(
    Effect.timeoutOrElse({
      duration: operationTimeout,
      orElse: () =>
        statusErrorWithoutBody(pending.response, upstream, () =>
          operationTimeoutError(upstream, operationTimeout, { attempts: attemptCount, sent }),
        ),
    }),
  );
  const guarded = withCircuitBreaker(deadlined, upstream, params.circuitBreaker, {
    isIgnoredSuccess: isAcceptedNotFound,
  });
  // The streaming caller records success when its scope closes to include the body read.
  const recordSuccess = bodyScope === undefined;
  return yield* measureRequest(guarded, { upstream, method: request.method, recordSuccess });
});

/**
 * Runs `guarded` while it counts in the active-requests gauge, then records the request counter and
 * latency with its outcome: the attempt's own label on success (skipped when `recordSuccess` is
 * `false`, for a caller that records it after consuming the body), or the failure's label from
 * `classifyFailureOutcome`. An interruption from outside (a caller's own timeout) is recorded too,
 * as a `network_error`. The failure is passed on unchanged.
 */
const measureRequest = Effect.fnUntraced(function* <A, R>(
  guarded: Effect.Effect<DecodedAttempt<A>, HttpError, R>,
  labels: { readonly upstream: string; readonly method: string; readonly recordSuccess: boolean },
): Effect.fn.Return<A, HttpError, R> {
  const { upstream, method } = labels;
  const activeGauge = Metric.withAttributes(Metrics.httpActiveRequests, { upstream });
  const startMs = yield* Clock.currentTimeMillis;
  /**
   * Records the request metric for how `guarded` ended.
   */
  const record = (exit: Exit.Exit<DecodedAttempt<A>, HttpError>): Effect.Effect<void> => {
    if (Exit.isSuccess(exit) && !labels.recordSuccess) {
      return Effect.void;
    }

    const outcome = Exit.isSuccess(exit)
      ? exit.value.outcome
      : Metrics.classifyFailureOutcome(exit.cause);
    return Effect.flatMap(Clock.currentTimeMillis, (endMs) =>
      Metrics.recordHttpRequestMetric({
        upstream,
        method,
        outcome,
        durationMs: Math.max(0, endMs - startMs),
      }),
    );
  };
  const measured = Effect.acquireUseRelease(
    Metric.modify(activeGauge, 1),
    () => guarded,
    () => Metric.modify(activeGauge, -1),
  );
  return (yield* Effect.onExit(measured, record)).value;
});

/**
 * Reports whether an attempt succeeded only because `acceptNotFound` turned a 404 into a value. The
 * circuit breaker ignores those instead of counting them as a healthy response.
 */
const isAcceptedNotFound = <A>(attempt: DecodedAttempt<A>): boolean =>
  attempt.outcome === 'not_found';

/**
 * Runs a request requiring an HTTP 2xx response and decodes the body.
 */
const execute2xxRequest = <A, R = never>(
  params: ExecuteRequestBaseParams,
  decode2xx: (response: HttpClientResponse.HttpClientResponse) => Effect.Effect<A, HttpError, R>,
  bodyScope?: Scope.Scope,
): Effect.Effect<A, HttpError, R> =>
  executeCoreRequest(
    params,
    (response) => {
      if (params.acceptNotFound === true && response.status === 404) {
        return Effect.map(decode2xx(response), (value) => ({
          value,
          outcome: 'not_found' as const,
        }));
      }

      return Effect.flatMap(ensureSuccessStatus(response, params.upstream), () =>
        Effect.map(decode2xx(response), (value) => ({ value, outcome: 'success' as const })),
      );
    },
    bodyScope,
  );

/**
 * Reads and decodes the JSON body of `response` with the request's `schema`, within its
 * `maxBodyBytes`. Fails as `decodeJsonBody` does.
 */
const decodeJsonResponse = <S extends Schema.Constraint>(
  params: ExecuteJsonRequestParams<S>,
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<S['Type'], HttpRequestError, S['DecodingServices']> =>
  decodeJsonBody({
    response,
    schema: params.schema,
    upstream: params.upstream,
    maxBodyBytes: params.maxBodyBytes,
  });

/**
 * Executes an HTTP request and decodes the 2xx JSON body using an Effect `Schema`. `acceptNotFound`
 * is ignored here, because a 404 body is not JSON of the expected shape; use
 * `executeOptionalJsonRequest` to treat a 404 as a value.
 */
export const executeJsonRequest = <S extends Schema.Constraint>(
  params: ExecuteJsonRequestParams<S>,
): Effect.Effect<S['Type'], HttpError, S['DecodingServices']> =>
  execute2xxRequest({ ...params, acceptNotFound: false }, (response) =>
    decodeJsonResponse(params, response),
  );

/**
 * Executes an HTTP request where a 404 is a value: it gives `Option.none()` without decoding (the
 * body is cancelled, the request is not retried, and the outcome is `not_found`), and a 2xx gives
 * `Option.some` of the decoded JSON body. Every other failure behaves like `executeJsonRequest`.
 */
export const executeOptionalJsonRequest = <S extends Schema.Constraint>(
  params: ExecuteJsonRequestParams<S>,
): Effect.Effect<Option.Option<S['Type']>, HttpError, S['DecodingServices']> =>
  executeCoreRequest<Option.Option<S['Type']>, S['DecodingServices']>(params, (response) => {
    if (response.status === 404) {
      return Effect.map(Body.cancelResponseBody(response), () => ({
        value: Option.none(),
        outcome: 'not_found',
      }));
    }

    return Effect.flatMap(ensureSuccessStatus(response, params.upstream), () =>
      Effect.map(decodeJsonResponse(params, response), (data) => ({
        value: Option.some(data),
        outcome: 'success',
      })),
    );
  });

/**
 * Executes a conditional HTTP request (`If-None-Match` / `If-Modified-Since`) and handles `304 Not
 * Modified`.
 */
export const executeConditionalJsonRequest = <S extends Schema.Constraint>(
  params: ExecuteConditionalJsonRequestParams<S>,
): Effect.Effect<ConditionalResponse<S['Type']>, HttpError, S['DecodingServices']> => {
  let request = params.request;
  if (params.ifNoneMatch !== undefined && params.ifNoneMatch !== null) {
    request = HttpClientRequest.setHeader(request, 'if-none-match', params.ifNoneMatch);
  }

  if (params.ifModifiedSince !== undefined && params.ifModifiedSince !== null) {
    request = HttpClientRequest.setHeader(request, 'if-modified-since', params.ifModifiedSince);
  }

  return executeCoreRequest<ConditionalResponse<S['Type']>, S['DecodingServices']>(
    { ...params, request },
    (response) => {
      if (response.status === 304) {
        return Effect.succeed({
          value: {
            _tag: 'NotModified',
            etag: HeadersUtil.extractEtag(response.headers) ?? params.ifNoneMatch ?? null,
            lastModified:
              HeadersUtil.extractLastModified(response.headers) ?? params.ifModifiedSince ?? null,
            cacheControl: Option.getOrNull(Headers.get(response.headers, 'cache-control')),
          },
          outcome: 'not_modified',
        });
      }

      return Effect.flatMap(ensureSuccessStatus(response, params.upstream), () =>
        Effect.map(
          decodeSizedJsonBody({
            response,
            schema: params.schema,
            upstream: params.upstream,
            maxBodyBytes: params.maxBodyBytes,
          }),
          ({ data, bodyBytes }) => ({
            value: {
              _tag: 'Modified',
              data,
              bodyBytes,
              etag: HeadersUtil.extractEtag(response.headers),
              lastModified: HeadersUtil.extractLastModified(response.headers),
              status: response.status,
              cacheControl: Option.getOrNull(Headers.get(response.headers, 'cache-control')),
            },
            outcome: 'success',
          }),
        ),
      );
    },
  );
};

/**
 * Executes an HTTP request and reads a byte-bounded UTF-8 text body.
 */
export const executeTextRequest = (
  params: ExecuteRequestBaseParams,
): Effect.Effect<string, HttpError> =>
  execute2xxRequest(params, (response) =>
    Body.readBoundedText({ response, upstream: params.upstream, maxBytes: params.maxBodyBytes }),
  );

/**
 * Executes an HTTP request and reads a byte-bounded `Uint8Array` body.
 */
export const executeBytesRequest = (
  params: ExecuteRequestBaseParams,
): Effect.Effect<Uint8Array, HttpError> =>
  execute2xxRequest(params, (response) =>
    Body.readBoundedBytes({ response, upstream: params.upstream, maxBytes: params.maxBodyBytes }),
  );

/**
 * Executes an HTTP request and returns the validated 2xx `HttpClientResponse` with its body already
 * buffered. The body read is part of the attempt, so it is covered by `attemptTimeout`, the
 * `maxBodyBytes` cap and retries; a body over the cap fails with `body_too_large`. `content-length`
 * and `transfer-encoding` are dropped from the returned headers because they describe the original
 * wire body, and so is `content-encoding` when the transport decoded the body (fetch and Bun do;
 * Undici returns it as sent).
 */
export const executeResponseRequest = (
  params: ExecuteRequestBaseParams,
): Effect.Effect<HttpClientResponse.HttpClientResponse, HttpError> =>
  execute2xxRequest(params, (response) =>
    bufferResponse(response, params.upstream, params.maxBodyBytes),
  );

/**
 * Executes an HTTP request and returns the validated 2xx `HttpClientResponse` with its body unread.
 * The caller's scope owns the body: it is outside the attempt timeout, the byte cap and retries. A
 * failure is recorded as a request metric as usual. The caller records success once the body has
 * been consumed.
 */
export const executeStreamingResponseRequest = (
  params: ExecuteRequestBaseParams,
): Effect.Effect<HttpClientResponse.HttpClientResponse, HttpError, Scope.Scope> =>
  Effect.flatMap(Effect.scope, (scope) => execute2xxRequest(params, Effect.succeed, scope));
