import type { Schema, Stream } from 'effect';
import type { HttpClientResponse } from 'effect/http';

import { Context, Duration, Effect, Layer, Option, Schedule, Semaphore } from 'effect';
import { HttpBody, HttpClient, HttpClientRequest } from 'effect/http';
import { CircuitBreaker, RateLimiter } from 'effect/persistence';

import type { ConditionalResponse, EtagCacheShape } from './conditional.ts';
import type { HttpError } from './errors.ts';
import type { HttpPolicy, ResolvedHttpPolicy } from './policy.ts';
import type { HttpRequestOptions } from './request-options.ts';
import type { StreamingRuntime } from './streaming.ts';

import { runCachedJsonRequest } from './cached-json.ts';
import { EtagCache, makeEtagCache } from './conditional.ts';
import { tagHttpError } from './error-meta.ts';
import { HttpRequestError } from './errors.ts';
import * as Execute from './execute.ts';
import * as Policy from './policy.ts';
import { prepareRequestParams, validateRequestConfig } from './request-options.ts';
import { runStream } from './streaming.ts';
import { runWithWebResponse } from './web-response.ts';

export type { HttpRequestOptions } from './request-options.ts';

/**
 * Shorthand Effect type for operations that can fail with an `HttpError`.
 */
type HttpEffect<A, R = never> = Effect.Effect<A, HttpError, R>;

/**
 * Capacity of the ETag cache a client creates for itself when no shared `EtagCache` service is
 * provided. It has `makeEtagCache`'s default byte budget (32 MiB) too.
 */
const LOCAL_ETAG_CACHE_ENTRIES = 250;

/**
 * Upstream-bound HTTP client preconfigured with base URL, headers, bulkhead concurrency, ETag
 * cache, and resilience.
 */
export interface ResilientHttpClient {
  /**
   * Normalized policy governing this client instance.
   */
  readonly policy: ResolvedHttpPolicy;

  /**
   * Sends a GET request and decodes the JSON response body using `schema`.
   */
  readonly getJson: <S extends Schema.Constraint>(
    pathOrUrl: string,
    schema: S,
    options?: HttpRequestOptions,
  ) => HttpEffect<S['Type'], S['DecodingServices']>;

  /**
   * Sends a GET request and decodes the JSON body into `Option.some`; an HTTP 404 gives
   * `Option.none()` without decoding, after one attempt, with the metric outcome `not_found`.
   */
  readonly getOptionalJson: <S extends Schema.Constraint>(
    pathOrUrl: string,
    schema: S,
    options?: HttpRequestOptions,
  ) => HttpEffect<Option.Option<S['Type']>, S['DecodingServices']>;

  /**
   * Sends a conditional GET request (`If-None-Match` / `If-Modified-Since`) and handles `304 Not
   * Modified`.
   */
  readonly getConditionalJson: <S extends Schema.Constraint>(
    pathOrUrl: string,
    schema: S,
    options?: HttpRequestOptions,
  ) => HttpEffect<ConditionalResponse<S['Type']>, S['DecodingServices']>;

  /**
   * Sends a GET request coordinated with `EtagCache`, serving cached data transparently on `304 Not
   * Modified`. With `staleIfError` enabled it also serves a cached payload within that age when the
   * upstream fails, on a best-effort basis since the cache is a bounded LRU. The cache is
   * partitioned by the identity of `schema`, so pass a stable instance defined at module level: a
   * schema built on every call never finds its entry and is never served from the cache.
   */
  readonly getCachedJson: <S extends Schema.Constraint>(
    pathOrUrl: string,
    schema: S,
    options?: HttpRequestOptions,
  ) => HttpEffect<S['Type'], S['DecodingServices']>;

  /**
   * Sends a POST request with a JSON body and decodes the JSON response using `schema`.
   */
  readonly postJson: <S extends Schema.Constraint>(
    pathOrUrl: string,
    body: unknown,
    schema: S,
    options?: HttpRequestOptions,
  ) => HttpEffect<S['Type'], S['DecodingServices']>;

  /**
   * Sends a GET request and reads a byte-bounded UTF-8 text response body.
   */
  readonly getText: (pathOrUrl: string, options?: HttpRequestOptions) => HttpEffect<string>;

  /**
   * Sends a GET request and reads a byte-bounded `Uint8Array` response body.
   */
  readonly getBytes: (pathOrUrl: string, options?: HttpRequestOptions) => HttpEffect<Uint8Array>;

  /**
   * Sends a GET request and returns the validated 2xx `HttpClientResponse` with its body buffered
   * within `maxBodyBytes` during the attempt. A larger body fails with `HttpRequestError`.
   */
  readonly getResponse: (
    pathOrUrl: string,
    options?: HttpRequestOptions,
  ) => HttpEffect<HttpClientResponse.HttpClientResponse>;

  /**
   * Sends a GET request and hands the streaming Web `Response` to `use` (e.g., for `HTMLRewriter`).
   * `use` runs once, inside the bulkhead permit, with its own `attemptTimeout` budget; its body
   * errors past `maxBodyBytes`, and its failures are never retried.
   */
  readonly withWebResponse: <A, E>(
    pathOrUrl: string,
    use: (response: Response, finalUrl: string) => Effect.Effect<A, E>,
    options?: HttpRequestOptions,
  ) => Effect.Effect<A, E | HttpError>;

  /**
   * Executes a custom `HttpClientRequest` through the upstream's resilience pipeline and returns
   * the 2xx response with its body buffered within `maxBodyBytes`, like `getResponse`.
   */
  readonly execute: (
    request: HttpClientRequest.HttpClientRequest,
    options?: HttpRequestOptions,
  ) => HttpEffect<HttpClientResponse.HttpClientResponse>;

  /**
   * Streams the body of a GET without buffering it. The request phase (rate limiter, circuit
   * breaker, SSRF guard, attempt timeout, retries on the status) is the same as for `getBytes`;
   * once a 2xx arrives nothing is retried. The body is capped at `maxBodyBytes`, fails as stalled
   * after `idleTimeout` without data, and fails once the whole call passes `operationTimeout`. The
   * stream is lazy, holds a bulkhead permit while it runs, and cancels the body when it ends, fails
   * or is interrupted.
   */
  readonly stream: (
    pathOrUrl: string,
    options?: HttpRequestOptions,
  ) => Stream.Stream<Uint8Array, HttpError>;
}

/**
 * Internal runtime state captured when constructing a `ResilientHttpClient`.
 */
interface HttpClientRuntime extends StreamingRuntime {
  /**
   * Conditional ETag cache instance.
   */
  readonly etagCache: EtagCacheShape;

  /**
   * Wrapper applying captured `RateLimiter`, `CircuitBreaker`, and bulkhead `Semaphore` permits.
   */
  readonly wrapExecution: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
}

/**
 * Builds the refusal for a JSON body that cannot be sent: never retried, ignored by the circuit
 * breaker, labelled `blocked`, with the encoder's reason in the message.
 */
const bodyEncodeError = (upstream: string, reason: string): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({
      upstream,
      status: null,
      message: `Failed to encode ${upstream} request JSON body: ${reason}`,
    }),
    { outcome: 'blocked', retryable: false, circuit: 'ignore' },
  );

/**
 * Sets `body` as the JSON body of `request`. Fails with `bodyEncodeError` before anything is sent
 * when `JSON.stringify` returns `undefined` or throws. Keeps the prepared request's Content-Type,
 * defaulting to `application/json`.
 */
const encodeJsonBody = (
  upstream: string,
  request: HttpClientRequest.HttpClientRequest,
  body: unknown,
): Effect.Effect<HttpClientRequest.HttpClientRequest, HttpRequestError> => {
  const encoded = HttpBody.json(body, request.headers['content-type']).pipe(
    Effect.mapError((error) => bodyEncodeError(upstream, describeEncodeFailure(error))),
  );
  return Effect.flatMap(encoded, (json) => {
    // A JSON document always contains bytes. Effect encodes a stringify result of undefined as empty.
    if (json.contentLength === 0) {
      return Effect.fail(bodyEncodeError(upstream, 'the body is undefined'));
    }

    return Effect.succeed(HttpClientRequest.setBody(request, json));
  });
};

/**
 * Returns the reason a JSON body failed to encode: the message of the underlying cause (`Do not
 * know how to serialize a BigInt`), else the error's own message, else a generic reason.
 */
const describeEncodeFailure = (error: HttpBody.HttpBodyError): string => {
  if (error.cause instanceof Error && error.cause.message !== '') {
    return error.cause.message;
  }

  return error.message === '' ? 'the body could not be encoded' : error.message;
};

/**
 * Assembles the `ResilientHttpClient` method table from a configured `HttpClientRuntime`.
 */
const buildResilientHttpClientMethods = (runtime: HttpClientRuntime): ResilientHttpClient => {
  const { policy, wrapExecution } = runtime;
  /**
   * Fails with a typed `HttpRequestError` when the policy or the per-request `options` hold an
   * invalid value, before anything is sent; otherwise runs `build()`.
   */
  const validated = <A, E, R>(
    options: HttpRequestOptions | undefined,
    build: () => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | HttpRequestError, R> =>
    Effect.flatMap(validateRequestConfig(policy, options), build);
  /**
   * Validates like `validated`, then runs the request built by `build()` with the resilience
   * services and a bulkhead permit. `build` runs only after validation passes.
   */
  const run = <A, E, R>(
    options: HttpRequestOptions | undefined,
    build: () => Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | HttpRequestError, R> => validated(options, () => wrapExecution(build()));
  /**
   * Builds the request parameters for a GET of `pathOrUrl` under this client's policy. With
   * `acceptJson` it sends `Accept: application/json` unless the caller set an `Accept` header.
   */
  const getParams = (
    pathOrUrl: string,
    options: HttpRequestOptions | undefined,
    acceptJson = false,
  ): Execute.ExecuteRequestBaseParams =>
    prepareRequestParams(runtime, HttpClientRequest.get(pathOrUrl), options, acceptJson);
  /**
   * Executes a conditional GET request and decodes the response payload with `schema`.
   */
  const getConditionalJson = <S extends Schema.Constraint>(
    url: string,
    schema: S,
    options?: HttpRequestOptions,
    client = runtime.httpClient,
  ): HttpEffect<ConditionalResponse<S['Type']>, S['DecodingServices']> =>
    run(options, () =>
      Execute.executeConditionalJsonRequest({
        ...getParams(url, options, true),
        client,
        schema,
        ifNoneMatch: options?.ifNoneMatch,
        ifModifiedSince: options?.ifModifiedSince,
      }),
    );
  return {
    policy,
    getJson: (pathOrUrl, schema, options) =>
      run(options, () =>
        Execute.executeJsonRequest({ ...getParams(pathOrUrl, options, true), schema }),
      ),
    getOptionalJson: (pathOrUrl, schema, options) =>
      run(options, () =>
        Execute.executeOptionalJsonRequest({ ...getParams(pathOrUrl, options, true), schema }),
      ),
    getConditionalJson: (url, schema, options) => getConditionalJson(url, schema, options),
    getCachedJson: (pathOrUrl, schema, options) =>
      validated(options, () =>
        runCachedJsonRequest({ runtime, getConditionalJson, pathOrUrl, schema, options }),
      ),
    postJson: (pathOrUrl, body, schema, options) =>
      Effect.gen(function* () {
        yield* validateRequestConfig(policy, options);
        const baseParams = prepareRequestParams(
          runtime,
          HttpClientRequest.post(pathOrUrl),
          options,
          true,
        );
        const request = yield* encodeJsonBody(policy.upstream, baseParams.request, body);
        return yield* wrapExecution(Execute.executeJsonRequest({ ...baseParams, request, schema }));
      }),
    getText: (pathOrUrl, options) =>
      run(options, () => Execute.executeTextRequest(getParams(pathOrUrl, options))),
    getBytes: (pathOrUrl, options) =>
      run(options, () => Execute.executeBytesRequest(getParams(pathOrUrl, options))),
    getResponse: (pathOrUrl, options) =>
      run(options, () => Execute.executeResponseRequest(getParams(pathOrUrl, options))),
    withWebResponse: (pathOrUrl, use, options) =>
      runWithWebResponse({ runtime, pathOrUrl, use, options }),
    stream: (pathOrUrl, options) => runStream({ runtime, pathOrUrl, options }),
    execute: (rawRequest, options) =>
      run(options, () =>
        Execute.executeResponseRequest(prepareRequestParams(runtime, rawRequest, options)),
      ),
  };
};

/**
 * Constructs a `ResilientHttpClient` bound to `policy`, capturing `HttpClient` and optional
 * resilience services.
 */
export const makeResilientHttpClient = Effect.fnUntraced(function* (
  policy: HttpPolicy,
): Effect.fn.Return<ResilientHttpClient, never, HttpClient.HttpClient> {
  const resolved = Policy.resolveHttpPolicy(policy);
  const httpClient = yield* HttpClient.HttpClient;
  const rateLimiterOpt = yield* Effect.serviceOption(RateLimiter.RateLimiter);
  const circuitBreakerOpt = yield* Effect.serviceOption(CircuitBreaker.CircuitBreaker);
  const etagCacheOpt = yield* Effect.serviceOption(EtagCache);
  const localEtagCache = yield* makeEtagCache(LOCAL_ETAG_CACHE_ENTRIES);
  const etagCache = Option.getOrElse(etagCacheOpt, () => localEtagCache);
  const semaphore =
    resolved.maxConcurrency !== null ? yield* Semaphore.make(resolved.maxConcurrency) : null;
  /**
   * Provides the captured `RateLimiter` and `CircuitBreaker` to a request effect.
   */
  const provideResilience = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> => {
    const withLimiter = Option.isSome(rateLimiterOpt)
      ? Effect.provideService(effect, RateLimiter.RateLimiter, rateLimiterOpt.value)
      : effect;
    return Option.isSome(circuitBreakerOpt)
      ? Effect.provideService(withLimiter, CircuitBreaker.CircuitBreaker, circuitBreakerOpt.value)
      : withLimiter;
  };
  /**
   * Runs a request effect with the resilience services and one bulkhead permit.
   */
  const wrapExecution = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
    semaphore !== null
      ? semaphore.withPermits(1)(provideResilience(effect))
      : provideResilience(effect);
  return buildResilientHttpClientMethods({
    httpClient,
    policy: resolved,
    etagCache,
    wrapExecution,
    provideResilience,
    holdPermit: semaphore !== null ? semaphore.takeScoped(1) : Effect.void,
  });
});

/**
 * Factory service producing policy-bound `ResilientHttpClient` instances sharing the application
 * transport.
 */
export class ResilientHttp extends Context.Service<
  ResilientHttp,
  {
    /**
     * Creates a `ResilientHttpClient` configured with `policy`. Build it once and reuse it:
     * `maxConcurrency` and the local ETag cache (used without an `EtagCache` service) belong to the
     * returned instance, while the rate limiter and circuit breaker are shared by key.
     */
    readonly client: (policy: HttpPolicy) => Effect.Effect<ResilientHttpClient>;
  }
>()('@yieldkit/http/ResilientHttp') {
  /**
   * Layer constructing `ResilientHttp` from the current `HttpClient.HttpClient` context.
   */
  static readonly layer: Layer.Layer<ResilientHttp, never, HttpClient.HttpClient> = Layer.effect(
    this,
    Effect.gen(function* () {
      const layerContext = yield* Effect.context<HttpClient.HttpClient>();
      return ResilientHttp.of({
        client: (policy) =>
          Effect.gen(function* () {
            const callContext = yield* Effect.context();
            return yield* Effect.provide(
              makeResilientHttpClient(policy),
              Context.merge(layerContext, callContext),
            );
          }),
      });
    }),
  );
}

/**
 * Settings of the shared circuit breaker built by `makeHttpResilienceLayer`.
 */
export type HttpResilienceOptions = {
  /**
   * Consecutive breaker failures that open a circuit; defaults to
   * `DEFAULT_CIRCUIT_CONSECUTIVE_FAILURES` (5). Must be a positive safe integer, or building the
   * layer dies.
   */
  readonly consecutiveFailures?: number | undefined;

  /**
   * First open period of a tripped circuit; defaults to `DEFAULT_CIRCUIT_OPEN_DURATION` (5
   * seconds). Each reopening after a failed probe doubles it. Must be a finite, positive duration,
   * or building the layer dies.
   */
  readonly openDuration?: Duration.Input | undefined;

  /**
   * How long the open period keeps doubling, measured over the schedule's elapsed time; defaults to
   * `DEFAULT_CIRCUIT_OPEN_BACKOFF_LIMIT` (5 minutes), which gives 5, 10, 20, 40, 80, then 160
   * seconds with the default `openDuration`. Must be a finite, positive duration, or building the
   * layer dies.
   */
  readonly openBackoffLimit?: Duration.Input | undefined;
};

/**
 * Returns why the breaker settings in `options` cannot work, or null when they can. A duration must
 * parse with `Policy.checkedDuration` and be above zero.
 */
const resilienceOptionsIssue = (options: HttpResilienceOptions | undefined): string | null => {
  const failures = options?.consecutiveFailures;
  if (failures !== undefined && !Policy.POSITIVE_SAFE_INTEGER.accepts(failures)) {
    return `consecutiveFailures must be ${Policy.POSITIVE_SAFE_INTEGER.description}`;
  }

  for (const name of ['openDuration', 'openBackoffLimit'] as const) {
    const input = options?.[name];
    const duration = input === undefined ? null : Policy.checkedDuration(input);
    if (input !== undefined && (duration === null || Duration.isZero(duration))) {
      return `${name} must be a finite, positive duration`;
    }
  }

  return null;
};

/**
 * Builds the shared in-memory resilience layer: one `RateLimiter`, one `CircuitBreaker` and one
 * `EtagCache` for every client built in its scope. The breaker opens a key after
 * `consecutiveFailures` consecutive failures, for `openDuration` doubling on each failed probe
 * within `openBackoffLimit`. A response's `Retry-After` can extend an opening up to one hour.
 * Calling it never throws: settings that cannot work (see `HttpResilienceOptions`) make building
 * the layer die with a `RangeError` that names them.
 */
export const makeHttpResilienceLayer = (
  options?: HttpResilienceOptions,
): Layer.Layer<RateLimiter.RateLimiter | CircuitBreaker.CircuitBreaker | EtagCache> =>
  Layer.suspend(() => {
    const issue = resilienceOptionsIssue(options);
    if (issue !== null) {
      throw new RangeError(`makeHttpResilienceLayer: ${issue}`);
    }

    return Layer.mergeAll(
      RateLimiter.layer.pipe(Layer.provide(RateLimiter.layerStoreMemory)),
      CircuitBreaker.layer({
        tripPolicy: CircuitBreaker.consecutive(
          options?.consecutiveFailures ?? Policy.DEFAULT_CIRCUIT_CONSECUTIVE_FAILURES,
        ),
        openDuration: Schedule.exponential(
          options?.openDuration ?? Policy.DEFAULT_CIRCUIT_OPEN_DURATION,
        ).pipe(
          Schedule.upTo({
            duration: options?.openBackoffLimit ?? Policy.DEFAULT_CIRCUIT_OPEN_BACKOFF_LIMIT,
          }),
        ),
      }).pipe(Layer.provide(CircuitBreaker.layerStoreMemory())),
      EtagCache.layer,
    );
  });

/**
 * `makeHttpResilienceLayer()` with its defaults: a breaker that opens after 5 consecutive failures
 * for 5, 10, 20, 40, 80, then 160 seconds. A rate-limit delay can extend an opening up to one
 * hour.
 */
export const HttpResilienceLive: Layer.Layer<
  RateLimiter.RateLimiter | CircuitBreaker.CircuitBreaker | EtagCache
> = makeHttpResilienceLayer();
