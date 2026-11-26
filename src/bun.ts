import type { HttpClient } from 'effect/http';
import type { CircuitBreaker, RateLimiter } from 'effect/persistence';

import * as BunHttpClient from '@effect/platform-bun/BunHttpClient';
import { Layer } from 'effect';

import type { EtagCache } from './conditional.ts';

import { HttpResilienceLive, ResilientHttp } from './client.ts';

/**
 * Bun HTTP stack: `BunHttpClient.layer` as the transport, `HttpResilienceLive` (in-memory rate
 * limiter, circuit breaker and ETag cache) and `ResilientHttp.layer` over both. Requires
 * `@effect/platform-bun`; on Node use `@yieldkit/http/node`, elsewhere provide your own
 * `HttpClient` with `HttpResilienceLive` and `ResilientHttp.layer`.
 */
export const ResilientHttpLive: Layer.Layer<
  | HttpClient.HttpClient
  | RateLimiter.RateLimiter
  | CircuitBreaker.CircuitBreaker
  | EtagCache
  | ResilientHttp
> = Layer.mergeAll(
  BunHttpClient.layer,
  HttpResilienceLive,
  ResilientHttp.layer.pipe(Layer.provide(BunHttpClient.layer), Layer.provide(HttpResilienceLive)),
);
