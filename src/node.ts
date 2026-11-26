import type { HttpClient } from 'effect/http';
import type { CircuitBreaker, RateLimiter } from 'effect/persistence';

import * as NodeHttpClient from '@effect/platform-node/NodeHttpClient';
import { Layer } from 'effect';

import type { EtagCache } from './conditional.ts';

import { HttpResilienceLive, ResilientHttp } from './client.ts';

/**
 * Node HTTP stack: `NodeHttpClient.layerUndici` as the transport, `HttpResilienceLive` (in-memory
 * rate limiter, circuit breaker and ETag cache) and `ResilientHttp.layer` over both. Requires
 * `@effect/platform-node` and runs on Node only; on Bun use `@yieldkit/http/bun`. The Undici
 * transport returns each 3xx as it is, so redirects follow the package's own rules, and it ignores
 * an ambient `FetchHttpClient.RequestInit`.
 */
export const ResilientHttpLive: Layer.Layer<
  | HttpClient.HttpClient
  | RateLimiter.RateLimiter
  | CircuitBreaker.CircuitBreaker
  | EtagCache
  | ResilientHttp
> = Layer.mergeAll(
  NodeHttpClient.layerUndici,
  HttpResilienceLive,
  ResilientHttp.layer.pipe(
    Layer.provide(NodeHttpClient.layerUndici),
    Layer.provide(HttpResilienceLive),
  ),
);
