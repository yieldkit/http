import type { HttpClientResponse } from 'effect/http';

import { Effect, Stream } from 'effect';

import type { HttpError } from './errors.ts';
import type { HttpRequestOptions } from './request-options.ts';
import type { StreamingRuntime } from './streaming.ts';

import { hasNoBody } from './body.ts';
import { copyWebResponse } from './buffered-response.ts';
import { openStream } from './streaming.ts';
import { attemptTimeoutError } from './timeouts.ts';

/**
 * Builds the Web `Response` handed to `use`: status, status text, headers and post-redirect URL of
 * `response`, around `body`. `Content-Length` and `Transfer-Encoding` are dropped as in
 * `getResponse`, and so is `Content-Encoding` when the transport decoded the body (see
 * `copyWebResponse`). A response without a body (204, 205, 304, HEAD) gets a null body, because a
 * `Response` with one of those statuses cannot carry a stream.
 */
const toWebResponse = (
  response: HttpClientResponse.HttpClientResponse,
  body: ReadableStream<Uint8Array>,
): Response => copyWebResponse(response, hasNoBody(response) ? null : body, true);

/**
 * Fetches `pathOrUrl` and runs `use` on a streaming Web `Response`, built on the same body stream
 * as `stream`. The request phase (status, rate-limit checks, retries) runs first; `use` then runs
 * once, never retried, with its own `attemptTimeout` budget. Its body errors once more than
 * `maxBodyBytes` have been read or when no data arrives for `idleTimeout`. A declared
 * `Content-Length` over the cap is not refused up front, because `use` may read only a prefix (an
 * HTML `<head>`). The body is cancelled and the bulkhead permit released when `use` finishes, fails
 * or is interrupted, even if `use` still holds a reader. The request metric is recorded once, after
 * `use`.
 */
export const runWithWebResponse = <A, E>(params: {
  readonly runtime: StreamingRuntime;
  readonly pathOrUrl: string;
  readonly use: (response: Response, finalUrl: string) => Effect.Effect<A, E>;
  readonly options?: HttpRequestOptions | undefined;
}): Effect.Effect<A, E | HttpError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const { runtime, options, use } = params;
      const opened = yield* openStream({
        ...params,
        refuseDeclaredOverflow: false,
        bodyUnderOperationTimeout: false,
      });
      // Closing the scope aborts the signal, which stops the body even while `use` holds a reader.
      const signal = yield* Effect.abortSignal;
      const readable = Stream.toReadableStreamWith(opened.body, yield* Effect.context(), {
        signal,
      });
      // Start the cancel without awaiting it: when the transport keeps a clone of the response, the
      // source settles a cancel only after every branch is cancelled, which would hold the permit.
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          if (!readable.locked) {
            void readable.cancel().catch(() => undefined);
          }
        }),
      );
      const budget = options?.attemptTimeout ?? runtime.policy.attemptTimeout;
      return yield* Effect.suspend(() =>
        use(toWebResponse(opened.response, readable), opened.finalUrl),
      ).pipe(
        Effect.timeoutOrElse({
          duration: budget,
          orElse: () => Effect.fail(attemptTimeoutError(runtime.policy.upstream, budget)),
        }),
      );
    }),
  );
