import type { Duration } from 'effect';

import { Effect, Stream } from 'effect';

import { tagHttpError } from './error-meta.ts';
import { HttpRequestError } from './errors.ts';
import { formatTimeoutInput } from './timeouts.ts';

/**
 * Time limits applied to a streamed response body.
 */
export type StreamTimeoutParams = {
  /**
   * Logical upstream identifier for error attribution.
   */
  readonly upstream: string;

  /**
   * Longest wait for the next chunk. A body that sends nothing for this long fails as stalled.
   */
  readonly idleTimeout: Duration.Input;

  /**
   * Time left for the whole body, or `null` for no limit beyond `idleTimeout`. Formatted into the
   * error as `totalLabel`.
   */
  readonly totalTimeout: Duration.Input | null;

  /**
   * How the total limit is named in its error message, such as `operation timeout (10 minutes)`.
   */
  readonly totalLabel: string;
};

/**
 * Builds a `timeout` error for a body stream. It is never retried: some of the body may already
 * have reached the caller, so the request cannot be replayed safely.
 */
const streamTimeoutError = (upstream: string, detail: string): HttpRequestError =>
  tagHttpError(new HttpRequestError({ upstream, status: null, message: `${upstream} ${detail}` }), {
    outcome: 'timeout',
    retryable: false,
  });

/**
 * Fails `body` with a `timeout` `HttpRequestError` when no chunk arrives within `idleTimeout`, or
 * when the whole stream runs past `totalTimeout`. Either failure interrupts the read, which cancels
 * the upstream body.
 */
export const withStreamTimeouts = <A, E>(
  body: Stream.Stream<A, E>,
  params: StreamTimeoutParams,
): Stream.Stream<A, E | HttpRequestError> => {
  const { upstream, idleTimeout, totalTimeout } = params;
  const idleLimited = Stream.timeoutOrElse(body, {
    duration: idleTimeout,
    orElse: () =>
      Stream.fail(
        streamTimeoutError(
          upstream,
          `response body stalled: no data for ${formatTimeoutInput(idleTimeout)}`,
        ),
      ),
  });
  if (totalTimeout === null) {
    return idleLimited;
  }

  return Stream.interruptWhen(
    idleLimited,
    Effect.flatMap(Effect.sleep(totalTimeout), () =>
      Effect.fail(streamTimeoutError(upstream, `response body ran past the ${params.totalLabel}`)),
    ),
  );
};
