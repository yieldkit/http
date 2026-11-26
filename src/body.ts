import type { Duration } from 'effect';
import type { HttpClientError } from 'effect/http';

import { Effect, Exit, Option, Stream } from 'effect';
import { Headers, HttpClientResponse } from 'effect/http';

import { tagHttpError } from './error-meta.ts';
import { HttpRequestError } from './errors.ts';
import { DEFAULT_MAX_BODY_BYTES, POSITIVE_SAFE_INTEGER } from './policy.ts';
import { currentSentValueMask } from './sent-values.ts';
import { describeClientError } from './transport.ts';

/**
 * Shared UTF-8 decoder used to convert bounded byte buffers into strings.
 */
const UTF8_DECODER = new TextDecoder('utf-8');

/**
 * Statuses that never carry a body (RFC 9110), whatever the transport reports. A Web `Response`
 * with one of them cannot be constructed around a body either.
 */
export const BODYLESS_STATUSES: ReadonlySet<number> = new Set([204, 205, 304]);

/**
 * Time allowed for reading the start of a body shared by the 403 edge-block check and the non-2xx
 * snippet. A stalled body costs only the inspection, never the status error.
 */
const SHARED_BODY_PREFIX_TIMEOUT = '2 seconds';

/**
 * Parameters for reading a byte-bounded HTTP response body stream.
 */
export interface ReadBoundedBodyParams {
  /**
   * Incoming HTTP client response whose body stream will be consumed.
   */
  readonly response: HttpClientResponse.HttpClientResponse;

  /**
   * Logical upstream identifier used for error attribution.
   */
  readonly upstream: string;

  /**
   * Maximum allowed payload size in bytes; defaults to `DEFAULT_MAX_BODY_BYTES`, which also
   * replaces a value that is not a positive safe integer (`NaN`, `Infinity`, zero), so the cap
   * never turns off.
   */
  readonly maxBytes?: number | undefined;
}

/**
 * Parameters for reading at most `maxBytes` from the start of a response body.
 */
type ReadBodyPrefixParams = {
  /**
   * Response whose body is read and then cancelled.
   */
  readonly response: HttpClientResponse.HttpClientResponse;

  /**
   * Maximum number of bytes kept from the start of the body.
   */
  readonly maxBytes: number;

  /**
   * Time allowed for the read. When it runs out, the bytes read so far are returned as truncated.
   */
  readonly timeout?: Duration.Input | undefined;
};

/**
 * Start of a response body, decoded as UTF-8.
 */
type BodyPrefix = {
  /**
   * Decoded text of the bytes read. A multi-byte character cut at the limit is dropped.
   */
  readonly text: string;

  /**
   * `true` when the body continued past `maxBytes` or failed before it ended.
   */
  readonly truncated: boolean;
};

/**
 * Bytes read from the start of a body, kept so a second reader of the same response gets them.
 */
type StoredPrefix = {
  /**
   * Bytes read, at most the `maxBytes` of the first read.
   */
  readonly bytes: Uint8Array;

  /**
   * `true` when the body continued past what was read, failed or ran out of time.
   */
  readonly truncated: boolean;
};

/**
 * Prefixes read by `readSharedBodyPrefix`, keyed by response. A `WeakMap` lets them be collected
 * with the response.
 */
const SHARED_PREFIXES = new WeakMap<HttpClientResponse.HttpClientResponse, StoredPrefix>();

/**
 * Mutable accumulator state used while folding response body byte chunks.
 */
interface ByteChunkAccumulator {
  /**
   * Collected byte chunks received so far.
   */
  readonly chunks: Array<Uint8Array>;

  /**
   * Cumulative byte length across all collected chunks.
   */
  total: number;
}

/**
 * Builds the error for a body over `maxBytes`. It is never retried, because the same request
 * returns the same oversized body.
 */
const bodyTooLarge = (
  response: HttpClientResponse.HttpClientResponse,
  upstream: string,
  detail: string,
): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({ upstream, status: response.status, message: `${upstream} ${detail}` }),
    { outcome: 'body_too_large', retryable: false },
  );

/**
 * Cancels the unread body of `response` so the connection is released. A response backed by a Web
 * `Response` (fetch, Bun) has cancellation started without waiting for a retained clone. Opening
 * and closing the Effect stream then runs transport cleanup, including stopping a streamed fetch
 * upload. For a non-Web response (Undici through NodeHttpClient), its stream is opened and closed
 * directly, which works on a fully or partly read body. Cancel errors are ignored, and absent or
 * locked Web bodies are skipped.
 */
export const cancelResponseBody = (
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<void> => {
  const web = HttpClientResponse.toWeb(response);
  if (Option.isNone(web)) {
    return Effect.ignore(Effect.scoped(Stream.toPull(response.stream)));
  }

  const body = web.value.body;
  if (body === null || body.locked) {
    return Effect.void;
  }

  return Effect.andThen(
    Effect.sync(() => {
      void body.cancel().catch(() => undefined);
    }),
    Effect.ignore(Effect.scoped(Stream.toPull(response.stream))),
  );
};

/**
 * Returns `true` when `response` has no body to read: a 204, 205 or 304, the answer to a `HEAD`, or
 * a Web response whose body is `null`. Reading such a body fails in the transport, so it is treated
 * as empty instead.
 */
export const hasNoBody = (response: HttpClientResponse.HttpClientResponse): boolean => {
  if (BODYLESS_STATUSES.has(response.status) || response.request.method === 'HEAD') {
    return true;
  }

  const web = HttpClientResponse.toWeb(response);
  return Option.isSome(web) && web.value.body === null;
};

/**
 * Returns the declared `Content-Length` of `response`, or `null` when it is missing or not a finite
 * number.
 */
const declaredContentLength = (response: HttpClientResponse.HttpClientResponse): number | null => {
  const header = Headers.get(response.headers, 'content-length');
  if (Option.isNone(header)) {
    return null;
  }

  const declared = Number(header.value.trim());
  return Number.isFinite(declared) ? declared : null;
};

/**
 * Fails with `body_too_large` and cancels the unread body when the declared `Content-Length` of
 * `response` is over `maxBytes`. A missing or unparseable header passes.
 */
export const rejectDeclaredOverflow = (
  response: HttpClientResponse.HttpClientResponse,
  upstream: string,
  maxBytes: number,
): Effect.Effect<void, HttpRequestError> => {
  const declaredLength = declaredContentLength(response);
  if (declaredLength === null || declaredLength <= maxBytes) {
    return Effect.void;
  }

  return Effect.flatMap(cancelResponseBody(response), () =>
    Effect.fail(
      bodyTooLarge(
        response,
        upstream,
        `response content-length (${String(declaredLength)} bytes) exceeds maxBytes (${String(maxBytes)})`,
      ),
    ),
  );
};

/**
 * Concatenates collected chunks into one buffer of `total` bytes.
 */
const concatChunks = (chunks: ReadonlyArray<Uint8Array>, total: number): Uint8Array => {
  const output = new Uint8Array(total);
  let offset = 0;

  for (const chunk of chunks) {
    output.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return output;
};

/**
 * Parameters for streaming a byte-bounded response body.
 */
export interface StreamBoundedBodyParams extends ReadBoundedBodyParams {
  /**
   * `false` skips the up-front check of the declared `Content-Length`, for a caller that may read
   * only a prefix of a large body. The per-chunk cap still applies. Defaults to `true`.
   */
  readonly refuseDeclaredOverflow?: boolean | undefined;
}

/**
 * Returns `maxBytes` when it is a positive safe integer, otherwise `DEFAULT_MAX_BODY_BYTES`.
 */
const effectiveMaxBytes = (maxBytes: number | undefined): number =>
  maxBytes !== undefined && POSITIVE_SAFE_INTEGER.accepts(maxBytes)
    ? maxBytes
    : DEFAULT_MAX_BODY_BYTES;

/**
 * Builds the error for a transport failure partway through the body. It has `status: null` because
 * no complete response arrived, and its message names the status, method and origin, never the path
 * or query, with the credentials the request sent masked (`currentSentValueMask`).
 */
const bodyReadError = Effect.fnUntraced(function* (
  response: HttpClientResponse.HttpClientResponse,
  upstream: string,
  cause: HttpClientError.HttpClientError,
): Effect.fn.Return<never, HttpRequestError> {
  const mask = yield* currentSentValueMask(response.request);
  return yield* tagHttpError(
    new HttpRequestError({
      upstream,
      status: null,
      message: `${upstream} failed reading response body (HTTP ${String(response.status)}): ${describeClientError(cause, mask)}`,
    }),
    { outcome: 'network_error' },
  );
});

/**
 * Streams an HTTP response body chunk by chunk without buffering it. A response without a body (see
 * `hasNoBody`) is an empty stream. A declared `Content-Length` over the cap fails before anything
 * is read, unless `refuseDeclaredOverflow` is `false`. The stream fails with a `body_too_large`
 * `HttpRequestError` (never retried) as soon as more than `maxBytes` have passed, and with a
 * `network_error` one when the transport fails partway. Ending, failing or interrupting the stream
 * cancels the body.
 */
export const streamBoundedBytes = (
  params: StreamBoundedBodyParams,
): Stream.Stream<Uint8Array, HttpRequestError> => {
  const { response, upstream } = params;
  if (hasNoBody(response)) {
    return Stream.unwrap(Effect.as(cancelResponseBody(response), Stream.empty));
  }

  const maxBytes = effectiveMaxBytes(params.maxBytes);
  const capped = Stream.suspend(() => {
    let total = 0;
    return response.stream.pipe(
      Stream.catch((cause) => Stream.fromEffect(bodyReadError(response, upstream, cause))),
      Stream.mapEffect((chunk) => {
        total += chunk.byteLength;
        return total > maxBytes
          ? Effect.fail(
              bodyTooLarge(
                response,
                upstream,
                `response body exceeded maxBytes (${String(maxBytes)} bytes)`,
              ),
            )
          : Effect.succeed(chunk);
      }),
    );
  });
  return params.refuseDeclaredOverflow === false
    ? capped
    : Stream.unwrap(Effect.as(rejectDeclaredOverflow(response, upstream, maxBytes), capped));
};

/**
 * Reads an HTTP response body into a `Uint8Array` through `streamBoundedBytes`: zero bytes for a
 * response without a body, `body_too_large` (never retried) for a declared or actual body over
 * `maxBytes`, and a `network_error` with `status: null` for a transport failure partway, which is
 * retried (subject to the method rule) and counts against the circuit breaker like any other
 * transport error.
 */
export const readBoundedBytes = (
  params: ReadBoundedBodyParams,
): Effect.Effect<Uint8Array, HttpRequestError> =>
  Effect.map(
    Stream.runFold(
      streamBoundedBytes(params),
      (): ByteChunkAccumulator => ({ chunks: [], total: 0 }),
      (acc, chunk) => {
        acc.chunks.push(chunk);
        acc.total += chunk.byteLength;
        return acc;
      },
    ),
    (folded) => concatChunks(folded.chunks, folded.total),
  );

/**
 * Streams a UTF-8 text response body with a strict byte ceiling (`maxBytes`).
 */
export const readBoundedText = (
  params: ReadBoundedBodyParams,
): Effect.Effect<string, HttpRequestError> => Effect.map(readBoundedBytes(params), decodeUtf8);

/**
 * Decodes UTF-8 bytes into a string, replacing invalid sequences, as `readBoundedText` does.
 */
export const decodeUtf8 = (bytes: Uint8Array): string => UTF8_DECODER.decode(bytes);

/**
 * Reads at most `maxBytes` from the start of a response body and cancels the rest. It never fails:
 * a read error, a body that was already consumed or locked, or a read that runs past `timeout`
 * gives whatever was read so far, marked truncated. A response without a body gives no bytes.
 */
const readPrefixBytes = (params: ReadBodyPrefixParams): Effect.Effect<StoredPrefix> =>
  Effect.suspend(() => {
    const { response, maxBytes } = params;
    if (hasNoBody(response)) {
      return Effect.succeed({ bytes: new Uint8Array(0), truncated: false });
    }

    const acc: ByteChunkAccumulator = { chunks: [], total: 0 };
    let truncated = false;
    const read = response.stream.pipe(
      Stream.runForEachWhile((chunk) => {
        const room = maxBytes - acc.total;
        if (chunk.byteLength > room) {
          acc.chunks.push(chunk.subarray(0, room));
          acc.total = maxBytes;
          truncated = true;
          return Effect.succeed(false);
        }

        acc.chunks.push(chunk);
        acc.total += chunk.byteLength;
        return Effect.succeed(true);
      }),
    );
    const timed =
      params.timeout === undefined
        ? read
        : read.pipe(
            Effect.timeoutOrElse({
              duration: params.timeout,
              orElse: () =>
                Effect.sync(() => {
                  truncated = true;
                }),
            }),
          );
    return Effect.exit(timed).pipe(
      Effect.map((exit) => ({
        bytes: concatChunks(acc.chunks, acc.total),
        truncated: truncated || Exit.isFailure(exit),
      })),
    );
  });

/**
 * Decodes stored prefix bytes as UTF-8. A multi-byte character cut at the end is dropped.
 */
const decodePrefix = (prefix: StoredPrefix): BodyPrefix => ({
  text: new TextDecoder('utf-8').decode(prefix.bytes, { stream: true }),
  truncated: prefix.truncated,
});

/**
 * Turns a caller's byte limit into a whole, non-negative number of bytes. `NaN`, a negative value
 * and an infinite one read nothing, so a bad limit can neither throw nor lift the cap.
 */
const normalizePrefixBytes = (maxBytes: number): number =>
  Number.isFinite(maxBytes) ? Math.max(0, Math.floor(maxBytes)) : 0;

/**
 * Reads at most `maxBytes` from the start of a response body and cancels the rest. It never fails:
 * a read error, a body that was already consumed or locked, or a read that runs past `timeout`
 * gives whatever was read so far. A `maxBytes` that is not a finite, non-negative number reads
 * nothing. Used to inspect error and block pages without buffering them.
 */
export const readBodyPrefix = (params: ReadBodyPrefixParams): Effect.Effect<BodyPrefix> =>
  Effect.map(
    readPrefixBytes({ ...params, maxBytes: normalizePrefixBytes(params.maxBytes) }),
    decodePrefix,
  );

/**
 * Reads the start of a response body once per response, under a 2 second budget, and hands every
 * later caller the same bytes, cut to its own `maxBytes`. The 403 edge-block check and the non-2xx
 * snippet both read through it, so the snippet still sees a body the check already read. A later
 * call that asks for more than the first read kept gets only what was kept. A `maxBytes` that is
 * not a finite, non-negative number reads nothing. Callers must not read the same response
 * concurrently: the bytes are shared only once the first read has finished. Never fails.
 */
export const readSharedBodyPrefix = (params: {
  /**
   * Response whose body is read once and then cancelled.
   */
  readonly response: HttpClientResponse.HttpClientResponse;

  /**
   * Maximum number of bytes this caller wants from the start of the body.
   */
  readonly maxBytes: number;
}): Effect.Effect<BodyPrefix> =>
  Effect.suspend(() => {
    const { response } = params;
    const maxBytes = normalizePrefixBytes(params.maxBytes);
    const stored = SHARED_PREFIXES.get(response);
    if (stored !== undefined) {
      return Effect.succeed(
        decodePrefix({
          bytes: stored.bytes.subarray(0, maxBytes),
          truncated: stored.truncated || stored.bytes.byteLength > maxBytes,
        }),
      );
    }

    return readPrefixBytes({ response, maxBytes, timeout: SHARED_BODY_PREFIX_TIMEOUT }).pipe(
      Effect.tap((prefix) =>
        Effect.sync(() => {
          SHARED_PREFIXES.set(response, prefix);
        }),
      ),
      Effect.map(decodePrefix),
    );
  });
