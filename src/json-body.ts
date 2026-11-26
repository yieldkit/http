import type { HttpClientResponse } from 'effect/http';

import { Effect, Schema } from 'effect';

import { decodeUtf8, readBoundedBytes, readBoundedText } from './body.ts';
import { tagHttpError } from './error-meta.ts';
import { HttpRequestError } from './errors.ts';
import { currentSentValueMask } from './sent-values.ts';
import { bodyExcerpt, issueExcerpt } from './status-error.ts';

/**
 * Parameters for decoding a JSON response body.
 */
type DecodeJsonBodyParams<S extends Schema.Constraint> = {
  /**
   * Response whose body is read and decoded.
   */
  readonly response: HttpClientResponse.HttpClientResponse;

  /**
   * Schema the parsed JSON is decoded with, through its JSON codec.
   */
  readonly schema: S;

  /**
   * Logical upstream identifier for error attribution.
   */
  readonly upstream: string;

  /**
   * Byte cap for the body; defaults to `DEFAULT_MAX_BODY_BYTES`.
   */
  readonly maxBodyBytes?: number | undefined;
};

/**
 * Parses JSON text into an unknown value without the `any` of `JSON.parse`.
 */
const parseJsonText = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));

/**
 * Builds a decode failure. The same request returns the same body, so it is never retried. The
 * message only names the upstream and `reason`; the start of the body goes in `bodySnippet`
 * (`undefined` when empty) and the schema issue, when there is one, in `decodeIssue`, both already
 * cleaned by the caller.
 */
const decodeFailure = (params: {
  readonly response: HttpClientResponse.HttpClientResponse;
  readonly upstream: string;
  readonly reason: string;
  readonly bodySnippet: string;
  readonly decodeIssue?: string | undefined;
}): HttpRequestError =>
  tagHttpError(
    new HttpRequestError({
      upstream: params.upstream,
      status: params.response.status,
      message: `Failed to decode ${params.upstream} response: ${params.reason}`,
      bodySnippet: params.bodySnippet === '' ? undefined : params.bodySnippet,
      decodeIssue: params.decodeIssue,
    }),
    { outcome: 'decode_error', retryable: false },
  );

/**
 * Reads a response body within `maxBodyBytes`, parses it as JSON and decodes it with `schema`. An
 * empty body parses as `null`. Invalid JSON and schema mismatches fail with a non-retryable
 * `HttpRequestError` labelled `decode_error` whose message reads `Failed to decode <upstream>
 * response: invalid JSON` or `…: schema mismatch`, with the first 200 cleaned characters of the
 * body in `bodySnippet` and, for a mismatch, the schema issue in `decodeIssue` (the request's sent
 * credentials masked in both); an oversized body fails with `body_too_large`.
 */
export const decodeJsonBody = <S extends Schema.Constraint>(
  params: DecodeJsonBodyParams<S>,
): Effect.Effect<S['Type'], HttpRequestError, S['DecodingServices']> =>
  Effect.flatMap(
    readBoundedText({
      response: params.response,
      upstream: params.upstream,
      maxBytes: params.maxBodyBytes,
    }),
    (text) => decodeJsonText(params, text),
  );

/**
 * Decodes a JSON body like `decodeJsonBody` and also returns its size in bytes as read, for a
 * caller that stores the value against a byte budget.
 */
export const decodeSizedJsonBody = <S extends Schema.Constraint>(
  params: DecodeJsonBodyParams<S>,
): Effect.Effect<
  { readonly data: S['Type']; readonly bodyBytes: number },
  HttpRequestError,
  S['DecodingServices']
> =>
  Effect.flatMap(
    readBoundedBytes({
      response: params.response,
      upstream: params.upstream,
      maxBytes: params.maxBodyBytes,
    }),
    (bytes) =>
      Effect.map(decodeJsonText(params, decodeUtf8(bytes)), (data) => ({
        data,
        bodyBytes: bytes.byteLength,
      })),
  );

/**
 * Parses and decodes the body `text` of `params.response`; see `decodeJsonBody`.
 */
const decodeJsonText = Effect.fnUntraced(function* <S extends Schema.Constraint>(
  params: DecodeJsonBodyParams<S>,
  text: string,
): Effect.fn.Return<S['Type'], HttpRequestError, S['DecodingServices']> {
  const { response, schema, upstream } = params;
  const mask = yield* currentSentValueMask(response.request);
  const json: unknown =
    text === ''
      ? null
      : yield* parseJsonText(text).pipe(
          Effect.mapError(() =>
            decodeFailure({
              response,
              upstream,
              reason: 'invalid JSON',
              bodySnippet: bodyExcerpt(text, mask),
            }),
          ),
        );
  return yield* Schema.decodeUnknownEffect(Schema.toCodecJson(schema))(json).pipe(
    Effect.mapError((issue) =>
      decodeFailure({
        response,
        upstream,
        reason: 'schema mismatch',
        bodySnippet: bodyExcerpt(text, mask),
        decodeIssue: issueExcerpt(issue.message, mask),
      }),
    ),
  );
});
