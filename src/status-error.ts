import type { HttpClientResponse } from 'effect/http';

import { isSensitiveLogKey, REDACTED_LOG_VALUE } from '@yieldkit/logger';
import { Clock, Effect, Option, Schema } from 'effect';
import { Headers } from 'effect/http';

import type { SentValueMask } from './sent-values.ts';

import { cancelResponseBody, readSharedBodyPrefix } from './body.ts';
import { tagHttpError } from './error-meta.ts';
import { HttpRequestError } from './errors.ts';
import { parseRetryAfterSeconds } from './headers.ts';
import { classifyErrorOutcome } from './metrics.ts';
import { isTransientHttpStatus } from './retry.ts';
import { currentSentValueMask } from './sent-values.ts';

/**
 * Bytes read from a non-2xx body to build the error's body snippet.
 */
const ERROR_BODY_SNIFF_BYTES = 4 * 1024;

/**
 * Maximum characters of an error-body snippet before it is cut and marked with `…`.
 */
const ERROR_SNIPPET_MAX_CHARS = 256;

/**
 * Maximum characters of the body excerpt quoted in a decode failure.
 */
const DECODE_EXCERPT_MAX_CHARS = 200;

/**
 * Maximum characters of a schema issue quoted in a decode failure. Issues can echo body values.
 */
const DECODE_ISSUE_MAX_CHARS = 500;

/**
 * Characters taken from a body that is not complete JSON before cleaning an excerpt, so cleaning a
 * huge body stays cheap. The cut happens after the sent-value mask ran on the whole text, so it
 * never leaves part of a sent credential behind.
 */
const EXCERPT_SOURCE_MAX_CHARS = 2000;

/**
 * Deepest nesting walked when masking a JSON body by key name. Anything deeper is written as
 * `<redacted>`, so a hostile, deeply nested body cannot overflow the stack.
 */
const MAX_JSON_MASK_DEPTH = 32;

/**
 * Response headers holding an upstream request id, in order of preference.
 */
const REQUEST_ID_HEADERS = ['x-request-id', 'x-amzn-requestid', 'cf-ray'];

/**
 * Shape a request id must have to be quoted in an error message.
 */
const REQUEST_ID_RE = /^[A-Za-z0-9_.:=-]{1,128}$/;

/**
 * ASCII and C1 control characters, replaced by spaces in snippets.
 */
const CONTROL_CHARS_RE = /\p{Cc}/gu;

/**
 * Runs of whitespace, collapsed to one space in snippets.
 */
const WHITESPACE_RUN_RE = /\s+/g;

/**
 * Content types of an HTML or XHTML document.
 */
const HTML_CONTENT_TYPE_RE = /text\/html|application\/xhtml\+xml/i;

/**
 * Start of a body that is an HTML or XHTML document whatever its content type says.
 */
const HTML_BODY_START_RE = /^\s*(?:<\?xml[^>]*>\s*)?(?:<!doctype html|<html[\s>])/i;

/**
 * Text of the first HTML `<title>` element.
 */
const HTML_TITLE_RE = /<title[^>]*>([^<]*)<\/title>/i;

/**
 * Decodes body text as a JSON document. Returns `None` for anything `Schema.Json` rejects,
 * including truncated JSON and documents with non-finite numbers such as `1e999`.
 */
const parseJsonBody = Schema.decodeUnknownOption(Schema.fromJsonString(Schema.Json));

/**
 * Returns a copy of a parsed JSON value in which the value under every object key that
 * `isSensitiveLogKey` matches (`access_token`, `password`, `apiKey`), unless it is `null`, is
 * `<redacted>`, whole arrays and objects included. Other values are kept as they are; values nested
 * deeper than `MAX_JSON_MASK_DEPTH` become `<redacted>`.
 */
const maskJsonByKeyName = (value: Schema.Json, depth = 0): Schema.Json => {
  if (typeof value !== 'object' || value === null) {
    return value;
  }

  if (depth >= MAX_JSON_MASK_DEPTH) {
    return REDACTED_LOG_VALUE;
  }

  if (Array.isArray(value)) {
    return value.map((item: Schema.Json) => maskJsonByKeyName(item, depth + 1));
  }

  return Object.fromEntries(
    Object.entries(value).map(([key, inner]) => [
      key,
      inner !== null && isSensitiveLogKey(key)
        ? REDACTED_LOG_VALUE
        : maskJsonByKeyName(inner, depth + 1),
    ]),
  );
};

/**
 * Parses `text` as a complete JSON object or array, masks it by key name (see `maskJsonByKeyName`)
 * and writes it back compactly. Returns `None` for anything else, including JSON cut off by the
 * read limit.
 */
const maskJsonBody = (text: string): Option.Option<string> => {
  const trimmed = text.trim();
  if (!(trimmed.startsWith('{') || trimmed.startsWith('['))) {
    return Option.none();
  }

  return Option.map(parseJsonBody(trimmed), (value) => JSON.stringify(maskJsonByKeyName(value)));
};

/**
 * `SentValueMask` that changes nothing, for a snippet built without a request.
 */
const keepText: SentValueMask = (text) => text;

/**
 * Masks untrusted body text. A complete JSON body is masked by key name first (see `maskJsonBody`),
 * and `mask` then runs on the text written back, so a sent value the body echoed in an escaped JSON
 * form (`\u002B`, `\/`) is masked once decoded. Any other body is masked with `mask` only, then cut
 * at `EXCERPT_SOURCE_MAX_CHARS`; nothing else in it is masked.
 */
const maskBodyText = (text: string, mask: SentValueMask): string =>
  Option.match(maskJsonBody(text), {
    onSome: mask,
    onNone: () => mask(text).slice(0, EXCERPT_SOURCE_MAX_CHARS),
  });

/**
 * Replaces control characters with spaces and collapses runs of whitespace, so quoted upstream text
 * stays on one line.
 */
const collapseWhitespace = (text: string): string =>
  text.replaceAll(CONTROL_CHARS_RE, ' ').replaceAll(WHITESPACE_RUN_RE, ' ').trim();

/**
 * Cleans untrusted body text for an error field: masked with `maskBodyText` (the credentials the
 * request sent with `mask`), then put on one line with `collapseWhitespace`. The result is not cut
 * to any length, so a caller that cuts it never leaves part of a sent value behind.
 */
const sanitizeBodyText = (text: string, mask: SentValueMask): string =>
  collapseWhitespace(maskBodyText(text, mask));

/**
 * Returns the first 200 characters of a response body cleaned with `sanitizeBodyText` (the
 * request's sent credentials masked by `mask`, when given, before the cut), for the `bodySnippet`
 * of a decode failure.
 */
export const bodyExcerpt = (text: string, mask: SentValueMask = keepText): string =>
  sanitizeBodyText(text, mask).slice(0, DECODE_EXCERPT_MAX_CHARS);

/**
 * Returns a schema issue message with the request's sent credentials masked by `mask`, when given,
 * put on one line and cut at 500 characters, for the `decodeIssue` of a decode failure. Issues
 * quote the values that failed to decode, so the result is as untrusted as the body.
 */
export const issueExcerpt = (message: string, mask: SentValueMask = keepText): string =>
  collapseWhitespace(mask(message)).slice(0, DECODE_ISSUE_MAX_CHARS);

/**
 * Builds the `bodySnippet` of a non-2xx error: the body cleaned with `sanitizeBodyText` (the
 * credentials the request sent masked by `mask`, when given), cut at 256 characters with `…`
 * appended when cut or when the body was longer than what was read. An HTML or XHTML body (by
 * content type, or by a leading doctype or `<html>` tag whatever the content type) gives only its
 * `<title>` text, or no snippet when it has none. Returns `null` for an empty result.
 */
export const errorBodySnippet = (params: {
  readonly contentType: string | null;
  readonly text: string;
  readonly truncated: boolean;
  readonly mask?: SentValueMask | undefined;
}): string | null => {
  const mask = params.mask ?? keepText;
  const isHtml =
    (params.contentType !== null && HTML_CONTENT_TYPE_RE.test(params.contentType)) ||
    HTML_BODY_START_RE.test(params.text);
  if (isHtml) {
    const title = HTML_TITLE_RE.exec(params.text)?.[1];
    const cleanTitle = title === undefined ? '' : sanitizeBodyText(title, mask);
    return cleanTitle.length > 0 ? cleanTitle.slice(0, ERROR_SNIPPET_MAX_CHARS) : null;
  }

  const clean = sanitizeBodyText(params.text, mask);
  if (clean.length === 0) {
    return null;
  }

  if (clean.length > ERROR_SNIPPET_MAX_CHARS) {
    return `${clean.slice(0, ERROR_SNIPPET_MAX_CHARS)}…`;
  }

  return params.truncated ? `${clean}…` : clean;
};

/**
 * Returns the upstream request id from `x-request-id`, `x-amzn-requestid` or `cf-ray`, or `null`
 * when none is present, the first one present has an unexpected shape, or it holds a credential the
 * request sent (`mask` changes it), as when an upstream copies the API key into `x-request-id`.
 */
export const extractRequestId = (
  headers: Headers.Headers,
  mask: SentValueMask = keepText,
): string | null => {
  for (const name of REQUEST_ID_HEADERS) {
    const value = Headers.get(headers, name);
    if (Option.isSome(value)) {
      const trimmed = value.value.trim();
      return REQUEST_ID_RE.test(trimmed) && mask(trimmed) === trimmed ? trimmed : null;
    }
  }

  return null;
};

/**
 * Formats the message of a non-2xx error: `<upstream> responded with HTTP <status>`, followed by `
 * (request id <id>)` when the request id is known. The body is never quoted in the message; it goes
 * in the error's `bodySnippet`.
 */
export const formatStatusMessage = (params: {
  readonly upstream: string;
  readonly status: number;
  readonly requestId: string | null;
}): string => {
  const requestIdPart = params.requestId !== null ? ` (request id ${params.requestId})` : '';
  return `${params.upstream} responded with HTTP ${String(params.status)}${requestIdPart}`;
};

/**
 * Reads the snippet of a non-2xx body through `readSharedBodyPrefix`, under its 2 second budget, so
 * a 403 body the edge-block check already read still gives its snippet. A stalled, broken or
 * already consumed body gives what was read, or `null` when that is nothing. Credentials the
 * request sent are masked by `mask` wherever the body echoes them (see `sanitizeBodyText`).
 */
const readErrorSnippet = (
  response: HttpClientResponse.HttpClientResponse,
  mask: SentValueMask,
): Effect.Effect<string | null> =>
  Effect.map(readSharedBodyPrefix({ response, maxBytes: ERROR_BODY_SNIFF_BYTES }), (prefix) =>
    errorBodySnippet({
      contentType: Option.getOrNull(Headers.get(response.headers, 'content-type')),
      text: prefix.text,
      truncated: prefix.truncated,
      mask,
    }),
  );

/**
 * Returns the `Retry-After` wait in whole seconds of a response whose status is retried (a 503, a
 * 408), or `undefined` for any other status or when the header is missing or invalid.
 */
const readTransientRetryAfter = (
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<number | undefined> => {
  if (!isTransientHttpStatus(response.status, true)) {
    return Effect.undefined;
  }

  return Effect.map(
    Clock.currentTimeMillis,
    (nowMs) => parseRetryAfterSeconds(response.headers, nowMs) ?? undefined,
  );
};

/**
 * Builds the `HttpRequestError` of a non-2xx `response` with the given body `snippet` as its
 * `bodySnippet` (`undefined` when `null`): the message carries the upstream request id (unless it
 * holds a credential the request sent, see `extractRequestId`), the metric outcome comes from the
 * status alone, and a retried status (a 503, a 408) with a `Retry-After` header is tagged with its
 * wait, which the retry schedule honours like a rate limit's.
 */
const buildStatusError = Effect.fnUntraced(function* (
  response: HttpClientResponse.HttpClientResponse,
  upstream: string,
  snippet: string | null,
  mask: SentValueMask,
): Effect.fn.Return<HttpRequestError> {
  const { status } = response;
  const retryAfterSeconds = yield* readTransientRetryAfter(response);
  const error = new HttpRequestError({
    upstream,
    status,
    message: formatStatusMessage({
      upstream,
      status,
      requestId: extractRequestId(response.headers, mask),
    }),
    bodySnippet: snippet ?? undefined,
  });
  return tagHttpError(error, { outcome: classifyErrorOutcome(error), retryAfterSeconds });
});

/**
 * Fails with the status error of a pending response without reading its body, and cancels the body.
 * A deadline that expires while reading an error body preserves its status. With no pending
 * response, fails with the error returned by `fallback`.
 */
export const statusErrorWithoutBody = (
  pending: Option.Option<HttpClientResponse.HttpClientResponse>,
  upstream: string,
  fallback: () => HttpRequestError,
): Effect.Effect<never, HttpRequestError> => {
  if (Option.isNone(pending)) {
    return Effect.fail(fallback());
  }

  const response = pending.value;
  const statusError = Effect.andThen(
    cancelResponseBody(response),
    Effect.flatMap(currentSentValueMask(response.request), (mask) =>
      buildStatusError(response, upstream, null, mask),
    ),
  );
  return Effect.flatMap(statusError, Effect.fail);
};

/**
 * Succeeds for a 2xx response. Otherwise fails with `HttpRequestError` whose message carries the
 * upstream request id and whose `bodySnippet` holds a bounded, cleaned body snippet (see
 * `buildStatusError`).
 */
export const ensureSuccessStatus = (
  response: HttpClientResponse.HttpClientResponse,
  upstream: string,
): Effect.Effect<void, HttpRequestError> => {
  const { status } = response;
  if (status >= 200 && status < 300) {
    return Effect.void;
  }

  return Effect.gen(function* () {
    const mask = yield* currentSentValueMask(response.request);
    const snippet = yield* readErrorSnippet(response, mask);
    const error = yield* buildStatusError(response, upstream, snippet, mask);
    return yield* error;
  });
};
