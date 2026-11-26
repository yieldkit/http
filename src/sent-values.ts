import { isSensitiveLogKey, isSensitiveQueryParamName, REDACTED_LOG_VALUE } from '@yieldkit/logger';
import { Effect, Option } from 'effect';
import { FetchHttpClient, HttpClientRequest } from 'effect/http';

import { requestRedactedNames } from './url-redaction.ts';

/**
 * Shortest value or part of a value under a credential name that is masked when it is echoed back.
 * Shorter values (`42`, `en`, `Bearer`) are too common in ordinary text to replace without mangling
 * it, and too short to be a credential.
 */
const MIN_MASKED_VALUE_CHARS = 8;

/**
 * Separators between the parts of a header value (`Bearer <token>`, `a=1; session=<id>`), so each
 * part is masked on its own as well as the whole value.
 */
const VALUE_PART_SEPARATOR_RE = /[\s,;]+/;

/**
 * A `name=value` part (a cookie, a `key=value` parameter), capturing the value. A part whose `=`
 * only starts base64 padding (`dGVzdA==`) does not match.
 */
const NAMED_PART_RE = /^[^=]+=([^=].*)$/s;

/**
 * Splits `value` into the candidates to mask: the whole value, each part between separators, the
 * value half of a `name=value` part and a base64 part without its `=` padding. The name half of a
 * `name=value` part is never a candidate: a cookie name such as `session_id` is not a secret, and
 * masking it would rename the matching key in a JSON error body, hiding the value under it from the
 * key-name masking.
 */
const valueParts = (value: string): ReadonlyArray<string> => {
  const parts = [value];
  for (const part of value.split(VALUE_PART_SEPARATOR_RE)) {
    parts.push(part);
    const named = NAMED_PART_RE.exec(part);
    if (named?.[1] === undefined) {
      parts.push(part.replace(/=+$/, ''));
    } else {
      parts.push(named[1]);
    }
  }

  return parts;
};

/**
 * Adds the parts of a value sent under a credential name to `values`: the whole value and each part
 * of at least `MIN_MASKED_VALUE_CHARS` characters (see `valueParts`: the token of `Bearer <token>`,
 * the value of `session=<id>` in a cookie), in raw form and, when it differs, in the form
 * `JSON.stringify` writes inside a string (`"` and `\` escaped), so a value echoed in a JSON body
 * that is decoded and written back is masked too.
 */
const addMaskableParts = (values: Set<string>, value: string): void => {
  for (const part of valueParts(value)) {
    if (part.length >= MIN_MASKED_VALUE_CHARS) {
      values.add(part);
      values.add(JSON.stringify(part).slice(1, -1));
    }
  }
};

/**
 * Collects the values `request` sent under a credential name, which an upstream might echo back in
 * an error body: headers (on the request and in `ambientHeaders`, the headers of an ambient
 * `FetchHttpClient.RequestInit` that fetch adds) whose name `isSensitiveLogKey` matches
 * (`Authorization`, `Cookie`, `X-Api-Key`), query parameters whose name `isSensitiveQueryParamName`
 * matches (`access_token`, `key`, `sig`, `code`), plus every query parameter the caller wrapped in
 * `Redacted`. Values under any other name are never collected, whatever they look like: send a
 * credential under such a name as a `Redacted` query parameter to have it masked. The result is
 * ordered longest first, so a whole value is masked before its parts.
 */
const sentValues = (
  request: HttpClientRequest.HttpClientRequest,
  ambientHeaders: RequestInit['headers'],
): ReadonlyArray<string> => {
  const values = new Set<string>();
  for (const [name, value] of Object.entries(request.headers)) {
    if (isSensitiveLogKey(name)) {
      addMaskableParts(values, value);
    }
  }

  if (ambientHeaders !== undefined) {
    for (const [name, value] of new globalThis.Headers(ambientHeaders)) {
      if (isSensitiveLogKey(name)) {
        addMaskableParts(values, value);
      }
    }
  }

  const redactedParams = new Set(requestRedactedNames(request, []));
  const url = HttpClientRequest.toUrl(request);
  if (Option.isSome(url)) {
    for (const [name, value] of url.value.searchParams) {
      if (redactedParams.has(name) || isSensitiveQueryParamName(name)) {
        addMaskableParts(values, value);
      }
    }
  }

  return [...values].toSorted((left, right) => right.length - left.length);
};

/**
 * Replaces every credential a request sent with `<redacted>` in a text. Built by
 * `makeSentValueMask`, which collects the values on first use only, so building one on a path that
 * usually succeeds costs nothing, and once however many texts are masked.
 */
export type SentValueMask = (text: string) => string;

/**
 * Builds the `SentValueMask` of `request`: it replaces every occurrence in a text of a credential
 * the request sent (see `sentValues`), including headers from an ambient
 * `FetchHttpClient.RequestInit` passed as `ambientHeaders`, with `<redacted>`. This masks an
 * upstream that echoes the caller's API key in prose ("Incorrect API key provided: sk-live-…"),
 * which no field-name rule can recognise. Matching is exact: a value the upstream re-encoded in
 * some other way (percent-encoded, or `\u` escapes in a JSON body cut off by the read limit, which
 * is never decoded) is not found.
 */
export const makeSentValueMask = (
  request: HttpClientRequest.HttpClientRequest,
  ambientHeaders?: RequestInit['headers'],
): SentValueMask => {
  let values: ReadonlyArray<string> | undefined;
  return (text) => {
    values ??= sentValues(request, ambientHeaders);
    return values.reduce((masked, value) => masked.replaceAll(value, REDACTED_LOG_VALUE), text);
  };
};

/**
 * Builds the `SentValueMask` of `request` with the headers of the ambient
 * `FetchHttpClient.RequestInit`, if any, which fetch sent along with the request's own.
 */
export const currentSentValueMask = (
  request: HttpClientRequest.HttpClientRequest,
): Effect.Effect<SentValueMask> =>
  Effect.map(Effect.serviceOption(FetchHttpClient.RequestInit), (init) =>
    makeSentValueMask(request, Option.isSome(init) ? init.value.headers : undefined),
  );

/**
 * Replaces every occurrence in `text` of a credential `request` sent (see `makeSentValueMask`) with
 * `<redacted>`. Use `makeSentValueMask` to mask several texts of one request.
 */
export const maskSentValues = (
  text: string,
  request: HttpClientRequest.HttpClientRequest,
  ambientHeaders?: RequestInit['headers'],
): string => makeSentValueMask(request, ambientHeaders)(text);
