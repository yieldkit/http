import { Effect, Option } from 'effect';
import { Cookies, HttpClientResponse } from 'effect/http';

import type { HttpRequestError } from './errors.ts';

import { BODYLESS_STATUSES, cancelResponseBody, hasNoBody, readBoundedBytes } from './body.ts';

/**
 * Headers describing the framing of the original wire body. They do not describe the buffered or
 * re-streamed bytes, so the rebuilt response drops them.
 */
const WIRE_BODY_HEADERS = ['content-length', 'transfer-encoding'];

/**
 * Returns one `Set-Cookie` field per cookie Effect parsed from `response`, or `undefined` when one
 * of them cannot be written back (a non-ASCII `Path`, an out-of-range `Max-Age`), in which case the
 * caller keeps the folded header as the transport gave it.
 */
const setCookieFields = (
  response: HttpClientResponse.HttpClientResponse,
): ReadonlyArray<string> | undefined => {
  try {
    return Cookies.toSetCookieHeaders(response.cookies);
  } catch {
    return undefined;
  }
};

/**
 * Builds a Web `Response` around `body` with the status, status text and headers of `response`,
 * keeping its post-redirect URL (`Response.url` is read-only and empty for constructed responses,
 * so it is defined on the instance) and every `Set-Cookie` field. With `dropWireHeaders` the
 * headers in `WIRE_BODY_HEADERS` are left out, and so is `Content-Encoding` when a Web `Response`
 * backs `response`, because fetch and Bun decode the body while Undici returns it as sent.
 */
export const copyWebResponse = (
  response: HttpClientResponse.HttpClientResponse,
  body: Uint8Array | ReadableStream<Uint8Array> | null,
  dropWireHeaders: boolean,
): Response => {
  const web = HttpClientResponse.toWeb(response);
  const headers = new globalThis.Headers(
    Option.isSome(web) ? web.value.headers : Object.entries(response.headers),
  );
  if (Option.isNone(web)) {
    // The header record folds repeated Set-Cookie fields into one; the parsed cookies keep each.
    const cookies = setCookieFields(response);
    if (cookies !== undefined) {
      headers.delete('set-cookie');
      for (const cookie of cookies) {
        headers.append('set-cookie', cookie);
      }
    }
  }

  if (dropWireHeaders) {
    for (const name of WIRE_BODY_HEADERS) {
      headers.delete(name);
    }

    if (Option.isSome(web)) {
      headers.delete('content-encoding');
    }
  }

  const copy = new Response(body, {
    status: response.status,
    statusText: Option.isSome(web) ? web.value.statusText : '',
    headers,
  });
  if (response.url.length > 0) {
    Object.defineProperty(copy, 'url', { value: response.url });
  }

  return copy;
};

/**
 * Builds a Web `Response` around the buffered `bytes` of `response` (see `copyWebResponse`),
 * without the wire body headers. A 204, 205 or 304 gets a null body, because `new Response(bytes,
 * …)` throws a `TypeError` for those statuses.
 */
export const rebuildWebResponse = (
  response: HttpClientResponse.HttpClientResponse,
  bytes: Uint8Array,
): Response =>
  copyWebResponse(response, BODYLESS_STATUSES.has(response.status) ? null : bytes, true);

/**
 * Reads the body of `response` within `maxBodyBytes` and returns a response backed by the buffered
 * bytes. A response without a body (see `hasNoBody`: a 204, 205 or 304, or the answer to a HEAD
 * request) keeps all its headers (a HEAD answer's `Content-Length` describes the resource) but
 * never a body: any body a transport sent anyway is cancelled, so it cannot escape the cap and the
 * attempt timeout.
 */
export const bufferResponse = (
  response: HttpClientResponse.HttpClientResponse,
  upstream: string,
  maxBodyBytes: number | undefined,
): Effect.Effect<HttpClientResponse.HttpClientResponse, HttpRequestError> => {
  if (hasNoBody(response)) {
    return Effect.as(
      cancelResponseBody(response),
      HttpClientResponse.fromWeb(response.request, copyWebResponse(response, null, false)),
    );
  }

  return Effect.map(readBoundedBytes({ response, upstream, maxBytes: maxBodyBytes }), (bytes) =>
    HttpClientResponse.fromWeb(response.request, rebuildWebResponse(response, bytes)),
  );
};
