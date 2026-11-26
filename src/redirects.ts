import type { HttpClientResponse } from 'effect/http';

import { Option } from 'effect';
import { Headers, HttpBody, HttpClientRequest } from 'effect/http';

import { hopRedactedNames, requestRedactedNames } from './url-redaction.ts';

/**
 * HTTP statuses whose `Location` header is followed as a redirect.
 */
const REDIRECT_STATUSES: ReadonlySet<number> = new Set([301, 302, 303, 307, 308]);

/**
 * Redirect status after which any method other than GET or HEAD is replayed as a bodyless GET.
 */
const SEE_OTHER_STATUS = 303;

/**
 * Redirect statuses after which a POST, and only a POST, is replayed as a bodyless GET, as the
 * Fetch standard does. Other methods keep their method and body.
 */
const POST_TO_GET_STATUSES: ReadonlySet<number> = new Set([301, 302]);

/**
 * Methods that keep their method and (empty) body across every redirect status.
 */
const BODYLESS_METHODS: ReadonlySet<string> = new Set(['GET', 'HEAD']);

/**
 * Request headers that describe a body and are dropped with it when a redirect switches to GET.
 */
const BODY_HEADERS: ReadonlySet<string> = new Set([
  'content-encoding',
  'content-language',
  'content-length',
  'content-location',
  'content-type',
]);

/**
 * The only request headers kept when a redirect crosses origins, so credentials such as
 * `authorization`, `cookie`, `proxy-authorization` and custom API-key headers never reach another
 * origin.
 */
const CROSS_ORIGIN_SAFE_HEADERS: ReadonlySet<string> = new Set([
  'accept',
  'accept-encoding',
  'accept-language',
  'user-agent',
]);

/**
 * Returns the raw `Location` of a 301, 302, 303, 307 or 308 response, or null for any other
 * response, including a redirect status without a `Location` header.
 */
export const redirectLocation = (
  response: HttpClientResponse.HttpClientResponse,
): string | null => {
  if (!REDIRECT_STATUSES.has(response.status)) {
    return null;
  }

  return Option.getOrNull(Headers.get(response.headers, 'location'));
};

/**
 * Returns `true` when the redirect hop is sent as a bodyless GET: a 303 for any method but GET and
 * HEAD, and a 301 or 302 for a POST (the Fetch standard's rule). Every other hop keeps the method
 * and the body.
 */
const switchesToGet = (method: string, status: number): boolean => {
  if (BODYLESS_METHODS.has(method)) {
    return false;
  }

  return status === SEE_OTHER_STATUS || (POST_TO_GET_STATUSES.has(status) && method === 'POST');
};

/**
 * Returns `true` when the hop would send the request body again: the hop keeps the method
 * (`switchesToGet` is false) and the body is not empty, whatever the method, so a GET that carries
 * a body counts too.
 */
const replaysBody = (request: HttpClientRequest.HttpClientRequest, status: number): boolean =>
  !switchesToGet(request.method, status) && !isEmptyBody(request.body);

/**
 * Returns `true` when following the redirect would forward a non-empty request body to another
 * origin: a hop that keeps the body (a 307 or 308, a 301 or 302 of a method other than POST, or any
 * redirect of a GET or HEAD), across origins, with a body that is not empty. Replaying a body there
 * would hand it to a party the caller never addressed, so the caller refuses the hop. A bodyless
 * request, a hop that switches to GET and a same-origin hop are never blocked.
 */
export const blocksBodyReplay = (params: {
  readonly request: HttpClientRequest.HttpClientRequest;
  readonly status: number;
  readonly from: URL;
  readonly to: URL;
}): boolean => {
  const { request, status, from, to } = params;
  return replaysBody(request, status) && isCrossOrigin(from, to);
};

/**
 * Returns `true` when following the redirect would have to send a one-shot body (see
 * `isOneShotBody`) again. The first hop consumed it, so the replay would send nothing; the caller
 * refuses the hop instead.
 */
export const replaysStreamBody = (
  request: HttpClientRequest.HttpClientRequest,
  status: number,
): boolean => isOneShotBody(request.body) && replaysBody(request, status);

/**
 * Returns `true` for a body that can be read only once: an `HttpBody.stream`, or an `HttpBody.raw`
 * around a Web `ReadableStream`, an async iterable such as a Node `Readable`, or a synchronous
 * iterator such as a generator. Other body shapes return `false`.
 */
export const isOneShotBody = (body: HttpBody.HttpBody): boolean => {
  if (body._tag === 'Stream') {
    return true;
  }

  if (body._tag !== 'Raw' || typeof body.body !== 'object' || body.body === null) {
    return false;
  }

  const raw = body.body;
  return (
    raw instanceof ReadableStream ||
    Symbol.asyncIterator in raw ||
    ('next' in raw && typeof raw.next === 'function')
  );
};

/**
 * Returns `true` for a body that sends no bytes: the empty body or a zero-length byte array.
 */
const isEmptyBody = (body: HttpBody.HttpBody): boolean =>
  body._tag === 'Empty' || (body._tag === 'Uint8Array' && body.body.byteLength === 0);

/**
 * Builds the request for the next redirect hop to `to`. The URL comes from `to` alone and is used
 * as written, so a query such as `?q=a%20b&flag&x=~` reaches the next hop byte for byte (a signed
 * URL stays valid); the fragment is dropped, as fetch does. A hop that `switchesToGet` becomes a
 * GET with an empty body and no body headers, and a hop to another origin keeps only the
 * cross-origin safelisted headers. Any other hop keeps the method and body; a caller must refuse a
 * hop that `blocksBodyReplay` or `replaysStreamBody` reports before calling this. Query names
 * marked for redaction carry over without changing the redirect URL.
 */
export const redirectRequest = (params: {
  readonly request: HttpClientRequest.HttpClientRequest;
  readonly status: number;
  readonly from: URL;
  readonly to: URL;
}): HttpClientRequest.HttpClientRequest => {
  const { request, status, from, to } = params;
  let next = HttpClientRequest.setUrl(withoutQueryParams(request, to), hrefWithoutHash(to));
  if (switchesToGet(request.method, status)) {
    next = HttpClientRequest.setBody(HttpClientRequest.setMethod(next, 'GET'), HttpBody.empty);
    next = removeHeadersWhere(next, (name) => BODY_HEADERS.has(name));
  }

  if (isCrossOrigin(from, to)) {
    next = removeHeadersWhere(next, (name) => !CROSS_ORIGIN_SAFE_HEADERS.has(name));
  }

  // The filter narrows the union type to the strings allowed in hopRedactedNames.
  const carried = requestRedactedNames(request, []).filter((name) => typeof name === 'string');
  if (carried.length > 0) {
    hopRedactedNames.set(next, carried);
  }

  return next;
};

/**
 * Returns `true` when a hop from `from` to `to` changes scheme, host or port.
 */
export const isCrossOrigin = (from: URL, to: URL): boolean => from.origin !== to.origin;

/**
 * Keeps only the cross-origin safelisted entries of fetch default headers, such as the headers of
 * an ambient `FetchHttpClient.RequestInit`, so they cannot reintroduce credentials after a
 * cross-origin hop.
 */
export const safelistedHeaderInit = (headers: RequestInit['headers']): RequestInit['headers'] => {
  if (headers === undefined) {
    return undefined;
  }

  const kept = new globalThis.Headers();
  new globalThis.Headers(headers).forEach((value, name) => {
    if (CROSS_ORIGIN_SAFE_HEADERS.has(name)) {
      kept.set(name, value);
    }
  });
  return kept;
};

/**
 * Points `request` at `to` without its query or fragment, and clears the query parameters and
 * fragment the request kept apart from its URL string (setting a `URL` object replaces both with
 * the URL's own, and this one has none). The caller then sets the full URL string, so the raw query
 * is sent as written instead of being rebuilt from parsed parameters.
 */
const withoutQueryParams = (
  request: HttpClientRequest.HttpClientRequest,
  to: URL,
): HttpClientRequest.HttpClientRequest => {
  const bare = new URL(to.href);
  bare.search = '';
  bare.hash = '';
  return HttpClientRequest.setUrl(request, bare);
};

/**
 * Returns `url` as written, without its fragment.
 */
const hrefWithoutHash = (url: URL): string => {
  const copy = new URL(url.href);
  copy.hash = '';
  return copy.href;
};

/**
 * Removes every request header whose lower-case name matches `predicate`.
 */
const removeHeadersWhere = (
  request: HttpClientRequest.HttpClientRequest,
  predicate: (name: string) => boolean,
): HttpClientRequest.HttpClientRequest =>
  Object.keys(request.headers)
    .filter((name) => predicate(name.toLowerCase()))
    .reduce((current, name) => HttpClientRequest.removeHeader(current, name), request);
