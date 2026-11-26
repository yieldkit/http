import { isSensitiveQueryParamName, REDACTED_LOG_VALUE } from '@yieldkit/logger';
import { Option } from 'effect';
import { HttpClientRequest, UrlParams } from 'effect/http';

/**
 * Base used to parse a relative URL such as `/items?token=x`, removed again from the result.
 */
const RELATIVE_URL_BASE = 'http://relative.invalid';

/**
 * Written in place of a URL that cannot be parsed, so unparseable input is never echoed.
 */
export const UNPARSEABLE_URL_LOG_VALUE = '<unparseable url>';

/**
 * Returns the name of one `name=value` pair of a query string as `URLSearchParams` decodes it
 * (`api%5Fkey` gives `api_key`, `a+b` gives `a b`).
 */
const decodedParamName = (pair: string): string =>
  new URLSearchParams(pair).keys().next().value ?? '';

/**
 * Masks the values in a query string (without its `?`) whose decoded name
 * `isSensitiveQueryParamName` matches. Every other pair, and the spelling of the kept ones, is left
 * exactly as written.
 */
const maskQuery = (query: string): string =>
  query
    .split('&')
    .map((pair) => {
      const separator = pair.indexOf('=');
      return separator !== -1 && isSensitiveQueryParamName(decodedParamName(pair))
        ? `${pair.slice(0, separator + 1)}${REDACTED_LOG_VALUE}`
        : pair;
    })
    .join('&');

/**
 * Makes a URL safe to log: userinfo and the fragment are dropped, and the value of every query
 * parameter whose decoded name is a credential name (`isSensitiveQueryParamName`: `key`, `sig`,
 * `code`, `access_token`, `client_secret`, `api%5Fkey`, `X-Amz-Signature`, `creds[token]`) becomes
 * `<redacted>`. Other query values are kept: pass a credential under any other name as a `Redacted`
 * query parameter, or add its name to `UrlParams.CurrentRedactedNames`, for the client span and
 * logs of this package to mask it. A relative URL that starts with `/` keeps its relative form.
 * Input that does not parse as a URL gives `<unparseable url>`.
 */
export const redactUrlForLogs = (rawUrl: string): string => {
  const relative = rawUrl.startsWith('/') && !rawUrl.startsWith('//');
  let parsed: URL;
  try {
    parsed = new URL(rawUrl, relative ? RELATIVE_URL_BASE : undefined);
  } catch {
    return UNPARSEABLE_URL_LOG_VALUE;
  }

  parsed.username = '';
  parsed.password = '';
  parsed.hash = '';
  const { href, search } = parsed;
  const masked =
    search === ''
      ? href
      : `${href.slice(0, href.length - search.length)}?${maskQuery(search.slice(1))}`;
  return relative ? masked.slice(RELATIVE_URL_BASE.length) : masked;
};

/**
 * Query names marked for redaction on the request a redirect hop was built from. A request without
 * carried names has no entry.
 */
export const hopRedactedNames = new WeakMap<
  HttpClientRequest.HttpClientRequest,
  ReadonlyArray<string>
>();

/**
 * Returns `redactedNames` (the fiber's `UrlParams.CurrentRedactedNames`) plus the names of the
 * query parameters whose values were wrapped in `Redacted` on this request or an earlier hop. Names
 * are normalised as `URLSearchParams` reads them. If no names were marked, returns `redactedNames`
 * as given.
 */
export const requestRedactedNames = (
  request: HttpClientRequest.HttpClientRequest,
  redactedNames: ReadonlyArray<string | RegExp>,
): ReadonlyArray<string | RegExp> => {
  const keys = [
    ...request.urlParams.params
      .filter(([, value]) => typeof value !== 'string')
      .map(([key]) => key),
    ...(hopRedactedNames.get(request) ?? []),
  ];
  return keys.length === 0
    ? redactedNames
    : [
        ...redactedNames,
        ...new URLSearchParams(keys.map((key): [string, string] => [key, ''])).keys(),
      ];
};

/**
 * The URL of `request` as the package logs it: query values named in `redactedNames` (the fiber's
 * `UrlParams.CurrentRedactedNames`) or passed as `Redacted` are masked as Effect's client span
 * masks them, then `redactUrlForLogs` removes userinfo and the fragment and masks query values
 * under sensitive key names. A relative URL starting with a single slash stays relative. Returns
 * `undefined` for URLs that cannot be parsed as either form.
 */
export const redactRequestUrlForLogs = (
  request: HttpClientRequest.HttpClientRequest,
  redactedNames: ReadonlyArray<string | RegExp>,
): string | undefined => {
  const relative = request.url.startsWith('/') && !request.url.startsWith('//');
  const url = HttpClientRequest.toUrl(
    relative ? HttpClientRequest.prependUrl(request, RELATIVE_URL_BASE) : request,
  );
  if (Option.isNone(url)) {
    return undefined;
  }

  const names = requestRedactedNames(request, redactedNames);
  const masked = redactUrlForLogs(UrlParams.redactUrl(url.value.href, names));
  return relative ? masked.slice(RELATIVE_URL_BASE.length) : masked;
};
