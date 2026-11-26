import { Effect, Schema } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import {
  exitErrorMeta,
  exitMessage,
  exitOutcome,
  ItemPayloadSchema,
  makeClientResponse,
  makeTrackedStream,
  squashExitFailure,
} from './http.fixture.ts';
import { decodeJsonBody } from './json-body.ts';

const UPSTREAM = 'test:json';

/**
 * Decodes `response` as an `ItemPayloadSchema` body and returns the exit.
 */
const decodeItem = (response: Response, maxBodyBytes?: number) =>
  Effect.runPromiseExit(
    decodeJsonBody({
      response: makeClientResponse(response),
      schema: ItemPayloadSchema,
      upstream: UPSTREAM,
      maxBodyBytes,
    }),
  );

describe('decodeJsonBody', () => {
  it('decodes a valid body with the schema', async () => {
    const value = await Effect.runPromise(
      decodeJsonBody({
        response: makeClientResponse(new Response('{"id":7,"title":"Seven"}')),
        schema: ItemPayloadSchema,
        upstream: UPSTREAM,
      }),
    );
    expect(value).toEqual({ id: 7, title: 'Seven' });
  });

  it('decodes through the JSON codec of the schema', async () => {
    const value = await Effect.runPromise(
      decodeJsonBody({
        response: makeClientResponse(new Response('{"at":"2026-01-15T12:00:00.000Z"}')),
        schema: Schema.Struct({ at: Schema.Date }),
        upstream: UPSTREAM,
      }),
    );
    expect(value.at).toEqual(new Date('2026-01-15T12:00:00.000Z'));
  });

  it('parses an empty body as null', async () => {
    const value = await Effect.runPromise(
      decodeJsonBody({
        response: makeClientResponse(new Response('')),
        schema: Schema.NullOr(ItemPayloadSchema),
        upstream: UPSTREAM,
      }),
    );
    expect(value).toBeNull();
  });

  it('parses a 204 without a body as null', async () => {
    const value = await Effect.runPromise(
      decodeJsonBody({
        response: makeClientResponse(new Response(null, { status: 204 })),
        schema: Schema.Null,
        upstream: UPSTREAM,
      }),
    );
    expect(value).toBeNull();
  });

  it('fails a 204 as a decode error when the schema does not accept null', async () => {
    const exit = await decodeItem(new Response(null, { status: 204 }));
    expect(exitOutcome(exit)).toBe('decode_error');
    expect(exitErrorMeta(exit)).toMatchObject({ outcome: 'decode_error', retryable: false });
    expect(squashExitFailure(exit)).toMatchObject({ status: 204 });
  });

  it('masks every element of a credential array in the decode snippet', async () => {
    const exit = await decodeItem(
      new Response('{"id":"seven","access_token":["aaa-secret","bbb-secret"]}'),
    );
    expect(squashExitFailure(exit)).toMatchObject({
      bodySnippet: '{"id":"seven","access_token":"<redacted>"}',
    });
  });

  it('fails an empty body when the schema does not accept null, without a snippet', async () => {
    const exit = await decodeItem(new Response(''));
    expect(exitOutcome(exit)).toBe('decode_error');
    expect(exitMessage(exit)).toBe(`Failed to decode ${UPSTREAM} response: schema mismatch`);
    const error = squashExitFailure(exit);
    expect(error?.bodySnippet).toBeUndefined();
    expect(error).toMatchObject({ decodeIssue: expect.any(String) });
  });

  it('fails invalid JSON with a short message and the cleaned body in bodySnippet, never retryable', async () => {
    const exit = await decodeItem(new Response('{not json\n\n  more detail'));
    expect(squashExitFailure(exit)).toMatchObject({
      _tag: 'HttpRequestError',
      upstream: UPSTREAM,
      status: 200,
      message: `Failed to decode ${UPSTREAM} response: invalid JSON`,
      bodySnippet: '{not json more detail',
    });
    expect(exitOutcome(exit)).toBe('decode_error');
    expect(exitErrorMeta(exit)).toMatchObject({
      outcome: 'decode_error',
      retryable: false,
    });
  });

  it('fails a schema mismatch with the issue and the start of the body in their own fields', async () => {
    const exit = await decodeItem(new Response(JSON.stringify({ id: 'seven', title: 'Wrong' })));
    expect(exitOutcome(exit)).toBe('decode_error');
    expect(squashExitFailure(exit)).toMatchObject({
      _tag: 'HttpRequestError',
      status: 200,
      message: `Failed to decode ${UPSTREAM} response: schema mismatch`,
      bodySnippet: '{"id":"seven","title":"Wrong"}',
      decodeIssue: expect.stringContaining('["id"]'),
    });
  });

  it('masks credential fields in the decode snippet', async () => {
    const exit = await decodeItem(
      new Response(JSON.stringify({ access_token: 'x-secret-1', id: 'seven', title: 'T' })),
    );
    expect(squashExitFailure(exit)).toMatchObject({
      bodySnippet: '{"access_token":"<redacted>","id":"seven","title":"T"}',
    });
  });

  it('quotes no more than the first 200 characters of the body', async () => {
    const exit = await decodeItem(new Response(`{"id":"x","title":"${'t'.repeat(1000)}"}`));
    const error = squashExitFailure(exit);
    const snippet = error?._tag === 'HttpRequestError' ? (error.bodySnippet ?? '') : '';
    expect(snippet).toHaveLength(200);
    expect(exitMessage(exit)).not.toContain('ttt');
  });

  it('reports the status of the response that failed to decode', async () => {
    const exit = await decodeItem(new Response('nope', { status: 201 }));
    expect(squashExitFailure(exit)).toMatchObject({ status: 201 });
  });

  it('refuses a JSON body whose Content-Length is over the cap without pulling it', async () => {
    const { stream, tracker } = makeTrackedStream({ totalBytes: 4096, chunkBytes: 256 });
    const exit = await decodeItem(
      new Response(stream, {
        headers: { 'content-type': 'application/json', 'content-length': '4096' },
      }),
      1024,
    );
    expect(exitOutcome(exit)).toBe('body_too_large');
    expect(tracker.pulledBytes).toBe(0);
    expect(tracker.cancelled).toBe(true);
  });

  it('fails a chunked JSON body over the cap and cancels the stream', async () => {
    const { stream, tracker } = makeTrackedStream({
      totalBytes: 1024 * 1024,
      chunkBytes: 1024,
      prefix: '{"id":1,"title":"',
    });
    const exit = await decodeItem(new Response(stream), 4096);
    expect(exitOutcome(exit)).toBe('body_too_large');
    expect(tracker.pulledBytes).toBeLessThanOrEqual(5 * 1024);
    expect(tracker.cancelled).toBe(true);
  });
});
