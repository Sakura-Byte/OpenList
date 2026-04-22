import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

if (!globalThis.btoa) {
  globalThis.btoa = (input) => Buffer.from(input, 'binary').toString('base64');
}
if (!globalThis.atob) {
  globalThis.atob = (input) => Buffer.from(input, 'base64').toString('binary');
}

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const workerPath = resolve(__dirname, '../worker.js');
const workerSource = readFileSync(workerPath, 'utf8');
const tokenMatch = workerSource.match(/const TOKEN = "([^"]+)";/);
if (!tokenMatch) {
  throw new Error('failed to extract TOKEN from worker.js');
}
const TOKEN = tokenMatch[1];

const { default: worker } = await import(new URL('../worker.js', import.meta.url));

const REPORT_ENDPOINT = '/api/admin/setting/ratelimit/report';
const FAIRQUEUE_ACQUIRE_ENDPOINT = '/api/admin/fairqueue/acquire';
const FAIRQUEUE_POLL_ENDPOINT = '/api/admin/fairqueue/poll';
const FAIRQUEUE_ABANDON_ENDPOINT = '/api/admin/fairqueue/abandon';
const FAIRQUEUE_ACTIVATE_ENDPOINT = '/api/admin/fairqueue/activate';
const FAIRQUEUE_RELEASE_ENDPOINT = '/api/admin/fairqueue/release';
const EXPECTED_FAIRQUEUE_ENDPOINTS = new Set([
  FAIRQUEUE_ACQUIRE_ENDPOINT,
  FAIRQUEUE_POLL_ENDPOINT,
  FAIRQUEUE_ABANDON_ENDPOINT,
  FAIRQUEUE_ACTIVATE_ENDPOINT,
  FAIRQUEUE_RELEASE_ENDPOINT,
]);


const textEncoder = new TextEncoder();

async function hmacSha256Base64Url(message, secret) {
  const key = await crypto.subtle.importKey(
    'raw',
    textEncoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const sigBuf = await crypto.subtle.sign(
    { name: 'HMAC', hash: 'SHA-256' },
    key,
    textEncoder.encode(message),
  );
  return Buffer.from(sigBuf)
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

function base64UrlEncodeJson(value) {
  return Buffer.from(JSON.stringify(value), 'utf8')
    .toString('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=+$/g, '');
}

async function makeSignedRequest(pathname, controller) {
  const payloadB64 = base64UrlEncodeJson({
    p: pathname,
    e: Math.floor(Date.now() / 1000) + 3600,
  });
  const sig = await hmacSha256Base64Url(payloadB64, `${TOKEN}-download`);
  const url = `https://worker.example${encodeURI(pathname)}?sign=v2.${payloadB64}.${sig}`;
  return new Request(url, {
    method: 'GET',
    headers: {
      Origin: 'https://unit.test',
      'CF-Connecting-IP': '203.0.113.10',
    },
    signal: controller.signal,
  });
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json;charset=UTF-8',
    },
  });
}

function abortError() {
  return new DOMException('The operation was aborted.', 'AbortError');
}

function createCtx() {
  const waitUntilPromises = [];
  return {
    waitUntilPromises,
    waitUntil(promise) {
      waitUntilPromises.push(Promise.resolve(promise));
    },
  };
}

async function flushWaitUntil(ctx) {
  await Promise.all(ctx.waitUntilPromises);
}

async function parseJsonBody(input, init) {
  if (init?.body) {
    return JSON.parse(init.body);
  }
  if (input instanceof Request) {
    const text = await input.text();
    return text ? JSON.parse(text) : null;
  }
  return null;
}

function assertFairQueueCallsStayWithin(calls, allowedEndpoints) {
  const unexpectedFairQueueCalls = calls.filter((call) =>
    call.pathname.startsWith('/api/admin/fairqueue/') && !EXPECTED_FAIRQUEUE_ENDPOINTS.has(call.pathname),
  );
  assert.deepEqual(unexpectedFairQueueCalls, []);

  const actualFairQueueEndpoints = new Set(
    calls
      .filter((call) => call.pathname.startsWith('/api/admin/fairqueue/'))
      .map((call) => call.pathname),
  );
  assert.deepEqual(actualFairQueueEndpoints, new Set(allowedEndpoints));
}

function pickCallSequence(calls, pathnames) {
  return calls
    .filter((call) => pathnames.includes(call.pathname) || (call.url && call.url.startsWith('https://download.example/')))
    .map((call) => (call.url && call.url.startsWith('https://download.example/') ? 'upstream_fetch' : call.pathname));
}

function installFetchMock(routes, calls) {
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async (input, init) => {
    const request = input instanceof Request ? input : null;
    const url = new URL(request ? request.url : String(input));
    const pathname = url.pathname;
    const body = await parseJsonBody(input, init);
    calls.push({ pathname, body, url: url.toString() });

    const route = routes[pathname];
    if (route) {
      return await route({ input, init, request, url, pathname, body, calls });
    }

    if (url.hostname === 'download.example') {
      return await routes.__download__({ input, init, request, url, pathname, body, calls });
    }

    throw new Error(`unexpected fetch: ${url}`);
  };
  return () => {
    globalThis.fetch = originalFetch;
  };
}

test('排队中 abort：只调用 /fairqueue/abandon，不调用 /fairqueue/release', async () => {
  const calls = [];
  const controller = new AbortController();
  let pollCount = 0;

  const restoreFetch = installFetchMock({
    [REPORT_ENDPOINT]: async () => jsonResponse({ code: 200 }),
    [FAIRQUEUE_ACQUIRE_ENDPOINT]: async () => jsonResponse({
      code: 200,
      data: { result: 'pending', waitToken: 'wait-pending-1' },
    }),
    [FAIRQUEUE_POLL_ENDPOINT]: async () => {
      pollCount += 1;
      controller.abort();
      return jsonResponse({
        code: 200,
        data: { result: 'pending', waitToken: 'wait-pending-1', retryAfter: 0 },
      });
    },
    [FAIRQUEUE_ABANDON_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'abandoned' } }),
    [FAIRQUEUE_RELEASE_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'ok' } }),
    __download__: async () => {
      throw new Error('download should not start while request is still pending');
    },
  }, calls);

  try {
    const ctx = createCtx();
    const request = await makeSignedRequest('/queued/file.bin', controller);
    const response = await worker.fetch(request, {}, ctx);
    await flushWaitUntil(ctx);

    assert.equal(response.status, 499);
    assert.equal(pollCount, 1);
    assert.equal(calls.filter((call) => call.pathname === FAIRQUEUE_ABANDON_ENDPOINT).length, 1);
    assert.equal(calls.filter((call) => call.pathname === FAIRQUEUE_RELEASE_ENDPOINT).length, 0);
    assertFairQueueCallsStayWithin(calls, [
      FAIRQUEUE_ACQUIRE_ENDPOINT,
      FAIRQUEUE_POLL_ENDPOINT,
      FAIRQUEUE_ABANDON_ENDPOINT,
    ]);
  } finally {
    restoreFetch();
  }
});

test('已 granted 但未 activate 时 abort：调用 /fairqueue/abandon，不调用 /fairqueue/release', async () => {
  const calls = [];
  const controller = new AbortController();

  const restoreFetch = installFetchMock({
    [REPORT_ENDPOINT]: async () => jsonResponse({ code: 200 }),
    [FAIRQUEUE_ACQUIRE_ENDPOINT]: async () => jsonResponse({
      code: 200,
      data: { result: 'granted', waitToken: 'wait-link-abort-1', slotToken: 'slot-link-abort-1' },
    }),
    [FAIRQUEUE_ABANDON_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'abandoned' } }),
    [FAIRQUEUE_RELEASE_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'ok' } }),
    '/api/fs/link': async () => {
      controller.abort();
      throw abortError();
    },
    __download__: async () => {
      throw new Error('upstream download must not start before activate');
    },
  }, calls);

  try {
    const ctx = createCtx();
    const request = await makeSignedRequest('/link-abort/file.bin', controller);
    const response = await worker.fetch(request, {}, ctx);
    await flushWaitUntil(ctx);

    assert.equal(response.status, 499);
    assert.equal(calls.filter((call) => call.pathname === FAIRQUEUE_ABANDON_ENDPOINT).length, 1);
    assert.equal(calls.filter((call) => call.pathname === FAIRQUEUE_ACTIVATE_ENDPOINT).length, 0);
    assert.equal(calls.filter((call) => call.pathname === FAIRQUEUE_RELEASE_ENDPOINT).length, 0);
    assertFairQueueCallsStayWithin(calls, [
      FAIRQUEUE_ACQUIRE_ENDPOINT,
      FAIRQUEUE_ABANDON_ENDPOINT,
    ]);
  } finally {
    restoreFetch();
  }
});

test('activate 后 abort：调用 /fairqueue/release 且 reason === client_abort', async () => {
  const calls = [];
  const controller = new AbortController();

  const restoreFetch = installFetchMock({
    [REPORT_ENDPOINT]: async () => jsonResponse({ code: 200 }),
    [FAIRQUEUE_ACQUIRE_ENDPOINT]: async () => jsonResponse({
      code: 200,
      data: { result: 'granted', waitToken: 'wait-active-1', slotToken: 'slot-active-1' },
    }),
    [FAIRQUEUE_ACTIVATE_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'activated' } }),
    [FAIRQUEUE_ABANDON_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'abandoned' } }),
    [FAIRQUEUE_RELEASE_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'ok' } }),
    '/api/fs/link': async () => jsonResponse({
      code: 200,
      data: {
        url: 'https://download.example/active/file.bin',
      },
    }),
    __download__: async () => {
      controller.abort();
      throw abortError();
    },
  }, calls);

  try {
    const ctx = createCtx();
    const request = await makeSignedRequest('/active/file.bin', controller);
    const response = await worker.fetch(request, {}, ctx);
    await flushWaitUntil(ctx);

    assert.equal(response.status, 499);
    assert.equal(calls.filter((call) => call.pathname === FAIRQUEUE_ACTIVATE_ENDPOINT).length, 1);
    assert.equal(calls.filter((call) => call.pathname === FAIRQUEUE_ABANDON_ENDPOINT).length, 0);
    assertFairQueueCallsStayWithin(calls, [
      FAIRQUEUE_ACQUIRE_ENDPOINT,
      FAIRQUEUE_ACTIVATE_ENDPOINT,
      FAIRQUEUE_RELEASE_ENDPOINT,
    ]);

    const releaseCalls = calls.filter((call) => call.pathname === FAIRQUEUE_RELEASE_ENDPOINT);
    assert.equal(releaseCalls.length, 1);
    assert.equal(releaseCalls[0].body.slotToken, 'slot-active-1');
    assert.equal(releaseCalls[0].body.reason, 'client_abort');
  } finally {
    restoreFetch();
  }
});

test('activate 请求已发出但响应前中断：cleanup 走 /fairqueue/release，不走 /fairqueue/abandon', async () => {
  const calls = [];
  const controller = new AbortController();

  const restoreFetch = installFetchMock({
    [REPORT_ENDPOINT]: async () => jsonResponse({ code: 200 }),
    [FAIRQUEUE_ACQUIRE_ENDPOINT]: async () => jsonResponse({
      code: 200,
      data: { result: 'granted', waitToken: 'wait-activate-race-1', slotToken: 'slot-activate-race-1' },
    }),
    [FAIRQUEUE_ACTIVATE_ENDPOINT]: async () => {
      controller.abort();
      throw abortError();
    },
    [FAIRQUEUE_ABANDON_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'abandoned' } }),
    [FAIRQUEUE_RELEASE_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'ok' } }),
    '/api/fs/link': async () => jsonResponse({
      code: 200,
      data: {
        url: 'https://download.example/activate-race/file.bin',
      },
    }),
    __download__: async () => {
      throw new Error('upstream download must not start after activate response is interrupted');
    },
  }, calls);

  try {
    const ctx = createCtx();
    const request = await makeSignedRequest('/activate-race/file.bin', controller);
    const response = await worker.fetch(request, {}, ctx);
    await flushWaitUntil(ctx);

    assert.equal(response.status, 499);
    assert.ok(calls.filter((call) => call.pathname === FAIRQUEUE_ACTIVATE_ENDPOINT).length >= 1);
    assert.equal(calls.filter((call) => call.pathname === FAIRQUEUE_ABANDON_ENDPOINT).length, 0);
    assertFairQueueCallsStayWithin(calls, [
      FAIRQUEUE_ACQUIRE_ENDPOINT,
      FAIRQUEUE_ACTIVATE_ENDPOINT,
      FAIRQUEUE_RELEASE_ENDPOINT,
    ]);

    const releaseCalls = calls.filter((call) => call.pathname === FAIRQUEUE_RELEASE_ENDPOINT);
    assert.equal(releaseCalls.length, 1);
    assert.equal(releaseCalls[0].body.slotToken, 'slot-activate-race-1');
    assert.equal(releaseCalls[0].body.reason, 'client_abort');
  } finally {
    restoreFetch();
  }
});

test('正常 stream end：调用 /fairqueue/release 且 reason === stream_end', async () => {
  const calls = [];
  const controller = new AbortController();
  let linkReturnedAt = 0;

  const restoreFetch = installFetchMock({
    [REPORT_ENDPOINT]: async () => jsonResponse({ code: 200 }),
    [FAIRQUEUE_ACQUIRE_ENDPOINT]: async () => jsonResponse({
      code: 200,
      data: { result: 'granted', waitToken: 'wait-stream-1', slotToken: 'slot-stream-1' },
    }),
    [FAIRQUEUE_ACTIVATE_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'activated' } }),
    [FAIRQUEUE_ABANDON_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'abandoned' } }),
    [FAIRQUEUE_RELEASE_ENDPOINT]: async () => jsonResponse({ code: 200, data: { result: 'ok' } }),
    '/api/fs/link': async () => {
      await new Promise((resolve) => setTimeout(resolve, 15));
      linkReturnedAt = Date.now();
      return jsonResponse({
        code: 200,
        data: {
          url: 'https://download.example/stream/file.bin',
        },
      });
    },
    __download__: async () => new Response(
      new ReadableStream({
        start(streamController) {
          streamController.enqueue(textEncoder.encode('hello worker'));
          streamController.close();
        },
      }),
      {
        status: 200,
        headers: {
          'content-type': 'application/octet-stream',
        },
      },
    ),
  }, calls);

  try {
    const ctx = createCtx();
    const request = await makeSignedRequest('/stream/file.bin', controller);
    const response = await worker.fetch(request, {}, ctx);
    const body = await response.text();
    await flushWaitUntil(ctx);

    assert.equal(response.status, 200);
    assert.equal(body, 'hello worker');
    assert.equal(calls.filter((call) => call.pathname === FAIRQUEUE_ACTIVATE_ENDPOINT).length, 1);
    assert.equal(calls.filter((call) => call.pathname === FAIRQUEUE_ABANDON_ENDPOINT).length, 0);
    assertFairQueueCallsStayWithin(calls, [
      FAIRQUEUE_ACQUIRE_ENDPOINT,
      FAIRQUEUE_ACTIVATE_ENDPOINT,
      FAIRQUEUE_RELEASE_ENDPOINT,
    ]);

    const releaseCalls = calls.filter((call) => call.pathname === FAIRQUEUE_RELEASE_ENDPOINT);
    assert.equal(releaseCalls.length, 1);
    assert.equal(releaseCalls[0].body.slotToken, 'slot-stream-1');
    assert.equal(releaseCalls[0].body.reason, 'stream_end');
    assert.ok(releaseCalls[0].body.hitUpstreamAtMs >= linkReturnedAt);
    assert.deepEqual(
      pickCallSequence(calls, ['/api/fs/link', FAIRQUEUE_ACTIVATE_ENDPOINT]),
      ['/api/fs/link', FAIRQUEUE_ACTIVATE_ENDPOINT, 'upstream_fetch'],
    );
  } finally {
    restoreFetch();
  }
});
