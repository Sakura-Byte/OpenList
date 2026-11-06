// Runs against ../worker.js, a copy of the deployed worker (not committed).
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { createHmac } from 'node:crypto';

const workerSource = readFileSync(resolve(dirname(fileURLToPath(import.meta.url)), '../worker.js'), 'utf8');
const TOKEN = workerSource.match(/const TOKEN = "([^"]+)";/)[1];
const { default: worker } = await import(new URL('../worker.js', import.meta.url));

const GOOGLE_HOST = 'www.googleapis.com';
const quotaBody = JSON.stringify({ error: { errors: [{ reason: 'downloadQuotaExceeded' }] } });
const json = (data, status = 200) => new Response(JSON.stringify(data), { status, headers: { 'content-type': 'application/json' } });

function signedRequest(path) {
  const payload = Buffer.from(JSON.stringify({ p: path, e: 0 })).toString('base64url');
  const sig = createHmac('sha256', `${TOKEN}-download`).update(payload).digest('base64url');
  return new Request(`https://proxy.test${encodeURI(path)}?sign=v2.${payload}.${sig}`, {
    headers: { 'CF-Connecting-IP': '1.2.3.4', Range: 'bytes=0-9' },
  });
}

function run(routes) {
  const calls = [];
  const original = globalThis.fetch;
  globalThis.fetch = async (input, init = {}) => {
    const url = new URL(input.url || input);
    const body = init.body ? JSON.parse(init.body) : undefined;
    calls.push({ pathname: url.pathname, host: url.host, body, search: url.search, auth: input.headers?.get?.('authorization') });
    const key = url.host === GOOGLE_HOST ? 'google' : url.pathname.split('/').pop();
    return routes[key]({ url, body, calls });
  };
  const waits = [];
  return {
    calls,
    fetchWorker: async (path) => {
      const response = await worker.fetch(signedRequest(path), {}, { waitUntil: (p) => waits.push(p) });
      const text = await response.text();
      await Promise.allSettled(waits);
      return { status: response.status, text };
    },
    restore: () => { globalThis.fetch = original; },
  };
}

const baseRoutes = {
  report: () => json({ code: 200 }),
  acquire: () => json({ code: 200, data: { result: 'granted', waitToken: 'w', slotToken: 's' } }),
  activate: () => json({ code: 200, data: { result: 'activated' } }),
  release: () => json({ code: 200, data: { result: 'ok' } }),
  abandon: () => json({ code: 200, data: { result: 'abandoned' } }),
};
const linkFor = (storage) => json({
  code: 200,
  data: { url: `https://${GOOGLE_HOST}/drive/${storage}`, header: { Authorization: [`Bearer ${storage}`] }, storage },
});

test('over the IP limit: explains why and never contacts Google', async () => {
  const h = run({
    ...baseRoutes,
    acquire: () => json({ code: 200, data: { result: 'rejected', reason: 'gd_ip_daily_limit', limit: 77, used: 77, reset_at: 1800000000 } }),
    link: () => { throw new Error('must not fetch a link'); },
  });
  try {
    const { status, text } = await h.fetchWorker('/Galgames/a.bin');
    const body = JSON.parse(text);
    assert.equal(status, 429);
    assert.equal(body.reason, 'gd_ip_daily_limit');
    assert.equal(body.limit, 77);
    assert.match(body.message, /77/);
    assert.doesNotMatch(body.message, /concurrent/);
    assert.equal(h.calls.filter((c) => c.host === GOOGLE_HOST).length, 0);
  } finally {
    h.restore();
  }
});

test('quota error: retries another account, keeps the Range, and streams it', async () => {
  const links = [];
  const h = run({
    ...baseRoutes,
    link: ({ body }) => { links.push(body); return linkFor(body.avoid ? 'b' : 'a'); },
    google: ({ url }) => (url.pathname.endsWith('/a') ? new Response(quotaBody, { status: 403 }) : new Response('payload', { status: 206 })),
  });
  try {
    const { status, text } = await h.fetchWorker('/Galgames/a.bin');
    assert.equal(status, 206);
    assert.equal(text, 'payload');
    assert.deepEqual(links, [{ path: '/Galgames/a.bin' }, { path: '/Galgames/a.bin', avoid: 'a' }]);
    assert.equal(h.calls.filter((c) => c.pathname.endsWith('/activate')).length, 1);
    assert.equal(h.calls.filter((c) => c.pathname.endsWith('/release')).length, 1);
    assert.equal(h.calls.filter((c) => c.pathname.endsWith('/acquire')).length, 1);
  } finally {
    h.restore();
  }
});

test('every account over quota: stops after the retries and returns the refusal', async () => {
  let next = 0;
  const h = run({
    ...baseRoutes,
    link: () => linkFor(`s${next++}`),
    google: () => new Response(quotaBody, { status: 403 }),
  });
  try {
    const { status, text } = await h.fetchWorker('/Galgames/a.bin');
    assert.equal(status, 403);
    assert.match(text, /downloadQuotaExceeded/);
    assert.equal(h.calls.filter((c) => c.host === GOOGLE_HOST).length, 4);
    assert.equal(h.calls.filter((c) => c.pathname.endsWith('/release')).length, 1);
  } finally {
    h.restore();
  }
});

test('no other account available: does not loop on the same storage', async () => {
  const h = run({
    ...baseRoutes,
    link: () => linkFor('only'),
    google: () => new Response(quotaBody, { status: 403 }),
  });
  try {
    const { status } = await h.fetchWorker('/Galgames/a.bin');
    assert.equal(status, 403);
    assert.equal(h.calls.filter((c) => c.host === GOOGLE_HOST).length, 1);
  } finally {
    h.restore();
  }
});

test('other 403 and links without a storage are returned untouched', async () => {
  const h = run({
    ...baseRoutes,
    link: () => linkFor('a'),
    google: () => new Response('{"error":"forbidden"}', { status: 403 }),
  });
  try {
    const { status } = await h.fetchWorker('/Galgames/a.bin');
    assert.equal(status, 403);
    assert.equal(h.calls.filter((c) => c.pathname.endsWith('/link')).length, 1);
  } finally {
    h.restore();
  }
});
