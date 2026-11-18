// Docs screenshot capture: run by scripts/docs-screenshots.sh inside a Playwright container on
// the lab's Docker network (on a remote Docker daemon the published port is not reachable from
// here, and the app's CSRF check wants the public URL's origin, so the script serves
// http://localhost:8080 itself and forwards it to the web container).
//
// Creates and deploys the pipelines the pages show (the lab seeds only connections, a runtime and
// a template), leaves one draft for the wizard, then captures every screen the docs reference in
// light mode at 1440×900 as JPEGs in /out. The names are what docs/guide/*.md reference.
//
// Logs go to stderr; stdout is reserved for the tar stream of /out.

/* global process, console, setTimeout, fetch */
import { mkdirSync } from 'node:fs';
import net from 'node:net';
import { chromium } from 'playwright-core';

const UPSTREAM = { host: process.env.SHOTS_WEB_HOST ?? 'web', port: 8080 };
const BASE = 'http://localhost:8080';
const PASSWORD = process.env.SHOTS_ADMIN_PASSWORD;
const OUT = '/out';
const ONLY = new Set((process.env.SHOTS_ONLY ?? '').split(',').filter(Boolean));
const log = (...args) => console.error('[shots]', ...args);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

if (!PASSWORD) throw new Error('SHOTS_ADMIN_PASSWORD is required');

// localhost:8080 → web:8080
net
  .createServer((client) => {
    const upstream = net.connect(UPSTREAM);
    client.pipe(upstream).pipe(client);
    const close = () => { client.destroy(); upstream.destroy(); };
    client.on('error', close);
    upstream.on('error', close);
  })
  .listen(8080, '127.0.0.1');

// --- API ------------------------------------------------------------------------------------

let cookie = '';
let csrf = '';
async function api(method, path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: { 'content-type': 'application/json', cookie, 'x-csrf-token': csrf, origin: BASE },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`${method} ${path}: ${res.status} ${text}`);
  return text ? JSON.parse(text) : undefined;
}

async function signIn() {
  for (let i = 0; i < 60; i++) {
    try {
      const res = await fetch(`${BASE}/api/v1/auth/login`, {
        method: 'POST', headers: { 'content-type': 'application/json', origin: BASE },
        body: JSON.stringify({ email: 'admin@bezisync.local', password: PASSWORD }),
      });
      if (res.ok) {
        const cookies = res.headers.getSetCookie().map((c) => c.split(';')[0]);
        cookie = cookies.join('; ');
        csrf = cookies.find((c) => c.startsWith('bz_csrf='))?.slice('bz_csrf='.length) ?? '';
        return;
      }
      log('sign in:', res.status, await res.text());
    } catch (e) {
      log('waiting for the app:', e.message);
    }
    await sleep(5000);
  }
  throw new Error('could not sign in');
}

async function waitFor(what, fn, ms) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    const v = await fn();
    if (v) return v;
    await sleep(5000);
  }
  throw new Error(`timed out: ${what}`);
}

// --- seed -----------------------------------------------------------------------------------

await signIn();
const me = await api('GET', '/api/v1/auth/me');
const project = me.projects.find((p) => p.slug === 'sandbox');
if (!project) throw new Error('no Sandbox project: is this the lab stack?');
const base = `/api/v1/projects/${project.id}`;
const connections = await api('GET', `${base}/connections`);
const conn = (name) => {
  const list = Array.isArray(connections) ? connections : connections.items;
  const c = list.find((x) => x.name === name);
  if (!c) throw new Error(`no connection ${name}: ${list.map((x) => x.name).join(', ')}`);
  return c;
};
const runtimes = await api('GET', `${base}/runtimes`);
const runtime = (Array.isArray(runtimes) ? runtimes : runtimes.items)[0];
const existing = await api('GET', `${base}/pipelines`);
const pipelines = Array.isArray(existing) ? existing : existing.items;

async function pipeline(name, source, sink, tables, spec = {}, deploy = true) {
  let p = pipelines.find((x) => x.name === name);
  if (!p) {
    p = await api('POST', `${base}/pipelines`, {
      name, owner: 'data-platform', source_id: conn(source).id, sink_id: conn(sink).id, runtime_id: runtime.id,
      spec: { source: { tables: { include: tables } }, ...spec },
    });
    log('created', name);
  }
  if (deploy && p.lifecycle !== 'streaming') {
    const pre = await api('POST', `${base}/pipelines/${p.id}/preflight`);
    if (!pre.ok) log(`preflight of ${name}:`, pre.summary);
    await api('POST', `${base}/pipelines/${p.id}/deploy`, pre.ok ? {} : { force: true, reason: 'docs screenshots' }).catch((e) => log(e.message));
  }
  return p;
}

const kafka = await pipeline('shop-orders-to-kafka', 'demo-shop', 'demo-kafka', ['shop.orders']);
const warehouse = await pipeline('mysql-shop-to-warehouse', 'demo-mysql', 'demo-warehouse', ['shop.orders']);
const draft = await pipeline('customers-to-http', 'demo-shop', 'demo-http', ['shop.customers'], {}, false);

for (const p of [kafka, warehouse]) {
  await waitFor(`${p.name} streaming`, async () => {
    const now = await api('GET', `${base}/pipelines/${p.id}`);
    if (now.lifecycle === 'failed') throw new Error(`${p.name} failed: ${JSON.stringify(now.status)}`);
    return now.lifecycle === 'streaming' && now.health === 'healthy';
  }, 600_000);
  log(p.name, 'is streaming');
}
// Let throughput and lag charts gather a few points.
await sleep(Number(process.env.SHOTS_WAIT_SECONDS ?? 90) * 1000);

// --- capture --------------------------------------------------------------------------------

mkdirSync(OUT, { recursive: true });
const browser = await chromium.launch();
const context = await browser.newContext({ baseURL: BASE, viewport: { width: 1440, height: 900 }, colorScheme: 'light', deviceScaleFactor: 1 });
const page = await context.newPage();
await page.goto('/login');
await page.getByLabel('Email').fill('admin@bezisync.local');
await page.getByLabel('Password').fill(PASSWORD);
await page.getByRole('button', { name: 'Sign in' }).click();
await page.waitForURL(/\/p\//);
// The "New here?" card would show on every capture: dismiss it (it stays dismissed).
await page.getByRole('button', { name: 'Dismiss the tour suggestion' }).click({ timeout: 5000 }).catch(() => undefined);

const p = `/p/${project.slug}`;
const SHOTS = [
  ['pipelines', `${p}/pipelines`],
  ['pipeline-overview', `${p}/pipelines/${kafka.id}`],
  ['connections', `${p}/connections`],
  ['alerts', `${p}/alerts`],
  ['tour', `${p}/tour`],
  ['wizard-tables', `${p}/pipelines/new?draft=${draft.id}&step=tables`],
  ['wizard-destination', `${p}/pipelines/new?draft=${draft.id}&step=sink`],
  ['wizard-transform', `${p}/pipelines/new?draft=${draft.id}&step=transform`],
  ['wizard-review', `${p}/pipelines/new?draft=${draft.id}&step=review`],
  ['settings-people', `${p}/settings/people`],
  ['settings-runtimes', `${p}/settings/runtimes`],
  ['settings-engines', `${p}/settings/engines`],
  ['settings-policies', `${p}/settings/policies`],
  ['settings-secret-stores', `${p}/settings/secret-stores`],
  ['settings-sign-in', `${p}/settings/sign-in`],
  ['settings-mcp', `${p}/settings/mcp`],
  ['connection-checks', `${p}/connections`, async (page) => {
    await page.getByText('legacy-postgres', { exact: true }).first().click();
    await page.getByRole('button', { name: /check|test/i }).first().click().catch(() => undefined);
    await page.waitForTimeout(8000);
  }],
];

let failed = 0;
for (const [name, url, before] of SHOTS) {
  if (ONLY.size && !ONLY.has(name)) continue;
  try {
    await page.goto(url);
    await page.waitForLoadState('networkidle').catch(() => undefined);
    if (before) await before(page);
    await page.waitForTimeout(1500);
    await page.screenshot({ path: `${OUT}/${name}.jpg`, type: 'jpeg', quality: 82 });
    log('captured', name);
  } catch (e) {
    failed++;
    log(`FAILED ${name}:`, e.message);
    await page.screenshot({ path: `${OUT}/_debug-${name}.jpg`, type: 'jpeg' }).catch(() => undefined);
  }
}
await browser.close();
process.exit(failed ? 1 : 0);
