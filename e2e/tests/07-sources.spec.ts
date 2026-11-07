import { expect, test, type Page } from '@playwright/test';
import mysql from 'mysql2/promise';
import mssql from 'mssql';
import { MongoClient } from 'mongodb';
import pg from 'pg';
import { Api, defaultProject, signIn, unique } from './helpers';
import { COCKROACHDB, COCKROACHDB_FROM_RUNNER, KAFKA, MONGO, MONGO_FROM_RUNNER, MSSQL, MSSQL_FROM_RUNNER, MYSQL, MYSQL_FROM_RUNNER, ORACLE, VITESS, VITESS_FROM_RUNNER } from './fixtures';
import { oracleSQL, waitFor } from './data';

test.describe.configure({ mode: 'serial' });

interface Report { ok: boolean; summary: string; checks: { code: string; status: string; message: string }[] }
type Rec = { value: Record<string, unknown> };
type Impact = {
  retained_bytes: number | null; log_status: string | null; limit_bytes: number | null; reading_file: string | null;
  window_seconds: number | null; headroom_bytes: number | null; slot: string | null; risk: string | null;
  position_kept?: boolean | null; log_used_bytes?: number | null; behind_seconds?: number | null; retention_seconds?: number | null;
};
interface Live { api: Api; url: string; path: string }

/** The source impact once the controller has measured it (it probes every 30 s). */
async function measuredImpact(api: Api, url: string, ready: (i: Impact) => boolean): Promise<Impact> {
  return waitFor('source impact measured', async () => {
    const i = (await api.get<{ status: { source_impact?: Impact } }>(url)).status.source_impact;
    return i && ready(i) ? i : undefined;
  }, 180_000);
}

async function streamThrough(
  label: string,
  source: { type: string; provider?: string; config: Record<string, unknown>; password: string },
  table: string,
  extraSpec: Record<string, unknown>,
  insert: (marker: string) => Promise<void>,
  find: (records: Rec[], marker: string) => boolean,
  whileLive?: (live: Live) => Promise<void>,
  policies: Record<string, unknown> = {},
): Promise<Rec[]> {
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique(label);
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, {
    kind: 'source', type: source.type, provider: source.provider ?? 'self_hosted', name: `src-${tag}`, config: source.config, secrets: { password: source.password },
  }, 201);
  const report = await api.send<Report>('POST', `${base}/connections/${src.id}/check`, { tables: [table] });
  expect(report.ok, JSON.stringify(report.checks.filter((c) => c.status === 'fail'))).toBe(true);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `k-${tag}`, config: KAFKA }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: [table] }, ...extraSpec }, sink: { naming: `src.${tag}.{table}` }, policies },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, {});
  await waitFor(`${label} streaming`, async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 480_000);
  const marker = `LIVE-${tag}`;
  await insert(marker);
  const records = await waitFor(`${label} live change delivered`, async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=200`);
    return find(e.records, marker) ? e.records : undefined;
  }, 240_000);
  await whileLive?.({ api, url, path: `/p/${project.slug}/pipelines/${p.id}` });
  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor(`${label} deleted`, async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
  return records;
}

test('MySQL → Kafka with a masked column', async ({ page }: { page: Page }) => {
  test.setTimeout(900_000);
  const { password, ...config } = MYSQL;
  const records = await streamThrough('mysql', { type: 'mysql', config: { ...config, ssl_mode: 'disabled' }, password }, 'shop.orders',
    { columns: { 'shop.orders': { mask: ['email'], exclude: [] } } },
    async (marker) => {
      const db = await mysql.createConnection(MYSQL_FROM_RUNNER);
      await db.execute('INSERT INTO orders (status, email, total) VALUES (?, ?, 1)', [marker, 'secret@example.com']);
      await db.end();
    },
    (recs, marker) => recs.some((r) => r.value.status === marker),
    async ({ api, url, path }) => {
      // Source protection in MySQL's terms: the binlog file being read, against the retention limit.
      const impact = await measuredImpact(api, url, (i) => Boolean(i.reading_file));
      expect(impact.reading_file).toMatch(/\.\d+$/);
      expect(['ok', 'oldest']).toContain(impact.log_status);
      expect(impact.limit_bytes).toBe(10 * 1024 ** 3);
      expect(impact.slot).toBeNull();
      await signIn(page);
      await page.goto(path);
      const card = page.getByRole('region', { name: 'Source protection' });
      await expect(card.getByText('Reading binlog file')).toBeVisible({ timeout: 60_000 });
      await expect(card.getByText(impact.reading_file!)).toBeVisible();
      await expect(card.getByRole('meter', { name: 'Binlog waiting for this pipeline' })).toHaveAttribute('aria-valuetext', /of 10 GB limit$/);
      await expect(card).not.toContainText('Replication slot');
      await expect(card).not.toContainText(/unlimited/i);
    });
  const live = records.find((r) => String(r.value.status).startsWith('LIVE-'))!;
  expect(live.value.email).not.toBe('secret@example.com');
  expect(records.filter((r) => r.value.status === 'NEW').length).toBeGreaterThanOrEqual(2);
});

test('SQL Server → Kafka', async ({ page }: { page: Page }) => {
  test.setTimeout(900_000);
  const { password, ...config } = MSSQL;
  await streamThrough('mssql', { type: 'sqlserver', config: { ...config, encrypt: false, trust_server_certificate: true }, password }, 'dbo.orders', {},
    async (marker) => {
      const pool = await mssql.connect({ ...MSSQL_FROM_RUNNER, options: { encrypt: false, trustServerCertificate: true } });
      await pool.request().input('m', mssql.NVarChar, marker).query('INSERT INTO dbo.orders (status, email, total) VALUES (@m, NULL, 1)');
      await pool.close();
    },
    (recs, marker) => recs.some((r) => r.value.status === marker),
    async ({ api, url, path }) => {
      // SQL Server in its own terms: CDC retention, the stored position inside it, the shared log. Nothing "for this pipeline".
      const impact = await measuredImpact(api, url, (i) => i.position_kept !== null && i.position_kept !== undefined);
      expect(impact.position_kept).toBe(true);
      expect(impact.window_seconds).toBe(3 * 86400); // the cleanup job's default retention
      expect(typeof impact.log_used_bytes).toBe('number');
      expect(impact.retained_bytes).toBeNull();
      expect(impact.limit_bytes).toBeNull();
      await signIn(page);
      await page.goto(path);
      const card = page.getByRole('region', { name: 'Source protection' });
      await expect(card.getByText('CDC keeps changes for')).toBeVisible({ timeout: 60_000 });
      await expect(card.getByText('3 d', { exact: true })).toBeVisible();
      await expect(card.getByText('Inside that window')).toBeVisible();
      await expect(card.getByRole('meter', { name: 'Transaction log used' })).toBeVisible();
      await expect(card.getByText('OK', { exact: true })).toBeVisible();
      await expect(page.getByText(/Log kept for this pipeline/)).toHaveCount(0);
    });
});

test('MongoDB → Kafka', async () => {
  test.setTimeout(900_000);
  const { password, ...config } = MONGO;
  await streamThrough('mongo', { type: 'mongodb', config: { ...config, auth_source: 'admin' }, password }, 'shop.customers', {},
    async (marker) => {
      const client = await MongoClient.connect(MONGO_FROM_RUNNER);
      await client.db('shop').collection('customers').insertOne({ name: marker });
      await client.close();
    },
    (recs, marker) => recs.some((r) => r.value.name === marker),
    async ({ api, url }) => {
      // The oplog is shared: no per-pipeline limit, and its window or "filling" is what is reported.
      const impact = await measuredImpact(api, url, () => true);
      expect(impact.limit_bytes).toBeNull();
      if (impact.log_status === 'filling') expect(impact.window_seconds).toBeNull();
      else if (impact.log_status !== null) expect(typeof impact.window_seconds).toBe('number');
    });
});

test('Oracle → Kafka with a masked column, a re-copy and the delivery canary', async ({ page }: { page: Page }) => {
  test.setTimeout(1_200_000);
  const { password, ...config } = ORACLE;
  const records = await streamThrough('oracle', { type: 'oracle', config, password }, 'SHOP.ORDERS',
    { columns: { 'SHOP.ORDERS': { mask: ['EMAIL'], exclude: [] } } },
    async (marker) => { oracleSQL(`INSERT INTO shop.orders (status, email, total) VALUES ('${marker}', 'secret@example.com', 3);`); },
    (recs, marker) => recs.some((r) => r.value.STATUS === marker),
    async ({ api, url, path }) => {
      // Re-copy while streaming: the rows come again as reads.
      await api.send('POST', `${url}/snapshots`, { tables: ['SHOP.ORDERS'] }, 202);
      await waitFor('re-copied rows', async () => {
        const e = await api.get<{ records: Rec[] }>(`${url}/events?table=SHOP.ORDERS&limit=200`);
        return e.records.filter((r) => r.value.STATUS === 'NEW').length >= 2;
      }, 300_000);
      // Source protection in Oracle's terms: how far back the archived logs reach, and the position against it.
      const impact = await measuredImpact(api, url, (i) => typeof i.retention_seconds === 'number' && typeof i.behind_seconds === 'number');
      expect(impact.risk).toBeNull();
      expect(impact.retained_bytes).toBeNull();
      await signIn(page);
      await page.goto(path);
      const card = page.getByRole('region', { name: 'Source protection' });
      await expect(card.getByText('Archived logs reach back')).toBeVisible({ timeout: 60_000 });
      await expect(card.getByText(/ behind$/)).toBeVisible();
      await expect(card.getByRole('meter', { name: 'Window used' })).toBeVisible();
      await expect(card).not.toContainText('Replication slot');
      // The delivery canary goes through BZ_CANARY_<id> in the connector user's schema.
      const canary = await waitFor('canary verified', async () => {
        const cur = await api.get<{ status: { canary?: { ok?: boolean } } }>(url);
        return cur.status.canary?.ok ? cur.status.canary : undefined;
      }, 420_000);
      expect(canary.ok).toBe(true);
    }, { delivery_canary: true });
  const live = records.find((r) => String(r.value.STATUS).startsWith('LIVE-'))!;
  expect(live.value.EMAIL).not.toBe('secret@example.com');
});

test('CockroachDB → Kafka through a sinkless changefeed', async ({ page }: { page: Page }) => {
  test.setTimeout(900_000);
  await streamThrough('crdb', { type: 'cockroachdb', config: COCKROACHDB, password: '' }, 'public.orders',
    { columns: { 'public.orders': { mask: ['email'], exclude: [] } } },
    async (marker) => {
      const db = new pg.Client(COCKROACHDB_FROM_RUNNER);
      await db.connect();
      await db.query("INSERT INTO orders (status, email, total) VALUES ($1, 'secret@example.com', 3)", [marker]);
      await db.end();
    },
    (recs, marker) => recs.some((r) => r.value.status === marker && r.value.email !== 'secret@example.com'),
    async ({ api, url, path }) => {
      // Re-copy while streaming.
      await api.send('POST', `${url}/snapshots`, { tables: ['public.orders'] }, 202);
      await waitFor('re-copied rows', async () => {
        const e = await api.get<{ records: Rec[] }>(`${url}/events?table=public.orders&limit=200`);
        return e.records.filter((r) => r.value.status === 'NEW').length >= 2;
      }, 300_000);
      // Source protection in CockroachDB's terms: the GC TTL a stopped changefeed can resume within.
      const impact = await measuredImpact(api, url, (i) => typeof i.retention_seconds === 'number' && typeof i.behind_seconds === 'number');
      expect(impact.retention_seconds).toBe(4 * 3600); // gc.ttlseconds default
      expect(impact.risk).toBeNull();
      await signIn(page);
      await page.goto(path);
      const card = page.getByRole('region', { name: 'Source protection' });
      await expect(card.getByText('Changes kept (GC TTL)')).toBeVisible({ timeout: 60_000 });
      await expect(card.getByText('4 h', { exact: true })).toBeVisible();
      await expect(card.getByText('OK', { exact: true })).toBeVisible();
      await page.getByRole('tab', { name: 'Snapshots' }).click();
      await expect(page.getByText('Re-copies from CockroachDB')).toBeVisible();
    });
});

test('Vitess → Kafka across two shards', async ({ page }: { page: Page }) => {
  test.setTimeout(900_000);
  const records = await streamThrough('vitess', { type: 'vitess', config: VITESS, password: '' }, 'shop.orders',
    { columns: { 'shop.orders': { mask: ['email'], exclude: [] } } },
    async (marker) => {
      const db = await mysql.createConnection(VITESS_FROM_RUNNER);
      await db.execute('INSERT INTO orders (id, status, email, total) VALUES (?, ?, ?, 4)', [Date.now(), marker, 'secret@example.com']);
      await db.end();
    },
    (recs, marker) => recs.some((r) => r.value.status === marker),
    async ({ path }) => {
      await signIn(page);
      await page.goto(path);
      // Binlog retention on the tablets is not visible through VTGate: grey, never "OK".
      const card = page.getByRole('region', { name: 'Source protection' });
      await expect(card.getByText(/not visible through VTGate/)).toBeVisible({ timeout: 120_000 });
      await expect(card.getByText('OK', { exact: true })).toHaveCount(0);
      await page.getByRole('tab', { name: 'Snapshots' }).click();
      await expect(page.getByText(/Vitess can’t copy tables again while it streams/)).toBeVisible();
      await expect(page.getByRole('button', { name: 'Start snapshot' })).toHaveCount(0);
    });
  // Both shards' existing rows were copied, and the live row's email is masked.
  expect(records.filter((r) => ['NEW', 'PAID'].includes(String(r.value.status))).length).toBeGreaterThanOrEqual(3);
  const live = records.find((r) => String(r.value.status).startsWith('LIVE-'))!;
  expect(live.value.email).not.toBe('secret@example.com');
});
