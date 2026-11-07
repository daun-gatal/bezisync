import { expect, test, type Page } from '@playwright/test';
import { Api, defaultProject, signIn, unique } from './helpers';
import { ELASTICSEARCH, KAFKA, OCEANBASE, PAIMON, SOURCE_PG } from './fixtures';
import { ownerSql, waitFor } from './data';
import { walkWizard } from './walk';

/*
 * ADR 0009: the destinations Flink CDC 3.7.0 writes that BeziSync added (Kafka on Flink CDC,
 * Elasticsearch, Apache Paimon, OceanBase), each end to end on the real engine: the connection's
 * checks pass, a pipeline copies and streams inserts, updates and deletes, the rows are read back
 * where BeziSync can read them, and "drop destinations" removes what it can (and says what stays).
 * Needs a Flink CDC image with these connectors (E2E_FLINK_IMAGE).
 */
const FLINK_IMAGE = process.env.E2E_FLINK_IMAGE ?? '';

test.describe.configure({ mode: 'serial' });
test.skip(!FLINK_IMAGE, 'set E2E_FLINK_IMAGE to run the Flink CDC journeys');

type Rec = { key?: unknown; value: Record<string, unknown> | null };
type Check = { code: string; status: string; message: string };

async function registerFlinkImage(api: Api): Promise<void> {
  const catalog = await api.get<{ entries: { image: string }[] }>('/api/v1/debezium-catalog?engine=flink-cdc');
  if (catalog.entries.some((e) => e.image === FLINK_IMAGE)) return;
  await api.send('POST', '/api/v1/debezium-catalog', { image: FLINK_IMAGE, engine: 'flink-cdc', is_default: true, label: 'e2e' }, 201);
}

interface Journey {
  type: string;
  config: Record<string, unknown>;
  secrets: Record<string, unknown>;
  /** Rows as BeziSync reads them back (null: it can't read this destination). */
  readable: boolean;
  /** What "drop destinations" does with the table. */
  cleanup: 'removed' | 'retained';
}

const { password: esPassword, ...esConfig } = ELASTICSEARCH;
const { password: obPassword, ...obConfig } = OCEANBASE;
const { secret_access_key: pmSecret, ...pmConfig } = PAIMON;
const JOURNEYS: Journey[] = [
  { type: 'kafka', config: { ...KAFKA, security_protocol: 'PLAINTEXT', create_topics: true, partitions: 3, replication_factor: 1 }, secrets: {}, readable: true, cleanup: 'removed' },
  { type: 'elasticsearch', config: esConfig, secrets: { password: esPassword }, readable: true, cleanup: 'removed' },
  { type: 'oceanbase', config: obConfig, secrets: { password: obPassword }, readable: true, cleanup: 'removed' },
  { type: 'paimon', config: pmConfig, secrets: { secret_access_key: pmSecret }, readable: false, cleanup: 'retained' },
];

async function journey(page: Page, j: Journey) {
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique(`fs-${j.type.slice(0, 5)}`);
  const table = `shop.fs_${tag.replace(/-/g, '_')}`;
  await ownerSql(`CREATE TABLE ${table} (id bigint PRIMARY KEY, status text, total numeric(10,2)); ALTER TABLE ${table} REPLICA IDENTITY FULL;
    INSERT INTO ${table} VALUES (1, 'copied', 10.50), (2, 'gone', 2)`);
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: j.type, name: `d-${tag}`, config: j.config, secrets: j.secrets }, 201);

  // The connection's own checks run against the real server.
  const report = await api.send<{ checks: Check[]; status: string }>('POST', `${base}/connections/${dst.id}/check`, {});
  expect(report.checks.filter((c) => c.status === 'fail'), JSON.stringify(report.checks)).toEqual([]);

  const p = await api.send<{ id: string; engine: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
    spec: { source: { tables: { include: [table] } }, sink: { naming: `fs_${tag.replace(/-/g, '_')}_{table}` }, snapshot: { mode: 'initial' } },
  }, 201);
  expect(p.engine).toBe('flink-cdc');
  const url = `${base}/pipelines/${p.id}`;
  await signIn(page);
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });

  const pre = await api.send<{ sections: { report: { checks: Check[] } }[] }>('POST', `${url}/preflight`, {});
  const failed = pre.sections.flatMap((s) => s.report.checks).filter((c) => c.status === 'fail');
  expect(failed, JSON.stringify(failed)).toEqual([]);
  await api.send('POST', `${url}/deploy`, {});
  await waitFor(`Flink CDC → ${j.type} streaming`, async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 900_000);
  await ownerSql(`INSERT INTO ${table} VALUES (3, 'streamed', 7.25); UPDATE ${table} SET status = 'changed' WHERE id = 1; DELETE FROM ${table} WHERE id = 2`);

  if (j.readable) {
    // Kafka reads the change log (the last change per key wins); the others read the rows.
    await waitFor(`the copied and streamed rows in ${j.type}`, async () => {
      const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=200`);
      const rows = new Map<number, Record<string, unknown> | null>();
      for (const r of e.records) {
        const v = r.value as Record<string, unknown> | null;
        const row = (v && 'after' in v ? (v.after as Record<string, unknown> | null) : v) ?? null;
        const id = Number((v && 'after' in v ? ((v.after ?? v.before) as Record<string, unknown>)?.id : v?.id) ?? NaN);
        if (!Number.isNaN(id)) rows.set(id, row);
      }
      return rows.get(1)?.status === 'changed' && rows.get(3)?.status === 'streamed' && (rows.get(2) === null || !rows.has(2));
    }, 600_000);
  } else {
    // No reader: the pipeline keeps streaming, and saves positions as it commits.
    await waitFor(`Flink CDC → ${j.type} saved a position`, async () => {
      const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
      if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
      const saved = await api.get<{ positions?: unknown[] } | unknown[]>(`${url}/positions`).catch(() => [] as unknown[]);
      const list = Array.isArray(saved) ? saved : (saved.positions ?? []);
      return list.length > 0;
    }, 600_000);
    expect((await api.get<{ lifecycle: string }>(url)).lifecycle).toBe('streaming');
  }

  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor(`Flink CDC → ${j.type} deleted`, async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 300_000);
  await ownerSql(`DROP TABLE IF EXISTS ${table}`);
}

for (const j of JOURNEYS) {
  test(`Flink CDC: Postgres → ${j.type}: checks, copy, inserts, updates and deletes, then drop`, async ({ page }) => {
    test.setTimeout(1_500_000);
    await journey(page, j);
  });
}
