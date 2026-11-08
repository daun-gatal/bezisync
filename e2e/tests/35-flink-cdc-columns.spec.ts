import { expect, test } from '@playwright/test';
import { Api, defaultProject, unique } from './helpers';
import { DORIS, ELASTICSEARCH, ICEBERG_REST, KAFKA, PAIMON, SOURCE_PG, STARROCKS } from './fixtures';
import { ownerSql, waitFor } from './data';

/*
 * ADR 0012: a Flink CDC job writes the destination's columns in the destination's order, or doesn't
 * start. On the real engine, per destination: columns the source adds and renames while it runs land
 * by name; a version that changes the columns the pipeline writes is refused while it would resume a
 * saved position (the change would be dropped); after Start over the deploy plans the start, adds the
 * new columns to the destination and writes every value into its own column; and a saved position
 * from before can't be restored. Every value names its own column (a3, bb3, …), so a shifted one shows.
 * Needs E2E_FLINK_IMAGE; StarRocks and Doris also E2E_OLAP (deploy/e2e/fixtures-olap).
 */
const FLINK_IMAGE = process.env.E2E_FLINK_IMAGE ?? '';
const OLAP = !!process.env.E2E_OLAP;

test.describe.configure({ mode: 'serial' });
test.skip(!FLINK_IMAGE, 'set E2E_FLINK_IMAGE to run the Flink CDC journeys');

type Check = { code: string; status: string; message: string; fix?: { code?: string } | null };
type Rec = { value: Record<string, unknown> | null };

interface Journey {
  type: string;
  config: Record<string, unknown>;
  secrets: Record<string, unknown>;
  /** BeziSync reads the rows back (Paimon: no reader). */
  readable: boolean;
  /** The soft-delete column (StarRocks reserves __op). */
  op: string;
  /** New source columns: sent live, or held (destinations that refuse source defaults). */
  newColumns: 'send' | 'hold';
  /** BeziSync can't add columns: the planned start is refused with the statements to run. */
  byHand?: boolean;
  olap?: boolean;
}

const strip = <T extends Record<string, unknown>>(o: T, key: string) => {
  const { [key]: secret, ...rest } = o;
  return { rest, secret };
};
const ice = strip(ICEBERG_REST, 'secret_access_key');
const pm = strip(PAIMON, 'secret_access_key');
const es = strip(ELASTICSEARCH, 'password');
const sr = strip(STARROCKS, 'password');
const dr = strip(DORIS, 'password');
const JOURNEYS: Journey[] = [
  { type: 'iceberg', config: { ...ice.rest, namespace: 'e2e_cols' }, secrets: { secret_access_key: ice.secret }, readable: true, op: '__op', newColumns: 'send' },
  { type: 'kafka', config: { ...KAFKA, security_protocol: 'PLAINTEXT', create_topics: true, partitions: 1, replication_factor: 1 }, secrets: {}, readable: true, op: '__op', newColumns: 'send' },
  { type: 'elasticsearch', config: es.rest, secrets: { password: es.secret }, readable: true, op: '__op', newColumns: 'send' },
  { type: 'paimon', config: pm.rest, secrets: { secret_access_key: pm.secret }, readable: false, op: '__op', newColumns: 'send', byHand: true },
  { type: 'starrocks', config: sr.rest, secrets: { password: sr.secret }, readable: true, op: '__bz_op', newColumns: 'hold', olap: true },
  { type: 'doris', config: dr.rest, secrets: { password: dr.secret }, readable: true, op: '__op', newColumns: 'hold', olap: true },
];

async function registerFlinkImage(api: Api): Promise<void> {
  const catalog = await api.get<{ entries: { image: string }[] }>('/api/v1/debezium-catalog?engine=flink-cdc');
  if (catalog.entries.some((e) => e.image === FLINK_IMAGE)) return;
  await api.send('POST', '/api/v1/debezium-catalog', { image: FLINK_IMAGE, engine: 'flink-cdc', is_default: true, label: 'e2e' }, 201);
}

/** The rows the destination holds, by id (Kafka: the last change per key). */
async function rows(api: Api, url: string, table: string): Promise<Map<number, Record<string, unknown>>> {
  const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=500`);
  const out = new Map<number, Record<string, unknown>>();
  for (const r of [...e.records].reverse()) {
    const v = r.value;
    const row = (v && 'after' in v ? (v.after as Record<string, unknown> | null) : v) ?? null;
    const id = Number(row?.id ?? NaN);
    if (row && !Number.isNaN(id)) out.set(id, row);
  }
  return out;
}

async function lifecycle(api: Api, url: string, want: string, timeout = 900_000) {
  await waitFor(`${url} ${want}`, async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed' && want !== 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === want;
  }, timeout);
}

async function journey(j: Journey) {
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique(`cl-${j.type.slice(0, 4)}`);
  const table = `shop.cl_${tag.replace(/-/g, '_')}`;
  await ownerSql(`CREATE TABLE ${table} (id bigint PRIMARY KEY, a text, b text); ALTER TABLE ${table} REPLICA IDENTITY FULL;
    INSERT INTO ${table} VALUES (1, 'a1', 'b1')`);
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: j.type, name: `d-${tag}`, config: j.config, secrets: j.secrets }, 201);
  const transforms = [
    { type: 'unwrap' },
    { type: 'soft_delete' },
    { type: 'add_field', id: 'addf1', name: 'f1', tables: [table], value: { kind: 'static', text: 'one' } },
  ];
  const spec = {
    source: { tables: { include: [table] } }, sink: { naming: `cl_${tag.replace(/-/g, '_')}_{table}` }, snapshot: { mode: 'initial' },
    flink_cdc: { new_columns: j.newColumns }, transforms,
  };
  const name = `p-${tag}`;
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, { name, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc', spec }, 201);
  const url = `${base}/pipelines/${p.id}`;

  // L1: the first start creates the table; nothing to plan.
  await api.send('POST', `${url}/deploy`, {});
  await lifecycle(api, url, 'streaming');
  if (j.readable) {
    await waitFor(`${j.type}: the copied row`, async () => {
      const r = (await rows(api, url, table)).get(1);
      return r?.a === 'a1' && r?.b === 'b1' && r?.f1 === 'one' && r?.[j.op] === '+I';
    }, 600_000);
  }

  // Live source changes: an added column and (where new columns are sent) a rename land by name.
  if (j.newColumns === 'send') {
    await ownerSql(`ALTER TABLE ${table} ADD COLUMN c text; ALTER TABLE ${table} RENAME COLUMN b TO bb`);
    await ownerSql(`INSERT INTO ${table} (id, a, bb, c) VALUES (2, 'a2', 'bb2', 'c2')`);
  } else {
    await ownerSql(`ALTER TABLE ${table} ADD COLUMN c text`);
    await ownerSql(`INSERT INTO ${table} (id, a, b, c) VALUES (2, 'a2', 'b2', 'c2')`);
  }
  if (j.readable) {
    await waitFor(`${j.type}: the live row after the source changed`, async () => {
      const r = (await rows(api, url, table)).get(2);
      if (!r) return false;
      expect(r.a, JSON.stringify(r)).toBe('a2');
      expect(r.f1, JSON.stringify(r)).toBe('one');
      expect(r[j.op], JSON.stringify(r)).toBe('+I');
      if (j.newColumns === 'send') {
        expect(r.bb, JSON.stringify(r)).toBe('bb2');
        expect(r.c, JSON.stringify(r)).toBe('c2');
      } else {
        expect(r.b, JSON.stringify(r)).toBe('b2');
      }
      return true;
    }, 600_000);
  }

  // L5: a version that changes the columns the pipeline writes is refused while it would resume.
  const cur = await api.get<{ spec: typeof spec; version: number }>(url);
  const changed = { ...cur.spec, transforms: [...transforms,
    { type: 'add_field', id: 'addf2', name: 'f2', tables: [table], value: { kind: 'static', text: 'two' } },
    { type: 'computed', id: 'label1', table, name: 'label', expression: 'UPPER(`a`)' }] };
  await api.patch(url, { spec: changed }, cur.version);
  await waitFor(`${j.type}: a saved position to resume`, async () => {
    const saved = await api.get<{ positions?: unknown[] }>(`${url}/positions`);
    return (saved.positions ?? []).length > 0;
  }, 600_000);
  const pre = await api.send<{ sections: { report: { checks: Check[] } }[] }>('POST', `${url}/preflight`, {});
  const changedCheck = pre.sections.flatMap((s) => s.report.checks).find((c) => c.code === 'flink_columns_changed');
  expect(changedCheck?.status, JSON.stringify(changedCheck)).toBe('fail');
  expect(changedCheck?.message).toContain('adds f2, label');
  const refused = await api.raw('POST', `${url}/deploy`, { force_reason: 'e2e: a forced deploy can not skip it' });
  expect(refused.status(), await refused.text()).toBe(409);

  // L7: Start over, then the deploy plans the start.
  await api.send('POST', `${url}/pause`);
  await lifecycle(api, url, 'paused', 300_000);
  const before = await api.get<{ positions: { id: number }[] }>(`${url}/positions`);
  await api.send('POST', `${url}/offsets/reset`, { mode: 'skip_to_now', confirm: name });
  const planned = await api.send<{ sections: { report: { checks: Check[] } }[] }>('POST', `${url}/preflight`, {});
  const destCheck = planned.sections.flatMap((s) => s.report.checks).find((c) => c.code === 'flink_destination_columns');
  if (j.byHand) {
    // Paimon: BeziSync can't add the columns; the check gives the statements to run.
    expect(destCheck?.status, JSON.stringify(destCheck)).toBe('fail');
    expect(destCheck?.fix?.code ?? '').toContain('ADD `f2` STRING');
    await api.send('DELETE', `${url}?drop_destinations=true`);
    await ownerSql(`DROP TABLE IF EXISTS ${table}`);
    return;
  }
  if (j.type !== 'kafka' && j.type !== 'elasticsearch') {
    expect(destCheck?.status, JSON.stringify(destCheck)).toBe('pass');
    expect(destCheck?.message, JSON.stringify(destCheck)).toContain('f2');
  }
  await api.send('POST', `${url}/deploy`, {});
  await api.send('POST', `${url}/resume`);
  await lifecycle(api, url, 'streaming');
  await ownerSql(j.newColumns === 'send'
    ? `INSERT INTO ${table} (id, a, bb, c) VALUES (3, 'a3', 'bb3', 'c3')`
    : `INSERT INTO ${table} (id, a, b, c) VALUES (3, 'a3', 'b3', 'c3')`);
  await waitFor(`${j.type}: the row written after the planned start, every value in its own column`, async () => {
    const all = await rows(api, url, table);
    const r = all.get(3);
    if (!r) return false;
    expect(r.a, JSON.stringify(r)).toBe('a3');
    expect(r.f1, JSON.stringify(r)).toBe('one');
    expect(r.f2, JSON.stringify(r)).toBe('two');
    expect(r.label, JSON.stringify(r)).toBe('A3');
    expect(r[j.op], JSON.stringify(r)).toBe('+I');
    if (j.newColumns === 'send') expect(r.bb, JSON.stringify(r)).toBe('bb3');
    // The rows written before keep their values where they were.
    expect(all.get(1)?.a, JSON.stringify(all.get(1))).toBe('a1');
    expect(all.get(1)?.f1, JSON.stringify(all.get(1))).toBe('one');
    return true;
  }, 600_000);

  // L16: a position saved before the planned start can't be restored.
  await api.send('POST', `${url}/pause`);
  await lifecycle(api, url, 'paused', 300_000);
  const old = before.positions[before.positions.length - 1];
  const restore = await api.raw('POST', `${url}/positions/${old.id}/restore`, { confirm: name });
  expect(restore.status(), await restore.text()).toBe(409);
  expect(await restore.text()).toMatch(/position_before_(layout|tables)|started over/);

  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor(`${j.type} deleted`, async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 300_000);
  await ownerSql(`DROP TABLE IF EXISTS ${table}`);
}

for (const j of JOURNEYS) {
  test(`Flink CDC → ${j.type}: columns land in their own place across live changes, a refused change, Start over and a planned start`, async () => {
    test.skip(!!j.olap && !OLAP, 'set E2E_OLAP to run the StarRocks and Doris journeys');
    test.setTimeout(2_400_000);
    await journey(j);
  });
}
