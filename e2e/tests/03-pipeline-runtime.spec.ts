import { expect, test } from '@playwright/test';
import { Api, defaultProject, unique } from './helpers';
import { KAFKA, SOURCE_PG } from './fixtures';
import { sql, waitFor } from './data';

interface Pipeline {
  id: string;
  lifecycle: string;
  health: string;
  status: { summary?: string; reasons?: { code: string; message: string }[]; metrics?: Record<string, unknown> };
  artifacts: { cleanup?: { kind: string; name: string; outcome: string }[]; destinations?: Record<string, string> };
  version: number;
  current_version: number;
}

test.describe.configure({ mode: 'serial' });

test('Postgres → Kafka on Kubernetes: deploy, stream real changes, pause, resume, delete with cleanup', async () => {
  test.setTimeout(600_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('rt');

  const runtime = await api.send<{ id: string; last_test: { ok: boolean; message: string } }>('POST', `${base}/runtimes`, {
    name: `k8s-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime', in_cluster: true },
  }, 201);
  expect(runtime.last_test.ok, runtime.last_test.message).toBe(true);
  const { password, ...pgConfig } = SOURCE_PG;
  const source = await api.send<{ id: string }>('POST', `${base}/connections`, {
    kind: 'source', type: 'postgresql', name: `shop-${tag}`, config: { ...pgConfig, ssl_mode: 'disable' }, secrets: { password },
  }, 201);
  const sink = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `kafka-${tag}`, config: KAFKA }, 201);

  const naming = `e2e.${tag}.{table}`;
  const pipeline = await api.send<Pipeline>('POST', `${base}/pipelines`, {
    name: `orders-${tag}`, owner: 'e2e', source_id: source.id, sink_id: sink.id, runtime_id: runtime.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, sink: { naming }, policies: { heartbeat_seconds: 5 } },
  }, 201);
  const url = `${base}/pipelines/${pipeline.id}`;

  const pre = await api.send<{ ok: boolean; summary: string }>('POST', `${url}/preflight`);
  expect(pre.ok, pre.summary).toBe(true);
  await api.send('POST', `${url}/deploy`, {});

  const streaming = await waitFor('pipeline streaming', async () => {
    const p = await api.get<Pipeline>(url);
    if (p.lifecycle === 'failed') throw new Error(`failed: ${JSON.stringify(p.status)}`);
    return p.lifecycle === 'streaming' && p.health === 'healthy' ? p : undefined;
  }, 420_000);
  expect(streaming.status.metrics).toBeTruthy();

  const topic = `e2e.${tag}.orders`;
  const events = async () => api.get<{ destination: string; records: { value: Record<string, unknown> }[]; masked_fields: string[] }>(
    `${url}/events?table=shop.orders&limit=500`);
  // The initial snapshot copies the seeded rows; then a live insert must arrive too.
  const snap = await waitFor('snapshot rows delivered', async () => {
    const e = await events();
    return e.records.length >= 200 ? e : undefined;
  }, 120_000);
  expect(snap.destination).toBe(topic);
  const [row] = await sql<{ id: number }>("INSERT INTO shop.orders (customer_id, status, total) VALUES (1, 'E2E-LIVE', 9.99) RETURNING id");
  const live = await waitFor('live insert delivered', async () =>
    (await events()).records.map((r) => r.value).find((v) => v.id === row!.id && v.status === 'E2E-LIVE'), 120_000);
  expect(live.__op).toBe('c');
  expect(typeof live.__source_ts_ms).toBe('number');

  const slot = `bz_${pipeline.id.replace(/-/g, '').slice(0, 12)}`;
  await waitFor('slot active', async () => (await sql<{ active: boolean }>('SELECT active FROM pg_replication_slots WHERE slot_name = $1', [slot]))[0]?.active);

  // Operate: the source impact probe and the metric history come from real observations.
  type Impact = { retained_bytes: number | null; slot_active: boolean | null; summary: string };
  const impact = await waitFor('source impact measured', async () => {
    const st = (await api.get<Pipeline & { status: { source_impact?: Impact } }>(url)).status.source_impact;
    return st && st.slot_active ? st : undefined;
  }, 120_000);
  expect(typeof impact.retained_bytes).toBe('number');
  const series = await waitFor('metric samples', async () => {
    const m = await api.get<{ points: { events_per_second: number | null; lag_ms: number | null }[] }>(`${url}/metrics?range=1h`);
    return m.points.some((pt) => pt.events_per_second !== null) ? m : undefined;
  }, 120_000);
  expect(series.points.length).toBeGreaterThan(0);

  await api.send('POST', `${url}/pause`);
  await waitFor('paused', async () => (await api.get<Pipeline>(url)).lifecycle === 'paused', 120_000);
  await sql('INSERT INTO shop.orders (customer_id, status, total) VALUES (2, $1, 1)', [`PAUSED-${tag}`]);
  await api.send('POST', `${url}/resume`);
  await waitFor('streaming again', async () => (await api.get<Pipeline>(url)).lifecycle === 'streaming', 300_000);
  await waitFor('change made while paused is delivered after resume', async () =>
    (await events()).records.some((r) => r.value.status === `PAUSED-${tag}`), 180_000);

  const logs = await api.get<{ lines: string[] }>(`${url}/logs?tail=500`);
  expect(logs.lines.join('\n')).not.toContain(SOURCE_PG.password);

  await api.send('DELETE', `${url}?drop_destinations=true`);
  const gone = await waitFor('cleanup finished', async () => {
    const p = await api.get<Pipeline>(url);
    return p.lifecycle === 'deleted' || p.lifecycle === 'cleanup_failed' ? p : undefined;
  }, 240_000);
  expect(gone.lifecycle, JSON.stringify(gone.artifacts.cleanup)).toBe('deleted');
  const outcomes = Object.fromEntries((gone.artifacts.cleanup ?? []).map((r) => [`${r.kind}:${r.name}`, r.outcome]));
  expect(outcomes[`replication_slot:${slot}`]).toBe('removed');
  expect(outcomes[`publication:${slot}`]).toBe('removed');
  expect(outcomes[`topic:${topic}`]).toBe('removed');
  expect(await sql('SELECT 1 FROM pg_replication_slots WHERE slot_name = $1', [slot])).toHaveLength(0);
});

test('admin connects a Kubernetes runtime from Settings and sees it ready', async ({ page }) => {
  const { signIn } = await import('./helpers');
  await signIn(page);
  await page.goto('/p/default/settings/runtimes');
  await page.getByRole('button', { name: 'Add runtime' }).click();
  const dialog = page.getByRole('dialog');
  const name = unique('ui-k8s');
  await dialog.getByLabel('Name', { exact: true }).fill(name);
  await dialog.getByLabel('Namespace').fill('bezisync-e2e-runtime');
  await dialog.getByRole('button', { name: 'Add runtime' }).click();
  await expect(page.getByText(new RegExp(`Added ${name}: Can run connectors`))).toBeVisible();
  await expect(page.getByText(name, { exact: true })).toBeVisible();
});
