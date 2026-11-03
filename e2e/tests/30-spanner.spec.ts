import { expect, test } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';
import { KAFKA, SPANNER, SPANNER_REST_FROM_RUNNER } from './fixtures';
import { waitFor } from './data';

type Rec = { value: Record<string, unknown> };

/** Writes a row through the emulator's REST API (a commit with one mutation). */
async function spannerInsert(table: string, columns: string[], values: unknown[]): Promise<void> {
  const session = (await (await fetch(`${SPANNER_REST_FROM_RUNNER}/sessions`, { method: 'POST', body: '{}' })).json()) as { name: string };
  const res = await fetch(`http://bezisync-e2e-k3s:30920/v1/${session.name}:commit`, {
    method: 'POST',
    body: JSON.stringify({ singleUseTransaction: { readWrite: {} }, mutations: [{ insertOrUpdate: { table, columns, values: [values] } }] }),
  });
  expect(res.ok, await res.text()).toBe(true);
}

test('Spanner → Kafka: coordination Kafka picked from the project, a removed column, no copy of existing rows', async ({ page }) => {
  test.setTimeout(900_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('sp');
  const kafka = await api.send<{ id: string; name: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `coord-${tag}`, config: KAFKA }, 201);

  // The connection form offers the project's Kafka connections and the change stream choice.
  await signIn(page);
  await page.goto(`/p/${project.slug}/connections`);
  await page.getByRole('button', { name: 'Add source' }).first().click();
  const dialog = page.getByRole('dialog');
  const card = dialog.getByRole('button', { name: /Google Cloud Spanner/ });
  await expect(card.getByText('Preview')).toBeVisible();
  await card.click();
  await choose(dialog.getByLabel('Coordination Kafka'), kafka.name);
  await expect(dialog.getByLabel('Change stream name')).toHaveCount(0);
  await choose(dialog.getByLabel('Change stream', { exact: true }), /Use an existing change stream/);
  await expect(dialog.getByLabel('Change stream name')).toBeVisible();
  await page.keyboard.press('Escape');

  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string; config: Record<string, unknown> }>('POST', `${base}/connections`, {
    kind: 'source', type: 'spanner', provider: 'gcp', name: `src-${tag}`, config: { ...SPANNER, coordination_kafka: kafka.name },
  }, 201);
  expect(src.config.coordination_kafka).toBe(kafka.id);
  const report = await api.send<{ ok: boolean; checks: { code: string; status: string; message: string }[] }>('POST', `${base}/connections/${src.id}/check`, { tables: ['orders.Orders'] });
  expect(report.ok, JSON.stringify(report.checks.filter((c) => c.status === 'fail'))).toBe(true);
  expect(report.checks.map((c) => c.code)).toEqual(expect.arrayContaining(['sp_database', 'sp_change_stream', 'sp_coordination', 'kafka_create_topics']));
  // The coordination Kafka cannot be deleted while the source names it.
  const blocked = await api.raw('DELETE', `${base}/connections/${kafka.id}`);
  expect(blocked.status()).toBe(409);

  // A row written before the pipeline starts is never copied: Spanner streams from the start.
  await spannerInsert('Orders', ['OrderId', 'Status', 'Email', 'Total'], ['1', 'OLD', 'old@example.com', '1']);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `k-${tag}`, config: KAFKA }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['orders.Orders'] }, columns: { 'orders.Orders': { exclude: ['Email'], mask: [] } } }, sink: { naming: `src.${tag}.{table}` } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, {});
  await waitFor('spanner streaming', async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 480_000);
  const marker = `LIVE-${tag}`;
  const records = await waitFor('live change delivered', async () => {
    await spannerInsert('Orders', ['OrderId', 'Status', 'Email', 'Total'], [String(Date.now()), marker, 'secret@example.com', '3']);
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=orders.Orders&limit=200`);
    return e.records.some((r) => r.value.Status === marker) ? e.records : undefined;
  }, 300_000, 15_000);
  const live = records.find((r) => r.value.Status === marker)!;
  expect(live.value).not.toHaveProperty('Email'); // left out by the pipeline's change stream
  expect(live.value.Total).toBe('3');
  expect(records.some((r) => r.value.Status === 'OLD')).toBe(false);

  // Source protection in Spanner's terms; no re-copy offered.
  const impact = await waitFor('source impact measured', async () => {
    const i = (await api.get<{ status: { source_impact?: { retention_seconds?: number | null; behind_seconds?: number | null } } }>(url)).status.source_impact;
    return i && typeof i.retention_seconds === 'number' && typeof i.behind_seconds === 'number' ? i : undefined;
  }, 180_000);
  expect(impact.retention_seconds).toBe(7 * 86400);
  await page.goto(`/p/${project.slug}/pipelines/${p.id}`);
  const protection = page.getByRole('region', { name: 'Source protection' });
  await expect(protection.getByText('Change stream keeps changes for')).toBeVisible({ timeout: 60_000 });
  await expect(protection.getByText('7 d', { exact: true })).toBeVisible();
  await page.getByRole('tab', { name: 'Snapshots' }).click();
  await expect(page.getByText(/Google Cloud Spanner can’t copy the rows a table already has/)).toBeVisible();

  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor('deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
});
