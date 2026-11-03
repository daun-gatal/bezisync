import { expect, test } from '@playwright/test';
import { Api, defaultProject, signIn, unique } from './helpers';
import { HTTP_ECHO, SOURCE_PG } from './fixtures';
import { ownerSql, waitFor } from './data';

/** Schema drift is detected and reviewed; snapshots wait for the maintenance window. */
test('a breaking schema change is flagged and reviewed; snapshots wait for the window', async ({ page }) => {
  test.setTimeout(600_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('sch').replace(/-/g, '_');
  const table = `shop.drift_${tag}`;
  await ownerSql(`CREATE TABLE ${table} (id serial PRIMARY KEY, amount int)`);
  try {
    const { password, ...pg } = SOURCE_PG;
    const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`.replace(/_/g, '-'), kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
    const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `src-${tag}`.replace(/_/g, '-'), config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
    const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'http', name: `h-${tag}`.replace(/_/g, '-'), config: { url: `${HTTP_ECHO}/drift` } }, 201);
    // A window that is closed now (opens in two hours).
    const opens = new Date(Date.now() + 2 * 3600_000);
    const closes = new Date(Date.now() + 3 * 3600_000);
    const hhmm = (d: Date) => d.toISOString().slice(11, 16);
    const window = `${hhmm(opens)}-${hhmm(closes)}`;
    const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
      name: `drift-${tag}`.replace(/_/g, '-'), source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
      spec: { source: { tables: { include: [table] } }, snapshot: { mode: 'no_data', window } },
    }, 201);
    const url = `${base}/pipelines/${p.id}`;
    await api.send('POST', `${url}/deploy`, {});
    await waitFor('streaming', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'streaming', 420_000);
    await waitFor('schema baseline', async () => {
      const s = await api.get<{ tables: Record<string, unknown[]> }>(`${url}/schema`);
      return Boolean(s.tables[table]);
    }, 120_000);

    await ownerSql(`ALTER TABLE ${table} ALTER COLUMN amount TYPE bigint, ADD COLUMN note text`);

    await signIn(page);
    await page.goto(`/p/default/pipelines/${p.id}?tab=schema`);
    await expect(page.getByText('1 breaking change to review')).toBeVisible({ timeout: 120_000 });
    await expect(page.getByText(`${table}.amount changed type integer → bigint`)).toBeVisible();
    await expect(page.getByText(`${table}.note added (text)`)).toBeVisible();
    await page.getByRole('button', { name: 'Mark as reviewed' }).click();
    await expect(page.getByText('1 breaking change to review')).toHaveCount(0);

    // Snapshots wait for the maintenance window unless run now.
    await page.goto(`/p/default/pipelines/${p.id}?tab=offsets`);
    await page.getByRole('checkbox', { name: table }).check();
    await page.getByRole('button', { name: 'Start snapshot' }).click();
    await expect(page.getByText(`Snapshot scheduled for the ${window} UTC window`)).toBeVisible();
    await expect(page.getByText('Snapshot waiting for the maintenance window')).toBeVisible({ timeout: 60_000 });

    await api.send('DELETE', url);
    await waitFor('deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
  } finally {
    await ownerSql(`DROP TABLE IF EXISTS ${table}`);
  }
});

test('the delivery canary proves changes reach the destination', async ({ page }) => {
  test.setTimeout(600_000);
  const { REDIS } = await import('./fixtures');
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('can');
  const { password, ...pg } = SOURCE_PG;
  const { password: redisPassword, ...redis } = REDIS;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'redis', name: `r-${tag}`, config: redis, secrets: { password: redisPassword } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `canary-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, sink: { naming: `c-${tag}-{table}` }, snapshot: { mode: 'no_data' }, policies: { delivery_canary: true } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, {});
  const canary = await waitFor('canary verified', async () => {
    const cur = await api.get<{ lifecycle: string; status: { canary?: { ok?: boolean; latency_ms?: number } } }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.status.canary?.ok ? cur.status.canary : undefined;
  }, 420_000);
  expect(canary.latency_ms).toBeGreaterThanOrEqual(0);

  await signIn(page);
  await page.goto(`/p/default/pipelines/${p.id}`);
  await expect(page.getByText('End-to-end delivery')).toBeVisible();
  await expect(page.getByText(/A canary row reached the destination/)).toBeVisible();

  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor('deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
});
