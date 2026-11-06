import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';
import { Api, defaultProject, signIn, unique } from './helpers';
import { ICEBERG_REST, SOURCE_PG } from './fixtures';
import { ownerSql, waitFor } from './data';
import { layoutProblems } from './layout';
import { walkWizard } from './walk';

/*
 * The Flink CDC engine end to end: an Admin registers the image, an engineer creates a
 * Postgres → Iceberg pipeline on it, deploys it, sees rows arrive, and walks every wizard step
 * (autosave round-trips). BeziSync publishes no Flink CDC image, so this runs only when the run
 * provides one: `E2E_FLINK_IMAGE=<local image> scripts/e2e.sh …` imports it into the cluster.
 */
const FLINK_IMAGE = process.env.E2E_FLINK_IMAGE ?? '';

test.describe.configure({ mode: 'serial' });
test.skip(!FLINK_IMAGE, 'set E2E_FLINK_IMAGE to run the Flink CDC journeys');

type Rec = { value: Record<string, unknown> };

async function registerFlinkImage(api: Api): Promise<void> {
  const catalog = await api.get<{ entries: { image: string }[] }>('/api/v1/debezium-catalog?engine=flink-cdc');
  if (catalog.entries.some((e) => e.image === FLINK_IMAGE)) return;
  await api.send('POST', '/api/v1/debezium-catalog', { image: FLINK_IMAGE, engine: 'flink-cdc', is_default: true, label: 'e2e' }, 201);
}

test('Flink CDC: Postgres → Iceberg streams, and every wizard step round-trips', async ({ page }) => {
  test.setTimeout(1_200_000);
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const engines = await api.get<{ id: string; available: boolean; reason?: string }[]>('/api/v1/engines');
  const flink = engines.find((e) => e.id === 'flink-cdc');
  expect(flink?.available, flink?.reason ?? 'no Flink CDC engine').toBe(true);

  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('flink');
  // Its own table: Flink CDC needs the whole previous row of an update (REPLICA IDENTITY FULL),
  // which the shared shop tables don't log.
  const table = `shop.flink_${tag.replace(/-/g, '_')}`;
  await ownerSql(`CREATE TABLE ${table} (id serial PRIMARY KEY, status text, total int); ALTER TABLE ${table} REPLICA IDENTITY FULL`);
  const { password, ...pg } = SOURCE_PG;
  const { secret_access_key, ...iceberg } = ICEBERG_REST;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'iceberg', name: `ice-${tag}`, config: iceberg, secrets: { secret_access_key } }, 201);
  const naming = `f_${tag.replace(/-/g, '_')}_{table}`;
  const p = await api.send<{ id: string; engine: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
    spec: { source: { tables: { include: [table] } }, sink: { naming }, snapshot: { mode: 'no_data' } },
  }, 201);
  expect(p.engine).toBe('flink-cdc');
  const url = `${base}/pipelines/${p.id}`;

  // Every wizard step before the first deploy.
  await signIn(page);
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });

  await api.send('POST', `${url}/deploy`, {});
  await waitFor('Flink CDC pipeline streaming', async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 900_000);

  const marker = `FLINK-${tag}`;
  await ownerSql(`INSERT INTO ${table} (status, total) VALUES ('${marker}', 1)`);
  await waitFor('row readable in Iceberg through BeziSync', async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=100`);
    return e.records.some((r) => r.value.status === marker);
  }, 420_000);

  // The pipeline page speaks Flink CDC: the cluster card and Positions instead of Snapshots.
  await page.goto(`/p/${project.slug}/pipelines/${p.id}`);
  await expect(page.getByText('Flink CDC cluster')).toBeVisible();
  await expect(page.getByRole('tab', { name: 'Positions' })).toBeVisible();
  await page.getByRole('tab', { name: 'Positions' }).click();
  await expect(page.getByText('Saved positions')).toBeVisible();

  // Every wizard step of the deployed pipeline (an edit starts a new draft version).
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });

  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor('Flink CDC pipeline deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 300_000);
  await ownerSql(`DROP TABLE IF EXISTS ${table}`);
});

test('Flink CDC: a custom cluster is packed at deploy, streams, and round-trips', async ({ page }) => {
  test.setTimeout(1_200_000);
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('flinkc');
  const table = `shop.flink_${tag.replace(/-/g, '_')}`;
  await ownerSql(`CREATE TABLE ${table} (id serial PRIMARY KEY, status text, total int); ALTER TABLE ${table} REPLICA IDENTITY FULL`);
  const { password, ...pg } = SOURCE_PG;
  const { secret_access_key, ...iceberg } = ICEBERG_REST;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'iceberg', name: `ice-${tag}`, config: iceberg, secrets: { secret_access_key } }, 201);
  // Three readers: not a preset's shape (two slots per TaskManager).
  const cluster = { parallelism: 3, slot: { cpu: '0.5', memory: '768Mi' }, job_manager: { cpu: '0.5', memory: '1Gi' }, shape: 'auto' };
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
    spec: { source: { tables: { include: [table] } }, sink: { naming: `f_${tag.replace(/-/g, '_')}_{table}` }, snapshot: { mode: 'no_data' },
      policies: { resources: 'custom' }, flink_cdc: { cluster } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;

  // The wizard keeps the cluster on every step (Custom shown in Review).
  await signIn(page);
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });
  expect((await api.get<{ spec: { flink_cdc: { cluster: unknown } } }>(url)).spec.flink_cdc.cluster).toEqual(cluster);

  // The k3s runtime reports its nodes: the fit is checked, never left unknown.
  const pre = await api.send<{ sections: { key: string; report: { checks: { code: string; status: string; message: string }[] } }[] }>('POST', `${url}/preflight`, {});
  const fit = pre.sections.find((s) => s.key === 'runtime')?.report.checks.find((c) => c.code === 'flink_cluster_fit');
  expect(fit?.status, fit?.message).toBe('pass');
  expect(fit?.message).toContain('1 TaskManager × 3 slots');

  await api.send('POST', `${url}/deploy`, {});
  const deployed = await api.get<{ artifacts: { flink_shape?: { size: { slots: number; readers: number } } } }>(url);
  expect(deployed.artifacts.flink_shape?.size).toMatchObject({ slots: 3, readers: 3 });
  await waitFor('custom Flink CDC cluster streaming', async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 900_000);
  const marker = `FLINKC-${tag}`;
  await ownerSql(`INSERT INTO ${table} (status, total) VALUES ('${marker}', 1)`);
  await waitFor('row readable in Iceberg', async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=100`);
    return e.records.some((r) => r.value.status === marker);
  }, 420_000);

  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor('custom Flink CDC pipeline deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 300_000);
  await ownerSql(`DROP TABLE IF EXISTS ${table}`);
});

test('Flink CDC: an Expert sizes a custom cluster in Review, automatic then manual', async ({ page }) => {
  test.setTimeout(600_000);
  const api = await Api.signIn();
  await registerFlinkImage(api);
  await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'expert' });
  const tag = unique('flinks');
  const table = `shop.flink_${tag.replace(/-/g, '_')}`;
  try {
    const project = await defaultProject(api);
    const base = `/api/v1/projects/${project.id}`;
    await ownerSql(`CREATE TABLE ${table} (id serial PRIMARY KEY, status text, total int); ALTER TABLE ${table} REPLICA IDENTITY FULL`);
    const { password, ...pg } = SOURCE_PG;
    const { secret_access_key, ...iceberg } = ICEBERG_REST;
    const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
    const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
    const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'iceberg', name: `ice-${tag}`, config: iceberg, secrets: { secret_access_key } }, 201);
    const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
      name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
      spec: { source: { tables: { include: [table] } }, snapshot: { mode: 'no_data' } },
    }, 201);
    const url = `${base}/pipelines/${p.id}`;
    type Saved = { spec: { policies: { resources: string }; flink_cdc: { cluster?: Record<string, unknown> } } };

    await signIn(page);
    await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=review`);
    await page.getByRole('radio', { name: /Custom/ }).click();
    const card = page.getByRole('region', { name: 'Cluster' });
    await expect(card).toBeVisible();
    // It starts from Small: the same 2 readers. One more, and the diagram shows how it packs.
    await expect(card.getByLabel('Readers', { exact: true })).toHaveValue('2');
    await card.getByRole('button', { name: 'Increase' }).click();
    await expect(card.getByText(/1 TaskManager × 3 slots/).first()).toBeVisible({ timeout: 20_000 });
    await expect(card.getByText(/Fits on one node|Split across/)).toBeVisible();
    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
    let saved = await api.get<Saved>(url);
    expect(saved.spec.policies.resources).toBe('custom');
    expect(saved.spec.flink_cdc.cluster).toEqual({ parallelism: 3, slot: { cpu: '0.5', memory: '768Mi' }, job_manager: { cpu: '0.5', memory: '1Gi' }, shape: 'auto' });

    // Manual: seeded from the packed shape (rounded up to a choice), then two TaskManagers.
    await card.getByRole('button', { name: 'Manual' }).click();
    await card.getByLabel('TaskManagers', { exact: true }).fill('2');
    await card.getByLabel('TaskManagers', { exact: true }).blur();
    await expect(card.getByText(/2 TaskManagers × 3 slots/).first()).toBeVisible({ timeout: 20_000 });
    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
    saved = await api.get<Saved>(url);
    expect(saved.spec.flink_cdc.cluster).toMatchObject({ shape: 'manual', task_managers: { count: 2, slots: 3, cpu: '1.5', memory: '3Gi' } });
    // An impossible shape says why and isn't saved.
    await card.getByLabel('Slots per TaskManager', { exact: true }).fill('1');
    await card.getByLabel('Slots per TaskManager', { exact: true }).blur();
    await expect(card.getByText('can’t hold 3 readers')).toBeVisible();
    await card.getByLabel('Slots per TaskManager', { exact: true }).fill('3');
    await card.getByLabel('Slots per TaskManager', { exact: true }).blur();
    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });

    // Narrow screens: the card and its diagram wrap; and it is accessible.
    await page.setViewportSize({ width: 390, height: 900 });
    expect(await layoutProblems(page)).toEqual([]);
    const axe = await new AxeBuilder({ page }).include('[aria-label="Cluster"]').withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
    expect(axe.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical').map((v) => v.id)).toEqual([]);
    await page.setViewportSize({ width: 1280, height: 900 });

    // Every step keeps the cluster, and nothing keeps saving.
    await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });
    expect((await api.get<Saved>(url)).spec.flink_cdc.cluster).toMatchObject({ shape: 'manual', parallelism: 3 });
    await api.send('DELETE', url);
  } finally {
    await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'guided' });
    await ownerSql(`DROP TABLE IF EXISTS ${table}`);
  }
});
