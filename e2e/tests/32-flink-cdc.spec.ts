import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';
import { ICEBERG_REST, KAFKA, MONGO, ORACLE, ORACLE_XE, SOURCE_PG } from './fixtures';
import { oracleSQL, ownerSql, waitFor } from './data';
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
  // A computed column, and the routed Iceberg table partitioned into buckets of its key.
  const computed = { type: 'computed', id: 'cc01', table, name: 'status_upper', function: 'upper', args: [{ column: 'status' }] };
  const partitionBy = { [table]: [{ n: 4, column: 'id', transform: 'bucket' }] };
  // The table goes to a routed name instead of the naming pattern's.
  const routedTo = `r_${tag.replace(/-/g, '_')}`;
  // Three readers: not a preset's shape (two slots per TaskManager); smaller once it streams.
  const cluster = {
    parallelism: 3, slot: { cpu: '0.5', memory: '768Mi' }, streaming_slot: { cpu: '0.25', memory: '512Mi' }, job_manager: { cpu: '0.5', memory: '1Gi' }, shape: 'auto',
  };
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
    spec: { source: { tables: { include: [table] } }, sink: { naming: `f_${tag.replace(/-/g, '_')}_{table}`, options: { partition_by: partitionBy } }, snapshot: { mode: 'no_data' },
      policies: { resources: 'custom' }, flink_cdc: { cluster, commit_seconds: 30 },
      transforms: [{ type: 'soft_delete', id: 'sd01' }, { type: 'route', id: 'rt01', routes: [{ match: table, to: routedTo }] }, computed] },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;

  // The wizard keeps the cluster on every step (Custom shown in Review).
  await signIn(page);
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });
  const walked = await api.get<{ spec: { flink_cdc: { cluster: unknown }; transforms: unknown[]; sink: { options: { partition_by?: unknown } } } }>(url);
  expect(walked.spec.flink_cdc.cluster).toEqual(cluster);
  expect(walked.spec.transforms).toContainEqual(computed);
  expect(walked.spec.sink.options.partition_by).toEqual(partitionBy);

  // The k3s runtime reports its nodes: the fit is checked, never left unknown.
  const pre = await api.send<{ sections: { key: string; report: { checks: { code: string; status: string; message: string }[] } }[] }>('POST', `${url}/preflight`, {});
  const fit = pre.sections.find((s) => s.key === 'runtime')?.report.checks.find((c) => c.code === 'flink_cluster_fit');
  expect(fit?.status, fit?.message).toBe('pass');
  expect(fit?.message).toContain('1 TaskManager × 3 slots');
  const parts = pre.sections.flatMap((s) => s.report.checks).find((c) => c.code === 'iceberg_partition_column');
  expect(parts?.status, parts?.message).toBe('pass');

  await api.send('POST', `${url}/deploy`, {});
  type Shape = { artifacts: { flink_shape?: { phase: string; size: { slots: number; readers: number; task_manager_memory: string }; streaming_size: { task_manager_memory: string } } } };
  const deployed = await api.get<Shape>(url);
  expect(deployed.artifacts.flink_shape?.size).toMatchObject({ slots: 3, readers: 3, task_manager_memory: '2816Mi' });
  expect(deployed.artifacts.flink_shape?.phase).toBe('copy');
  await waitFor('custom Flink CDC cluster streaming', async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 900_000);
  const marker = `FLINKC-${tag}`;
  await ownerSql(`INSERT INTO ${table} (status, total) VALUES ('${marker}', 1)`);
  await waitFor('row readable in Iceberg', async () => {
    const e = await api.get<{ records: Rec[]; destination: string }>(`${url}/events?table=${table}&limit=100`);
    expect(e.destination).toContain(routedTo);
    return e.records.some((r) => r.value.status === marker && r.value.status_upper === marker.toUpperCase());
  }, 420_000);

  // The copy is done and a position saved after it: the controller shrinks the cluster, which
  // restarts from its saved position and goes on streaming (R25, R17).
  await waitFor('cluster shrunk after its copy', async () => (await api.get<Shape>(url)).artifacts.flink_shape?.phase === 'streaming', 420_000);
  const events = await api.get<{ items: { kind: string; actor_label: string }[] }>(`/api/v1/projects/${project.id}/events?resource_id=${p.id}&limit=100`);
  expect(events.items.filter((e) => e.kind === 'pipeline.resized').map((e) => e.actor_label)).toEqual(['controller']);
  await waitFor('streaming again on the smaller shape', async () => {
    const cur = await api.get<{ lifecycle: string; status: { flink?: { taskmanagers_wanted?: number } } }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 600_000);
  const after = `FLINKC2-${tag}`;
  await ownerSql(`INSERT INTO ${table} (status, total) VALUES ('${after}', 2)`);
  await waitFor('a row written after the shrink is readable', async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=100`);
    return e.records.some((r) => r.value.status === after);
  }, 420_000);

  // T4: with a soft delete, a deleted row stays in the destination, marked with its change type.
  await ownerSql(`DELETE FROM ${table} WHERE status = '${after}'`);
  await waitFor('the deleted row kept, marked -D', async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=100`);
    return e.records.some((r) => r.value.status === after && r.value.__op === '-D');
  }, 420_000);

  // T18: the table exists, bucketed by 4; asking for 8 now would never apply, so it blocks.
  const now = await api.get<{ version: number; spec: { sink: { options: Record<string, unknown> } } & Record<string, unknown> }>(url);
  await api.patch(url, { spec: { ...now.spec, sink: { ...now.spec.sink, options: { ...now.spec.sink.options, partition_by: { [table]: [{ n: 8, column: 'id', transform: 'bucket' }] } } } } }, now.version);
  const repart = await api.send<{ sections: { report: { checks: { code: string; status: string; message: string }[] } }[] }>('POST', `${url}/preflight`, {});
  const existing = repart.sections.flatMap((s) => s.report.checks).find((c) => c.code === 'iceberg_partition_existing');
  expect(existing?.status).toBe('fail');
  expect(existing?.message).toContain('bucket[4](id)');

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

    // A streaming size no smaller than the copy size: each field says so, and the badge names them.
    await card.getByRole('switch', { name: /Shrink after the first copy/ }).click();
    await choose(card.getByLabel('Streaming reader CPU', { exact: true }), '0.5 CPU');
    await choose(card.getByLabel('Streaming reader memory', { exact: true }), '768 MiB');
    await expect(card.locator('[data-review-field="streaming-cpu"]').getByText('nothing to shrink to')).toBeVisible();
    await expect(card.locator('[data-review-field="streaming-memory"]').getByText('nothing to shrink to')).toBeVisible();
    await expect(page.getByRole('button', { name: 'Not saved: fix Streaming reader CPU and 1 more' })).toBeVisible();
    await card.getByRole('switch', { name: /Shrink after the first copy/ }).click();
    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });

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

    // Raw Flink CDC options: a pass-through one is kept (not verified), a catalog one refused.
    const addRow = page.getByRole('button', { name: 'Add a property' });
    await addRow.click();
    await page.getByLabel('Property 1 name').fill('sink.catalog.properties.uri');
    await page.getByLabel('Property 1 value').fill('http://elsewhere');
    await expect(page.getByText('Not allowed')).toBeVisible();
    // A refused option blocks the save in the browser: no request loop, the engine's own label.
    await expect(page.getByRole('group', { name: /Advanced Flink CDC options/ })).toBeVisible();
    const refusedSaves: string[] = [];
    const onSave = (r: { method(): string; url(): string }) => { if (r.method() === 'PATCH' && r.url().endsWith(url)) refusedSaves.push(r.url()); };
    page.on('request', onSave);
    await page.waitForTimeout(4_000);
    page.off('request', onSave);
    expect(refusedSaves.length, 'a refused option kept saving').toBeLessThanOrEqual(1);
    await page.getByLabel('Property 1 name').fill('sink.table.properties.write.parquet.compression-codec');
    await page.getByLabel('Property 1 value').fill('zstd');
    await expect(page.getByText('Not verified')).toBeVisible();
    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
    expect((await api.get<{ spec: { advanced: Record<string, string> } }>(url)).spec.advanced).toEqual({ 'sink.table.properties.write.parquet.compression-codec': 'zstd' });

    // Every step keeps the cluster, and nothing keeps saving.
    await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });
    expect((await api.get<Saved>(url)).spec.flink_cdc.cluster).toMatchObject({ shape: 'manual', parallelism: 3 });
    await api.send('DELETE', url);
  } finally {
    await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'guided' });
    await ownerSql(`DROP TABLE IF EXISTS ${table}`);
  }
});

test('Flink CDC: an editor adds a computed column and partitions the Iceberg table in the wizard', async ({ page }) => {
  test.setTimeout(300_000);
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('flinkx');
  const { password, ...pg } = SOURCE_PG;
  const { secret_access_key, ...iceberg } = ICEBERG_REST;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'iceberg', name: `ice-${tag}`, config: iceberg, secrets: { secret_access_key } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
    spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  type Saved = { spec: { transforms: Record<string, unknown>[]; sink: { options: { partition_by?: unknown } } } };
  await signIn(page);

  // Destination: Flink CDC can't write an Iceberg change log; the choice says why (ADR 0007 P4),
  // and Partition by has moved to Transform.
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=sink`);
  await expect(page.getByText('Partition by is chosen in Transform, after the key and renames.')).toBeVisible();
  await page.getByLabel('Rows').click();
  const append = page.getByRole('listbox').getByRole('option', { name: /Every change appended/ });
  await expect(append).toHaveAttribute('aria-disabled', 'true');
  await expect(append).toContainText("always writes rows by the table's key");
  await page.keyboard.press('Escape');
  await page.getByRole('button', { name: 'Continue to transform →' }).click();
  await expect(page.getByRole('heading', { name: 'Which rows and columns reach each table?' })).toBeVisible();

  // Transform: a computed column joining the status and the customer, picked, never typed as SQL.
  await page.getByRole('button', { name: 'Add transform' }).click();
  await page.getByRole('button', { name: /^Computed column/ }).click();
  const cc = page.getByRole('region', { name: /Computed column/ });
  await choose(cc.getByLabel(/^Table:/), 'shop.orders');
  await cc.getByLabel('New column').fill('status');
  await expect(cc.getByText('The event already has a field status.')).toBeVisible();
  await cc.getByLabel('New column').fill('order_label');
  await choose(cc.getByLabel(/^Function:/), 'Join');
  await choose(cc.getByLabel(/^Argument 1:/), 'status');
  await choose(cc.getByLabel(/^Argument 2 kind:/), 'A text');
  await cc.getByLabel('Argument 2', { exact: true }).fill('/');
  await cc.getByRole('button', { name: 'Add an argument' }).click();
  await choose(cc.getByLabel(/^Argument 3:/), 'customer_id');
  await cc.getByRole('button', { name: 'Done' }).click();
  await expect(page.getByText('shop.orders: order_label = Join(status, "/", customer_id)')).toBeVisible();

  // Partition by, on the same screen (ADR 0007): the computed column is offered at once, but rows
  // are upserted by key, so only the key partitions them (ADR 0005): a column outside it is offered
  // disabled, with the reason. The leading part of an increasing key gives insert-ordered ranges;
  // then buckets of it.
  const row = page.getByRole('region', { name: 'Partition by' });
  await expect(row.getByText('Not partitioned', { exact: true })).toBeVisible();
  await row.getByRole('button', { name: 'Edit' }).click();
  const part = page.getByRole('group', { name: 'Partition shop.orders' });
  await part.getByRole('button', { name: 'Add a partition field' }).click();
  await part.getByLabel('shop.orders field 1 column').click();
  for (const name of ['created_at', 'order_label']) {
    const option = page.getByRole('listbox').getByRole('option', { name, exact: true });
    await expect(option).toHaveAttribute('aria-disabled', 'true');
    await expect(option).toHaveAttribute('title', /Not part of the key/);
  }
  await page.getByRole('listbox').getByRole('option', { name: 'id', exact: true }).click();
  await choose(part.getByLabel('shop.orders field 1 transform'), 'Leading part');
  await part.getByLabel('shop.orders field 1 width').fill('100000');
  await part.getByRole('button', { name: 'Add a partition field' }).click();
  await choose(part.getByLabel('shop.orders field 2 column'), 'id');
  await choose(part.getByLabel('shop.orders field 2 transform'), 'Buckets of a hash');
  await part.getByLabel('shop.orders field 2 buckets').fill('2000');
  await expect(page.getByRole('button', { name: 'Continue to review →' })).toBeDisabled();
  await part.getByLabel('shop.orders field 2 buckets').fill('8');
  await expect(row.getByText('shop.orders: Leading part of id (100000), Buckets of id (8)')).toBeVisible();
  await page.getByRole('button', { name: 'Continue to review →' }).click();
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  const saved = await api.get<Saved>(url);
  // One save carries the computed column and Partition by together.
  expect(saved.spec.sink.options.partition_by).toEqual({ 'shop.orders': [{ n: 100000, column: 'id', transform: 'truncate' }, { n: 8, column: 'id', transform: 'bucket' }] });
  expect(saved.spec.transforms.find((t) => t.type === 'computed')).toMatchObject({
    table: 'shop.orders', name: 'order_label', function: 'concat', args: [{ column: 'status' }, { text: '/' }, { column: 'customer_id' }],
  });
  // The generated job carries it as SQL, quoted.
  const pre = await api.send<{ sections: { report: { checks: { code: string; status: string }[] } }[] }>('POST', `${url}/preflight`, {});
  const all = pre.sections.flatMap((s) => s.report.checks);
  expect(all.find((c) => c.code === 'iceberg_partition_column')?.status).toBe('pass');
  expect(all.find((c) => c.code === 'iceberg_partition_key')?.status).toBe('pass');
  expect(all.find((c) => c.code === 'transform_computed_input')).toBeUndefined();
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });
  // E13: the day the order was made is outside the key: an update that changes it would leave the
  // old row behind, so preflight blocks it (sent through the API, past the editor).
  const cur = await api.get<{ version: number; spec: { sink: { options: Record<string, unknown> } } & Record<string, unknown> }>(url);
  await api.patch(url, { spec: { ...cur.spec, sink: { ...cur.spec.sink, options: { ...cur.spec.sink.options, partition_by: { 'shop.orders': [{ n: 0, column: 'created_at', transform: 'day' }] } } } } }, cur.version);
  const unsafe = await api.send<{ sections: { report: { checks: { code: string; status: string; message: string }[] } }[] }>('POST', `${url}/preflight`, {});
  const key = unsafe.sections.flatMap((s) => s.report.checks).find((c) => c.code === 'iceberg_partition_key');
  expect(key?.status, key?.message).toBe('fail');
  expect(key?.message).toContain('day(created_at) (its key: id)');
  await api.send('DELETE', url);
});

test('Flink CDC: Keep deleted rows sits under Keep rows, and a filter goes before it', async ({ page }) => {
  test.setTimeout(240_000);
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('flinksd');
  const { password, ...pg } = SOURCE_PG;
  const { secret_access_key, ...iceberg } = ICEBERG_REST;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'iceberg', name: `ice-${tag}`, config: iceberg, secrets: { secret_access_key } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
    spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' }, transforms: [] },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=transform`);
  await expect(page.getByRole('heading', { name: 'Which rows and columns reach each table?' })).toBeVisible();
  await page.getByRole('button', { name: 'Add transform' }).click();
  await page.getByRole('button', { name: /^Keep deleted rows/ }).click();
  await expect(page.getByRole('region', { name: 'Step 1: Keep deleted rows' })).toBeVisible();
  await page.getByRole('button', { name: 'Add transform' }).click();
  await expect(page.getByRole('button', { name: /^Keep deleted rows/ })).toContainText('Already added: see Keep rows.');
  await page.getByRole('button', { name: /^Keep only some rows/ }).click();
  // The filter runs first: the server refuses a filter after the event shape.
  await expect(page.getByRole('region', { name: 'Step 1: Keep only some rows' })).toBeVisible();
  await expect(page.getByRole('region', { name: 'Step 2: Keep deleted rows' })).toBeVisible();
  await page.getByRole('button', { name: 'Continue to review →' }).click();
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  const saved = await api.get<{ spec: { transforms: { type: string }[] } }>(url);
  expect(saved.spec.transforms.map((t) => t.type)).toEqual(['filter', 'soft_delete']);
  await api.send('DELETE', url);
});

type Check = { code: string; status: string; blocking?: boolean; message: string };
type Pre = { ok: boolean; sections: { key: string; report: { checks: Check[] } }[] };
const checkIn = (pre: Pre, code: string) => {
  for (const s of pre.sections) {
    const c = s.report.checks.find((x) => x.code === code);
    if (c) return { section: s.key, ...c };
  }
  return undefined;
};

/*
 * Oracle on Flink CDC (ADR 0006): Oracle XE 21c → Iceberg. Preflight names what the reader needs,
 * Prepare creates the flush table in the CDB root, the first copy and a streamed row arrive, every
 * wizard step round-trips, and a version that adds a table is blocked while a saved position would
 * be restored (even when forced).
 */
test('Flink CDC: Oracle → Iceberg copies, streams, and blocks added tables', async ({ page }) => {
  test.setTimeout(1_500_000);
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const engines = await api.get<{ id: string; sources: string[] | null }[]>('/api/v1/engines');
  expect(engines.find((e) => e.id === 'flink-cdc')?.sources).toContain('oracle');

  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('flinko');
  const upper = tag.replace(/-/g, '_').toUpperCase();
  const table = `SHOP.FLINK_${upper}`;
  const added = `SHOP.FLINK_${upper}_B`;
  for (const t of [table, added]) {
    oracleSQL(`CREATE TABLE ${t} (ID NUMBER(10) PRIMARY KEY, STATUS VARCHAR2(40), TOTAL NUMBER(10,2));
ALTER TABLE ${t} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
INSERT INTO ${t} VALUES (1, 'BEFORE-${upper}', 1);`, 'source-oracle-xe');
  }
  const { password, ...ora } = ORACLE_XE;
  const { secret_access_key, ...iceberg } = ICEBERG_REST;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'oracle', name: `ora-${tag}`, config: ora, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'iceberg', name: `ice-${tag}`, config: iceberg, secrets: { secret_access_key } }, 201);
  const p = await api.send<{ id: string; engine: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
    spec: { source: { tables: { include: [table] } }, sink: { naming: `fo_${tag.replace(/-/g, '_')}_{table}` } },
  }, 201);
  expect(p.engine).toBe('flink-cdc');
  const url = `${base}/pipelines/${p.id}`;

  // Every wizard step on an Oracle Flink CDC pipeline.
  await signIn(page);
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });

  const pre = await api.send<Pre>('POST', `${url}/preflight`, {});
  const version = checkIn(pre, 'flink_oracle_version');
  expect(version?.status, JSON.stringify(version)).toBe('pass');
  expect(version?.section).toBe('config');
  // This deploy copies: the statistics ANALYZE replaces are named before it runs.
  expect(checkIn(pre, 'flink_oracle_analyze')?.status).toBe('warn');
  expect(checkIn(pre, 'flink_oracle_flush_table')?.status).toBe('pass');
  expect(checkIn(pre, 'ora_managed_tables'), 'a Debezium-only check on a Flink pipeline').toBeUndefined();

  await api.send('POST', `${url}/deploy`, {});
  await waitFor('Oracle Flink CDC pipeline streaming', async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 900_000);
  await waitFor('copied row readable in Iceberg', async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=100`);
    return e.records.some((r) => r.value.STATUS === `BEFORE-${upper}`);
  }, 420_000);
  const marker = `AFTER-${upper}`;
  oracleSQL(`INSERT INTO ${table} VALUES (2, '${marker}', 2);`, 'source-oracle-xe');
  await waitFor('streamed row readable in Iceberg', async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=100`);
    return e.records.some((r) => r.value.STATUS === marker);
  }, 420_000);

  // A version that adds a table can't restore the saved position: blocked, even when forced.
  await waitFor('a saved position', async () => {
    const sp = await api.get<{ positions: unknown[] }>(`${url}/positions`);
    return sp.positions.length > 0;
  }, 300_000);
  const cur = await api.get<{ version: number; spec: Record<string, unknown> }>(url);
  await api.patch(url, { spec: { ...cur.spec, source: { tables: { include: [table, added] } } } }, cur.version);
  const pre2 = await api.send<Pre>('POST', `${url}/preflight`, {});
  const blocked = checkIn(pre2, 'flink_tables_added');
  expect(blocked?.status, JSON.stringify(blocked)).toBe('fail');
  expect(blocked?.section).toBe('config');
  expect(blocked?.message).toContain(added);
  const forced = await api.raw('POST', `${url}/deploy`, { force_reason: 'e2e: a forced deploy must not skip flink_tables_added' });
  expect(forced.status(), await forced.text()).toBeGreaterThanOrEqual(400);

  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor('Oracle Flink CDC pipeline deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 300_000);
  for (const t of [table, added]) oracleSQL(`DROP TABLE ${t} PURGE;`, 'source-oracle-xe');
});

// E24: Oracle Database Free (here the 26ai build) can't be read by Flink CDC's Oracle reader, and
// preflight says so where a forced deploy can't skip it.
test('Flink CDC: an Oracle release its reader can\'t read is refused before deploy', async () => {
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('flinkof');
  const { password, ...ora } = ORACLE;
  const { secret_access_key, ...iceberg } = ICEBERG_REST;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'oracle', name: `oraf-${tag}`, config: ora, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'iceberg', name: `ice-${tag}`, config: iceberg, secrets: { secret_access_key } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
    spec: { source: { tables: { include: ['SHOP.ORDERS'] } }, sink: { naming: `fof_${tag.replace(/-/g, '_')}_{table}` } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  const pre = await api.send<Pre>('POST', `${url}/preflight`, {});
  const version = checkIn(pre, 'flink_oracle_version');
  expect(version?.status, JSON.stringify(version)).toBe('fail');
  expect(version?.blocking).toBe(true);
  expect(version?.section).toBe('config');
  expect(version?.message).toMatch(/Free|26ai/);
  await api.send('DELETE', url);
});

// ADR 0008: computed columns and filters run on the real Flink CDC engine as BeziSync renders
// them: typed functions on NULLs (they used to stop the job), a number joined into text (CAST to
// STRING didn't compile), equality with a number on a BIGINT (always false), a division by zero
// (NULL), and an expression column typed in the wizard and an expression filter.
test('Flink CDC: computed columns and filters, typed and as expressions, on NULLs and zeros', async ({ page }) => {
  test.setTimeout(1_200_000);
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('flinkx');
  const table = `shop.fx_${tag.replace(/-/g, '_')}`;
  await ownerSql(`CREATE TABLE ${table} (id bigint PRIMARY KEY, status text, total numeric(10,2), qty int, note text); ALTER TABLE ${table} REPLICA IDENTITY FULL;
    INSERT INTO ${table} VALUES (1, 'paid', 150.00, 3, 'hello'), (2, NULL, NULL, NULL, NULL), (3, 'gone', 10, 1, 'x'), (4, 'skip', 5, 0, 'y'), (5, 'zero', 7.50, 0, 'z')`);
  const { password, ...pg } = SOURCE_PG;
  const { secret_access_key, ...iceberg } = ICEBERG_REST;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'iceberg', name: `ice-${tag}`, config: iceberg, secrets: { secret_access_key } }, 201);
  const computed = (id: string, name: string, fn: string, args: unknown[]) => ({ type: 'computed', id, table, name, function: fn, args });
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
    spec: {
      source: { tables: { include: [table] } }, sink: { naming: `fx_${tag.replace(/-/g, '_')}_{table}` }, snapshot: { mode: 'initial' },
      transforms: [
        { type: 'filter', id: 'kp01', rules: [{ field: 'id', op: 'ne', value: 3 }] },
        computed('cc01', 'note_up', 'upper', [{ column: 'note' }]),
        computed('cc02', 'label', 'concat', [{ column: 'status' }, { text: '-' }, { column: 'qty' }]),
        computed('cc03', 'amount', 'multiply', [{ column: 'total' }, { column: 'qty' }]),
        computed('cc04', 'per_unit', 'divide', [{ column: 'total' }, { column: 'qty' }]),
      ],
    },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;

  // In the wizard (Expert mode): an expression column and an expression filter, checked as typed.
  await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'expert' });
  try {
    await signIn(page);
    await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=transform`);
    await page.getByRole('button', { name: 'Add transform' }).click();
    await page.getByRole('button', { name: /^Computed column/ }).click();
    const cc = page.getByRole('region', { name: /Computed column/ }).last();
    await choose(cc.getByLabel(/^Table:/), table);
    await cc.getByLabel('New column').fill('size');
    await cc.getByRole('radio', { name: 'An expression' }).click();
    const box = cc.getByLabel('Expression', { exact: true });
    await box.fill('case when total > 100 then \'big\' when total is null then \'none\' else \'small\' end || nope(1)');
    await expect(cc.getByRole('alert').filter({ hasText: 'nope' })).toContainText("isn't a function");
    await box.fill('case when total > 100 then \'big\' when total is null then \'none\' else \'small\' end');
    await expect(cc.getByText('Saved as')).toContainText("CASE WHEN `total` > 100 THEN 'big' WHEN `total` IS NULL THEN 'none' ELSE 'small' END");
    await box.blur();
    await expect(box).toHaveValue("CASE WHEN `total` > 100 THEN 'big' WHEN `total` IS NULL THEN 'none' ELSE 'small' END");
    await cc.getByRole('button', { name: 'Done' }).click();
    await page.getByRole('button', { name: 'Add transform' }).click();
    await page.getByRole('button', { name: /^Keep only some rows/ }).click();
    const flt = page.getByRole('region', { name: /Keep only some rows/ }).last();
    await flt.getByRole('radio', { name: 'An expression' }).click();
    const cond = flt.getByLabel('Keep rows where', { exact: true });
    await cond.fill('now() > ts');
    await expect(flt.getByRole('alert')).toContainText(/no column ts|can't use NOW/);
    await cond.fill("status is null or status <> 'skip'");
    await expect(flt.getByText('Saved as')).toBeVisible();
    await flt.getByRole('button', { name: 'Done' }).click();
    await page.getByRole('button', { name: 'Continue to review →' }).click();
    await expect(page).toHaveURL(/step=review/);
  } finally {
    await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'guided' });
  }
  type Saved = { spec: { transforms: Record<string, unknown>[] } };
  const saved = await api.get<Saved>(url);
  expect(saved.spec.transforms.find((t) => t.name === 'size')).toMatchObject({ expression: "CASE WHEN `total` > 100 THEN 'big' WHEN `total` IS NULL THEN 'none' ELSE 'small' END" });
  expect(saved.spec.transforms.find((t) => t.type === 'filter' && t.expression)).toMatchObject({ expression: "`status` IS NULL OR `status` <> 'skip'" });
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });

  const pre = await api.send<{ sections: { report: { checks: { code: string; status: string; message: string }[] } }[] }>('POST', `${url}/preflight`, {});
  const expr = pre.sections.flatMap((s) => s.report.checks).filter((c) => c.code === 'transform_expression' || c.code === 'transform_computed_input');
  expect(expr.filter((c) => c.status === 'fail'), JSON.stringify(expr)).toEqual([]);
  await api.send('POST', `${url}/deploy`, {});
  await waitFor('Flink CDC pipeline with expressions streaming', async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 900_000);
  await ownerSql(`INSERT INTO ${table} VALUES (6, 'late', 200, 4, 'new')`);
  const rows = new Map<number, Record<string, unknown>>();
  await waitFor('the copied and streamed rows, computed, in Iceberg', async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=100`);
    for (const r of e.records) rows.set(Number(r.value.id), r.value);
    return rows.has(6);
  }, 600_000);
  // Row 3 is filtered by its BIGINT id, row 4 by the expression; NULLs stay NULL, a division by zero is NULL.
  expect([...rows.keys()].sort()).toEqual([1, 2, 5, 6]);
  expect(rows.get(1)).toMatchObject({ note_up: 'HELLO', label: 'paid-3', amount: 450, per_unit: 50, size: 'big' });
  expect(rows.get(2)).toMatchObject({ note_up: null, label: null, amount: null, per_unit: null, size: 'none' });
  expect(rows.get(5)).toMatchObject({ note_up: 'Z', label: 'zero-0', amount: 0, per_unit: null, size: 'small' });
  expect(rows.get(6)).toMatchObject({ note_up: 'NEW', label: 'late-4', amount: 800, per_unit: 50, size: 'big' });

  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor('Flink CDC pipeline deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 300_000);
  await ownerSql(`DROP TABLE IF EXISTS ${table}`);
});

// ADR 0010: the Destination table shows each column's type, and the primary key takes any column
// the table writes. A computed column and an added field key an Iceberg table on the real engine
// (BeziSync writes them NOT NULL, as Iceberg requires of key columns); one that can be empty is
// offered disabled with the reason, and the API path past the editor is blocked by preflight.
test('Flink CDC: the destination table shows types, and computed and added columns key the table', async ({ page }) => {
  test.setTimeout(1_200_000);
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('flinkk');
  const table = `shop.fk_${tag.replace(/-/g, '_')}`;
  await ownerSql(`CREATE TABLE ${table} (id bigint PRIMARY KEY, region text NOT NULL, note text, made date NOT NULL); ALTER TABLE ${table} REPLICA IDENTITY FULL;
    INSERT INTO ${table} VALUES (1, 'eu', 'first', '2026-01-15'), (2, 'us', NULL, '2026-02-20')`);
  const { password, ...pg } = SOURCE_PG;
  const { secret_access_key, ...iceberg } = ICEBERG_REST;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'iceberg', name: `ice-${tag}`, config: iceberg, secrets: { secret_access_key } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, engine: 'flink-cdc',
    spec: {
      source: { tables: { include: [table] } }, sink: { naming: `fk_${tag.replace(/-/g, '_')}_{table}` }, snapshot: { mode: 'initial' },
      transforms: [
        { type: 'unwrap', id: 'uw01' },
        { type: 'add_field', id: 'af01', name: 'tenant', value: { kind: 'static', text: 'acme' } },
        { type: 'computed', id: 'cc01', table, name: 'month', function: 'date_format', args: [{ column: 'made' }, { text: 'yyyy-MM' }] },
        { type: 'computed', id: 'cc02', table, name: 'note_up', function: 'upper', args: [{ column: 'note' }] },
      ],
    },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  type Saved = { version: number; spec: Record<string, unknown> & { keys?: Record<string, string[]> } };
  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=transform`);

  // The destination table: each column with its type and where it comes from.
  const dest = page.getByRole('region', { name: 'Destination table' });
  const row = (name: string) => dest.getByRole('row').filter({ has: page.getByText(name, { exact: true }) });
  await expect(row('id')).toContainText('bigint');
  await expect(row('id')).toContainText('key');
  await expect(row('made')).toContainText('date');
  await expect(row('month')).toContainText('text');
  await expect(row('month')).toContainText('computed: Date as text(made, "yyyy-MM")');
  await expect(row('tenant')).toContainText('added: "acme"');

  // The key: every written column with its type; a computed column that can be empty is disabled.
  const keyRow = page.getByRole('region', { name: 'Primary key' });
  await keyRow.getByRole('button', { name: 'Edit' }).click();
  await keyRow.getByRole('button', { name: 'Change' }).click();
  await expect(keyRow.getByRole('checkbox', { name: /^month/ })).toBeEnabled();
  await expect(keyRow.getByRole('checkbox', { name: /^tenant/ })).toBeEnabled();
  await expect(keyRow.getByRole('checkbox', { name: /^note_up/ })).toBeDisabled();
  await expect(keyRow.getByText('Can be empty: it reads a column the source allows to be empty.')).toBeVisible();
  for (const name of ['id', 'month', 'tenant']) await keyRow.getByRole('checkbox', { name: new RegExp(`^${name}`) }).check();
  await expect(keyRow.getByText('month is computed: a row whose key changes is written as a new row, and the old one stays.')).toBeVisible();
  await expect(row('month')).toContainText('key');
  await expect(row('tenant')).toContainText('key');
  // The editor and the typed table at 390, 768 and 1280 px, in light and dark: no overflow, accessible.
  for (const scheme of ['light', 'dark'] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    for (const width of [390, 768, 1280]) {
      await page.setViewportSize({ width, height: 900 });
      // Colours transition on a theme change: measure the settled page.
      await page.evaluate(() => document.getAnimations().forEach((a) => a.finish()));
      expect(await layoutProblems(page), `${scheme} ${width}px`).toEqual([]);
      const axe = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
      expect(axe.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical').map((v) => `${v.id}: ${v.nodes.slice(0, 3).map((n) => n.target.join(' ')).join(' | ')}`), `${scheme} ${width}px`).toEqual([]);
    }
  }
  await page.emulateMedia({ colorScheme: 'light' });
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.getByRole('button', { name: 'Continue to review →' }).click();
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  const saved = await api.get<Saved>(url);
  expect(saved.spec.keys).toEqual({ [table]: ['id', 'month', 'tenant'] });

  // The job keys the table by them, written NOT NULL.
  const config = JSON.stringify(await api.get<unknown>(`${url}/config`));
  expect(config).toContain('primary-keys: \\"id,month,tenant\\"');
  expect(config).toContain("IFNULL('acme', '') AS `tenant`");
  const pre = await api.send<{ sections: { report: { checks: { code: string; status: string; message: string }[] } }[] }>('POST', `${url}/preflight`, {});
  const keyChecks = pre.sections.flatMap((s) => s.report.checks).filter((c) => c.code.startsWith('transform_key') || c.code === 'flink_primary_key');
  expect(keyChecks.filter((c) => c.status === 'fail'), JSON.stringify(keyChecks)).toEqual([]);
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });

  await api.send('POST', `${url}/deploy`, {});
  await waitFor('Flink CDC pipeline keyed by computed columns streaming', async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 900_000);
  // An update upserts by the whole key: still one row per id.
  await ownerSql(`INSERT INTO ${table} VALUES (3, 'eu', 'new', '2026-03-01'); UPDATE ${table} SET note = 'changed' WHERE id = 1`);
  const rows = await waitFor('rows keyed by id, month and tenant in Iceberg', async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=${table}&limit=100`);
    const vals = e.records.map((r) => r.value);
    return vals.some((v) => v.note === 'changed') && vals.some((v) => Number(v.id) === 3) ? vals : null;
  }, 600_000);
  expect(rows.filter((v) => Number(v.id) === 1)).toHaveLength(1);
  expect(rows.find((v) => Number(v.id) === 1)).toMatchObject({ month: '2026-01', tenant: 'acme', note: 'changed' });

  // Past the editor (the API): a key column that can be empty, or one the table doesn't write.
  for (const [keys, code] of [[['note_up'], 'transform_key_column_unfit'], [['gone'], 'transform_key_column_unknown']] as const) {
    const cur = await api.get<Saved>(url);
    await api.patch(url, { spec: { ...cur.spec, keys: { [table]: keys } } }, cur.version);
    const bad = await api.send<{ sections: { report: { checks: { code: string; status: string; message: string }[] } }[] }>('POST', `${url}/preflight`, {});
    const check = bad.sections.flatMap((s) => s.report.checks).find((c) => c.code === code);
    expect(check?.status, JSON.stringify(check)).toBe('fail');
  }

  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor('Flink CDC pipeline deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 300_000);
  await ownerSql(`DROP TABLE IF EXISTS ${table}`);
});

/*
 * ADR 0011 (W-1, W-4, W-5, W-10, W-15): the Destination step offers what an allowed engine runs from
 * the source, and names what it leaves out. Its own project, so its policies touch nothing else.
 */
test('Destination step: only what an allowed engine runs from the source, the rest named', async ({ page }) => {
  test.setTimeout(240_000);
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const tag = unique('fit');
  const project = await api.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: tag.slice(0, 40), name: `Fit ${tag}` }, 201);
  const base = `/api/v1/projects/${project.id}`;
  const { password, ...pg } = SOURCE_PG;
  const { password: mongoPassword, ...mongo } = MONGO;
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const mdb = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'mongodb', name: `mongo-${tag}`, config: { ...mongo, auth_source: 'admin' }, secrets: { password: mongoPassword } }, 201);
  await api.send('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `kafka-${tag}`, config: { ...KAFKA, security_protocol: 'PLAINTEXT' }, secrets: {} }, 201);
  await api.send('POST', `${base}/connections`, { kind: 'sink', type: 'starrocks', name: `sr-${tag}`, config: { host: 'starrocks-fe.invalid', user: 'root' }, secrets: {} }, 201);
  await signIn(page);

  // MongoDB: only Debezium reads it, and Debezium doesn't write StarRocks.
  const fromMongo = await api.send<{ id: string }>('POST', `${base}/pipelines`, { name: `m-${tag}`, source_id: mdb.id, spec: { source: { tables: { include: ['shop.customers'] } } } }, 201);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${fromMongo.id}&step=sink`);
  const cards = page.getByRole('radiogroup', { name: 'Destination' });
  await expect(cards.getByRole('radio', { name: new RegExp(`kafka-${tag}`) })).toBeVisible();
  await expect(cards.getByRole('radio', { name: new RegExp(`sr-${tag}`) })).toHaveCount(0);
  await expect(page.getByTestId('hidden-sinks')).toContainText(`sr-${tag}: no engine runs this source to them.`);
  await page.getByRole('button', { name: 'Add a destination' }).click();
  const dialog = page.getByRole('dialog');
  await expect(dialog.getByRole('button', { name: /^Kafka/ })).toBeVisible();
  await expect(dialog.getByRole('button', { name: /^StarRocks/ })).toHaveCount(0);
  await page.keyboard.press('Escape');

  // PostgreSQL, Debezium only: StarRocks is Flink CDC's, so it is left out for the policy.
  await api.send('PUT', `${base}/policies`, { allowed_engines: ['debezium'] }, 200);
  const fromPg = await api.send<{ id: string }>('POST', `${base}/pipelines`, { name: `p-${tag}`, source_id: src.id, spec: { source: { tables: { include: ['shop.orders'] } } } }, 201);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${fromPg.id}&step=sink`);
  await expect(cards.getByRole('radio', { name: new RegExp(`kafka-${tag}`) })).toBeVisible();
  await expect(page.getByTestId('hidden-sinks')).toContainText(`sr-${tag}: this project doesn’t allow Flink CDC, which writes them.`);

  // Flink CDC only: StarRocks shows; Kafka (both engines) is picked on Flink CDC, Debezium disabled.
  await api.send('PUT', `${base}/policies`, { allowed_engines: ['flink-cdc'] }, 200);
  await page.reload();
  await expect(cards.getByRole('radio', { name: new RegExp(`sr-${tag}`) })).toBeVisible();
  await expect(page.getByTestId('hidden-sinks')).toHaveCount(0);
  await cards.getByRole('radio', { name: new RegExp(`kafka-${tag}`) }).click();
  const engine = page.getByRole('radiogroup', { name: 'Engine' });
  await expect(engine.getByRole('radio', { name: /^Flink CDC/ })).toHaveAttribute('aria-checked', 'true');
  await expect(engine.getByRole('radio', { name: /^Debezium/ })).toContainText('Not allowed in this project.');
  await page.getByRole('button', { name: 'Continue to transform →' }).click();
  await expect(page.getByRole('heading', { name: 'How should events look?' })).toBeVisible();
  const saved = await api.get<{ engine: string; spec: { sink: { format: { type: string; include_schema: boolean } } } }>(`${base}/pipelines/${fromPg.id}`);
  expect(saved.engine).toBe('flink-cdc');
  expect(saved.spec.sink.format).toMatchObject({ type: 'json', include_schema: false });

  // A MongoDB draft now has nothing to go to (Flink CDC can't read it): the step says so.
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${fromMongo.id}&step=sink`);
  await expect(page.getByText('No destination takes MongoDB here')).toBeVisible();
  // And the Source step says why MongoDB can't be used.
  await page.goto(`/p/${project.slug}/pipelines/new`);
  await expect(page.getByRole('radiogroup', { name: 'Source' }).getByRole('radio', { name: new RegExp(`mongo-${tag}`) })).toContainText(
    'This project allows only Flink CDC, which can’t read this source.',
  );

  await api.send('PUT', `${base}/policies`, {}, 200);
  for (const id of [fromMongo.id, fromPg.id]) await api.send('DELETE', `${base}/pipelines/${id}`);
});

/*
 * ADR 0011 (W-14, W-15): Flink CDC writes Kafka as change events, so the Transform step, its preview,
 * Review and autosave speak events. The draft starts on Debezium with Avro; choosing Flink CDC in the
 * Destination step saves Kafka's one format.
 */
test('Flink CDC → Kafka: the wizard speaks events, not tables', async ({ page }) => {
  test.setTimeout(300_000);
  const api = await Api.signIn();
  await registerFlinkImage(api);
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('fkafka');
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `kafka-${tag}`, config: { ...KAFKA, security_protocol: 'PLAINTEXT' }, secrets: {} }, 201);
  const avro = { type: 'avro', include_schema: false, registry_url: 'http://registry.invalid:8080/apis/registry/v2', registry_kind: 'apicurio' };
  const p = await api.send<{ id: string; engine: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, sink: { naming: `fk_${tag.replace(/-/g, '_')}_{table}`, format: avro }, snapshot: { mode: 'no_data' } },
  }, 201);
  expect(p.engine).toBe('debezium');
  const url = `${base}/pipelines/${p.id}`;
  await signIn(page);

  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=sink`);
  const engine = page.getByRole('radiogroup', { name: 'Engine' });
  await expect(engine.getByRole('radio', { name: /^Flink CDC/ })).toContainText('Debezium-style JSON event');
  await engine.getByRole('radio', { name: /^Flink CDC/ }).click();
  await expect(page.getByLabel('Event format')).toHaveCount(0);
  await expect(page.getByText('Records are Flink CDC’s Debezium-style JSON (no schema)')).toBeVisible();
  await page.getByRole('button', { name: 'Continue to transform →' }).click();

  await expect(page.getByRole('heading', { name: 'How should events look?' })).toBeVisible();
  const saved = await api.get<{ engine: string; spec: { sink: { format: { type: string; include_schema: boolean; registry_url: string | null } } } }>(url);
  expect(saved.engine).toBe('flink-cdc');
  expect(saved.spec.sink.format).toMatchObject({ type: 'json', include_schema: false, registry_url: null });
  await expect(page.getByText('Which rows and columns reach each table?')).toHaveCount(0);
  await expect(page.getByText('Message key', { exact: true }).first()).toBeVisible();
  const preview = page.getByRole('heading', { name: 'Events', exact: true });
  await expect(preview).toBeVisible();
  await expect(page.getByRole('definition').filter({ hasText: 'TableId' })).toContainText('id');
  await expect(page.getByRole('columnheader', { name: 'Field' })).toBeVisible();
  await expect(page.getByText('Destination table')).toHaveCount(0);

  await page.getByRole('button', { name: 'Add transform' }).click();
  await expect(page.getByRole('button', { name: /^Keep only some events/ })).toBeVisible();
  await expect(page.getByRole('button', { name: /^Rename fields/ })).toBeVisible();
  await expect(page.getByRole('button', { name: /^Computed field/ })).toContainText('A new field');
  await page.getByRole('button', { name: /^Keep deleted rows/ }).click();
  // A new step opens for editing.
  const soft = page.getByRole('region', { name: 'Step 1: Keep deleted rows' });
  await expect(soft).toContainText('A delete is sent as an insert event marked __op = -D');
  await expect(soft).toContainText('insert event whose after carries __op = -D');
  await page.getByRole('button', { name: 'Add transform' }).click();
  await page.getByRole('button', { name: /^Keep only some events/ }).click();
  const filter = page.getByRole('region', { name: 'Step 1: Keep only some events' });
  await expect(filter).toContainText('Events a filter drops never reach the topic');
  await expect(page.getByText('Keep events', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: 'Continue to review →' }).click();

  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  await expect(page.getByText(`kafka-${tag} · JSON events (Flink CDC)`)).toBeVisible();
  await expect(page.getByText('Each change is sent as a Debezium-style JSON event')).toBeVisible();
  await expect(page.getByText('Tables are kept in sync')).toHaveCount(0);
  await expect(page.getByText('How much a restart may send again: consumers can see an event twice.')).toBeVisible();

  // Every step round-trips on Flink CDC → Kafka.
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });
  await api.send('DELETE', url);
});
