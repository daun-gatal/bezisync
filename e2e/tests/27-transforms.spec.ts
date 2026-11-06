import { expect, test } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';
import { KAFKA, SOURCE_PG, TARGET_PG } from './fixtures';
import { walkWizard } from './walk';
import { ownerSql, sql, waitFor } from './data';

/**
 * Transforms: an editor shapes events in the wizard with pickers and a live preview, real
 * Debezium delivers exactly what the preview showed, and a schema change that breaks a step is
 * flagged on the pipeline.
 */

interface Pipeline {
  id: string;
  lifecycle: string;
  health: string;
  version: number;
  status: Record<string, unknown>;
}
type Events = { records: { value: Record<string, unknown> | null; key?: unknown }[] };

async function setup(api: Api, tag: string) {
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>(
    'POST',
    `${base}/runtimes`,
    { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } },
    201,
  );
  const source = await api.send<{ id: string }>(
    'POST',
    `${base}/connections`,
    { kind: 'source', type: 'postgresql', name: `src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } },
    201,
  );
  const sink = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `dst-${tag}`, config: KAFKA }, 201);
  return { project, base, rt, source, sink };
}

async function streaming(api: Api, url: string) {
  return waitFor(
    'pipeline streaming',
    async () => {
      const p = await api.get<Pipeline>(url);
      if (p.lifecycle === 'failed') throw new Error(JSON.stringify(p.status));
      return p.lifecycle === 'streaming' && p.health === 'healthy' ? p : undefined;
    },
    420_000,
  );
}

test('an editor shapes events in the wizard and Debezium delivers what the preview showed', async ({ page }) => {
  test.setTimeout(900_000);
  const api = await Api.signIn();
  const tag = unique('tf');
  const { project, base } = await setup(api, tag);

  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/new`);
  await page.getByRole('radio', { name: new RegExp(`src-${tag}`) }).click();
  await page.getByRole('button', { name: 'Continue to tables →' }).click();
  await page.getByLabel('shop.orders').check();
  await page.getByLabel('shop.payment_events').check();
  await page.getByRole('button', { name: 'Continue to destination →' }).click();
  await page.getByRole('radio', { name: new RegExp(`dst-${tag}`) }).click();
  await page.getByLabel('Destination naming').fill(`tf.${tag}.{table}`);
  await page.getByRole('button', { name: 'Continue to transform →' }).click();

  // The step opens with the row shape; the preview shows base64 decimals and offers the fix.
  await expect(page.getByRole('heading', { name: 'How should events look?' })).toBeVisible();
  const preview = page.getByLabel('Preview event');
  await expect(preview).toContainText('"total": "', { timeout: 60_000 });
  await expect(page.getByText('Decimals arrive as base64 text')).toBeVisible();
  await page.getByRole('button', { name: 'Send decimals as numbers' }).click();
  await expect(preview).toContainText('"total": 129.9');

  // A table without a primary key gets key columns.
  await expect(page.getByText(/shop\.payment_events has no primary key/)).toBeVisible();
  await page.getByRole('region', { name: 'Message key' }).getByRole('checkbox', { name: 'order_id' }).check();

  // Keep only some events: pickers for the column and operator, a typed value.
  await page.getByRole('button', { name: 'Add transform' }).click();
  await page.getByRole('button', { name: /^Keep only some events/ }).click();
  const filter = page.getByRole('region', { name: 'Step 1: Keep only some events' });
  await filter.getByRole('radio', { name: 'Some tables' }).click();
  await filter.getByRole('button', { name: 'Condition' }).click();
  await choose(filter.getByLabel('Rule 1 column'), 'status');
  await choose(filter.getByLabel('Rule 1 operator'), 'is not');
  await filter.getByLabel('Rule 1 value').fill(`SKIP-${tag}`);
  await filter.getByRole('button', { name: 'Done' }).click();

  // Rename and add a field; invalid names are caught as they are typed.
  await page.getByRole('button', { name: 'Add transform' }).click();
  await page.getByRole('button', { name: /^Rename fields/ }).click();
  const rename = page.getByRole('region', { name: 'Step 3: Rename fields' });
  await choose(rename.getByLabel('Rename 1 from'), 'customer_id');
  await rename.getByLabel('Rename 1 to').fill('cust ref');
  await expect(rename.getByText('Did you mean cust_ref?')).toBeVisible();
  await rename.getByLabel('Rename 1 to').fill('cust_ref');
  await rename.getByRole('button', { name: 'Done' }).click();
  await page.getByRole('button', { name: 'Add transform' }).click();
  await page.getByRole('button', { name: /^Add a field/ }).click();
  const add = page.getByRole('region', { name: 'Step 4: Add a field' });
  await add.getByLabel('Field name').fill('status');
  await expect(add.getByText('The event already has a field status.')).toBeVisible();
  await add.getByLabel('Field name').fill('source_system');
  await add.getByLabel('Value text').fill('orders-eu');
  await add.getByRole('button', { name: 'Done' }).click();

  await expect(preview).toContainText('"cust_ref": 88');
  await expect(preview).toContainText('"source_system": "orders-eu"');
  await expect(page.getByText('Delivered', { exact: true })).toBeVisible();
  // An update to a skipped status is not delivered.
  await page.getByRole('radio', { name: 'Update' }).click();
  await expect(page.getByText('Delivered', { exact: true })).toBeVisible();

  await page.getByRole('button', { name: 'Continue to review →' }).click();
  await expect(page.getByRole('heading', { name: 'Review and deploy' })).toBeVisible();
  await expect(page.getByText('Rename fields: all tables: customer_id → cust_ref.')).toBeVisible();
  const name = `tf-${tag}`;
  await page.getByLabel('Name', { exact: true }).fill(name);
  await choose(page.getByLabel('Runtime'), `rt-${tag}`);
  await expect(page.getByText('Final checks', { exact: true })).toBeVisible({ timeout: 60_000 });
  await expect(page.getByLabel('Generated configuration')).toBeAttached();
  await page.getByRole('button', { name: 'Deploy', exact: true }).click();
  await expect(page.getByRole('heading', { name, level: 1 })).toBeVisible();

  const pipelines = await api.get<{ id: string; name: string }[]>(`${base}/pipelines`);
  const id = pipelines.find((p) => p.name === name)!.id;
  const url = `${base}/pipelines/${id}`;
  await streaming(api, url);
  await sql(`INSERT INTO shop.orders (customer_id, status, total) VALUES (3, $1, 12.50), (4, $2, 7.25)`, [`SKIP-${tag}`, `KEEP-${tag}`]);
  const events = async () => api.get<Events>(`${url}/events?table=shop.orders&limit=500`);
  const kept = await waitFor('kept row delivered', async () => (await events()).records.map((r) => r.value).find((v) => v?.status === `KEEP-${tag}`), 180_000);
  expect(kept).toMatchObject({ cust_ref: 4, total: 7.25, source_system: 'orders-eu', __deleted: 'false', __op: 'c' });
  expect(kept).not.toHaveProperty('customer_id');
  expect((await events()).records.some((r) => r.value?.status === `SKIP-${tag}`)).toBe(false);
  await sql(`INSERT INTO shop.payment_events (order_id, payload) VALUES (7, $1)`, [`pay-${tag}`]);
  const payments = await waitFor(
    'keyed payment delivered',
    async () => (await api.get<Events>(`${url}/events?table=shop.payment_events&limit=50`)).records.find((r) => r.value?.payload === `pay-${tag}`),
    180_000,
  );
  const key = typeof payments.key === 'string' ? payments.key : JSON.stringify(payments.key);
  expect(key.replace(/\s/g, '')).toContain('"order_id":7');

  // The Events tab compares real events with the preview; Overview lists the steps.
  await page.getByRole('tab', { name: 'Events' }).click();
  await expect(page.getByText(/Matches preview · checked \d+ event/)).toBeVisible({ timeout: 120_000 });
  await page.getByRole('tab', { name: 'Overview' }).click();
  const steps = page.getByRole('region', { name: 'Transforms' }).getByRole('list', { name: 'Steps' });
  await expect(steps).toContainText('Add a field');
  await expect(steps).toContainText('all tables: source_system = "orders-eu"');

  await api.send('DELETE', url);
});

test('a schema change that breaks a step is flagged, and the fix is one click away', async ({ page }) => {
  test.setTimeout(900_000);
  const api = await Api.signIn();
  const tag = unique('drift');
  const table = `drift_${tag.replace(/[^a-z0-9]/g, '_')}`.slice(0, 40);
  const { project, base, rt, source, sink } = await setup(api, tag);
  await ownerSql(`CREATE TABLE shop.${table} (id serial PRIMARY KEY, cust_id int, note text)`);
  const pipeline = await api.send<Pipeline>(
    'POST',
    `${base}/pipelines`,
    {
      name: `drift-${tag}`,
      owner: 'e2e',
      source_id: source.id,
      sink_id: sink.id,
      runtime_id: rt.id,
      spec: {
        source: { tables: { include: [`shop.${table}`] } },
        sink: { naming: `drift.${tag}.{table}` },
        transforms: [
          { type: 'unwrap', id: 'shape' },
          { type: 'rename', id: 'ren1', renames: [{ from: 'cust_id', to: 'customer' }] },
        ],
      },
    },
    201,
  );
  const url = `${base}/pipelines/${pipeline.id}`;
  await api.send('POST', `${url}/deploy`, {});
  await streaming(api, url);
  await ownerSql(`ALTER TABLE shop.${table} RENAME COLUMN cust_id TO customer_ref`);
  await waitFor('drift found', async () => ((await api.get<Pipeline>(url)).status.transform_drift as unknown[] | undefined)?.length, 300_000);

  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/${pipeline.id}`);
  await expect(page.getByText('1 to fix')).toBeVisible();
  await expect(page.getByText(`shop.${table}, step 2 (rename): No field cust_id at this point: nothing to rename.`)).toBeVisible();
  await page.getByRole('link', { name: 'Fix' }).click();
  await expect(page.getByRole('heading', { name: 'How should events look?' })).toBeVisible();
  const rename = page.getByRole('region', { name: 'Step 2: Rename fields' });
  await rename.getByRole('button', { name: 'Edit' }).click();
  await choose(rename.getByLabel('Rename 1 from'), 'customer_ref');
  await rename.getByRole('button', { name: 'Done' }).click();
  await page.getByRole('button', { name: 'Continue to review →' }).click();
  // The output name is unchanged, so this is not breaking for consumers.
  const impact = page.getByRole('region', { name: 'What changes for consumers' });
  await expect(impact).toContainText('now reads customer_ref; output names unchanged');
  await expect(impact).not.toContainText('Breaking');
  await api.send('DELETE', url);
});

test('the preview matches real Debezium events field for field', async () => {
  test.setTimeout(900_000);
  const api = await Api.signIn();
  const tag = unique('par');
  const { base, rt, source, sink } = await setup(api, tag);
  const spec = {
    source: { tables: { include: ['shop.orders'] } },
    sink: { naming: `par.${tag}.{table}` },
    values: { decimals: 'string', timestamps: 'isostring' },
    transforms: [
      { type: 'filter', id: 'keep', operations: ['insert', 'update', 'snapshot'], rules: [{ field: 'total', op: 'gte', value: 100 }] },
      { type: 'unwrap', id: 'shape', add_fields: ['op', 'source.table'] },
      { type: 'rename', id: 'ren1', renames: [{ from: 'customer_id', to: 'customer' }] },
      { type: 'add_field', id: 'add1', name: 'pipeline', value: { kind: 'pipeline' } },
      { type: 'flatten', id: 'flt1' },
    ],
  };
  const pipeline = await api.send<Pipeline>(
    'POST',
    `${base}/pipelines`,
    { name: `par-${tag}`, owner: 'e2e', source_id: source.id, sink_id: sink.id, runtime_id: rt.id, spec },
    201,
  );
  const url = `${base}/pipelines/${pipeline.id}`;
  await api.send('POST', `${url}/deploy`, {});
  await streaming(api, url);
  // The sample row's total (129.90) passes the rule; the second row does not.
  await sql(`INSERT INTO shop.orders (customer_id, status, total) VALUES (5, $1, 129.90), (5, 'NOT-${tag}', 1)`, [`PAR-${tag}`]);
  const real = await waitFor(
    'row delivered',
    async () => (await api.get<Events>(`${url}/events?table=shop.orders&limit=500`)).records.map((r) => r.value).find((v) => v?.status === `PAR-${tag}`),
    180_000,
  );
  const discovery = await api.send<{ tables: { schema: string; name: string; columns: { name: string; type: string }[]; primary_key: string[] }[] }>(
    'POST',
    `${base}/connections/${source.id}/discover`,
  );
  const orders = discovery.tables.find((t) => t.schema === 'shop' && t.name === 'orders')!;
  const sim = await api.send<{ outcome: string; fields: string[]; output: { value: Record<string, unknown> } }>('POST', `${url}/transforms:preview`, {
    table: 'shop.orders',
    operation: 'insert',
    columns: orders.columns,
    primary_key: orders.primary_key,
  });
  expect(sim.outcome).toBe('delivered');
  // Same fields and encodings (the Events API sorts keys; the order itself is pinned by the
  // backend tests, which were read from Debezium's own output).
  expect(Object.keys(real!).sort()).toEqual([...sim.fields].sort());
  for (const [k, v] of Object.entries(sim.output.value)) {
    if (['id', 'customer', 'status', 'total', 'created_at'].includes(k)) expect(typeof real![k], k).toBe(typeof v);
    else expect(real![k], k).toEqual(v);
  }
  expect(real!.total).toBe('129.90');
  expect(String(real!.created_at)).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$/);
  expect((await api.get<Events>(`${url}/events?table=shop.orders&limit=500`)).records.some((r) => r.value?.status === `NOT-${tag}`)).toBe(false);
  await api.send('DELETE', url);
});

test('a long list of steps scrolls inside the Transforms card, level with the cards beside it', async ({ page }) => {
  test.setTimeout(900_000);
  const api = await Api.signIn();
  const tag = unique('many');
  const { project, base, rt, source, sink } = await setup(api, tag);
  const adds = Array.from({ length: 10 }, (_, i) => ({ type: 'add_field', id: `add${i}`, name: `extra_${i}`, value: { kind: 'static', text: `v${i}` } }));
  const pipeline = await api.send<Pipeline>(
    'POST',
    `${base}/pipelines`,
    {
      name: `many-${tag}`,
      owner: 'e2e',
      source_id: source.id,
      sink_id: sink.id,
      runtime_id: rt.id,
      spec: {
        source: { tables: { include: ['shop.orders'] } },
        sink: { naming: `many.${tag}.{table}` },
        transforms: [{ type: 'unwrap', id: 'shape' }, ...adds],
      },
    },
    201,
  );
  const url = `${base}/pipelines/${pipeline.id}`;
  await api.send('POST', `${url}/deploy`, {});
  await streaming(api, url);

  await signIn(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto(`/p/${project.slug}/pipelines/${pipeline.id}`);
  const transforms = page.getByRole('region', { name: 'Transforms' });
  const steps = transforms.getByRole('list', { name: 'Steps' });
  await expect(steps.getByRole('listitem')).toHaveCount(10);
  await expect(steps).toContainText('extra_9 = "v9"');
  // Side by side, the left column sets the height: Transforms ends level with Source protection
  // and scrolls inside instead of growing.
  const protection = page.getByRole('region', { name: 'Source protection' });
  await expect(protection).toBeVisible({ timeout: 120_000 });
  const bottom = async (l: typeof transforms) => {
    const b = await l.boundingBox();
    return b ? b.y + b.height : Number.NaN;
  };
  await expect.poll(async () => Math.abs((await bottom(transforms)) - (await bottom(protection)))).toBeLessThanOrEqual(1);
  const scroller = transforms.getByRole('group', { name: 'Transform details' });
  expect(await scroller.evaluate((el) => el.scrollHeight > el.clientHeight + 1)).toBe(true);
  // The keyboard can scroll it.
  await scroller.focus();
  await page.keyboard.press('End');
  await expect.poll(() => scroller.evaluate((el) => el.scrollTop)).toBeGreaterThan(0);
  // Stacked on a narrow screen, the card takes its natural height and does not scroll.
  await page.setViewportSize({ width: 760, height: 900 });
  await expect.poll(() => scroller.evaluate((el) => el.scrollHeight <= el.clientHeight + 1)).toBe(true);
  await api.send('DELETE', url);
});

test('an editor keeps deleted rows and cuts long text; the preview shows both', async ({ page }) => {
  test.setTimeout(180_000);
  const api = await Api.signIn();
  const tag = unique('tfcut');
  const { project, base, rt, source, sink } = await setup(api, tag);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: source.id, sink_id: sink.id, runtime_id: rt.id, spec: { source: { tables: { include: ['shop.orders'] } } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=transform`);
  await expect(page.getByRole('heading', { name: 'How should events look?' })).toBeVisible();
  // The row shape drops deletes; Keep deleted rows then decides them instead, without changing that choice.
  const shape = page.getByRole('region', { name: /Row after change/ });
  await shape.getByRole('button', { name: 'Edit' }).click();
  await choose(shape.getByLabel(/^Deletes/), 'Drop delete events');
  await page.getByRole('button', { name: 'Add transform' }).click();
  await page.getByRole('button', { name: /^Keep deleted rows/ }).click();
  // It is drawn under Event shape, after the row shape (step 2).
  const soft = page.getByRole('region', { name: 'Step 2: Keep deleted rows' });
  await expect(soft).toBeVisible();
  await shape.getByRole('button', { name: 'Edit' }).click();
  await expect(shape.getByLabel(/^Deletes/)).toBeDisabled();
  await expect(shape.getByText('Set by Keep deleted rows (step 2). Remove that step to choose.')).toBeVisible();
  await expect(shape.getByRole('button', { name: 'Use the full change envelope instead' })).toBeDisabled();
  await expect(shape.getByText('Keep deleted rows needs the row: remove it first.')).toBeVisible();
  // One is enough.
  await page.getByRole('button', { name: 'Add transform' }).click();
  await expect(page.getByRole('button', { name: /^Keep deleted rows/ })).toBeDisabled();
  await expect(page.getByRole('button', { name: /^Keep deleted rows/ })).toContainText('Already added: see Event shape.');
  await page.keyboard.press('Escape');
  // Removing it gives the row shape its own choice back.
  await soft.getByRole('button', { name: 'More for step 2' }).click();
  await page.getByRole('menuitem', { name: 'Remove' }).click();
  await expect(soft).toHaveCount(0);
  await shape.getByRole('button', { name: 'Edit' }).click();
  await expect(shape.getByLabel(/^Deletes/)).toBeEnabled();
  await expect(shape.getByLabel(/^Deletes/)).toContainText('Drop delete events');
  await page.getByRole('button', { name: 'Add transform' }).click();
  await page.getByRole('button', { name: /^Keep deleted rows/ }).click();
  await page.getByRole('button', { name: 'Add transform' }).click();
  await page.getByRole('button', { name: /^Cut long text/ }).click();
  const cut = page.getByRole('region', { name: /Cut long text/ });
  await cut.getByRole('button', { name: 'Add a column' }).click();
  await choose(cut.getByLabel(/^Column 1:/), 'shop.orders.status');
  await cut.getByLabel('Column 1 length').fill('3');
  await cut.getByRole('button', { name: 'Done' }).click();
  // A delete in the preview keeps the row, marked.
  const preview = page.getByLabel('Preview event');
  await page.getByRole('radio', { name: 'Delete' }).click();
  await expect(preview).toContainText('"__deleted": "true"', { timeout: 30_000 });
  await page.getByRole('button', { name: 'Continue to review →' }).click();
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  const saved = await api.get<{ spec: { transforms: { type: string; columns?: unknown[] }[] } }>(url);
  expect(saved.spec.transforms.map((t) => t.type)).toEqual(expect.arrayContaining(['soft_delete', 'truncate']));
  expect(saved.spec.transforms.find((t) => t.type === 'truncate')?.columns).toEqual([{ table: 'shop.orders', column: 'status', length: 3 }]);
  // The row shape kept its own choice: Keep deleted rows decides deletes without rewriting it.
  expect((saved.spec.transforms as { type: string; delete_handling?: string }[]).find((t) => t.type === 'unwrap')?.delete_handling).toBe('drop');
  await api.send('DELETE', url);
});

test('a saved Keep deleted rows without a row shape is drawn and never re-saved', async ({ page }) => {
  test.setTimeout(240_000);
  const api = await Api.signIn();
  const tag = unique('tfsoft');
  const { project, base, rt, source, sink } = await setup(api, tag);
  await signIn(page);
  for (const transforms of [[{ type: 'soft_delete', id: 'sd01' }], [{ type: 'soft_delete', id: 'sd01' }, { type: 'soft_delete', id: 'sd02' }]]) {
    const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
      name: `p-${tag}-${transforms.length}`, source_id: source.id, sink_id: sink.id, runtime_id: rt.id,
      spec: { source: { tables: { include: ['shop.orders'] } }, transforms },
    }, 201);
    const url = `${base}/pipelines/${p.id}`;
    const before = { pipeline: await api.get<{ version: number; current_version: number; spec: unknown }>(url), config: await api.get<unknown>(`${url}/config`) };
    let saves = 0;
    const onRequest = (r: { method(): string; url(): string }) => {
      if (r.method() === 'PATCH' && r.url().endsWith(url)) saves++;
    };
    page.on('request', onRequest);
    await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=transform`);
    await expect(page.getByText('Keep deleted rows needs it; BeziSync adds it')).toBeVisible();
    await expect(page.getByRole('region', { name: /Keep deleted rows/ })).toHaveCount(transforms.length);
    for (let n = 1; n <= transforms.length; n++) {
      await page.getByRole('region', { name: `Step ${n}: Keep deleted rows` }).getByRole('button', { name: `More for step ${n}` }).click();
      await expect(page.getByRole('menuitem', { name: 'Remove' })).toBeEnabled();
      await page.keyboard.press('Escape');
    }
    await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });
    page.off('request', onRequest);
    expect(saves, 'opening the steps saved the pipeline').toBe(0);
    const after = { pipeline: await api.get<{ version: number; current_version: number; spec: unknown }>(url), config: await api.get<unknown>(`${url}/config`) };
    expect(after.pipeline.version).toBe(before.pipeline.version);
    expect(after.pipeline.current_version).toBe(before.pipeline.current_version);
    expect(after.pipeline.spec).toEqual(before.pipeline.spec);
    expect(after.config).toEqual(before.config);
    await api.send('DELETE', url);
  }
});

test('the Destination step says Keep deleted rows decides deletes', async ({ page }) => {
  test.setTimeout(120_000);
  const api = await Api.signIn();
  const tag = unique('tfjdbc');
  const { project, base, rt, source } = await setup(api, tag);
  const { password, ...target } = TARGET_PG;
  const jdbc = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'jdbc', name: `jdbc-${tag}`, config: target, secrets: { password } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: source.id, sink_id: jdbc.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, transforms: [{ type: 'unwrap', id: 'uw01' }, { type: 'soft_delete', id: 'sd01' }] },
  }, 201);
  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=sink`);
  await expect(page.getByText('Deletes: kept, marked deleted. Set by Keep deleted rows in Transform.')).toBeVisible();
  await api.send('DELETE', `${base}/pipelines/${p.id}`);
});

test('an editor routes tables to one destination; preflight refuses unlike tables, and Debezium delivers to the route', async ({ page }) => {
  test.setTimeout(600_000);
  const api = await Api.signIn();
  const tag = unique('tfroute');
  const { project, base, rt, source, sink } = await setup(api, tag);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: source.id, sink_id: sink.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders', 'shop.order_items', 'shop.customers'] } } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  type Pre = { sections: { report: { checks: { code: string; status: string; blocking?: boolean }[] } }[] };
  const check = (pre: Pre, code: string) => pre.sections.flatMap((s) => s.report.checks).find((c) => c.code === code);
  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=transform`);
  await expect(page.getByRole('heading', { name: 'How should events look?' })).toBeVisible();
  await page.getByRole('button', { name: 'Add transform' }).click();
  await page.getByRole('button', { name: /^Send tables elsewhere/ }).click();
  const step = page.getByRole('region', { name: /Send tables elsewhere/ });
  const where = step.getByRole('table', { name: 'Where tables go' });
  await expect(where.getByRole('row', { name: /shop\.customers/ })).toContainText('its own topic');
  // A pattern: the step says what it matches and where each table lands, live.
  await step.getByLabel('Route 1 tables').fill('shop.order*');
  await step.getByLabel('Route 1 destination').fill(`all-orders-${tag}`);
  await expect(step.getByText('Matches 2 captured tables.')).toBeVisible();
  await expect(where.getByRole('row', { name: /shop\.order_items/ })).toContainText(`all-orders-${tag}`);
  await expect(where.getByRole('row', { name: /shop\.customers/ })).toContainText('its own topic');
  // A table two routes match is refused before saving.
  await step.getByRole('button', { name: 'Add a route' }).click();
  await step.getByLabel('Route 2 tables').fill('shop.orders');
  await step.getByLabel('Route 2 destination').fill('other');
  await expect(page.getByText(/shop\.orders matches more than one route/)).toBeVisible();
  await step.getByRole('button', { name: 'Remove route 2' }).click();
  await step.getByRole('button', { name: 'Done' }).click();
  await page.getByRole('button', { name: 'Continue to review →' }).click();
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  const saved = await api.get<{ spec: { transforms: { type: string; routes?: unknown[] }[] } }>(url);
  expect(saved.spec.transforms.find((t) => t.type === 'route')?.routes).toEqual([{ match: 'shop.order*', to: `all-orders-${tag}` }]);

  // Orders and their items differ in shape: one destination can't hold both.
  const merged = await api.send<Pre>('POST', `${url}/preflight`, {});
  expect(check(merged, 'transform_route_merge')).toMatchObject({ status: 'fail', blocking: true });

  // Route orders alone (and a pattern that matches nothing, a warning) and deploy.
  const transforms = saved.spec.transforms.map((t) =>
    t.type === 'route' ? { ...t, routes: [{ match: 'shop.orders', to: `orders-${tag}` }, { match: 'shop.nothing_*', to: 'nowhere' }] } : t);
  const cur = await api.get<{ version: number; spec: Record<string, unknown> }>(url);
  await api.patch(url, { spec: { ...cur.spec, transforms } }, cur.version);
  const ok = await api.send<Pre>('POST', `${url}/preflight`, {});
  expect(check(ok, 'transform_route_merge')).toBeUndefined();
  expect(check(ok, 'transform_route_idle')?.status).toBe('warn');
  await api.send('POST', `${url}/deploy`, {});
  await streaming(api, url);
  await sql(`INSERT INTO shop.orders (customer_id, status, total) VALUES (5, $1, 3.00)`, [`ROUTE-${tag}`]);
  const events = await waitFor('routed row delivered', async () => {
    const r = await api.get<Events & { destination?: string }>(`${url}/events?table=shop.orders&limit=500`);
    return r.records.some((x) => x.value?.status === `ROUTE-${tag}`) ? r : undefined;
  }, 180_000);
  expect(events.destination).toBe(`orders-${tag}`);
  await api.send('DELETE', url);
});

test('a computed column is offered only on Flink CDC, and says why on Debezium (T16)', async ({ page }) => {
  const api = await Api.signIn();
  const tag = unique('tfcc');
  const { project, base, rt, source, sink } = await setup(api, tag);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: source.id, sink_id: sink.id, runtime_id: rt.id, spec: { source: { tables: { include: ['shop.orders'] } } },
  }, 201);
  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=transform`);
  await page.getByRole('button', { name: 'Add transform' }).click();
  const card = page.getByRole('button', { name: /^Computed column/ });
  await expect(card).toBeDisabled();
  await expect(page.getByText(/Runs only on Flink CDC/)).toBeVisible();
  // Sent anyway, the deploy is refused before anything runs.
  const url = `${base}/pipelines/${p.id}`;
  const cur = await api.get<{ version: number; spec: Record<string, unknown> }>(url);
  await api.patch(url, { spec: { ...cur.spec, transforms: [{ type: 'computed', id: 'cc01', table: 'shop.orders', name: 'x', function: 'upper', args: [{ column: 'status' }] }] } }, cur.version);
  const pre = await api.send<{ sections: { report: { checks: { code: string; status: string; message: string }[] } }[] }>('POST', `${url}/preflight`, {});
  expect(pre.sections.flatMap((s) => s.report.checks).some((c) => c.status === 'fail' && c.message.includes('runs only on Flink CDC'))).toBe(true);
  await api.send('DELETE', url);
});
