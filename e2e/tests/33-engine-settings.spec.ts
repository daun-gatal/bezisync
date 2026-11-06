import { expect, test, type Page } from '@playwright/test';
import { Api, choose, signIn, unique } from './helpers';
import { KAFKA, SOURCE_PG } from './fixtures';
import { walkWizard } from './walk';

/*
 * Engine settings in Review (Expert mode): typed settings from BeziSync's registry, raw
 * (advanced) properties checked by the server as they are typed, and autosave that settles with
 * both, whatever order the keys were typed in.
 */
test.describe.configure({ mode: 'serial' });

async function addSetting(page: Page, title: string) {
  await page.getByRole('region', { name: 'Engine settings' }).getByLabel('Add a setting').click();
  await page.getByRole('combobox', { name: 'Search settings' }).fill(title);
  await page.getByRole('option', { name: new RegExp(title) }).click();
}

test('an Expert tunes a Debezium pipeline: typed settings, raw properties, and autosave settles', async ({ page }) => {
  test.setTimeout(300_000);
  const api = await Api.signIn();
  await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'expert' });
  try {
    const tag = unique('tune');
    const project = await api.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: tag.slice(0, 40), name: `Tune ${tag}` }, 201);
    const base = `/api/v1/projects/${project.id}`;
    await api.send('PUT', `${base}/policies`, { allow_advanced_properties: true });
    const { password, ...pg } = SOURCE_PG;
    const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
    const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
    const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `k-${tag}`, config: KAFKA }, 201);
    const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
      name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, spec: { source: { tables: { include: ['shop.orders'] } } },
    }, 201);
    const url = `${base}/pipelines/${p.id}`;

    await signIn(page);
    await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=review`);
    const card = page.getByRole('region', { name: 'Engine settings' });
    await expect(card).toBeVisible();

    // A typed setting: picked from the list, bounded, with what it does.
    await addSetting(page, 'Snapshot threads');
    await expect(card.getByText('First copy')).toBeVisible();
    for (let i = 0; i < 3; i++) await card.getByRole('button', { name: 'Increase' }).click();

    // Raw properties, typed in an order the server stores differently (longer key first).
    const addRow = page.getByRole('button', { name: 'Add a property' });
    await addRow.click();
    await page.getByLabel('Property 1 name').fill('debezium.source.driver.loginTimeout');
    await page.getByLabel('Property 1 value').fill('30');
    await addRow.click();
    await page.getByLabel('Property 2 name').fill('debezium.source.driver.ssl');
    await page.getByLabel('Property 2 value').fill('true');
    await expect(page.getByText('Not allowed')).toBeVisible(); // a driver option that changes TLS
    await page.getByRole('button', { name: 'Remove property 2' }).click();
    await addRow.click();
    await page.getByLabel('Property 2 name').fill('debezium.source.a.b');
    await page.getByLabel('Property 2 value').fill('1');
    await expect(page.getByText('Not verified')).toHaveCount(2);
    // A raw key that is a typed setting becomes one.
    await addRow.click();
    await page.getByLabel('Property 3 name').fill('debezium.source.poll.interval.ms');
    await page.getByLabel('Property 3 value').fill('0250');
    await page.getByRole('button', { name: 'Use the typed setting' }).click();
    await expect(page.getByText('Poll interval')).toBeVisible();

    // A custom size: CPU and memory from lists, the heap worked out.
    await page.getByRole('radio', { name: /Custom/ }).click();
    await choose(page.getByLabel('CPU', { exact: true }), '1.5 CPU');
    await choose(page.getByLabel('Memory', { exact: true }), '2 GiB');
    await expect(page.getByText('1433 MiB')).toBeVisible();

    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
    const saved = await api.get<{ spec: { tuning?: Record<string, string>; advanced: Record<string, string>; policies: Record<string, unknown> } }>(url);
    expect(saved.spec.policies.custom_resources).toEqual({ cpu: '1.5', memory: '2Gi' });
    expect(saved.spec.tuning).toEqual({ 'debezium.source.poll.interval.ms': '250', 'debezium.source.snapshot.max.threads': '4' });
    expect(saved.spec.advanced).toEqual({ 'debezium.source.a.b': '1', 'debezium.source.driver.loginTimeout': '30' });

    // Every step round-trips, and nothing keeps saving.
    await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });

    // Preflight shows the raw ones as not verified, never as passed.
    const pre = await api.send<{ sections: { key: string; report: { summary: string; checks: { code: string; status: string }[] } }[] }>('POST', `${url}/preflight`, {});
    const config = pre.sections.find((s) => s.key === 'config')!;
    expect(config.report.checks.find((c) => c.code === 'advanced_unverified')?.status).toBe('unknown');
  } finally {
    await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'guided' });
  }
});

test('a deploy that changes settings says what each change does', async ({ page }) => {
  test.setTimeout(600_000);
  const api = await Api.signIn();
  const tag = unique('impact');
  const project = await api.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: tag.slice(0, 40), name: `Impact ${tag}` }, 201);
  const base = `/api/v1/projects/${project.id}`;
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `k-${tag}`, config: KAFKA }, 201);
  const p = await api.send<{ id: string; version: number }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, {});
  const deployed = await api.get<{ version: number }>(url);
  await api.patch(url, { spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' }, tuning: { 'debezium.source.snapshot.max.threads': '4', 'debezium.source.poll.interval.ms': '250' } } }, deployed.version);

  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=review`);
  const notice = page.getByRole('status').filter({ hasText: 'What changes in the settings' });
  await expect(notice).toBeVisible({ timeout: 60_000 });
  await expect(notice.getByText('Snapshot threads')).toBeVisible();
  await expect(notice.getByText('First copy', { exact: true })).toBeVisible();
  await expect(notice.getByText('Restart', { exact: true })).toBeVisible();

  // The version history says the same.
  await page.goto(`/p/${project.slug}/pipelines/${p.id}?tab=history`);
  await expect(page.getByText('Poll interval')).toBeVisible({ timeout: 30_000 });

  await api.send('DELETE', `${url}?drop_destinations=true`);
});
