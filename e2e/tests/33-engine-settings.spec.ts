import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from '@playwright/test';
import { Api, choose, signIn, unique } from './helpers';
import { KAFKA, SOURCE_PG } from './fixtures';
import { layoutProblems } from './layout';
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

test('preflight suggests a setting from its evidence; one click uses it (Guided mode too)', async ({ page }) => {
  test.setTimeout(180_000);
  const api = await Api.signIn();
  const tag = unique('suggest');
  const project = await api.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: tag.slice(0, 40), name: `Suggest ${tag}` }, 201);
  const base = `/api/v1/projects/${project.id}`;
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `k-${tag}`, config: KAFKA }, 201);
  // A column left out: an update that changed only it would arrive with nothing changed.
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] }, columns: { 'shop.orders': { exclude: ['status'], mask: [] } } } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=review`);
  await expect(page.getByText('A suggested setting')).toBeVisible({ timeout: 60_000 });
  await page.getByRole('button', { name: 'Use Skip unchanged updates on' }).click();
  await expect(page.getByText('A suggested setting')).toBeHidden();
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  expect((await api.get<{ spec: { tuning?: Record<string, string> } }>(url)).spec.tuning).toEqual({ 'debezium.source.skip.messages.without.change': 'true' });
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });
});

/** A project with a Postgres source, a Kafka destination and a draft pipeline on them. */
async function draftPipeline(api: Api, name: string, spec: Record<string, unknown> = {}, policies: Record<string, unknown> = {}) {
  const tag = unique(name);
  const project = await api.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: tag.slice(0, 40), name: `${name} ${tag}` }, 201);
  const base = `/api/v1/projects/${project.id}`;
  await api.send('PUT', `${base}/policies`, { allow_advanced_properties: true, ...policies });
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `k-${tag}`, config: KAFKA }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, spec: { source: { tables: { include: ['shop.orders'] } }, ...spec },
  }, 201);
  return { project, base, url: `${base}/pipelines/${p.id}`, pipelineId: p.id };
}

/** Counts the pipeline's saves (PATCH requests) from now on. */
function countSaves(page: Page, url: string): { count: () => number; stop: () => void } {
  let n = 0;
  const on = (r: { method(): string; url(): string }) => {
    if (r.method() === 'PATCH' && r.url().endsWith(url)) n++;
  };
  page.on('request', on);
  return { count: () => n, stop: () => page.off('request', on) };
}

test('a refused advanced property never loops', async ({ page }) => {
  test.setTimeout(180_000);
  const api = await Api.signIn();
  await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'expert' });
  try {
    const d = await draftPipeline(api, 'refuse');
    await signIn(page);
    await page.goto(`/p/${d.project.slug}/pipelines/new?draft=${d.pipelineId}&step=review`);
    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
    const saves = countSaves(page, d.url);
    await page.getByRole('button', { name: 'Add a property' }).click();
    await page.getByLabel('Property 1 name').fill('debezium.format.value');
    await page.getByLabel('Property 1 value').fill('json');
    await expect(page.getByText('The event format is chosen on the Destination step.')).toBeVisible();
    await expect(page.getByText(/^Not saved/)).toBeVisible();
    await page.waitForTimeout(4_000);
    expect(saves.count(), 'a refused key kept saving').toBeLessThanOrEqual(1);
    // Removing it saves once more, then nothing.
    await page.getByRole('button', { name: 'Remove property 1' }).click();
    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
    const settled = saves.count();
    await page.waitForTimeout(4_000);
    expect(saves.count()).toBe(settled);
    saves.stop();
  } finally {
    await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'guided' });
  }
});

test('a save the server refuses pauses until the draft changes', async ({ page }) => {
  test.setTimeout(180_000);
  const api = await Api.signIn();
  await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'expert' });
  try {
    const d = await draftPipeline(api, 'pause');
    await signIn(page);
    await page.goto(`/p/${d.project.slug}/pipelines/new?draft=${d.pipelineId}&step=review`);
    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
    // Batch size must stay below the queue size: a rule only the server checks.
    await addSetting(page, 'Queue size');
    await addSetting(page, 'Batch size');
    const queue = page.locator('[id="tuning-debezium.source.max.queue.size"]');
    const batch = page.locator('[id="tuning-debezium.source.max.batch.size"]');
    await queue.fill('8192');
    await queue.press('Enter');
    const saves = countSaves(page, d.url);
    await batch.fill('8192');
    await batch.press('Enter');
    await expect(page.getByText('Couldn’t save your changes')).toBeVisible({ timeout: 20_000 });
    await expect(page.getByRole('alert').filter({ hasText: /batch size \(8192\) must stay below the queue size/ })).toBeVisible();
    await expect(page.getByRole('button', { name: 'Try again' })).toHaveCount(0);
    await page.waitForTimeout(4_000);
    expect(saves.count(), 'a refused save was repeated').toBe(1);
    // Changing the draft tries again, and it saves.
    await queue.fill('16384');
    await queue.press('Enter');
    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
    await expect(page.getByText('Couldn’t save your changes')).toHaveCount(0);
    saves.stop();
    const saved = await api.get<{ spec: { tuning?: Record<string, string> } }>(d.url);
    expect(saved.spec.tuning).toEqual({ 'debezium.source.max.batch.size': '8192', 'debezium.source.max.queue.size': '16384' });
  } finally {
    await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'guided' });
  }
});

test('Guided mode still checks the raw properties a pipeline holds', async ({ page }) => {
  test.setTimeout(120_000);
  const api = await Api.signIn();
  const d = await draftPipeline(api, 'guidedraw', { advanced: { 'debezium.source.provide.transaction.metadata': 'false' } });
  await signIn(page);
  const classified = page.waitForRequest((r) => r.method() === 'POST' && r.url().endsWith(`/pipelines/${d.pipelineId}/settings:classify`), { timeout: 20_000 });
  await page.goto(`/p/${d.project.slug}/pipelines/new?draft=${d.pipelineId}&step=review`);
  await classified;
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
});

test('Try again saves once more after a network error', async ({ page }) => {
  test.setTimeout(120_000);
  const api = await Api.signIn();
  const d = await draftPipeline(api, 'retry');
  await signIn(page);
  await page.goto(`/p/${d.project.slug}/pipelines/new?draft=${d.pipelineId}&step=review`);
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  // The next save never reaches the server.
  let failNext = true;
  await page.route(`**${d.url}`, (route) => {
    if (route.request().method() === 'PATCH' && failNext) {
      failNext = false;
      return route.abort('connectionreset');
    }
    return route.continue();
  });
  const saves = countSaves(page, d.url);
  await page.getByRole('radio', { name: /Medium/ }).click();
  await expect(page.getByText('Couldn’t save your changes')).toBeVisible({ timeout: 20_000 });
  await page.waitForTimeout(3_000);
  expect(saves.count(), 'a failed save was repeated by itself').toBe(1);
  await page.getByRole('button', { name: 'Try again' }).click();
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  expect(saves.count()).toBe(2);
  saves.stop();
});

test('the badge names the field to fix and goes there', async ({ page }) => {
  test.setTimeout(180_000);
  const api = await Api.signIn();
  await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'expert' });
  try {
    const d = await draftPipeline(api, 'badge', {}, { max_resources: 'small' });
    await signIn(page);
    await page.goto(`/p/${d.project.slug}/pipelines/new?draft=${d.pipelineId}&step=review`);
    await page.getByRole('radio', { name: /Custom/ }).click();
    // Small allows 0.5 CPU and 768 MiB: the CPU fits, the memory doesn't.
    await choose(page.getByLabel('CPU', { exact: true }), '0.5 CPU');
    await choose(page.getByLabel('Memory', { exact: true }), '1 GiB');
    const memory = page.locator('[data-review-field="memory"]');
    const cpu = page.locator('[data-review-field="cpu"]');
    await expect(memory.getByText('This project allows at most 768 MiB.')).toBeVisible();
    await expect(cpu.getByRole('alert')).toHaveCount(0);
    const badge = page.getByRole('button', { name: 'Not saved: fix Memory' });
    await expect(badge).toBeVisible();
    await expect(page.getByText('Fix Memory first.').first()).toBeVisible();
    await page.evaluate(() => window.scrollTo(0, 0));
    await badge.click();
    await expect.poll(() => page.evaluate(() => document.activeElement?.closest('[data-review-field]')?.getAttribute('data-review-field'))).toBe('memory');
    // Narrow screens: the badge and its card fit; and they are accessible.
    await page.setViewportSize({ width: 390, height: 900 });
    expect(await layoutProblems(page)).toEqual([]);
    const axe = await new AxeBuilder({ page }).include('[aria-live="polite"]').include('[data-review-field="memory"]').withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
    expect(axe.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical').map((v) => v.id)).toEqual([]);
    await page.setViewportSize({ width: 1280, height: 900 });
    // Fixing it saves.
    await choose(page.getByLabel('Memory', { exact: true }), '768 MiB');
    await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  } finally {
    await api.send('PATCH', '/api/v1/auth/me', { ui_mode: 'guided' });
  }
});

test('Guided mode lists the settings that block saving, with Remove', async ({ page }) => {
  test.setTimeout(120_000);
  const api = await Api.signIn();
  const key = 'debezium.source.snapshot.max.threads';
  const d = await draftPipeline(api, 'guidedblock', { tuning: { [key]: '2' } });
  // The project hides that setting afterwards: the saved value is one this pipeline can't use.
  await api.send('PUT', `${d.base}/policies`, { allow_advanced_properties: true, setting_limits: { [key]: { hide: true } } });
  await signIn(page);
  await page.goto(`/p/${d.project.slug}/pipelines/new?draft=${d.pipelineId}&step=review`);
  const card = page.getByRole('region', { name: 'Settings that block saving' });
  await expect(card).toBeVisible();
  await expect(card.getByText(key)).toBeVisible();
  await expect(card.getByText('Switch to Expert mode (account menu) to edit these.')).toBeVisible();
  await expect(page.getByRole('region', { name: 'Engine settings' })).toHaveCount(0);
  // Nothing is unsaved (the value is stored), but it must go before a deploy.
  const badge = page.getByRole('button', { name: `Fix ${key} before deploying` });
  await expect(badge).toBeVisible();
  await badge.click();
  await expect(card.getByRole('button', { name: `Remove ${key}` })).toBeFocused();
  await card.getByRole('button', { name: `Remove ${key}` }).click();
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  await expect(card).toHaveCount(0);
  expect((await api.get<{ spec: { tuning?: Record<string, string> } }>(d.url)).spec.tuning ?? {}).toEqual({});
});
