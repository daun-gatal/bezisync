import { execSync } from 'node:child_process';
import { expect, test } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';
import { HTTP_ECHO, HTTP_ECHO_FROM_RUNNER, SOURCE_PG } from './fixtures';
import { sql, waitFor } from './data';

/** An editor upgrades a running pipeline to another Debezium image from the Pipelines list, then rolls it back. */
const CUSTOM_IMAGE = process.env.CUSTOM_IMAGE ?? 'bezisync-local/debezium-custom:e2e';
const RUNTIME_NS = 'bezisync-e2e-runtime';

interface PipelineState { lifecycle: string; health: string; deployed_version: number | null; status: unknown }

function deployedImages(runtimeNs: string, name: string): string {
  return execSync(
    `kubectl -n ${runtimeNs} get deploy -l app.bezisync.io/managed-by=bezisync -o jsonpath='{range .items[*]}{.metadata.annotations.app\\.bezisync\\.io/pipeline-name}={.spec.template.spec.containers[0].image}{"\\n"}{end}'`,
    { shell: '/bin/bash', encoding: 'utf8' },
  ).split('\n').find((l) => l.startsWith(`${name}=`))?.split('=')[1] ?? '';
}

test('an editor changes a pipeline\'s Debezium version from the Pipelines list, then rolls back', async ({ page }) => {
  test.setTimeout(900_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('dv');
  const { password, ...pg } = SOURCE_PG;
  const path = `/dv-${tag}`;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: RUNTIME_NS } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'http', name: `h-${tag}`, config: { url: `${HTTP_ECHO}${path}` } }, 201);
  const name = `p-${tag}`;
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, {});
  const healthyOn = async (version: number) => waitFor(`healthy on v${version}`, async () => {
    const cur = await api.get<PipelineState>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.deployed_version === version && cur.lifecycle === 'streaming' && cur.health === 'healthy';
  }, 420_000);
  const arrives = async (marker: string) => {
    await sql('INSERT INTO shop.orders (customer_id, status, total) VALUES (1, $1, 1)', [marker]);
    await waitFor(`${marker} delivered`, async () => {
      const reqs = (await (await fetch(`${HTTP_ECHO_FROM_RUNNER}/requests${path}`)).json()) as { body: string }[];
      return reqs.some((r) => r.body.includes(marker));
    }, 180_000);
  };
  await healthyOn(1);
  const defaultImage = deployedImages(RUNTIME_NS, name);
  expect(defaultImage).toContain('quay.io/debezium/server:');

  // Upgrading is pipeline work: an editor does it without Settings.
  const editor = `${unique('dv-editor')}@example.com`;
  await api.send('PUT', `${base}/members`, { email: editor, role: 'editor', initial_password: 'dv-editor-password-1' }, 200);
  await signIn(page, editor, 'dv-editor-password-1');
  await expect(page.getByRole('link', { name: 'Settings' })).toHaveCount(0);
  await page.goto(`/p/default/pipelines?q=${name}`);
  await page.getByLabel(`Select ${name}`).check();
  await page.getByRole('button', { name: 'Change Debezium version…' }).click();
  const dialog = page.getByRole('dialog');
  await choose(dialog.getByLabel('Version', { exact: true }), 'Another image…');
  await dialog.getByLabel('Image reference').fill(CUSTOM_IMAGE);
  await expect(dialog.getByLabel('Deploy now')).toBeChecked();
  await dialog.getByRole('button', { name: 'Change and deploy' }).click();
  await expect(dialog.getByText(new RegExp(`${name} → v2: Deploying the new version`))).toBeVisible();
  await dialog.getByRole('button', { name: 'Done' }).click();

  await healthyOn(2);
  expect(deployedImages(RUNTIME_NS, name)).toBe(CUSTOM_IMAGE);
  // The list filters by version, and the pipeline says what it runs and why.
  await page.goto(`/p/default/pipelines?debezium=${encodeURIComponent(CUSTOM_IMAGE)}`);
  await expect(page.getByRole('link', { name, exact: true })).toBeVisible({ timeout: 30_000 });
  await page.getByRole('link', { name, exact: true }).click();
  await expect(page.getByText('chosen for this pipeline')).toBeVisible();
  await expect(page.getByText('not offered by your Admins')).toBeVisible();
  await arrives(`DV-${tag}-custom`);

  // Roll back like any other change: deploy the previous version.
  await api.send('POST', `${url}/deploy`, { version: 1 });
  await healthyOn(1);
  expect(deployedImages(RUNTIME_NS, name)).toBe(defaultImage);
  await arrives(`DV-${tag}-rollback`);

  await api.send('DELETE', url);
  await waitFor('pipeline deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
});

test('a version change says what it does to the settings, and refuses a setting the release lacks', async ({ page }) => {
  test.setTimeout(300_000);
  const api = await Api.signIn();
  const tag = unique('dvset');
  const project = await api.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: tag.slice(0, 40), name: `Dv ${tag}` }, 201);
  const base = `/api/v1/projects/${project.id}`;
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: RUNTIME_NS } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'http', name: `h-${tag}`, config: { url: `${HTTP_ECHO}/dvset-${tag}` } }, 201);
  const name = `p-${tag}`;
  // Version changes are for deployed pipelines: deploy one that skips unchanged updates (2.0+).
  const p0 = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' }, tuning: { 'debezium.source.skip.messages.without.change': 'true' } },
  }, 201);
  await api.send('POST', `${base}/pipelines/${p0.id}/deploy`, {});
  const p = await api.get<{ id: string; current_version: number }>(`${base}/pipelines/${p0.id}`);
  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines?q=${name}`);
  await page.getByLabel(`Select ${name}`).check();
  await page.getByRole('button', { name: 'Change Debezium version…' }).click();
  const dialog = page.getByRole('dialog');
  await choose(dialog.getByLabel('Version', { exact: true }), 'Another image…');
  await dialog.getByLabel('Image reference').fill('quay.io/debezium/server:1.9.7.Final');
  const effects = dialog.getByRole('alert').filter({ hasText: 'What changes in the settings' });
  await expect(effects).toBeVisible({ timeout: 20_000 });
  await expect(effects.getByText('It arrived in Debezium 2.0')).toBeVisible();
  await dialog.getByRole('button', { name: /Change and deploy|Save new versions/ }).click();
  await expect(dialog.getByText('Not changed', { exact: true })).toBeVisible();
  await expect(dialog.getByText(/doesn't have debezium\.source\.skip\.messages\.without\.change/)).toBeVisible();
  expect((await api.get<{ current_version: number }>(`${base}/pipelines/${p.id}`)).current_version).toBe(p.current_version);
  await api.send('DELETE', `${base}/pipelines/${p.id}?drop_destinations=true`);
});
