import { expect, test } from '@playwright/test';
import { execSync } from 'node:child_process';
import { Api, choose, defaultProject, signIn, unique } from './helpers';
import { KAFKA, SOURCE_PG } from './fixtures';
import { waitFor } from './data';
import { walkWizard } from './walk';

function sh(cmd: string): string {
  return execSync(cmd, { encoding: 'utf8' });
}

test('admin edits, disables, enables and deletes a runtime from Settings', async ({ page }) => {
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const name = unique('mgmt');
  await api.send('POST', `/api/v1/projects/${project.id}/runtimes`, { name, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);

  await signIn(page);
  await page.goto('/p/default/settings/runtimes');
  const more = () => page.getByRole('button', { name: `More actions for ${currentName}` });
  let currentName = name;

  // Edit: rename and set a default image.
  await more().click();
  await page.getByRole('menuitem', { name: 'Edit' }).click();
  const dialog = page.getByRole('dialog');
  currentName = `${name}-r`;
  await dialog.getByLabel('Name', { exact: true }).fill(currentName);
  await expect(dialog.getByText('Pipeline manifests refer to runtimes by name')).toBeVisible();
  await choose(dialog.getByLabel('Default Debezium image'), 'quay.io/debezium/server:3.7.0.Final');
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  await expect(page.getByText(`Saved ${currentName}`)).toBeVisible();

  // Disable shows the effect first, then marks it.
  await more().click();
  await page.getByRole('menuitem', { name: 'Disable' }).click();
  await expect(page.getByRole('dialog').getByText('Nobody can create, move or deploy pipelines onto it')).toBeVisible();
  await page.getByRole('button', { name: 'Disable runtime' }).click();
  await expect(page.getByText('Running pipelines keep running; nothing new is deployed here.')).toBeVisible();
  const runtimes = await api.get<{ name: string; enabled: boolean }[]>(`/api/v1/projects/${project.id}/runtimes`);
  expect(runtimes.find((r) => r.name === currentName)?.enabled).toBe(false);

  await more().click();
  await page.getByRole('menuitem', { name: 'Enable' }).click();
  await expect(page.getByText(`Enabled ${currentName}`)).toBeVisible();

  // Delete needs the name typed.
  await more().click();
  await page.getByRole('menuitem', { name: 'Delete' }).click();
  const confirm = page.getByRole('dialog');
  await expect(confirm.getByRole('button', { name: 'Delete runtime' })).toBeDisabled();
  await confirm.getByRole('textbox').fill(currentName);
  await confirm.getByRole('button', { name: 'Delete runtime' }).click();
  await expect(page.getByText(`Deleted ${currentName}`)).toBeVisible();
  await expect(page.getByText(currentName, { exact: true })).toHaveCount(0);
  const audit = await api.get<{ items: { kind: string }[] }>(`/api/v1/projects/${project.id}/events?audit_only=true&limit=50`);
  expect(audit.items.map((e) => e.kind)).toEqual(expect.arrayContaining(['runtime.disabled', 'runtime.enabled', 'runtime.deleted']));
});

test('admin adds a placement profile; a pipeline picks it in Review and its pods carry it', async ({ page }) => {
  test.setTimeout(420_000);
  const api = await Api.signIn();
  const tag = unique('place');
  const project = await api.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: tag.slice(0, 40), name: `Place ${tag}` }, 201);
  const base = `/api/v1/projects/${project.id}`;
  const rt = await api.send<{ id: string; name: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);

  await signIn(page);
  await page.goto(`/p/${project.slug}/settings/runtimes`);
  await page.getByRole('button', { name: `More actions for ${rt.name}` }).click();
  await page.getByRole('menuitem', { name: 'Edit' }).click();
  const dialog = page.getByRole('dialog');
  const profiles = dialog.getByRole('region', { name: 'Placement profiles' });
  await profiles.getByRole('button', { name: 'Add a profile' }).click();
  await profiles.getByLabel('Profile name').fill('linux');
  await profiles.getByRole('button', { name: 'Add to node selector' }).click();
  await profiles.getByLabel('Node selector 1 key').fill('kubernetes.io/os');
  await profiles.getByLabel('Node selector 1 value').fill('linux');
  await profiles.getByRole('button', { name: 'Add to pod labels' }).click();
  await profiles.getByLabel('Pod labels 1 key').fill('placed-by');
  await profiles.getByLabel('Pod labels 1 value').fill(tag);
  // A template is refused before it is sent.
  await profiles.getByLabel('Node selector 1 value').fill('${OS}');
  await expect(dialog.getByRole('button', { name: 'Save changes' })).toBeDisabled();
  await profiles.getByLabel('Node selector 1 value').fill('linux');
  await dialog.getByRole('button', { name: 'Save changes' }).click();
  await expect(page.getByText(`Saved ${rt.name}`)).toBeVisible();
  const saved = await api.get<{ placement_profiles: { name: string; node_selector: Record<string, string> }[] }>(`${base}/runtimes/${rt.id}`);
  expect(saved.placement_profiles).toEqual([expect.objectContaining({ name: 'linux', node_selector: { 'kubernetes.io/os': 'linux' }, labels: { 'placed-by': tag } })]);

  // A pipeline picks it in Review; preflight checks the real nodes take it.
  const { password, ...pg } = SOURCE_PG;
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `k-${tag}`, config: KAFKA }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id, spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=review`);
  await choose(page.getByLabel('Placement'), 'linux');
  await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
  expect((await api.get<{ spec: { placement?: string } }>(url)).spec.placement).toBe('linux');
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });
  const pre = await api.send<{ sections: { key: string; report: { checks: { code: string; status: string; message: string }[] } }[] }>('POST', `${url}/preflight`, {});
  const placed = pre.sections.find((s) => s.key === 'runtime')?.report.checks.find((c) => c.code === 'placement');
  expect(placed?.status, placed?.message).toBe('pass');

  await api.send('POST', `${url}/deploy`, {});
  await waitFor('pipeline streaming', async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 300_000);
  const pod = JSON.parse(sh(`kubectl -n bezisync-e2e-runtime get pods -l placed-by=${tag} -o json`)) as { items: { spec: { nodeSelector?: Record<string, string> } }[] };
  expect(pod.items.length).toBe(1);
  expect(pod.items[0]!.spec.nodeSelector).toEqual({ 'kubernetes.io/os': 'linux' });

  // In use: removing the profile is refused, naming the pipeline.
  const cur = await api.get<{ version: number }>(`${base}/runtimes/${rt.id}`);
  const refused = await api.raw('PATCH', `${base}/runtimes/${rt.id}`, { placement_profiles: [] }, { 'if-match': `"${cur.version}"` });
  expect(refused.status()).toBe(409);
  expect(await refused.text()).toContain(`p-${tag}`);
  await api.send('DELETE', `${url}?drop_destinations=true`);
});
