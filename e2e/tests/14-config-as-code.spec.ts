import { execSync } from 'node:child_process';
import { expect, test } from '@playwright/test';
import { Api, BASE_URL, choose, defaultProject, signIn, unique } from './helpers';
import { HTTP_ECHO, SOURCE_PG } from './fixtures';

/** Config-as-code: export, import with a preview, adopt, promote, tokens and the real CLI. */
test('pipelines as YAML: export, preview, apply, adopt and promote', async ({ page }) => {
  test.setTimeout(300_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('cac');
  const { password, ...pg } = SOURCE_PG;
  await api.send('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  await api.send('POST', `${base}/connections`, { kind: 'sink', type: 'http', name: `hook-${tag}`, config: { url: `${HTTP_ECHO}/cac` } }, 201);
  const name = `yaml-${tag}`;
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name, source_id: src.id, spec: { source: { tables: { include: ['shop.orders'] } } },
  }, 201);

  // Export from the UI
  await signIn(page);
  await page.goto(`/p/default/pipelines/${p.id}?tab=config`);
  await page.getByRole('button', { name: 'Export YAML' }).click();
  const yamlText = (await page.getByLabel('Pipeline manifest').textContent()) ?? '';
  expect(yamlText).toContain('kind: Pipeline');
  expect(yamlText).toContain(`connection: src-${tag}`);
  expect(yamlText).not.toContain(password);
  await page.keyboard.press('Escape');

  // Import with a preview: give it a destination, runtime and a new naming
  const edited = yamlText
    .replace(/^ {2}sink:\n/m, `  runtime: rt-${tag}\n  sink:\n    connection: hook-${tag}\n`)
    .replace(/naming: .*/, `naming: ci.${tag}.{table}`);
  await page.goto('/p/default/pipelines');
  await page.getByRole('button', { name: 'Import YAML' }).click();
  const dialog = page.getByRole('dialog');
  await dialog.getByLabel('Manifest (YAML)').fill(edited);
  await dialog.getByRole('button', { name: 'Preview changes' }).click();
  await expect(dialog.getByText('will change')).toBeVisible();
  await expect(dialog.getByLabel('Changes')).toContainText(`ci.${tag}.{table}`);
  await dialog.getByRole('button', { name: 'Apply', exact: true }).click();
  await expect(dialog.getByText('updated', { exact: true })).toBeVisible();
  await dialog.getByRole('button', { name: 'Done' }).click();
  const after = await api.get<{ sink: { name: string } | null; runtime: { name: string } | null; current_version: number }>(`${base}/pipelines/${p.id}`);
  expect(after.sink?.name).toBe(`hook-${tag}`);
  expect(after.runtime?.name).toBe(`rt-${tag}`);
  // The undeployed draft is updated in place: one change, one version.
  expect(after.current_version).toBe(1);

  // Adopt an existing Debezium Server
  await page.getByRole('button', { name: 'Adopt existing' }).click();
  await dialog.getByLabel('application.properties').fill([
    'debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector',
    `debezium.source.database.hostname=${SOURCE_PG.host}`,
    'debezium.source.database.dbname=shop',
    'debezium.source.topic.prefix=legacy',
    'debezium.source.slot.name=old_slot',
    'debezium.source.table.include.list=shop.customers',
    'debezium.sink.type=http',
  ].join('\n'));
  await dialog.getByRole('button', { name: 'Analyse' }).click();
  await expect(dialog.getByText(/does not reuse 'old_slot'/)).toBeVisible();
  await dialog.getByLabel('Pipeline name').fill(`adopted-${tag}`);
  await choose(dialog.getByLabel('Source connection'), `src-${tag}`);
  await dialog.getByRole('button', { name: 'Create draft pipeline' }).click();
  await expect(page).toHaveURL(/step=tables/);
  await expect(page.getByRole('checkbox', { name: 'shop.customers', exact: true })).toBeChecked();

  // Promote to another project
  const target = await api.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: `prod-${tag}`.slice(0, 40), name: `Prod ${tag}` }, 201);
  const tbase = `/api/v1/projects/${target.id}`;
  await api.send('POST', `${tbase}/connections`, { kind: 'source', type: 'postgresql', name: `prod-src`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  await page.reload();
  await page.goto(`/p/default/pipelines/${p.id}?tab=config`);
  await page.getByRole('button', { name: 'Promote…' }).click();
  await choose(dialog.getByLabel('To project'), `Prod ${tag}`);
  await choose(dialog.getByLabel(/Source there/), 'prod-src');
  await dialog.getByRole('button', { name: 'Preview' }).click();
  await expect(dialog.getByText('will be created')).toBeVisible();
  await dialog.getByRole('button', { name: 'Promote', exact: true }).click();
  await expect(dialog.getByText('created', { exact: true })).toBeVisible();
  const promoted = await api.get<{ name: string; spec: unknown }[]>(`${tbase}/pipelines`);
  expect(promoted.map((x) => x.name)).toEqual([name]);
});

test('an API token drives the real CLI, and revoking it locks it out', async ({ page }) => {
  test.setTimeout(240_000);
  await signIn(page);
  await page.goto('/p/default/account');
  await page.getByRole('button', { name: 'New token' }).click();
  const tokenName = unique('ci');
  await page.getByRole('dialog').getByLabel('Name').fill(tokenName);
  await page.getByRole('dialog').getByRole('button', { name: 'Create token' }).click();
  const tokenBlock = page.getByLabel('API token', { exact: true });
  await expect(tokenBlock).toContainText('bzt_');
  const token = ((await tokenBlock.textContent()) ?? '').trim();
  expect(token).toMatch(/^bzt_/);
  // Shown once: Done needs “I’ve copied it”.
  await page.getByRole('checkbox', { name: /I’ve copied it/ }).check();
  await page.getByRole('button', { name: 'Done' }).click();
  await expect(page.getByText(tokenName)).toBeVisible();

  // A pipeline of its own, so the export has something to show.
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const { password, ...pg } = SOURCE_PG;
  const tag = unique('cli');
  const src = await api.send<{ id: string }>('POST', `/api/v1/projects/${project.id}/connections`, {
    kind: 'source', type: 'postgresql', name: `cli-src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password },
  }, 201);
  const draft = await api.send<{ id: string }>('POST', `/api/v1/projects/${project.id}/pipelines`, {
    name: `cli-${tag}`, source_id: src.id, spec: { source: { tables: { include: ['shop.orders'] } } },
  }, 201);

  // The CLI shipped in the backend image, run inside the cluster against the API. The pod's
  // output is read from its logs once it finished (attaching to a short-lived pod can miss it).
  const image = `bezisync-local/backend:${process.env.E2E_TAG}`;
  const pod = unique('cli');
  const kubectl = (args: string) => execSync(`kubectl -n bezisync-e2e ${args}`, { shell: '/bin/bash', encoding: 'utf8', timeout: 120_000 });
  kubectl(
    `run ${pod} --restart=Never --image=${image} --image-pull-policy=Never ` +
      `--env=BEZISYNC_URL=http://bezisync-api:8000 --env=BEZISYNC_TOKEN=${token} --env=BEZISYNC_PROJECT=default -- remote export cli-${tag}`,
  );
  kubectl(`wait --for=jsonpath='{.status.phase}'=Succeeded pod/${pod} --timeout=90s`);
  const out = kubectl(`logs ${pod}`);
  kubectl(`delete pod ${pod} --wait=false`);
  expect(out).toContain(`name: cli-${tag}`);
  expect(out).toContain('apiVersion: bezisync.io/v1');

  await page.getByRole('button', { name: 'Revoke' }).first().click();
  await page.getByRole('dialog').getByRole('button', { name: 'Revoke' }).click();
  await expect(page.getByText('Token revoked')).toBeVisible();
  const res = await fetch(`${BASE_URL}/api/v1/auth/me`, { headers: { authorization: `Bearer ${token}` } });
  await api.send('DELETE', `/api/v1/projects/${project.id}/pipelines/${draft.id}`);
  expect(res.status).toBe(401);
});
