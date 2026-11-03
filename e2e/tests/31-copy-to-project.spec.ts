import { expect, test } from '@playwright/test';
import { Api, choose, signIn, unique } from './helpers';
import { SOURCE_PG, VAULT } from './fixtures';

/**
 * Admins edit secret stores safely (a failing test saves nothing) and copy connections and stores
 * to another project: saved passwords stay behind, clashes are resolved, Promote offers the copy.
 */
test.describe.configure({ mode: 'serial' });

interface Project {
  id: string;
  slug: string;
  name: string;
}

async function newProject(api: Api, prefix: string): Promise<Project> {
  const name = unique(prefix);
  return api.send<Project>('POST', '/api/v1/projects', { slug: name.toLowerCase().slice(0, 40), name, description: '' }, 201);
}

const { password, ...pg } = SOURCE_PG;
const pgConfig = { ...pg, ssl_mode: 'disable' };

test('an Admin edits a secret store, a failing test saves nothing, and the store is copied to another project', async ({ page }) => {
  const api = await Api.signIn();
  const src = await newProject(api, 'stores');
  const dst = await newProject(api, 'stores-to');
  const store = unique('vault').toLowerCase().slice(0, 30);
  await api.send('POST', `/api/v1/projects/${src.id}/secret-stores`, { name: store, kind: 'vault', config: { address: VAULT.address }, secrets: { token: VAULT.token } }, 201);

  await signIn(page);
  await page.goto(`/p/default/settings/secret-stores?scope=${src.slug}`);
  const card = page.getByRole('region', { name: store });
  // The card says what the store is set to and which credentials are saved, never their values.
  await expect(card.getByText(VAULT.address)).toBeVisible();
  await expect(card.getByText(/Token · replaced/)).toBeVisible();
  await expect(page.getByText(VAULT.token)).toHaveCount(0);

  await card.getByRole('button', { name: 'Edit' }).click();
  const dialog = page.getByRole('dialog');
  await expect(dialog.getByLabel('Name', { exact: true })).toHaveValue(store);
  await dialog.getByLabel('Address').fill('http://127.0.0.1:1');
  await dialog.getByRole('button', { name: 'Test and save' }).click();
  await expect(dialog.getByText(/nothing was saved/)).toBeVisible();
  await expect(dialog.getByRole('button', { name: 'Save anyway' })).toBeVisible();
  await dialog.getByRole('button', { name: 'Cancel' }).click();
  await expect(card.getByText(VAULT.address)).toBeVisible();
  const stored = await api.get<{ config: { address: string }; version: number }[]>(`/api/v1/projects/${src.id}/secret-stores`);
  expect(stored[0]).toMatchObject({ config: { address: VAULT.address }, version: 1 });

  await card.getByRole('button', { name: `More actions for ${store}` }).click();
  await page.getByRole('menuitem', { name: /Copy to project/ }).click();
  const copy = page.getByRole('dialog');
  await choose(copy.getByLabel('Copy to'), dst.name);
  await copy.getByRole('button', { name: `Copy to ${dst.name}` }).click();
  await expect(page.getByText(`Copied ${store} in ${dst.name}`)).toBeVisible();
  const copied = await api.get<{ name: string; secret_fields_set: string[] }[]>(`/api/v1/projects/${dst.id}/secret-stores`);
  expect(copied).toEqual([expect.objectContaining({ name: store, secret_fields_set: [] })]);
});

test('connections are copied to another project without their passwords, and a clash copies under a new name', async ({ page }) => {
  const api = await Api.signIn();
  const src = await newProject(api, 'copy-from');
  const dst = await newProject(api, 'copy-to');
  const tag = unique('cp').toLowerCase();
  for (const n of ['a', 'b']) {
    await api.send('POST', `/api/v1/projects/${src.id}/connections`, { kind: 'source', type: 'postgresql', name: `${tag}-${n}`, config: pgConfig, secrets: { password } }, 201);
  }
  await api.send('POST', `/api/v1/projects/${dst.id}/connections`, { kind: 'source', type: 'postgresql', name: `${tag}-a`, config: pgConfig, secrets: { password } }, 201);

  await signIn(page);
  await page.goto(`/p/${src.slug}/connections`);
  await page.getByRole('checkbox', { name: `Select ${tag}-a` }).check();
  await page.getByRole('checkbox', { name: `Select ${tag}-b` }).check();
  await page.getByRole('button', { name: 'Copy to project…' }).click();
  const dialog = page.getByRole('dialog');
  await choose(dialog.getByLabel('Copy to'), dst.name);
  // The plan: one name is taken (copied under a free name by default), passwords stay behind.
  await expect(dialog.getByText(`${dst.name} already has ${tag}-a.`)).toBeVisible();
  await expect(dialog.getByRole('button', { name: `Copy as ${tag}-a-2` })).toHaveAttribute('aria-pressed', 'true');
  await expect(dialog.getByText(`Needs its password in ${dst.name}`)).toHaveCount(2);
  await dialog.getByRole('button', { name: `Copy 2 to ${dst.name}` }).click();
  await expect(dialog.getByRole('list', { name: 'Copied' }).getByText('Needs its password')).toHaveCount(2);
  await dialog.getByRole('button', { name: 'Done' }).click();

  const there = await api.get<{ name: string; missing_secrets: string[]; secret_fields_set: string[] }[]>(`/api/v1/projects/${dst.id}/connections`);
  const byName = Object.fromEntries(there.map((c) => [c.name, c]));
  expect(byName[`${tag}-a-2`]).toMatchObject({ missing_secrets: ['password'], secret_fields_set: [] });
  expect(byName[`${tag}-b`]).toMatchObject({ missing_secrets: ['password'] });
  expect(byName[`${tag}-a`]).toMatchObject({ missing_secrets: [], secret_fields_set: ['password'] });

  // There, the copy says what it lacks.
  await page.goto(`/p/${dst.slug}/connections`);
  await expect(page.getByRole('row', { name: new RegExp(`${tag}-a-2`) }).getByText('Needs its password')).toBeVisible();
  const audit = await api.get<{ items: { summary: string }[] }>(`/api/v1/projects/${src.id}/events?audit_only=true`);
  expect(audit.items.map((e) => e.summary)).toContain(`Copied source ${tag}-a to ${dst.name} as ${tag}-a-2, without its saved secrets`);
});

test('Promote offers to copy the source when the other project has none', async ({ page }) => {
  const api = await Api.signIn();
  const src = await newProject(api, 'promo-from');
  const dst = await newProject(api, 'promo-to');
  const tag = unique('pr').toLowerCase();
  const conn = await api.send<{ id: string }>('POST', `/api/v1/projects/${src.id}/connections`, { kind: 'source', type: 'postgresql', name: `${tag}-db`, config: pgConfig, secrets: { password } }, 201);
  const pipe = await api.send<{ id: string }>('POST', `/api/v1/projects/${src.id}/pipelines`, { name: `${tag}-cdc`, source_id: conn.id, spec: { source: { tables: { include: ['shop.customers'] } } } }, 201);

  await signIn(page);
  await page.goto(`/p/${src.slug}/pipelines/${pipe.id}?tab=config`);
  await page.getByRole('button', { name: 'Promote…' }).click();
  const promote = page.getByRole('dialog', { name: /Promote/ });
  await choose(promote.getByLabel('To project'), dst.name);
  await expect(promote.getByText(`${dst.name} has no PostgreSQL source yet.`)).toBeVisible();
  await promote.getByRole('button', { name: `Copy ${tag}-db to ${dst.name}…` }).click();
  const copy = page.getByRole('dialog', { name: /to another project/ });
  await copy.getByRole('button', { name: `Copy to ${dst.name}` }).click();
  await copy.getByRole('button', { name: 'Continue' }).click();
  // Back in Promote: the copy is picked, and it says the pipeline can't deploy until its password is in.
  await expect(promote.getByText(/needs its password in/)).toBeVisible();
  await promote.getByRole('button', { name: 'Preview' }).click();
  await expect(promote.getByText('will be created')).toBeVisible();
  await promote.getByRole('button', { name: 'Promote', exact: true }).click();
  await expect(promote.getByText('created', { exact: true })).toBeVisible();
  const promoted = await api.get<{ name: string; source: { name: string } }[]>(`/api/v1/projects/${dst.id}/pipelines`);
  expect(promoted).toEqual([expect.objectContaining({ name: `${tag}-cdc`, source: expect.objectContaining({ name: `${tag}-db` }) })]);
});
