import { expect, test } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';

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
