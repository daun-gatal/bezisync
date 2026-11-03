import { expect, test, type Page } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';
import { KAFKA, LEGACY_PG, SOURCE_PG } from './fixtures';

async function fillPostgres(page: Page, name: string, db: typeof SOURCE_PG) {
  const dialog = page.getByRole('dialog');
  await dialog.getByRole('button', { name: /PostgreSQL/ }).click();
  await dialog.getByLabel('Name', { exact: true }).fill(name);
  await dialog.getByLabel('Host').fill(db.host);
  await dialog.getByLabel('Port').fill(String(db.port));
  await dialog.getByLabel('Database').fill(db.database);
  await dialog.getByLabel('User').fill(db.user);
  await choose(dialog.getByLabel('TLS'), 'disable');
  await dialog.getByLabel('Password').fill(db.password);
}

test('add a CDC-ready PostgreSQL source through the UI and see it ready', async ({ page }) => {
  await signIn(page);
  await page.getByRole('link', { name: 'Connections' }).click();
  await page.getByRole('button', { name: 'Add source' }).first().click();
  const name = unique('shop-pg');
  await fillPostgres(page, name, SOURCE_PG);
  const dialog = page.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Test connection' }).click();
  const checks = dialog.getByRole('region', { name: 'Readiness checks' });
  await expect(checks.getByText(/Ready/)).toBeVisible();
  await expect(checks.getByText('WAL retention limit')).toBeVisible();
  await dialog.getByRole('button', { name: 'Save source' }).click();
  await expect(page.getByText(`Added ${name}`)).toBeVisible();
  const row = page.getByRole('row', { name: new RegExp(name) });
  await row.getByRole('button', { name: 'Re-check' }).click();
  await expect(row.getByText(/Ready/)).toBeVisible();
});

test('a database without logical replication gets a blocking check with the exact fix per provider', async ({ page }) => {
  await signIn(page);
  await page.goto('/p/default/connections');
  await page.getByRole('button', { name: 'Add source' }).first().click();
  await fillPostgres(page, unique('legacy'), LEGACY_PG);
  const dialog = page.getByRole('dialog');
  await dialog.getByRole('button', { name: 'Test connection' }).click();
  const checks = dialog.getByRole('region', { name: 'Readiness checks' });
  await expect(checks.getByText('1 issue blocks deploy')).toBeVisible();
  await expect(checks.getByText("The database isn't set up to publish changes yet.")).toBeVisible();
  await expect(checks.getByLabel('Fix for Logical replication')).toContainText('ALTER SYSTEM SET wal_level = logical;');
  await expect(checks.getByText(/needs a restart$/)).toBeVisible();

  await dialog.getByRole('radio', { name: 'AWS RDS / Aurora' }).click();
  await choose(dialog.getByLabel('TLS'), 'disable');
  await dialog.getByRole('button', { name: 'Test connection' }).click();
  await expect(checks.getByLabel('Fix for Logical replication')).toContainText('rds.logical_replication');
});

test('add Kafka as a destination and confirm topic permissions', async ({ page }) => {
  await signIn(page);
  await page.goto('/p/default/connections?kind=sink');
  await page.getByRole('button', { name: 'Add destination' }).first().click();
  const dialog = page.getByRole('dialog');
  await dialog.getByRole('button', { name: /^Kafka/ }).click();
  const name = unique('events-kafka');
  await dialog.getByLabel('Name', { exact: true }).fill(name);
  await dialog.getByLabel('Bootstrap servers').fill(KAFKA.bootstrap_servers);
  await dialog.getByRole('button', { name: 'Test connection' }).click();
  const checks = dialog.getByRole('region', { name: 'Readiness checks' });
  await expect(checks.getByText('Ready', { exact: true })).toBeVisible();
  await dialog.getByRole('button', { name: 'Save destination' }).click();
  await expect(page.getByRole('row', { name: new RegExp(name) })).toBeVisible();
});

test('secrets are write-only and deleting needs typed confirmation', async ({ page }) => {
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const name = unique('secret-pg');
  const conn = await api.send<{ id: string; secret_fields_set: string[] }>('POST', `/api/v1/projects/${project.id}/connections`, {
    kind: 'source', type: 'postgresql', name, config: { ...SOURCE_PG, password: undefined, ssl_mode: 'disable' }, secrets: { password: SOURCE_PG.password },
  }, 201);
  expect(conn.secret_fields_set).toEqual(['password']);
  const listed = JSON.stringify(await api.get(`/api/v1/projects/${project.id}/connections`));
  expect(listed).not.toContain(SOURCE_PG.password);

  await signIn(page);
  await page.goto('/p/default/connections');
  const row = page.getByRole('row', { name: new RegExp(name) });
  await row.getByRole('button', { name: 'Edit' }).click();
  await expect(page.getByRole('dialog').getByLabel('Password')).toHaveValue('');
  await expect(page.getByRole('dialog').getByText('Stored. Leave blank to keep the current value.')).toBeVisible();
  await page.getByRole('dialog').getByRole('button', { name: 'Save changes' }).click();
  await expect(page.getByText(`Saved ${name}`)).toBeVisible();
  await row.getByRole('button', { name: 'Re-check' }).click();
  await expect(row.getByText(/Ready/)).toBeVisible();

  await row.getByRole('button', { name: 'Delete' }).click();
  const confirm = page.getByRole('dialog');
  await expect(confirm.getByRole('button', { name: 'Delete' })).toBeDisabled();
  await confirm.getByRole('textbox').fill(name);
  await confirm.getByRole('button', { name: 'Delete' }).click();
  await expect(page.getByText(`Deleted ${name}`)).toBeVisible();
});
