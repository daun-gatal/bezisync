import { expect, test } from '@playwright/test';
import { ADMIN, Api, choose, defaultProject, signIn, unique } from './helpers';

test('wrong password shows a clear error and does not sign in', async ({ page }) => {
  await page.goto('/');
  await expect(page).toHaveURL(/\/login/);
  await page.getByLabel('Email').fill(ADMIN.email);
  await page.getByLabel('Password').fill('definitely-wrong');
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByRole('alert')).toContainText('Email or password is incorrect');
});

test('admin signs in, lands on pipelines and can sign out', async ({ page }) => {
  await signIn(page);
  await expect(page.getByRole('link', { name: 'New pipeline' })).toBeVisible();
  await page.getByRole('button', { name: ADMIN.email }).click();
  await page.getByRole('menuitem', { name: 'Sign out' }).click();
  await expect(page).toHaveURL(/\/login/);
  await page.goto('/p/default/pipelines');
  await expect(page).toHaveURL(/\/login\?next=/);
});

test('deep links survive sign-in', async ({ page }) => {
  await page.goto('/p/default/settings/audit');
  await expect(page).toHaveURL(/\/login\?next=/);
  await page.getByLabel('Email').fill(ADMIN.email);
  await page.getByLabel('Password').fill(ADMIN.password);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByRole('heading', { name: 'Audit log' })).toBeVisible();
  await expect(page.getByText('Created project Default').first()).toBeVisible();
});

test('admin adds a viewer who cannot see mutation controls', async ({ page, browser }) => {
  await signIn(page);
  const email = `${unique('viewer')}@example.com`;
  await page.goto('/p/default/settings/people');
  await page.getByRole('button', { name: 'Add person' }).click();
  const add = page.getByRole('dialog');
  await add.getByLabel('Email').fill(email);
  await add.getByLabel('Initial password').fill('viewer-password-123');
  await choose(add.getByLabel('Role', { exact: true }), 'Viewer');
  await add.getByRole('button', { name: 'Add person' }).click();
  await expect(page.getByText(`Added ${email} to Default as viewer`)).toBeVisible();

  const viewerPage = await (await browser.newContext()).newPage();
  await signIn(viewerPage, email, 'viewer-password-123');
  await expect(viewerPage.getByRole('link', { name: 'New pipeline' })).toHaveCount(0);
  // Settings are for Admins: no nav entry, and a direct link explains instead of showing them.
  await expect(viewerPage.getByRole('link', { name: 'Settings' })).toHaveCount(0);
  await viewerPage.goto('/p/default/settings/people');
  await expect(viewerPage.getByText('Settings are for Admins')).toBeVisible();
  await expect(viewerPage.getByRole('button', { name: 'Add person' })).toHaveCount(0);
});

test('admin creates a project and switches to it', async ({ page }) => {
  await signIn(page);
  const name = unique('Staging');
  await page.goto('/p/default/settings/projects');
  await page.getByLabel('Name').fill(name);
  await page.getByRole('button', { name: 'Create project' }).click();
  await expect(page.getByText(`Created ${name}`)).toBeVisible();
  await page.getByRole('button', { name: 'Switch project' }).click();
  await page.getByRole('menuitem', { name }).click();
  await expect(page.getByText(`Project: ${name}`)).toBeVisible();
});

test('API rejects state changes without the CSRF header and hides other projects', async () => {
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const res = await api.rawWithoutCsrf('PATCH', `/api/v1/projects/${project.id}`, { name: 'x' });
  expect(res.status()).toBe(403);
  expect((await res.json()).code).toBe('csrf_failed');
  await api.send('PATCH', `/api/v1/projects/${project.id}`, { name: 'Default' });
  const missing = await api.raw('GET', '/api/v1/projects/00000000-0000-0000-0000-000000000000');
  expect(missing.status()).toBe(404);
});
