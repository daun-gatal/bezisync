import { expect, test } from '@playwright/test';
import { ADMIN, Api, defaultProject, signIn, unique } from './helpers';

test('the sidebar collapses to an icon rail and remembers it', async ({ page }) => {
  await signIn(page);
  const sidebar = page.locator('aside');
  await expect(sidebar).toHaveAttribute('data-collapsed', 'false');
  const wide = (await sidebar.boundingBox())?.width ?? 0;

  await page.getByRole('button', { name: 'Collapse sidebar' }).click();
  await expect(sidebar).toHaveAttribute('data-collapsed', 'true');
  expect((await sidebar.boundingBox())?.width ?? 0).toBeLessThan(wide / 2);
  // Icons stay usable: the links keep their accessible names.
  await page.getByRole('link', { name: 'Connections' }).click();
  await expect(page.getByRole('heading', { name: 'Connections', level: 1 })).toBeVisible();

  await page.reload();
  await expect(sidebar).toHaveAttribute('data-collapsed', 'true');
  await page.getByRole('button', { name: 'Expand sidebar' }).click();
  await expect(sidebar).toHaveAttribute('data-collapsed', 'false');
});

test('on a phone the navigation folds behind a menu button', async ({ browser }) => {
  const context = await browser.newContext({ viewport: { width: 390, height: 844 } });
  const page = await context.newPage();
  await signIn(page);
  const nav = page.getByRole('navigation', { name: 'Primary' });
  await expect(nav).toBeHidden();
  await page.getByRole('button', { name: 'Open menu' }).click();
  await expect(nav).toBeVisible();
  await nav.getByRole('link', { name: 'Alerts' }).click();
  await expect(page.getByRole('heading', { name: 'Alerts', level: 1 })).toBeVisible();
  await expect(nav).toBeHidden(); // closes after navigating
  await context.close();
});

test('an editor has no Settings but manages their own API tokens under Account', async ({ browser }) => {
  const admin = await Api.signIn();
  const project = await defaultProject(admin);
  const email = `${unique('editor')}@example.com`;
  await admin.send('PUT', `/api/v1/projects/${project.id}/members`, { email, role: 'editor', initial_password: 'editor-password-123' });

  const page = await (await browser.newContext()).newPage();
  await signIn(page, email, 'editor-password-123');
  await expect(page.getByRole('link', { name: 'Settings' })).toHaveCount(0);
  await page.getByRole('button', { name: email }).click();
  await page.getByRole('menuitem', { name: 'Account & API tokens' }).click();
  await expect(page.getByRole('heading', { name: 'Account', level: 1 })).toBeVisible();
  await page.getByRole('button', { name: 'New token' }).click();
  const name = unique('cli');
  await page.getByRole('dialog').getByLabel('Name').fill(name);
  await page.getByRole('dialog').getByRole('button', { name: 'Create token' }).click();
  await expect(page.getByLabel('API token', { exact: true })).toContainText('bzt_');
  // Shown once: Done needs “I’ve copied it”.
  await page.getByRole('checkbox', { name: /I’ve copied it/ }).check();
  await page.getByRole('button', { name: 'Done' }).click();
  await expect(page.getByText(name)).toBeVisible();
});

test('the theme is chosen in the user menu or on Account, applies at once and is remembered', async ({ browser }) => {
  const context = await browser.newContext({ colorScheme: 'light' });
  const page = await context.newPage();
  await signIn(page);
  const html = page.locator('html');
  await expect(html).not.toHaveAttribute('data-theme', /.+/);

  await page.getByRole('button', { name: ADMIN.email }).click();
  await page.getByRole('menuitemradio', { name: /Dark/ }).click();
  await expect(html).toHaveAttribute('data-theme', 'dark');
  // The menu stays open, so the choice can be compared and changed again.
  await expect(page.getByRole('menuitemradio', { name: /Dark/ })).toHaveAttribute('aria-checked', 'true');
  await page.keyboard.press('Escape');

  // Applied before the first paint on every load (public/theme.js), not only after React starts.
  await page.reload();
  await expect(html).toHaveAttribute('data-theme', 'dark');

  await page.getByRole('button', { name: ADMIN.email }).click();
  await page.getByRole('menuitem', { name: 'Account & API tokens' }).click();
  const choice = page.getByRole('group', { name: 'Theme' });
  await expect(choice.getByRole('button', { name: 'Dark' })).toHaveAttribute('aria-pressed', 'true');
  await choice.getByRole('button', { name: 'System' }).click();
  await expect(html).not.toHaveAttribute('data-theme', /.+/);
  await page.reload();
  await expect(html).not.toHaveAttribute('data-theme', /.+/);
  await context.close();
});
