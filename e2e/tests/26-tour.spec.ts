import { expect, test } from '@playwright/test';
import { Api, defaultProject, signIn, unique } from './helpers';

test('a new member finds the Tour, explores it and answers the closing question', async ({ page }) => {
  const admin = await Api.signIn();
  const project = await defaultProject(admin);
  const email = `${unique('visitor')}@example.com`;
  await admin.send('PUT', `/api/v1/projects/${project.id}/members`, { email, role: 'editor', initial_password: 'visitor-password-1' }, 200);

  await signIn(page, email, 'visitor-password-1');
  // A first visit suggests the Tour; opening it retires the suggestion.
  const sidebar = page.getByRole('complementary');
  await expect(sidebar.getByText('New here?')).toBeVisible();
  await sidebar.getByRole('link', { name: 'Take the tour →' }).click();
  await expect(page.getByRole('heading', { name: 'Debezium, without the surprises.', level: 1 })).toBeVisible();
  await expect(sidebar.getByText('New here?')).toHaveCount(0);

  // Lifecycle and Admin previews are tabs.
  await page.getByRole('tab', { name: /Retire/ }).click();
  await expect(page.getByRole('tabpanel').filter({ hasText: 'Stage 6 of 6' })).toContainText('replication slot');
  await page.getByRole('tab', { name: /Audit log/ }).click();
  await expect(page.locator('#admin-panel')).toContainText('Export JSON Lines');

  // "Try it" links land on real pages of this project.
  await page.getByRole('link', { name: /Check a connection/ }).click();
  await expect(page).toHaveURL(new RegExp(`/p/${project.slug}/connections`));
  await page.goBack();

  // One click answers the closing question; it reaches the Admins as feedback.
  await page.getByRole('button', { name: 'Maybe, not yet' }).click();
  await expect(page.getByRole('status').filter({ hasText: 'what would make it a yes' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Yes, we run Debezium today' })).toBeDisabled();
  const sent = await admin.get<{ items: { email: string; message: string; page: string }[] }>('/api/v1/feedback?kind=other');
  expect(sent.items).toContainEqual(
    expect.objectContaining({ email, message: 'Would your team use BeziSync? Maybe, not yet', page: `/p/${project.slug}/tour` }),
  );
});

test('the Help menu holds the tour, getting started, Learn and search', async ({ page }) => {
  await signIn(page);
  await page.getByRole('button', { name: /Help & tour/ }).click();
  for (const item of ['Tour', 'Getting started', 'Learn', /Find anything/]) {
    await expect(page.getByRole('menuitem', { name: item })).toBeVisible();
  }
  // Not an open lab: no testing policy.
  await expect(page.getByRole('menuitem', { name: 'Testing policy' })).toHaveCount(0);
  await page.getByRole('menuitem', { name: /Find anything/ }).click();
  await expect(page.getByRole('dialog')).toBeVisible();
});
