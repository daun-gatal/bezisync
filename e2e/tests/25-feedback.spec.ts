import { expect, test } from '@playwright/test';
import { Api, defaultProject, signIn, unique } from './helpers';

test('a member sends a security report from the sidebar and an Admin resolves it', async ({ page, browser }) => {
  const admin = await Api.signIn();
  const project = await defaultProject(admin);
  const email = `${unique('tester')}@example.com`;
  await admin.send('PUT', `/api/v1/projects/${project.id}/members`, { email, role: 'viewer', initial_password: 'tester-password-1' }, 200);
  // Markup in a message stays text, for the Admin too.
  const message = `${unique('Report')}: <b>the kafka check</b> shows an internal address`;

  const tester = await (await browser.newContext()).newPage();
  await signIn(tester, email, 'tester-password-1');
  await tester.getByRole('button', { name: 'Feedback' }).click();
  const dialog = tester.getByRole('dialog', { name: 'Send feedback' });
  await dialog.getByRole('button', { name: /Security issue/ }).click();
  await expect(dialog.getByRole('status')).toContainText('Only Admins see security reports.');
  await dialog.getByLabel('Message').fill(message);
  await expect(dialog.getByText(new RegExp(`/p/${project.slug}/`))).toBeVisible();
  await dialog.getByRole('button', { name: 'Send security report' }).click();
  await expect(tester.getByRole('dialog', { name: 'Thanks, we got it' })).toBeVisible();

  await signIn(page);
  await page.goto(`/p/${project.slug}/settings/feedback`);
  await expect(page.getByRole('heading', { name: 'Feedback', level: 2 })).toBeVisible();
  const row = page.getByRole('listitem').filter({ hasText: message });
  await expect(row.getByText(message, { exact: true })).toBeVisible();
  await expect(row.getByText('Security')).toBeVisible();
  await row.getByRole('button', { name: new RegExp(email) }).click();
  await expect(row.getByRole('link', { name: 'Reply by email' })).toHaveAttribute('href', new RegExp(`^mailto:${encodeURIComponent(email)}`));
  await row.getByRole('button', { name: 'Mark resolved' }).click();
  await expect(page.getByText('Marked resolved')).toBeVisible();
  await expect(page.getByRole('listitem').filter({ hasText: message })).toHaveCount(0);
  await page.getByRole('button', { name: /^Resolved/ }).click();
  await expect(page.getByRole('listitem').filter({ hasText: message }).getByText('● Resolved')).toBeVisible();

  const events = await admin.get<{ items: { kind: string }[] }>(`/api/v1/projects/${project.id}/events?audit_only=true`);
  expect(events.items.map((e) => e.kind)).toEqual(expect.arrayContaining(['feedback.sent', 'feedback.resolved']));
});

test('outside an open lab there is no testing policy and the sign-in page stays plain', async ({ page }) => {
  await page.goto('/login');
  await expect(page.getByRole('heading', { name: 'Sign in to BeziSync' })).toBeVisible();
  await expect(page.getByText('BeziSync Lab is open.')).toHaveCount(0);
  await page.goto('/testing-policy');
  await expect(page.getByText('No testing policy here')).toBeVisible();
});
