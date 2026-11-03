import { expect, test } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';

test('an Admin adds a person to a team project, sees who can open it, then deactivates them', async ({ page, browser }) => {
  const admin = await Api.signIn();
  const projectName = unique('Analytics');
  const slug = projectName.toLowerCase();
  const project = await admin.send<{ id: string }>('POST', '/api/v1/projects', { name: projectName, slug, description: '' }, 201);
  const email = `${unique('newbie')}@example.com`;

  await signIn(page);
  // Settings opens on People; each group says what the Admin is doing.
  await page.getByRole('link', { name: 'Settings', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'People', exact: true, level: 2 })).toBeVisible();
  for (const group of ['People & projects', 'Running pipelines', 'Guardrails']) {
    await expect(page.getByRole('heading', { name: group })).toBeVisible();
  }
  await page.getByRole('button', { name: 'Add person' }).click();
  const add = page.getByRole('dialog');
  await add.getByLabel('Email').fill(email);
  await add.getByLabel('Initial password').fill('newbie-password-123');
  await choose(add.getByLabel('Project', { exact: true }), projectName);
  await choose(add.getByLabel('Role', { exact: true }), 'Editor');
  await add.getByLabel('Can approve deployments').check();
  await add.getByRole('button', { name: 'Add person' }).click();
  await expect(page.getByText(`Added ${email} to ${projectName} as editor`)).toBeVisible();

  // "Who can open Analytics?" — the same page, filtered: its members and every Admin.
  await choose(page.getByLabel('Who can open'), `Can open ${projectName}`);
  await expect(page).toHaveURL(new RegExp(`in=${project.id}`));
  await expect(page.getByLabel(`Role for ${email}`)).toContainText('Editor');
  await expect(page.getByRole('checkbox', { name: `Approver (${email})` })).toBeChecked();
  await expect(page.getByRole('listitem').filter({ hasText: 'admin@' }).getByText('Admin', { exact: true })).toBeVisible();

  const userPage = await (await browser.newContext()).newPage();
  await signIn(userPage, email, 'newbie-password-123');
  await expect(userPage).toHaveURL(new RegExp(`/p/${slug}/`));
  await expect(userPage.getByRole('link', { name: 'Settings', exact: true })).toHaveCount(0);

  // Deactivating signs them out everywhere.
  await page.getByRole('button', { name: `Manage ${email}` }).click();
  await page.getByRole('dialog', { name: email }).getByRole('button', { name: 'Deactivate' }).click();
  await page
    .getByRole('dialog', { name: `Deactivate ${email}?` })
    .getByRole('button', { name: 'Deactivate' })
    .click();
  await expect(page.getByRole('dialog', { name: email }).getByText('This account is deactivated.')).toBeVisible();
  await userPage.reload();
  await expect(userPage).toHaveURL(/\/login/);
});

test('an Admin renames and deletes an empty project', async ({ page }) => {
  const name = unique('Scratch');
  const slug = name.toLowerCase();
  const admin = await Api.signIn();
  await admin.send('POST', '/api/v1/projects', { name, slug, description: '' }, 201);
  await signIn(page);
  // Old Admin links still land on the same section.
  await page.goto('/p/default/admin/projects');
  await expect(page).toHaveURL(/\/p\/default\/settings\/projects$/);
  await page.getByRole('button', { name: `Edit ${name}` }).click();
  await page.getByRole('dialog').getByLabel('Name').fill(`${name} renamed`);
  await page.getByRole('dialog').getByRole('button', { name: 'Save changes' }).click();
  await expect(page.getByText(`Saved ${name} renamed`)).toBeVisible();
  await page.getByRole('button', { name: `Delete ${name} renamed` }).click();
  const confirm = page.getByRole('dialog');
  await confirm.getByRole('textbox').fill(slug);
  await confirm.getByRole('button', { name: 'Delete project' }).click();
  await expect(page.getByText(`Deleted ${name} renamed`)).toBeVisible();
  const projects = await admin.get<{ slug: string }[]>('/api/v1/projects');
  expect(projects.map((p) => p.slug)).not.toContain(slug);
});

test('approvers and editors run pipelines but never see Settings; per-project sections name their project', async ({ page, browser }) => {
  const admin = await Api.signIn();
  const project = await defaultProject(admin);
  const email = `${unique('approver')}@example.com`;
  await admin.send('PUT', `/api/v1/projects/${project.id}/members`, { email, role: 'editor', can_approve: true, initial_password: 'approver-password-1' }, 200);
  // There is no project admin role any more.
  const res = await admin.raw('PUT', `/api/v1/projects/${project.id}/members`, { email, role: 'admin' });
  expect(res.status()).toBe(422);

  const approver = await (await browser.newContext()).newPage();
  await signIn(approver, email, 'approver-password-1');
  await expect(approver.getByRole('link', { name: 'New pipeline' })).toBeVisible();
  await expect(approver.getByRole('link', { name: 'Settings', exact: true })).toHaveCount(0);
  await approver.goto('/p/default/settings/runtimes');
  await expect(approver.getByText('Settings are for Admins')).toBeVisible();

  // An Admin sees which project a per-project section applies to, and old Members links land on People.
  await signIn(page);
  await page.goto('/p/default/settings/runtimes');
  await expect(page.getByLabel('Project', { exact: true })).toContainText('Default');
  await page.goto('/p/default/settings/members');
  await expect(page).toHaveURL(/\/settings\/people$/);
  await expect(page.getByText('Applies to every project.')).toBeVisible();
});

test('long lists page 20 rows by default; the rows-per-page choice is the viewer’s and pages are links', async ({ page }) => {
  const admin = await Api.signIn();
  const project = await defaultProject(admin);
  const tag = unique('pg');
  for (let i = 0; i < 22; i++) {
    await admin.send('PUT', `/api/v1/projects/${project.id}/members`, { email: `${tag}-${String(i).padStart(2, '0')}@example.com`, role: 'viewer', initial_password: 'viewer-password-123' });
  }
  await signIn(page);
  await page.evaluate(() => window.localStorage.removeItem('bz.pageSize'));

  // People: everyone matching, 20 at a time, with the page in the URL.
  await page.goto(`/p/default/settings/people?q=${tag}`);
  await expect(page.getByText('1–20 of 22 people')).toBeVisible();
  const pages = page.getByRole('navigation', { name: 'Pages of people' });
  await pages.getByRole('button', { name: /Next/ }).click();
  await expect(page).toHaveURL(/page=2/);
  await expect(page.getByText('21–22 of 22 people')).toBeVisible();
  await expect(page.getByRole('list', { name: 'People' }).getByRole('listitem')).toHaveCount(2);
  await page.reload();
  await expect(page.getByText('21–22 of 22 people')).toBeVisible();

  // One choice for every list, remembered in this browser.
  await page.getByRole('button', { name: /20 per page/ }).click();
  await page.getByRole('menuitem', { name: '50 per page' }).click();
  await expect(page.getByText('22 people', { exact: true })).toBeVisible();
  await expect(pages).toHaveCount(0);
  await page.getByRole('button', { name: /50 per page/ }).click();
  await page.getByRole('menuitem', { name: '10 per page' }).click();

  // Cursor-paged lists step Newer / Older with the same choice.
  await page.goto('/p/default/settings/audit');
  await expect(page.getByText('Newest 10 changes')).toBeVisible();
  const audit = page.getByRole('navigation', { name: 'Pages of changes' });
  await expect(audit.getByRole('button', { name: 'Newer' })).toBeDisabled();
  await audit.getByRole('button', { name: /Older/ }).click();
  await expect(page.getByText('11–20 changes')).toBeVisible();
  await audit.getByRole('button', { name: 'Newer' }).click();
  await expect(page.getByText('Newest 10 changes')).toBeVisible();
});
