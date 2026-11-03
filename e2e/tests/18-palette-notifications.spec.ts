import AxeBuilder from '@axe-core/playwright';
import { expect, test } from '@playwright/test';
import { Api, defaultProject, signIn, unique } from './helpers';
import { SOURCE_PG } from './fixtures';

test('the command palette jumps anywhere from the keyboard', async ({ page }) => {
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('pal');
  const { password, ...pg } = SOURCE_PG;
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, { name: `find-${tag}`, source_id: src.id, spec: { source: { tables: { include: ['shop.orders'] } } } }, 201);

  await signIn(page);
  await page.keyboard.press('Control+K');
  const search = page.getByRole('combobox', { name: 'Search pipelines, connections and pages' });
  await expect(search).toBeFocused();
  const scan = await new AxeBuilder({ page }).include('[role="dialog"]').withTags(['wcag2a', 'wcag2aa']).analyze();
  expect(scan.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical').map((v) => v.id)).toEqual([]);
  await search.fill(`find-${tag}`);
  await expect(page.getByRole('option', { name: new RegExp(`find-${tag}`) })).toBeVisible();
  await page.keyboard.press('Enter');
  await expect(page).toHaveURL(new RegExp(`/pipelines/${p.id}`));

  await page.keyboard.press('Control+K');
  await search.fill('runtimes');
  await page.keyboard.press('Enter');
  await expect(page).toHaveURL(/settings\/runtimes/);

  await api.send('DELETE', `${base}/pipelines/${p.id}`);
});

test('people choose which events reach their inbox', async ({ page }) => {
  await signIn(page);
  await page.getByRole('button', { name: /admin@bezisync.local/ }).click();
  await page.getByRole('menuitem', { name: 'Notification settings' }).click();
  const resolved = page.getByRole('checkbox', { name: 'An alert resolves' });
  await expect(resolved).toBeChecked();
  await resolved.uncheck();
  await expect(resolved).not.toBeChecked();
  const api = await Api.signIn();
  const muted = async () => (await api.get<{ user: { muted_notifications: string[] } }>('/api/v1/auth/me')).user.muted_notifications;
  await expect.poll(muted).toEqual(['alert_resolved']);
  await resolved.check();
  await expect(resolved).toBeChecked();
  await expect.poll(muted).toEqual([]);
});
