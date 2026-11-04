import AxeBuilder from '@axe-core/playwright';
import { expect, test, type Page } from '@playwright/test';
import { signIn } from './helpers';

/** WCAG 2.2 AA gate: no serious or critical axe violations on the main screens (light and dark). */
const PAGES = [
  '/p/default/pipelines',
  '/p/default/pipelines/new',
  '/p/default/connections',
  '/p/default/alerts',
  '/p/default/learn',
  '/p/default/tour',
  '/p/default/welcome',
  '/p/default/settings/runtimes',
  '/p/default/settings/policies',
  '/p/default/settings/people',
  '/p/default/account',
  '/p/default/settings/engines',
  '/p/default/settings/engines?engine=flink-cdc',
  '/p/default/settings/sign-in',
  '/p/default/settings/mcp',
];

async function violations(page: Page): Promise<string[]> {
  const result = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
  return result.violations
    .filter((v) => v.impact === 'serious' || v.impact === 'critical')
    .map((v) => `${v.id} (${v.impact}): ${v.help} — ${v.nodes.slice(0, 3).map((n) => n.target.join(' ')).join(' | ')}`);
}

test('login page is accessible', async ({ page }) => {
  await page.goto('/login');
  await expect(page.getByRole('heading', { name: 'Sign in to BeziSync' })).toBeVisible();
  expect(await violations(page)).toEqual([]);
});

for (const scheme of ['light', 'dark'] as const) {
  test(`main screens are accessible (${scheme})`, async ({ page }) => {
    await page.emulateMedia({ colorScheme: scheme });
    await signIn(page);
    const found: string[] = [];
    for (const path of PAGES) {
      await page.goto(path);
      await page.waitForLoadState('networkidle');
      found.push(...(await violations(page)).map((v) => `${path}: ${v}`));
    }
    expect(found).toEqual([]);
  });
}
