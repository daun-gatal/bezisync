import { expect, test } from '@playwright/test';
import { Api, signIn, unique } from './helpers';

/** Same Debezium build under its own name (tagged in k3s by scripts/e2e.sh). */
const CUSTOM_IMAGE = 'bezisync-local/debezium-custom:e2e';

test('an Admin curates the Debezium versions and picks the default', async ({ page }) => {
  const api = await Api.signIn();
  const label = unique('tested');
  await signIn(page);
  await page.goto('/p/default/settings/debezium');
  await expect(page.getByRole('heading', { name: 'Debezium versions' })).toBeVisible();
  await expect(page.getByText('Install default', { exact: true })).toBeVisible();

  await page.getByLabel('Image', { exact: true }).fill(CUSTOM_IMAGE);
  await page.getByLabel('Label').fill(label);
  await page.getByLabel('Make it the default').check();
  await page.getByRole('button', { name: 'Add image' }).click();
  await expect(page.getByText(`Added ${CUSTOM_IMAGE}`)).toBeVisible();
  const catalog = await api.get<{ default_image: string }>('/api/v1/debezium-catalog');
  expect(catalog.default_image).toBe(CUSTOM_IMAGE);

  // Old links to the separate catalog and project image pages land here.
  for (const old of ['/p/default/settings/debezium-catalog', '/p/default/settings/images', '/p/default/admin/debezium']) {
    await page.goto(old);
    await expect(page).toHaveURL(/\/settings\/debezium$/);
  }
  const info = await api.get<{ default_debezium_image: string }>('/api/v1/system/info');
  expect(info.default_debezium_image).toBe(CUSTOM_IMAGE);

  // Back to the install default, deprecate, then remove it.
  await page.goto('/p/default/settings/debezium');
  await page.getByRole('button', { name: `Use install default instead of ${CUSTOM_IMAGE}` }).click();
  // Changing the default says what it affects first.
  await page.getByRole('dialog').getByRole('button', { name: 'Use install default' }).click();
  await expect(page.getByRole('button', { name: `Make default ${CUSTOM_IMAGE}` })).toBeVisible();
  await page.getByRole('button', { name: `Deprecate ${CUSTOM_IMAGE}` }).click();
  await expect(page.getByText('Deprecated', { exact: true })).toBeVisible();
  await page.getByRole('button', { name: `Remove ${CUSTOM_IMAGE}` }).click();
  await page.getByRole('dialog').getByRole('button', { name: 'Remove image' }).click();
  await expect(page.getByText(CUSTOM_IMAGE)).toHaveCount(0);
  const after = await api.get<{ entries: { image: string }[] }>('/api/v1/debezium-catalog');
  expect(after.entries.map((e) => e.image)).not.toContain(CUSTOM_IMAGE);
});
