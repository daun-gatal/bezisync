import { expect, test } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';
import { KAFKA, SOURCE_PG } from './fixtures';

test('create, deploy, observe and delete a pipeline entirely in the UI', async ({ page }) => {
  test.setTimeout(600_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('ui');
  const { password, ...pg } = SOURCE_PG;
  await api.send('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  await api.send(
    'POST',
    `${base}/connections`,
    { kind: 'source', type: 'postgresql', name: `src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } },
    201,
  );
  await api.send('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `dst-${tag}`, config: KAFKA }, 201);

  await signIn(page);
  await page.getByRole('link', { name: 'New pipeline' }).click();
  // 1. Source
  await page.getByRole('radio', { name: new RegExp(`src-${tag}`) }).click();
  await expect(
    page
      .getByRole('region', { name: `Readiness checks · src-${tag}` })
      .getByText(/Ready/)
      .first(),
  ).toBeVisible();
  await page.getByRole('button', { name: 'Continue to tables →' }).click();
  // 2. Tables
  await expect(page).toHaveURL(/step=tables/);
  await page.getByLabel('shop.customers').check();
  await page.getByLabel('shop.orders').check();
  const personal = page.getByRole('region', { name: 'Personal data' });
  await expect(personal.getByLabel('What to send for shop.customers.email')).toContainText('Mask');
  // Any other column can be protected too; Keep as is is a recorded choice.
  await choose(personal.getByLabel('What to send for shop.customers.phone'), 'Keep as is');
  // A number can't be hashed, so protecting it removes it.
  await choose(personal.getByLabel('Protect another column'), /^shop\.orders\.total/);
  await expect(personal.getByLabel('What to send for shop.orders.total')).toContainText('Remove');
  await page.getByRole('button', { name: 'Continue to destination →' }).click();
  // 3. Destination
  await page.getByRole('radio', { name: new RegExp(`dst-${tag}`) }).click();
  await page.getByLabel('Destination naming').fill(`ui.${tag}.{table}`);
  await expect(page.getByText(`Preview: ui.${tag}.customers, ui.${tag}.orders`)).toBeVisible();
  await page.getByRole('button', { name: 'Continue to transform →' }).click();
  // 4. Transform: the defaults are fine.
  await expect(page.getByRole('heading', { name: 'How should events look?' })).toBeVisible();
  await page.getByRole('button', { name: 'Continue to review →' }).click();
  // 4. Review & deploy
  await expect(page).toHaveURL(/step=review/);
  const name = `orders-${tag}`;
  await page.getByLabel('Name', { exact: true }).fill(name);
  await page.getByLabel('Owner').click();
  // A team new to this run, so a rerun on kept data (make e2e-quick) adds it rather than finding it.
  await page.getByRole('combobox', { name: 'Search or add a team' }).fill(`team-${tag}`);
  await page.getByRole('button', { name: `Use “team-${tag}”` }).click();
  await choose(page.getByLabel('Runtime'), `rt-${tag}`);
  // Settings save on their own; the final checks then run against them.
  await expect(page.getByText('Final checks', { exact: true })).toBeVisible({ timeout: 60_000 });
  await expect(page.getByText(/Ready/).first()).toBeVisible();
  // Every step and every autosave updated the one draft version.
  await expect(page.getByText('Draft saved · v1')).toBeVisible();
  await page.getByRole('button', { name: 'Deploy', exact: true }).click();

  // Operate
  await expect(page.getByRole('heading', { name, level: 1 })).toBeVisible();
  await expect(page.getByText('Streaming', { exact: true })).toBeVisible({ timeout: 420_000 });
  await expect(page.getByText('Healthy', { exact: true })).toBeVisible({ timeout: 120_000 });
  await expect(page.getByRole('heading', { name: 'Source protection' })).toBeVisible({ timeout: 120_000 });
  // Source protection in PostgreSQL's terms: the slot, WAL kept against the 10 GB default limit, never "unlimited".
  // The first probe can run before the slot exists ("Not measured yet"); the next one, 30 s later, measures it.
  const protection = page.getByRole('region', { name: 'Source protection' });
  await expect(protection.getByText('Replication slot')).toBeVisible();
  await expect(protection.getByText(/^bz_/)).toBeVisible();
  await expect(protection.getByRole('meter', { name: 'WAL kept for this pipeline' })).toHaveAttribute('aria-valuetext', /of 10 GB limit$/, { timeout: 120_000 });
  await expect(page.getByText(/Log kept for this pipeline/)).toBeVisible();
  await expect(protection).not.toContainText(/unlimited/i);
  // The default transforms read as what consumers receive.
  const transforms = page.getByRole('region', { name: 'Transforms' });
  await expect(transforms.getByRole('list', { name: 'What consumers receive' })).toContainText('The row after the change');
  await expect(transforms.getByRole('list', { name: 'What consumers receive' })).toContainText('__deleted = true');
  await expect(transforms).toContainText('No filters, renames or other steps.');
  // The columns end level: Transforms takes the height of Since start + Source protection.
  const bottom = async (l: typeof transforms) => {
    const b = await l.boundingBox();
    return b ? b.y + b.height : Number.NaN;
  };
  await expect.poll(async () => Math.abs((await bottom(transforms)) - (await bottom(protection)))).toBeLessThanOrEqual(1);
  // The chart's readout replaces the legend once there are samples.
  const chart = page.getByRole('region', { name: 'Throughput & latency' });
  await expect(chart.getByText('Changes per second', { exact: true })).toBeVisible({ timeout: 180_000 });
  await expect(chart.getByText(/^Latest/)).toBeVisible();
  await expect(chart.getByText(/^Peak in 6h:/)).toBeVisible();

  await page.getByRole('tab', { name: 'Events' }).click();
  await choose(page.getByLabel('Table', { exact: true }), 'shop.customers');
  await expect(page.getByText(/Masked: .*email/)).toBeVisible({ timeout: 120_000 });
  const events = page.locator('pre[aria-label="Events"]');
  await expect(events).toContainText('Customer 1');
  await expect(events).not.toContainText('c1@example.com');

  await page.getByRole('tab', { name: 'Logs' }).click();
  await expect(page.getByLabel('Connector logs')).toContainText('Debezium');
  await page.getByRole('tab', { name: 'History' }).click();
  const versions = page.getByRole('list', { name: 'Versions' });
  await expect(versions.getByRole('listitem')).toHaveCount(1);
  await expect(versions.getByRole('listitem').first()).toContainText('deployed');
  await expect(page.getByLabel('Compiled configuration')).toContainText('debezium.source.column.mask.hash.v2.SHA-256');
  await expect(page.getByLabel('Compiled configuration')).not.toContainText(SOURCE_PG.password);

  await page.getByRole('button', { name: 'Operate' }).click();
  await page.getByRole('menuitem', { name: /^Pause/ }).click();
  await expect(page.getByText('Paused', { exact: true }).first()).toBeVisible({ timeout: 120_000 });
  await page.getByRole('button', { name: 'Resume' }).click();
  await expect(page.getByText('Streaming', { exact: true })).toBeVisible({ timeout: 300_000 });

  await page.getByRole('tab', { name: 'Overview' }).click();
  await page.getByRole('button', { name: 'More actions' }).click();
  await page.getByRole('menuitem', { name: 'Delete…' }).click();
  const dialog = page.getByRole('dialog');
  await expect(dialog).toContainText(/replication slot bz_/);
  await dialog.getByRole('textbox').fill(name);
  await dialog.getByRole('button', { name: 'Delete pipeline' }).click();
  await expect(page.getByRole('heading', { name: 'Cleanup report' })).toBeVisible({ timeout: 240_000 });
  await expect(page.getByText('Deleted', { exact: true })).toBeVisible();
  await expect(page.getByRole('listitem').filter({ hasText: 'replication slot' })).toContainText('removed');
});

test('onboarding checklist reflects real project state', async ({ page }) => {
  await signIn(page);
  await page
    .getByRole('complementary')
    .getByRole('button', { name: /Help & tour/ })
    .click();
  await page.getByRole('menuitem', { name: 'Getting started' }).click();
  await expect(page.getByRole('heading', { name: 'Welcome to BeziSync' })).toBeVisible();
  await expect(page.getByText(/[0-9] of 3 done/)).toBeVisible();
});

test('pipelines list filters and searches', async ({ page }) => {
  await signIn(page);
  await page.goto('/p/default/pipelines?filter=attention');
  await expect(page.getByRole('button', { name: /Needs attention/ })).toHaveAttribute('aria-pressed', 'true');
  await page.getByLabel('Search pipelines').fill('zzz-no-match');
  await expect(page).toHaveURL(/q=zzz-no-match/);
});
