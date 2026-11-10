import { expect, test } from '@playwright/test';
import { Api, defaultProject, signIn, unique } from './helpers';
import { KAFKA, SOURCE_PG } from './fixtures';
import { waitFor } from './data';

interface P {
  id: string;
  lifecycle: string;
  version: number;
  current_version: number;
  deployed_version: number;
  deployed_at: string;
  has_undeployed_changes: boolean;
  status: { workload?: { started_at?: string | null; restarts?: number } };
}

// ADR 0017: a change to a deployed pipeline can be kept or discarded when the wizard closes, and
// discarded later from the pipeline page; the connector keeps running and doesn't restart.
test('discard a deployed pipeline’s undeployed change', async ({ page }) => {
  test.setTimeout(600_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('dc');
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>(
    'POST',
    `${base}/connections`,
    { kind: 'source', type: 'postgresql', name: `src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } },
    201,
  );
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `dst-${tag}`, config: KAFKA }, 201);
  const p = await api.send<P>(
    'POST',
    `${base}/pipelines`,
    {
      name: `dc-${tag}`,
      source_id: src.id,
      sink_id: dst.id,
      runtime_id: rt.id,
      spec: { source: { tables: { include: ['shop.orders'] } }, sink: { naming: `dc.${tag}.{table}` } },
    },
    201,
  );
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, {});
  const running = await waitFor(
    'deployed and streaming',
    async () => {
      const cur = await api.get<P>(url);
      if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
      return cur.lifecycle === 'streaming' ? cur : undefined;
    },
    420_000,
  );

  await signIn(page);
  const changeTables = async () => {
    await page.goto(`/p/${project.slug}/pipelines/${p.id}`);
    await page.getByRole('button', { name: 'Change', exact: true }).click();
    await page.getByRole('menuitem', { name: 'Tables and columns' }).click();
    await expect(page).toHaveURL(/step=tables/);
    await page.getByLabel('shop.customers').check();
    // Leaving the step saves it: the change is the next version.
    await page.getByRole('button', { name: 'Continue to destination →' }).click();
    await expect(page).toHaveURL(/step=sink/);
  };

  // Close asks: keep the change for later…
  await changeTables();
  await expect(page.getByText('Draft saved · v2')).toBeVisible();
  await page.getByRole('button', { name: 'Close' }).click();
  let dialog = page.getByRole('dialog', { name: 'Keep v2?' });
  await expect(dialog).toContainText('nothing restarts');
  await dialog.getByRole('button', { name: 'Keep v2' }).click();
  const notice = page.getByRole('status').filter({ hasText: 'v2 is saved but not deployed' });
  await expect(notice).toBeVisible();

  // …then discard it from the pipeline page.
  await notice.getByRole('button', { name: 'Discard v2' }).click();
  dialog = page.getByRole('dialog', { name: 'Discard v2?' });
  await expect(dialog).toContainText('The connector keeps running v1');
  await dialog.getByRole('button', { name: 'Discard v2' }).click();
  await expect(page.getByText('Discarded v2; v1 stays deployed')).toBeVisible();
  await expect(notice).toHaveCount(0);
  await expect(page.getByRole('link', { name: 'Review and deploy' })).toHaveCount(0);
  await page.getByRole('tab', { name: 'History' }).click();
  const versions = page.getByRole('list', { name: 'Versions' });
  await expect(versions.getByRole('listitem')).toHaveCount(2); // v2 stays, deployable
  await expect(versions.getByRole('listitem').first()).toContainText('v2');

  // Discarding straight from the wizard's Close; the next change is v3, never v2 again.
  await changeTables();
  await expect(page.getByText('Draft saved · v3')).toBeVisible();
  await page.getByRole('button', { name: 'Close' }).click();
  dialog = page.getByRole('dialog', { name: 'Keep v3?' });
  await dialog.getByRole('button', { name: 'Discard v3' }).click();
  await expect(page.getByRole('heading', { name: `dc-${tag}`, level: 1 })).toBeVisible();
  await expect(page.getByRole('dialog', { name: 'Leave without saving?' })).toHaveCount(0);
  await expect(page.getByText(/is saved but not deployed/)).toHaveCount(0);

  // Nothing that runs changed: the same deploy, the same connector.
  const after = await api.get<P>(url);
  expect(after).toMatchObject({ current_version: 1, deployed_version: 1, has_undeployed_changes: false, lifecycle: 'streaming', deployed_at: running.deployed_at });
  expect(after.status.workload?.started_at).toBe(running.status.workload?.started_at);

  await api.send('DELETE', url);
});
