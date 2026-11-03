import { expect, test } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';
import { HTTP_ECHO, HTTP_ECHO_FROM_RUNNER, KAFKA, LEGACY_PG } from './fixtures';
import { waitFor } from './data';

interface Hook {
  path: string;
  body: string;
}
async function hooks(path: string): Promise<{ state: string; title: string; message: string; fix: string }[]> {
  const res = await fetch(`${HTTP_ECHO_FROM_RUNNER}/requests${path}`);
  return ((await res.json()) as Hook[]).map((h) => JSON.parse(h.body));
}

test('a failing pipeline fires an alert, notifies a webhook, is acknowledged in the UI and resolves', async ({ page }) => {
  test.setTimeout(600_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('al');
  const hookPath = `/alerts-${tag}`;

  const channel = await api.send<{ id: string }>(
    'POST',
    `${base}/alert-channels`,
    { name: `hook-${tag}`, kind: 'webhook', secrets: { url: `${HTTP_ECHO}${hookPath}` } },
    201,
  );
  const test1 = await api.send<{ ok: boolean; message: string }>('POST', `${base}/alert-channels/${channel.id}/test`);
  expect(test1.ok, test1.message).toBe(true);
  const rules = await api.get<{ id: string; kind: string; channel_ids: string[] }[]>(`${base}/alert-rules`);
  const down = rules.find((r) => r.kind === 'pipeline_down')!;
  await api.send('PATCH', `${base}/alert-rules/${down.id}`, { for_seconds: 0, channel_ids: [...down.channel_ids, channel.id] });

  const { password, ...pg } = LEGACY_PG;
  const rt = await api.send<{ id: string }>(
    'POST',
    `${base}/runtimes`,
    { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } },
    201,
  );
  const src = await api.send<{ id: string }>(
    'POST',
    `${base}/connections`,
    { kind: 'source', type: 'postgresql', name: `legacy-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } },
    201,
  );
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `k-${tag}`, config: KAFKA }, 201);
  // legacy-pg has no logical replication and no table; deploy anyway (audited) to get a real failure.
  const p = await api.send<{ id: string; name: string }>(
    'POST',
    `${base}/pipelines`,
    {
      name: `doomed-${tag}`,
      owner: 'e2e',
      source_id: src.id,
      sink_id: dst.id,
      runtime_id: rt.id,
      spec: { source: { tables: { include: ['public.missing'] } } },
    },
    201,
  );
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, { force_reason: 'e2e: verify alerting on a broken source' });

  const fired = await waitFor(
    'webhook received firing alert',
    async () => (await hooks(hookPath)).find((h) => h.state === 'firing' && h.title.includes(p.name)),
    420_000,
  );
  expect(fired.title).toBe(`${p.name} is down`);
  expect(`${fired.message} ${fired.fix}`).toMatch(/logical|wal_level/i);

  await signIn(page);
  await expect(page.getByRole('navigation', { name: 'Primary' }).getByRole('link', { name: /Alerts\s*\d+ firing/ })).toBeVisible();
  await page.getByRole('link', { name: 'Alerts' }).first().click();
  const card = page
    .locator('section')
    .filter({ hasText: `${p.name} is down` })
    .first();
  await expect(card).toBeVisible();
  await expect(card.getByText('What to do:')).toBeVisible();
  await card.getByRole('button', { name: 'Acknowledge' }).click();
  await expect(page.getByText('Acknowledged', { exact: true })).toBeVisible();
  await expect(card).toContainText('acknowledged by admin@bezisync.local');

  await page.getByRole('button', { name: /Inbox/ }).click();
  await expect(page.getByRole('link', { name: new RegExp(`${p.name} is down`) }).first()).toBeVisible();
  await page.keyboard.press('Escape');

  await api.send('DELETE', url);
  await waitFor('webhook received resolution', async () => (await hooks(hookPath)).find((h) => h.state === 'resolved' && h.title.includes(p.name)), 240_000);
  await api.send('PATCH', `${base}/alert-rules/${down.id}`, { for_seconds: 120, channel_ids: down.channel_ids });
});

test('rules and channels are manageable in the UI', async ({ page }) => {
  await signIn(page);
  await page.goto('/p/default/alerts?tab=rules');
  await expect(page.getByText('Pipeline down', { exact: true })).toBeVisible();
  // A rule change saves at once and says so in its row.
  const latency = page.getByRole('row', { name: /Latency above target/ });
  await choose(latency.getByLabel('Duration of Latency above target'), '15 min');
  await expect(latency.getByText('Saved', { exact: true })).toBeVisible();
  await expect(latency.getByLabel('Duration of Latency above target')).toContainText('15 min');

  await page.goto('/p/default/alerts?tab=channels');
  await page.getByRole('button', { name: 'Add channel' }).click();
  const dialog = page.getByRole('dialog');
  const name = unique('ui-hook');
  await dialog.getByLabel('Name').fill(name);
  await choose(dialog.getByLabel('Type'), /Webhook/i);
  await dialog.getByLabel('Url').fill(`${HTTP_ECHO}/ui-test`);
  await dialog.getByRole('button', { name: 'Add channel' }).click();
  await expect(page.getByText(`Added ${name}`)).toBeVisible();

  // Deleting a channel says which rules stop notifying it first.
  await page.getByRole('button', { name: `Actions for ${name}` }).click();
  await page.getByRole('menuitem', { name: 'Delete' }).click();
  const confirm = page.getByRole('dialog', { name: `Delete ${name}?` });
  await expect(confirm.getByText('No rule sends here, so nothing stops paging.')).toBeVisible();
  await confirm.getByRole('button', { name: 'Delete channel' }).click();
  await expect(page.getByText(`Deleted ${name}`)).toBeVisible();
});
