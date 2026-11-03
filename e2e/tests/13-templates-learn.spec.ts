import { expect, test } from '@playwright/test';
import { Api, defaultProject, signIn, unique } from './helpers';
import { HTTP_ECHO, SOURCE_PG } from './fixtures';

test('publish a template from a pipeline and start a new pipeline from it', async ({ page }) => {
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('tpl');
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'http', name: `hook-${tag}`, config: { url: `${HTTP_ECHO}/tpl` } }, 201);
  const original = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `orig-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, sink: { naming: `team.${tag}.{table}`, format: { type: 'cloudevents' } }, policies: { resources: 'medium' } },
  }, 201);

  await signIn(page);
  await page.goto(`/p/default/pipelines/${original.id}?tab=config`);
  await page.getByRole('button', { name: 'Save as template…' }).click();
  const title = `Team standard ${tag}`;
  await page.getByRole('dialog').getByLabel('Title').fill(title);
  await page.getByRole('dialog').getByRole('button', { name: 'Publish template' }).click();
  await expect(page.getByText(`Published template “${title}”`)).toBeVisible();

  await page.goto('/p/default/settings/templates');
  const card = page.getByText(title, { exact: true });
  await expect(card).toBeVisible();
  await expect(page.getByText(`to hook-${tag} · CLOUDEVENTS · size medium`, { exact: false })).toBeVisible();
  // Use template → wizard opens with it preselected.
  const templates = await api.get<{ id: string; name: string; title: string }[]>(`${base}/templates`);
  const template = templates.find((t) => t.title === title)!;
  await page.goto(`/p/default/pipelines/new?template=${template.id}`);
  await expect(page.getByLabel('Start from')).toContainText(title);
  await page.getByRole('radio', { name: new RegExp(`src-${tag}`) }).click();
  await page.getByRole('button', { name: 'Continue to tables →' }).click();
  await page.getByLabel('shop.order_items').check();
  await page.getByRole('button', { name: 'Continue to destination →' }).click();
  await expect(page.getByRole('radio', { name: new RegExp(`hook-${tag}`) })).toBeChecked();
  await expect(page.getByLabel('Destination naming')).toHaveValue(`team.${tag}.{table}`);

  const draftId = new URL(page.url()).searchParams.get('draft')!;
  const draft = await api.get<{ labels: Record<string, string>; runtime: { id: string } | null; spec: { policies: { resources: string } } }>(`${base}/pipelines/${draftId}`);
  expect(draft.labels.template).toBe(template.name);
  expect(draft.runtime?.id).toBe(rt.id);
  expect(draft.spec.policies.resources).toBe('medium');

  for (const id of [draftId, original.id]) await api.send('DELETE', `${base}/pipelines/${id}`);
  await api.send('DELETE', `${base}/templates/${template.id}`, undefined, 204);
});

test('the Learn page explains Debezium terms in plain words', async ({ page }) => {
  await signIn(page);
  await page.getByRole('button', { name: /Help & tour/ }).click();
  await page.getByRole('menuitem', { name: 'Learn' }).click();
  await expect(page.getByRole('heading', { name: 'Learn', level: 1 })).toBeVisible();
  await page.getByLabel('Search the glossary').fill('slot');
  await expect(page.getByRole('heading', { name: /Replication slot/ })).toBeVisible();
  await expect(page.getByRole('heading', { name: /Heartbeat/ })).toHaveCount(0);
});
