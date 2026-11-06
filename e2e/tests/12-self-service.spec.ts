import { expect, test } from '@playwright/test';
import { Api, choose, signIn, unique } from './helpers';
import { HTTP_ECHO, HTTP_ECHO_FROM_RUNNER, KAFKA, SOURCE_PG } from './fixtures';
import { sql, waitFor } from './data';

/**
 * Self-service: an admin sets guardrails once; an editor who is not a Debezium expert builds
 * a pipeline in the guided wizard within those guardrails, and a second person approves.
 */
test('an editor ships a pipeline within project policies, after approval', async ({ browser }) => {
  test.setTimeout(900_000);
  const admin = await Api.signIn();
  const tag = unique('self');
  const slug = tag.slice(0, 40);
  const project = await admin.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug, name: `Self-service ${tag}` }, 201);
  const base = `/api/v1/projects/${project.id}`;
  const { password, ...pg } = SOURCE_PG;
  const path = `/self-${tag}`;
  const rt = await admin.send<{ id: string }>(
    'POST',
    `${base}/runtimes`,
    { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } },
    201,
  );
  // Shared (managed) connections: editors use them without seeing or editing secrets.
  await admin.send(
    'POST',
    `${base}/connections`,
    { kind: 'source', type: 'postgresql', name: `shop-db`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password }, managed: true },
    201,
  );
  await admin.send(
    'POST',
    `${base}/connections`,
    { kind: 'sink', type: 'http', name: `audit-hook`, config: { url: `${HTTP_ECHO}${path}` }, managed: true },
    201,
  );
  await admin.send('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `big-kafka`, config: KAFKA, managed: true }, 201);
  await admin.send('PUT', `${base}/policies`, {
    allowed_sink_types: ['http'],
    allowed_runtime_ids: [rt.id],
    max_resources: 'small',
    require_approval: true,
    allow_force_deploy: false,
  });
  const editorEmail = `editor-${tag}@example.com`;
  await admin.send('PUT', `${base}/members`, { email: editorEmail, role: 'editor', initial_password: 'editor-password-123' });

  // --- The editor, in guided mode
  const editorCtx = await browser.newContext();
  const page = await editorCtx.newPage();
  await signIn(page, editorEmail, 'editor-password-123');
  await page.goto(`/p/${project.slug}/pipelines/new`);
  await page.getByRole('radio', { name: /shop-db/ }).click();
  await page.getByRole('button', { name: 'Continue to tables →' }).click();
  await page.getByLabel('shop.customers').check();
  // Picking the table protects its personal data at once: every column is masked.
  const personal = page.getByRole('region', { name: 'Personal data' });
  await expect(personal.getByLabel('What to send for shop.customers.email')).toContainText('Mask');
  await expect(personal.getByLabel('What to send for shop.customers.phone')).toContainText('Mask');
  await page.getByRole('button', { name: 'Continue to destination →' }).click();
  await expect(page.getByText('This project allows these destinations: HTTP')).toBeVisible();
  await expect(page.getByRole('radio', { name: /big-kafka/ })).toHaveCount(0);
  await page.getByRole('radio', { name: /audit-hook/ }).click();
  await page.getByRole('button', { name: 'Continue to transform →' }).click();
  // 4. Transform: the defaults are fine.
  await expect(page.getByRole('heading', { name: 'How should events look?' })).toBeVisible();
  await page.getByRole('button', { name: 'Continue to review →' }).click();
  const name = `customers-${tag}`.slice(0, 60);
  await page.getByLabel('Name', { exact: true }).fill(name);
  await choose(page.getByLabel('Runtime'), `rt-${tag}`);
  // Sizes above the project limit stay visible but can't be picked.
  await expect(page.getByRole('radiogroup', { name: 'Size' }).getByRole('radio', { disabled: true })).toHaveCount(2);
  const policySection = page.getByRole('region', { name: 'Project policies' });
  await expect(policySection).toBeVisible({ timeout: 60_000 });
  await policySection.getByRole('button', { name: 'Show 1 passed check' }).click();
  await expect(policySection.getByText('Meets every project policy.')).toBeVisible();
  await expect(page.getByText('Deploys here need a second person’s approval.')).toBeVisible();
  await page.getByRole('button', { name: 'Request approval' }).click();
  await expect(page.getByText(new RegExp(`Sent ${name} v\\d+ for approval`))).toBeVisible();
  await expect(page.getByText(/^v\d+ is waiting for approval$/)).toBeVisible();
  await expect(page.getByRole('button', { name: 'Approve and deploy' })).toHaveCount(0);
  await expect(page.getByText('Someone else with approval rights must approve it.', { exact: false })).toBeVisible();

  // --- The approver
  const pipelines = await admin.get<{ id: string; name: string }[]>(`${base}/pipelines`);
  const pipelineId = pipelines.find((p) => p.name === name)!.id;
  const url = `${base}/pipelines/${pipelineId}`;
  const adminCtx = await browser.newContext();
  const adminPage = await adminCtx.newPage();
  await signIn(adminPage);
  await adminPage.goto(`/p/${project.slug}/pipelines/${pipelineId}`);
  await expect(adminPage.getByText(`Requested by ${editorEmail}`, { exact: false })).toBeVisible();
  await adminPage.getByRole('button', { name: 'Approve and deploy' }).click();
  await expect(adminPage.getByText(/^Approved: deploying v\d+$/)).toBeVisible();
  await waitFor(
    'streaming after approval',
    async () => {
      const cur = await admin.get<{ lifecycle: string; health: string; status: unknown }>(url);
      if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
      return cur.lifecycle === 'streaming' && cur.health === 'healthy';
    },
    420_000,
  );
  const email = `${tag}@example.com`;
  await sql('INSERT INTO shop.customers (name, email, phone) VALUES ($1, $2, $3)', [tag, email, '+15550000']);
  const body = await waitFor(
    'masked row delivered',
    async () => {
      const reqs = (await (await fetch(`${HTTP_ECHO_FROM_RUNNER}/requests${path}`)).json()) as { body: string }[];
      return reqs.find((r) => r.body.includes(tag))?.body;
    },
    180_000,
  );
  expect(body).not.toContain(email);

  // The editor is told it was approved.
  await page.reload();
  await page.getByRole('button', { name: /^Inbox/ }).click();
  await expect(page.getByText(new RegExp(`Approved: ${name} v\\d+`))).toBeVisible();

  // Masking is the pipeline's own choice: dropping it (through the API, without choosing "Keep
  // as is") is flagged by preflight as a warning naming the columns, never silently.
  const cur = await admin.get<{ version: number }>(url);
  await admin.patch(url, { spec: { source: { tables: { include: ['shop.customers'] }, columns: {} } } }, cur.version);
  const pre = await admin.send<{ sections: { key: string; report: { checks: { code: string; status: string; message: string }[] } }[] }>(
    'POST',
    `${url}/preflight`,
  );
  const personalCheck = pre.sections.find((s) => s.key === 'config')!.report.checks.find((c) => c.code === 'pii_unprotected');
  expect(personalCheck).toMatchObject({ status: 'warn' });
  expect(personalCheck!.message).toContain('shop.customers.email');

  await admin.send('DELETE', url);
  await waitFor('pipeline deleted', async () => (await admin.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
  await editorCtx.close();
  await adminCtx.close();
});

test('an admin decides whether pipelines may run scripted transforms', async ({ page }) => {
  const admin = await Api.signIn();
  const tag = unique('scripts');
  const project = await admin.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: tag.slice(0, 40), name: `Scripts ${tag}` }, 201);
  const base = `/api/v1/projects/${project.id}`;
  await signIn(page);
  await page.goto(`/p/${project.slug}/settings/policies`);
  const scripts = page.getByRole('switch', { name: 'Allow scripted transforms' });
  await expect(scripts).not.toBeChecked();
  await expect(page.getByText('next to its database credentials', { exact: false })).toBeVisible();
  await scripts.click();
  await expect(scripts).toBeChecked();
  await page.getByRole('button', { name: 'Save policies' }).click();
  await expect(page.getByText('Policies saved')).toBeVisible();
  expect((await admin.get<{ allow_scripted_transforms?: boolean }>(`${base}/policies`)).allow_scripted_transforms).toBe(true);
  await scripts.click();
  await expect(scripts).not.toBeChecked();
  await page.getByRole('button', { name: 'Save policies' }).click();
  await expect(page.getByRole('button', { name: 'Save policies' })).toBeDisabled();
  expect((await admin.get<{ allow_scripted_transforms?: boolean }>(`${base}/policies`)).allow_scripted_transforms).toBeUndefined();
});

test('an admin decides whether pipelines may pass advanced Debezium properties', async ({ page }) => {
  const admin = await Api.signIn();
  const tag = unique('raw');
  const project = await admin.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: tag.slice(0, 40), name: `Raw ${tag}` }, 201);
  const base = `/api/v1/projects/${project.id}`;
  await signIn(page);
  await page.goto(`/p/${project.slug}/settings/policies`);
  const raw = page.getByRole('switch', { name: 'Allow advanced Debezium properties' });
  await expect(raw).not.toBeChecked();
  await expect(page.getByText('can’t verify the rest', { exact: false })).toBeVisible();
  await raw.click();
  await page.getByRole('button', { name: 'Save policies' }).click();
  await expect(page.getByText('Policies saved')).toBeVisible();
  expect((await admin.get<{ allow_advanced_properties?: boolean }>(`${base}/policies`)).allow_advanced_properties).toBe(true);

  // A raw property BeziSync sets is refused when saved, with where to set it instead.
  const { password, ...pg } = SOURCE_PG;
  const src = await admin.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const refused = await admin.raw('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, advanced: { 'debezium.source.column.exclude.list': 'shop.orders.total' } },
  });
  expect(refused.status()).toBe(422);
  const problem = (await refused.json()) as { code: string; errors: { loc: string[]; message: string }[] };
  expect(problem.code).toBe('managed_property');
  expect(problem.errors[0]!.message).toContain('Tables');

  // Switching it off again is returned without the key. (The saved toast covers the Save button
  // until it closes, and a pointer resting on it keeps it open.)
  await page.mouse.move(0, 0);
  await expect(page.getByText('Policies saved')).toBeHidden();
  await raw.click();
  await page.getByRole('button', { name: 'Save policies' }).click();
  await expect(page.getByRole('button', { name: 'Save policies' })).toBeDisabled();
  expect((await admin.get<{ allow_advanced_properties?: boolean }>(`${base}/policies`)).allow_advanced_properties).toBeUndefined();
});

test('an admin sets resource limits; Policies counts the running pipelines they would block', async ({ page }) => {
  test.setTimeout(300_000);
  const admin = await Api.signIn();
  const tag = unique('limits');
  const project = await admin.send<{ id: string; slug: string }>('POST', '/api/v1/projects', { slug: tag.slice(0, 40), name: `Limits ${tag}` }, 201);
  const base = `/api/v1/projects/${project.id}`;
  const { password, ...pg } = SOURCE_PG;
  const rt = await admin.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await admin.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await admin.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `k-${tag}`, config: KAFKA }, 201);
  const p = await admin.send<{ id: string; footprint: { memory_mib: number } }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' }, policies: { resources: 'medium' } },
  }, 201);
  expect(p.footprint.memory_mib).toBe(1536);
  await admin.send('POST', `${base}/pipelines/${p.id}/deploy`, {});

  await signIn(page);
  await page.goto(`/p/${project.slug}/settings/policies`);
  await choose(page.getByLabel('Most memory per pipeline'), '1 GiB');
  await expect(page.getByText('1 running pipeline would be blocked at the next deploy')).toBeVisible();
  await expect(page.getByText('1536 MiB at its peak')).toBeVisible();
  await choose(page.getByLabel('Most readers per pipeline'), '4 readers');
  await page.getByRole('button', { name: 'Offer Shrink now' }).click();
  await page.getByRole('button', { name: 'Save policies' }).click();
  await expect(page.getByText('Policies saved')).toBeVisible();
  const saved = await admin.get<{ resource_limits?: Record<string, unknown>; shrink_after_copy?: string }>(`${base}/policies`);
  expect(saved.resource_limits).toEqual({ max_memory: '1Gi', max_parallelism: 4 });
  expect(saved.shrink_after_copy).toBe('suggest');

  // It keeps running; its next deploy is refused by the limit.
  const pre = await admin.send<{ sections: { key: string; report: { checks: { code: string; status: string }[] } }[] }>('POST', `${base}/pipelines/${p.id}/preflight`, {});
  expect(pre.sections.find((s) => s.key === 'policy')?.report.checks.find((c) => c.code === 'policy_resource_limits')?.status).toBe('fail');

  // Clearing them leaves nothing behind.
  await choose(page.getByLabel('Most memory per pipeline'), 'No limit');
  await choose(page.getByLabel('Most readers per pipeline'), 'No limit');
  await page.getByRole('button', { name: 'Shrink it' }).click();
  await page.getByRole('button', { name: 'Save policies' }).click();
  await expect(page.getByRole('button', { name: 'Save policies' })).toBeDisabled();
  const cleared = await admin.get<Record<string, unknown>>(`${base}/policies`);
  expect(cleared.resource_limits).toBeUndefined();
  expect(cleared.shrink_after_copy).toBeUndefined();
  await admin.send('DELETE', `${base}/pipelines/${p.id}?drop_destinations=true`);
});
