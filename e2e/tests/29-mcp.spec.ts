import { createHash } from 'node:crypto';
import { expect, request, test } from '@playwright/test';
import { Api, BASE_URL, defaultProject, signIn, unique } from './helpers';
import { SOURCE_PG } from './fixtures';

/** MCP: an Admin turns it on, agents connect with an API token or by signing in (OAuth). */

const CALLBACK = 'http://127.0.0.1:33418/callback';

async function rpc(token: string, method: string, params: unknown = {}) {
  const ctx = await request.newContext({ baseURL: BASE_URL });
  const res = await ctx.post('/api/v1/mcp', {
    data: { jsonrpc: '2.0', id: 1, method, params },
    headers: { Authorization: `Bearer ${token}`, Accept: 'application/json, text/event-stream', 'MCP-Protocol-Version': '2025-06-18' },
  });
  const body = res.status() === 200 ? await res.json() : null;
  await ctx.dispose();
  return { status: res.status(), body };
}

test.afterAll(async () => {
  const admin = await Api.signIn();
  await admin.send('PUT', '/api/v1/mcp/settings', { enabled: false, oauth_enabled: false });
});

test('an Admin turns on MCP and an agent connects with an API token', async ({ page }) => {
  const admin = await Api.signIn();
  await admin.send('PUT', '/api/v1/mcp/settings', { enabled: false, oauth_enabled: false });
  await signIn(page);
  await page.goto('/p/default/settings/mcp');
  await page.getByRole('switch', { name: 'Turn on MCP' }).click();
  await page.getByRole('switch', { name: 'Let agents sign in' }).click();
  await expect(page.getByLabel('MCP endpoint')).toContainText('/api/v1/mcp');
  // Destructive tools are off until an Admin turns them on.
  await page.getByLabel('Search tools').fill('delete_pipeline');
  await expect(page.getByRole('switch', { name: /Delete a pipeline/ })).toHaveAttribute('aria-checked', 'false');
  await page.getByRole('button', { name: 'Save' }).click();
  await expect(page.getByText('Saved the MCP settings')).toBeVisible();

  const { token } = await admin.send<{ token: string }>('POST', '/api/v1/auth/tokens', { name: `mcp-e2e-${Date.now()}`, read_only: true });
  const listed = await rpc(token, 'tools/list');
  expect(listed.status).toBe(200);
  const names = (listed.body.result.tools as { name: string }[]).map((t) => t.name);
  expect(names).toContain('list_pipelines');
  expect(names).not.toContain('create_pipeline'); // a read-only token gets read tools only
  const called = await rpc(token, 'tools/call', { name: 'list_projects', arguments: {} });
  expect(called.body.result.content[0].text).toContain('"slug": "default"');

  // An agent reads what a pipeline may tune, with its current values.
  const project = await defaultProject(admin);
  const tag = unique('mcpset');
  const { password, ...pg } = SOURCE_PG;
  const src = await admin.send<{ id: string }>('POST', `/api/v1/projects/${project.id}/connections`, {
    kind: 'source', type: 'postgresql', name: `src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password },
  }, 201);
  const draft = await admin.send<{ id: string }>('POST', `/api/v1/projects/${project.id}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, spec: { source: { tables: { include: ['shop.orders'] } }, tuning: { 'debezium.source.snapshot.max.threads': '2' } },
  }, 201);
  expect(names).toContain('list_engine_settings');
  const settings = await rpc(token, 'tools/call', { name: 'list_engine_settings', arguments: { project: 'default', pipeline: `p-${tag}` } });
  const text = settings.body.result.content[0].text as string;
  expect(settings.body.result.isError ?? false).toBe(false);
  expect(text).toContain('"key": "debezium.source.snapshot.max.threads"');
  expect(text).toContain('"value": "2"');
  await admin.send('DELETE', `/api/v1/projects/${project.id}/pipelines/${draft.id}`);

  // The Account page shows how to connect an agent.
  await page.goto('/p/default/account');
  await expect(page.getByLabel('Claude Code setup')).toContainText('claude mcp add --transport http bezisync');
  await expect(page.getByLabel('Claude Code setup')).not.toContainText('Authorization');
  // Each major agent has its own steps.
  await page.getByRole('group', { name: 'Your agent' }).getByRole('button', { name: 'Cursor' }).click();
  await expect(page.getByLabel('Cursor setup')).toContainText('"mcpServers"');
  await page.getByRole('group', { name: 'Your agent' }).getByRole('button', { name: 'Claude Code' }).click();
  // …or with an API token in a header.
  await page.getByRole('group', { name: 'How the agent connects' }).getByRole('button', { name: 'API token' }).click();
  await expect(page.getByLabel('Claude Code setup')).toContainText('--header "Authorization: Bearer <your API token>"');
  await expect(page.getByLabel('Check the connection setup')).toContainText('tools/list');
  // A new token's one-time dialog fills it into the agent setup.
  await page.getByRole('button', { name: 'New token' }).click();
  await page.getByRole('dialog').getByLabel('Name').fill(`mcp-ui-${Date.now()}`);
  await page.getByRole('dialog').getByRole('button', { name: 'Create token' }).click();
  const created = ((await page.getByLabel('API token', { exact: true }).textContent()) ?? '').trim();
  expect(created).toMatch(/^bzt_/);
  await expect(page.getByLabel('MCP setup with this token')).toContainText(`--header "Authorization: Bearer ${created}"`);
  await page.getByRole('checkbox', { name: /I’ve copied it/ }).check();
  await page.getByRole('button', { name: 'Done' }).click();
});

test('an agent signs in through the consent page and gets only what the person chose', async ({ page }) => {
  const anon = await request.newContext({ baseURL: BASE_URL });
  // Discovery works through the web server, as agents look it up at the site's root.
  const resource = await anon.get('/.well-known/oauth-protected-resource/api/v1/mcp');
  expect(resource.status()).toBe(200);
  expect((await resource.json()).resource).toMatch(/\/api\/v1\/mcp$/);
  expect((await anon.get('/.well-known/oauth-authorization-server')).status()).toBe(200);
  const reg = await anon.post('/api/v1/oauth/register', { data: { client_name: 'E2E agent', redirect_uris: [CALLBACK] } });
  expect(reg.status()).toBe(201);
  const clientId = (await reg.json()).client_id as string;

  const verifier = 'e2e-verifier-'.padEnd(64, 'x');
  const challenge = createHash('sha256').update(verifier).digest('base64url');
  await signIn(page);
  // The agent's callback runs on the person's computer; answer it here.
  await page.route(`${CALLBACK}**`, (route) => route.fulfill({ status: 200, body: 'ok' }));
  const params = new URLSearchParams({ response_type: 'code', client_id: clientId, redirect_uri: CALLBACK, code_challenge: challenge, code_challenge_method: 'S256', state: 'e2e' });
  await page.goto(`/api/v1/oauth/authorize?${params}`);
  await expect(page.getByRole('heading', { name: 'E2E agent wants to use BeziSync' })).toBeVisible();
  await page.getByRole('button', { name: 'Only read' }).click();
  await Promise.all([page.waitForURL(`${CALLBACK}**`), page.getByRole('button', { name: 'Allow', exact: true }).click()]);
  const back = new URL(page.url());
  expect(back.searchParams.get('state')).toBe('e2e');

  const tokenRes = await anon.post('/api/v1/oauth/token', {
    form: { grant_type: 'authorization_code', client_id: clientId, code: back.searchParams.get('code')!, redirect_uri: CALLBACK, code_verifier: verifier },
  });
  expect(tokenRes.status()).toBe(200);
  const access = (await tokenRes.json()).access_token as string;
  const names = ((await rpc(access, 'tools/list')).body.result.tools as { name: string }[]).map((t) => t.name);
  expect(names).toContain('get_pipeline');
  expect(names).not.toContain('deploy_pipeline');

  // The person disconnects it from their Account page; the token stops working.
  await page.goto('/p/default/account');
  const row = page.getByRole('list', { name: 'Your connected agents' }).getByRole('listitem').filter({ hasText: 'E2E agent' }).first();
  await row.getByRole('button', { name: 'Disconnect' }).click();
  await expect(page.getByText('Disconnected E2E agent')).toBeVisible();
  expect((await rpc(access, 'tools/list')).status).toBe(401);
  await anon.dispose();
});
