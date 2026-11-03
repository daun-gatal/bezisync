import { execSync } from 'node:child_process';
import { expect, test } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';
import { HTTP_ECHO, HTTP_ECHO_FROM_RUNNER, MOTO, MOTO_FROM_RUNNER, SOURCE_PG, VAULT, VAULT_FROM_RUNNER } from './fixtures';
import { sql, waitFor } from './data';

/** Connection secrets kept in Vault, AWS Secrets Manager and Kubernetes — referenced, never copied. */
test.describe.configure({ mode: 'serial' });

const RUNTIME_NS = 'bezisync-e2e-runtime';
interface CheckResult { ok: boolean; message: string }

async function vaultPut(path: string, data: Record<string, string>) {
  const res = await fetch(`${VAULT_FROM_RUNNER}/v1/secret/data/${path}`, {
    method: 'POST',
    headers: { 'x-vault-token': VAULT.token, 'content-type': 'application/json' },
    body: JSON.stringify({ data }),
  });
  expect(res.ok, await res.text()).toBeTruthy();
}

test('a pipeline streams with its database password read from Vault', async ({ page }) => {
  test.setTimeout(900_000);
  const tag = unique('ss');
  const store = `vault-${tag}`.slice(0, 40);
  await vaultPut(`cdc/${tag}`, { password: SOURCE_PG.password, note: 'managed by the DBA team' });

  await signIn(page);
  await page.goto('/p/default/settings/secret-stores');
  await page.getByRole('button', { name: 'Add store' }).click();
  await page.getByRole('menuitem', { name: /^HashiCorp Vault/ }).click();
  const dialog = page.getByRole('dialog');
  await dialog.getByLabel('Name', { exact: true }).fill(store);
  await dialog.getByLabel('Address').fill(VAULT.address);
  await dialog.getByLabel('Token').fill(VAULT.token);
  await dialog.getByRole('button', { name: 'Add store' }).click();
  await expect(page.getByText(new RegExp(`${store}: Signed in to Vault`))).toBeVisible();
  // One store exists per run of this test; only one "try" form is open at a time.
  const card = page.getByRole('region', { name: store });
  await card.getByRole('button', { name: 'Try a reference' }).click();
  await card.getByLabel('Path').fill(`cdc/${tag}`);
  await card.getByLabel('Key').fill('password');
  await card.getByRole('button', { name: 'Try reference', exact: true }).click();
  await expect(page.getByText(new RegExp(`Found a value .* Use ref\\+${store}://cdc/${tag}#password`))).toBeVisible();
  await expect(page.getByText(SOURCE_PG.password)).toHaveCount(0);

  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const { password: _unused, ...pg } = SOURCE_PG;
  const ref = `ref+${store}://cdc/${tag}#password`;
  const src = await api.send<{ id: string; secret_refs: Record<string, string> }>('POST', `${base}/connections`, {
    kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password: ref },
  }, 201);
  expect(src.secret_refs).toEqual({ password: ref });
  const report = await api.send<{ ok: boolean; checks: { code: string; status: string }[] }>('POST', `${base}/connections/${src.id}/check`, {});
  expect(report.checks.find((c) => c.code === 'pg_login')?.status).toBe('pass');

  // Editing shows the saved reference in the picker, resolved.
  await page.goto('/p/default/connections');
  await page.getByRole('row', { name: new RegExp(`pg-${tag}`) }).getByRole('button', { name: 'Edit' }).click();
  const edit = page.getByRole('dialog');
  await expect(edit.getByRole('button', { name: 'From a secret store' })).toHaveAttribute('aria-pressed', 'true');
  await expect(edit.getByText(`Found in ${store}. Only the reference ${ref} is saved.`)).toBeVisible();
  await page.keyboard.press('Escape');

  // A new source picks its password from the store: store, secret (listed), key (pre-picked).
  await page.getByRole('button', { name: 'Add source' }).first().click();
  const add = page.getByRole('dialog');
  await add.getByRole('button', { name: /PostgreSQL/ }).click();
  await add.getByLabel('Name', { exact: true }).fill(`picked-${tag}`);
  await add.getByLabel('Host').fill(SOURCE_PG.host);
  await add.getByLabel('Port').fill(String(SOURCE_PG.port));
  await add.getByLabel('Database').fill(SOURCE_PG.database);
  await add.getByLabel('User').fill(SOURCE_PG.user);
  await choose(add.getByLabel('TLS'), 'disable');
  await add.getByRole('button', { name: 'From a secret store' }).click();
  await choose(add.getByLabel('Store', { exact: true }), store);
  await add.getByLabel('Secret', { exact: true }).click();
  await page.getByRole('option', { name: `cdc/${tag}`, exact: true }).click();
  await expect(add.getByLabel('Key', { exact: true })).toContainText('password');
  await expect(add.getByText(`Found in ${store}. Only the reference ${ref} is saved.`)).toBeVisible();
  await add.getByRole('button', { name: 'Save source' }).click();
  await expect(page.getByText(`Added picked-${tag}`)).toBeVisible();

  const path = `/ss-${tag}`;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: RUNTIME_NS } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'http', name: `h-${tag}`, config: { url: `${HTTP_ECHO}${path}` } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, {});
  await waitFor('streaming with a Vault password', async () => {
    const cur = await api.get<{ lifecycle: string; health: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming' && cur.health === 'healthy';
  }, 420_000);
  const marker = `SS-${tag}`;
  await sql('INSERT INTO shop.orders (customer_id, status, total) VALUES (1, $1, 1)', [marker]);
  await waitFor('row delivered', async () => {
    const reqs = (await (await fetch(`${HTTP_ECHO_FROM_RUNNER}/requests${path}`)).json()) as { body: string }[];
    return reqs.some((r) => r.body.includes(marker));
  }, 180_000);

  // A store that is in use cannot be deleted.
  const stores = await api.get<{ id: string; name: string; used_by: string[] }[]>(`${base}/secret-stores`);
  const vault = stores.find((s) => s.name === store)!;
  expect(vault.used_by).toEqual([`pg-${tag}`, `picked-${tag}`]);
  expect((await api.raw('DELETE', `${base}/secret-stores/${vault.id}`)).status()).toBe(409);

  await api.send('DELETE', url);
  await waitFor('pipeline deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
});

test('AWS Secrets Manager and Kubernetes Secret references resolve', async () => {
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('ref');

  // AWS (moto): create a JSON secret, then read one key through BeziSync.
  const res = await fetch(MOTO_FROM_RUNNER, {
    method: 'POST',
    headers: {
      'content-type': 'application/x-amz-json-1.1',
      'x-amz-target': 'secretsmanager.CreateSecret',
      authorization: 'AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/secretsmanager/aws4_request, SignedHeaders=host, Signature=x',
    },
    body: JSON.stringify({ Name: `cdc/${tag}`, SecretString: JSON.stringify({ password: 'from-aws' }) }),
  });
  expect(res.ok, await res.text()).toBeTruthy();
  const aws = await api.send<{ id: string; last_test: CheckResult }>('POST', `${base}/secret-stores`, {
    name: `aws-${tag}`.slice(0, 40), kind: 'aws', config: MOTO, secrets: { access_key_id: 'test', secret_access_key: 'test' },
  }, 201);
  expect(aws.last_test.ok, aws.last_test.message).toBe(true);
  const fromAws = await api.send<CheckResult>('POST', `${base}/secret-stores/${aws.id}/check-ref`, { path: `cdc/${tag}`, key: 'password' });
  expect(fromAws.ok, fromAws.message).toBe(true);
  expect(fromAws.message).not.toContain('from-aws');
  const missing = await api.send<CheckResult>('POST', `${base}/secret-stores/${aws.id}/check-ref`, { path: 'cdc/missing', key: 'password' });
  expect(missing).toMatchObject({ ok: false });
  expect(missing.message).toContain("no secret 'cdc/missing'");

  // Kubernetes: a Secret in the runtime namespace, read with BeziSync's service account.
  execSync(`kubectl -n ${RUNTIME_NS} create secret generic db-${tag} --from-literal=password=from-k8s`, { shell: '/bin/bash' });
  const k8s = await api.send<{ id: string; last_test: CheckResult }>('POST', `${base}/secret-stores`, {
    name: `k8s-${tag}`.slice(0, 40), kind: 'kubernetes', config: { namespace: RUNTIME_NS },
  }, 201);
  expect(k8s.last_test.ok, k8s.last_test.message).toBe(true);
  const fromK8s = await api.send<CheckResult>('POST', `${base}/secret-stores/${k8s.id}/check-ref`, { path: `db-${tag}`, key: 'password' });
  expect(fromK8s.ok, fromK8s.message).toBe(true);
  for (const id of [aws.id, k8s.id]) await api.send('DELETE', `${base}/secret-stores/${id}`, undefined, 204);
});

test('a Kubernetes store signs in to another cluster with a pasted kubeconfig', async ({ page }) => {
  const tag = unique('kc');
  const store = `kc-${tag}`.slice(0, 40);
  const sa = `reader-${tag}`.slice(0, 50);
  const sh = (cmd: string) => execSync(cmd, { shell: '/bin/bash', encoding: 'utf8' });
  // A ServiceAccount that may only read Secrets in one namespace, as the guide tells people to make.
  sh(`kubectl -n ${RUNTIME_NS} create serviceaccount ${sa}`);
  sh(`kubectl -n ${RUNTIME_NS} create role ${sa} --verb=get,list --resource=secrets`);
  sh(`kubectl -n ${RUNTIME_NS} create rolebinding ${sa} --role=${sa} --serviceaccount=${RUNTIME_NS}:${sa}`);
  sh(`kubectl -n ${RUNTIME_NS} create secret generic db-${tag} --from-literal=password=from-kubeconfig`);
  const token = sh(`kubectl -n ${RUNTIME_NS} create token ${sa} --duration=1h`).trim();
  const ca = Buffer.from(sh(`kubectl -n ${RUNTIME_NS} get configmap kube-root-ca.crt -o jsonpath='{.data.ca\\.crt}'`)).toString('base64');
  const kubeconfig = [
    'apiVersion: v1',
    'kind: Config',
    'current-context: reader',
    `clusters: [{name: e2e, cluster: {server: "https://kubernetes.default.svc", certificate-authority-data: ${ca}}}]`,
    'contexts: [{name: reader, context: {cluster: e2e, user: reader}}]',
    `users: [{name: reader, user: {token: ${token}}}]`,
  ].join('\n');

  await signIn(page);
  await page.goto('/p/default/settings/secret-stores');
  await page.getByRole('button', { name: 'Add store' }).click();
  await page.getByRole('menuitem', { name: /^Kubernetes Secrets/ }).click();
  const dialog = page.getByRole('dialog');
  await dialog.getByLabel('Name', { exact: true }).fill(store);
  await dialog.getByLabel('Namespace').fill(RUNTIME_NS);
  // The kubeconfig is asked for exactly when the store reads another cluster.
  await expect(dialog.getByLabel(/Kubeconfig/)).toHaveCount(0);
  await dialog.getByRole('button', { name: 'Another cluster' }).click();
  await dialog.getByLabel(/Kubeconfig/).fill(kubeconfig);
  await dialog.getByRole('button', { name: 'Add store' }).click();
  await expect(page.getByText(new RegExp(`${store}: Can read Secrets in namespace ${RUNTIME_NS}`))).toBeVisible();

  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const stores = await api.get<{ id: string; name: string; secret_fields_set: string[] }[]>(`${base}/secret-stores`);
  const created = stores.find((s) => s.name === store)!;
  expect(created.secret_fields_set).toEqual(['kubeconfig']);
  const found = await api.send<CheckResult>('POST', `${base}/secret-stores/${created.id}/check-ref`, { path: `db-${tag}`, key: 'password' });
  expect(found.ok, found.message).toBe(true);
  await api.send('DELETE', `${base}/secret-stores/${created.id}`, undefined, 204);
  sh(`kubectl -n ${RUNTIME_NS} delete secret/db-${tag} rolebinding/${sa} role/${sa} serviceaccount/${sa}`);
});
