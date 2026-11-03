import { execSync } from 'node:child_process';
import { expect, test } from '@playwright/test';
import { Api, defaultProject, unique } from './helpers';
import { BASTION, BASTION_KEY, HTTP_ECHO, HTTP_ECHO_FROM_RUNNER, SOURCE_PG } from './fixtures';
import { sql, waitFor } from './data';

/** A database reached through an SSH bastion: checks, then a connector with a tunnel sidecar. */
test('a pipeline streams through an SSH bastion', async () => {
  test.setTimeout(600_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('ssh');
  const { password, ...pg } = SOURCE_PG;
  const config = { ...pg, ssl_mode: 'disable', ...BASTION };
  const secrets = { password, ssh_private_key: BASTION_KEY };

  const report = await api.send<{ checks: { code: string; status: string }[] }>('POST', `${base}/connections:check`, { kind: 'source', type: 'postgresql', config, secrets });
  const codes = Object.fromEntries(report.checks.map((c) => [c.code, c.status]));
  expect(codes.pg_ssh_reachable).toBe('pass');
  expect(codes.pg_ssh_tunnel).toBe('warn'); // host key not pinned
  expect(codes.pg_login).toBe('pass');
  expect(JSON.stringify(report)).not.toContain('PRIVATE KEY');

  const path = `/ssh-${tag}`;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `jump-${tag}`, config, secrets }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'http', name: `h-${tag}`, config: { url: `${HTTP_ECHO}${path}` } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `jump-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  const pre = await api.send<{ properties: string }>('POST', `${url}/preflight`, {});
  expect(pre.properties).toContain('debezium.source.database.hostname=127.0.0.1');
  expect(pre.properties).not.toContain('PRIVATE KEY');
  await api.send('POST', `${url}/deploy`, {});
  await waitFor('streaming through the tunnel', async () => {
    const cur = await api.get<{ lifecycle: string; health: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming' && cur.health === 'healthy';
  }, 420_000);
  const short = /slot\.name=bz_(\w+)/.exec(pre.properties)?.[1];
  const sidecars = execSync(`kubectl -n bezisync-e2e-runtime get pods -l app.bezisync.io/pipeline=${short} -o jsonpath='{.items[0].spec.initContainers[*].name}'`, { shell: '/bin/bash', encoding: 'utf8' });
  expect(sidecars).toContain('ssh-tunnel');

  const marker = `SSH-${tag}`;
  await sql('INSERT INTO shop.orders (customer_id, status, total) VALUES (1, $1, 1)', [marker]);
  await waitFor('row delivered through the tunnel', async () => {
    const reqs = (await (await fetch(`${HTTP_ECHO_FROM_RUNNER}/requests${path}`)).json()) as { body: string }[];
    return reqs.some((r) => r.body.includes(marker));
  }, 180_000);

  await api.send('DELETE', url);
  await waitFor('deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
});
