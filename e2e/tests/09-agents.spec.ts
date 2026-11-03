import { execSync } from 'node:child_process';
import { expect, test } from '@playwright/test';
import { Api, defaultProject, signIn, unique } from './helpers';
import { HTTP_ECHO, HTTP_ECHO_FROM_RUNNER, SOURCE_PG } from './fixtures';
import { sql, waitFor } from './data';

/**
 * Agent-mode runtimes: the agent is installed the way a user would (Helm chart from the
 * repo, the command BeziSync generates), dials out to the API, and runs connectors on a
 * Kubernetes namespace or on a separate Docker host (DinD). Losing the agent must turn
 * health Unknown — never keep showing Healthy.
 */
test.describe.configure({ mode: 'serial' });

const AGENT_NS = process.env.AGENT_NS ?? 'bezisync-e2e-agent';
const DATA_NS = process.env.DATA_NS ?? 'bezisync-e2e-data';

function sh(cmd: string): string {
  return execSync(cmd, { shell: '/bin/bash', encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
}

interface PipelineState { lifecycle: string; health: string; status: { reasons?: { code: string }[] } }

async function streamThroughRuntime(api: Api, base: string, runtimeId: string, tag: string) {
  const { password, ...pg } = SOURCE_PG;
  const path = `/agent-${tag}`;
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'http', name: `h-${tag}`, config: { url: `${HTTP_ECHO}${path}` } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: runtimeId,
    spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, {});
  await waitFor('pipeline streaming through the agent', async () => {
    const cur = await api.get<PipelineState>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming' && cur.health === 'healthy';
  }, 420_000);
  const arrives = async (marker: string) => {
    await sql('INSERT INTO shop.orders (customer_id, status, total) VALUES (1, $1, 1)', [marker]);
    await waitFor(`${marker} delivered`, async () => {
      const reqs = (await (await fetch(`${HTTP_ECHO_FROM_RUNNER}/requests${path}`)).json()) as { body: string }[];
      return reqs.some((r) => r.body.includes(marker));
    }, 180_000);
  };
  await arrives(`AGENT-${tag}-1`);
  return { url, arrives };
}

async function deletePipeline(api: Api, url: string) {
  await api.send('DELETE', url);
  await waitFor('pipeline deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
}

test('Kubernetes agent: install from the UI, stream, lose the agent → Unknown, reconnect', async ({ page }) => {
  test.setTimeout(1_200_000);
  await signIn(page);
  await page.goto('/p/default/settings/runtimes');
  await page.getByRole('button', { name: 'Add runtime' }).click();
  const dialog = page.getByRole('dialog');
  const name = unique('agent-k8s');
  await dialog.getByLabel('Name', { exact: true }).fill(name);
  await expect(dialog.getByRole('button', { name: 'Another cluster' })).toBeVisible();
  await dialog.getByRole('radio', { name: /^Through an agent/ }).click();
  // The agent uses its own in-cluster identity: no credentials asked for in agent mode.
  await expect(dialog.getByRole('button', { name: 'Another cluster' })).toHaveCount(0);
  await dialog.getByLabel('Namespace').fill(AGENT_NS);
  await dialog.getByRole('button', { name: 'Add runtime' }).click();

  const install = page.getByRole('dialog', { name: new RegExp(`Install the agent for ${name}`) });
  await expect(install.getByText('The token is shown only once', { exact: false })).toBeVisible();
  const command = (await install.getByLabel('Install command').textContent()) ?? '';
  expect(command).toContain('helm install bezisync-agent deploy/helm/bezisync-agent');
  expect(command).toMatch(/--set token=bza_/);
  // The token is shown once: Done needs it acknowledged.
  await expect(install.getByRole('button', { name: 'Done' })).toBeDisabled();
  await install.getByRole('checkbox', { name: /I’ve copied it/ }).check();
  await install.getByRole('button', { name: 'Done' }).click();
  await expect(page.getByText('agent never connected')).toBeVisible();

  sh(command);

  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const rt = (await api.get<{ id: string; name: string }[]>(`${base}/runtimes`)).find((r) => r.name === name);
  if (!rt) throw new Error('runtime missing');
  expect(JSON.stringify(await api.get(`${base}/runtimes/${rt.id}`))).not.toContain('bza_');
  await waitFor('agent connected', async () => (await api.send<{ ok: boolean }>('POST', `${base}/runtimes/${rt.id}/test`, {})).ok, 240_000);
  await page.reload();
  await expect(page.getByText(/agent .* seen/).first()).toBeVisible();

  const tag = unique('ak');
  const { url, arrives } = await streamThroughRuntime(api, base, rt.id, tag);
  // Preflight checks run on the agent, inside the runtime's network.
  const pre = await api.send<{ sections: { key: string; title: string }[] }>('POST', `${url}/preflight`, {});
  for (const key of ['source', 'sink']) {
    expect(pre.sections.find((s) => s.key === key)?.title).toContain('(checked by the agent)');
  }
  const logs = await api.get<{ lines: string[]; available: boolean }>(`${url}/logs`);
  expect(logs.available).toBe(true);
  expect(logs.lines.length).toBeGreaterThan(0);

  sh(`kubectl -n ${AGENT_NS} scale deployment/bezisync-agent --replicas=0`);
  const lost = await waitFor('health Unknown without the agent', async () => {
    const cur = await api.get<PipelineState>(url);
    return cur.health === 'unknown' ? cur : undefined;
  }, 180_000);
  expect(lost.status.reasons?.[0]?.code).toBe('runtime_unreachable');
  expect(lost.lifecycle).toBe('streaming'); // last known state, not a false "failed"

  sh(`kubectl -n ${AGENT_NS} scale deployment/bezisync-agent --replicas=1`);
  await waitFor('healthy again after reconnect', async () => (await api.get<PipelineState>(url)).health === 'healthy', 300_000);
  await arrives(`AGENT-${tag}-2`);

  await deletePipeline(api, url);
  await waitFor('connector removed by the agent', async () =>
    sh(`kubectl -n ${AGENT_NS} get deploy -l app.bezisync.io/runtime=${rt.id} -o name`).trim() === '', 120_000);
  sh(`helm uninstall bezisync-agent -n ${AGENT_NS}`);
});

test('Docker agent runs connectors on a remote Docker host', async () => {
  test.setTimeout(900_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const rt = await api.send<{ id: string; agent_token: string; agent_install: string }>(
    'POST', `${base}/runtimes`, { name: unique('agent-docker'), kind: 'docker', mode: 'agent', config: {} }, 201);
  expect(rt.agent_install).toContain('docker run');
  const controlPlane = /BEZISYNC_AGENT_URL=(\S+)/.exec(rt.agent_install)?.[1];
  const image = /\s(\S+) agent\s*$/.exec(rt.agent_install)?.[1];
  expect(controlPlane && image).toBeTruthy();
  const [repository, imageTag] = [image!.slice(0, image!.lastIndexOf(':')), image!.slice(image!.lastIndexOf(':') + 1)];
  // Same agent, packaged with the chart instead of `docker run`, pointed at the DinD host.
  sh(`helm install bezisync-agent-docker deploy/helm/bezisync-agent -n ${AGENT_NS} --create-namespace \
    --set kind=docker --set docker.host=tcp://dind.${DATA_NS}.svc.cluster.local:2375 \
    --set controlPlane.url=${controlPlane} --set token=${rt.agent_token} \
    --set image.repository=${repository} --set image.tag=${imageTag} --set image.pullPolicy=IfNotPresent`);
  await waitFor('docker agent connected', async () => (await api.send<{ ok: boolean; message: string }>('POST', `${base}/runtimes/${rt.id}/test`, {})).ok, 240_000);

  const tag = unique('ad');
  const { url } = await streamThroughRuntime(api, base, rt.id, tag);
  const dind = `kubectl -n ${DATA_NS} exec deploy/dind -- docker -H tcp://127.0.0.1:2375`;
  expect(sh(`${dind} ps -q --filter label=app.bezisync.io/runtime=${rt.id}`).trim()).not.toBe('');

  await deletePipeline(api, url);
  await waitFor('container removed from the Docker host', async () =>
    sh(`${dind} ps -aq --filter label=app.bezisync.io/runtime=${rt.id}`).trim() === '', 120_000);

  // A rotated token locks the old agent out.
  await api.send('POST', `${base}/runtimes/${rt.id}/agent-token`, {});
  await waitFor('old agent rejected', async () =>
    sh(`kubectl -n ${AGENT_NS} logs deploy/bezisync-agent-docker --since=30s`).includes('rejected the agent token'), 60_000);
  sh(`helm uninstall bezisync-agent-docker -n ${AGENT_NS}`);
});
