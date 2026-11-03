import { execSync } from 'node:child_process';
import { expect, test } from '@playwright/test';
import { Api, defaultProject, signIn, unique } from './helpers';
import { HTTP_ECHO, HTTP_ECHO_FROM_RUNNER, SOURCE_PG } from './fixtures';
import { ownerSql, sql, waitFor } from './data';

/** Chaos and scale: nothing is lost when parts die, and big inventories stay usable. */
test.describe.configure({ mode: 'serial' });

const kubectl = (args: string) => execSync(`kubectl ${args}`, { shell: '/bin/bash', encoding: 'utf8', timeout: 120_000 });

interface State {
  lifecycle: string;
  health: string;
  status: { as_of?: string };
}

test('killing the connector mid-stream loses nothing; killing controllers keeps status fresh', async () => {
  test.setTimeout(900_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('chaos');
  const { password, ...pg } = SOURCE_PG;
  const path = `/chaos-${tag}`;
  const rt = await api.send<{ id: string }>(
    'POST',
    `${base}/runtimes`,
    { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } },
    201,
  );
  const src = await api.send<{ id: string }>(
    'POST',
    `${base}/connections`,
    { kind: 'source', type: 'postgresql', name: `src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } },
    201,
  );
  const dst = await api.send<{ id: string }>(
    'POST',
    `${base}/connections`,
    { kind: 'sink', type: 'http', name: `h-${tag}`, config: { url: `${HTTP_ECHO}${path}` } },
    201,
  );
  const p = await api.send<{ id: string }>(
    'POST',
    `${base}/pipelines`,
    {
      name: `chaos-${tag}`,
      source_id: src.id,
      sink_id: dst.id,
      runtime_id: rt.id,
      spec: { source: { tables: { include: ['shop.orders'] } }, snapshot: { mode: 'no_data' } },
    },
    201,
  );
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, {});
  const healthy = (what: string) =>
    waitFor(
      what,
      async () => {
        const cur = await api.get<State>(url);
        if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
        return cur.lifecycle === 'streaming' && cur.health === 'healthy';
      },
      420_000,
    );
  await healthy('streaming');
  const delivered = async () => {
    const reqs = (await (await fetch(`${HTTP_ECHO_FROM_RUNNER}/requests${path}`)).json()) as { body: string }[];
    return reqs.map((r) => r.body).join('\n');
  };
  const insert = (marker: string) => sql('INSERT INTO shop.orders (customer_id, status, total) VALUES (1, $1, 1)', [marker]);

  // 1. Kill the connector pod while changes keep coming.
  const config = await api.get<{ properties: string }>(`${url}/config`);
  const short = /slot\.name=bz_(\w+)/.exec(config.properties)?.[1];
  expect(short).toBeTruthy();
  const before = Array.from({ length: 5 }, (_, i) => `CHAOS-${tag}-before-${i}`);
  for (const m of before) await insert(m);
  await waitFor(
    'rows before the crash delivered',
    async () => {
      const body = await delivered();
      return before.every((m) => body.includes(m));
    },
    120_000,
  );
  kubectl(`-n bezisync-e2e-runtime delete pod -l app.bezisync.io/pipeline=${short} --wait=false`);
  const during = Array.from({ length: 5 }, (_, i) => `CHAOS-${tag}-during-${i}`);
  for (const m of during) await insert(m);
  await healthy('healthy again after the connector restarted');
  await waitFor(
    'rows written during the crash delivered (resumed from stored offsets)',
    async () => {
      const body = await delivered();
      return during.every((m) => body.includes(m));
    },
    240_000,
  );

  // 2. Kill each controller pod in turn: whichever was the leader, the other takes over.
  const pods = kubectl(`-n bezisync-e2e get pods -l app.kubernetes.io/component=controller -o jsonpath='{.items[*].metadata.name}'`).trim().split(/\s+/);
  expect(pods.length).toBeGreaterThanOrEqual(2);
  for (const pod of pods) {
    const killedAt = Date.now();
    kubectl(`-n bezisync-e2e delete pod ${pod} --wait=false`);
    await waitFor(
      `status refreshed after ${pod} was killed`,
      async () => {
        const cur = await api.get<State>(url);
        return cur.status.as_of !== undefined && Date.parse(cur.status.as_of) > killedAt + 1000 && cur.health === 'healthy';
      },
      180_000,
    );
  }
  const after = `CHAOS-${tag}-after`;
  await insert(after);
  await waitFor('rows after the failovers delivered', async () => (await delivered()).includes(after), 120_000);

  await api.send('DELETE', url);
  await waitFor('deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
});

test('1,000 tables and 200 pipelines stay quick and usable', async ({ page }) => {
  test.setTimeout(600_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('scale').replace(/-/g, '_');
  const schema = `perf_${tag}`;
  await ownerSql(`CREATE SCHEMA ${schema}; DO $$ BEGIN FOR i IN 1..1000 LOOP
    EXECUTE format('CREATE TABLE ${schema}.t_%s (id int PRIMARY KEY, email text, v int)', i); END LOOP; END $$;`);
  try {
    const { password, ...pg } = SOURCE_PG;
    const src = await api.send<{ id: string }>(
      'POST',
      `${base}/connections`,
      { kind: 'source', type: 'postgresql', name: `big-${tag}`.replace(/_/g, '-'), config: { ...pg, ssl_mode: 'disable' }, secrets: { password } },
      201,
    );
    const started = Date.now();
    const discovery = await api.send<{ tables: { schema: string }[] }>('POST', `${base}/connections/${src.id}/discover`, {});
    const discoverMs = Date.now() - started;
    expect(discovery.tables.filter((t) => t.schema === schema)).toHaveLength(1000);
    expect(discoverMs).toBeLessThan(15_000);

    // 200 draft pipelines (no runtime cost) for the list.
    const ids: string[] = [];
    for (let i = 0; i < 200; i++) {
      const p = await api.send<{ id: string }>(
        'POST',
        `${base}/pipelines`,
        {
          name: `bulk-${tag}-${i}`.replace(/_/g, '-'),
          source_id: src.id,
          spec: { source: { tables: { include: [`${schema}.t_${i + 1}`] } } },
        },
        201,
      );
      ids.push(p.id);
    }
    const listStarted = Date.now();
    const list = await api.get<unknown[]>(`${base}/pipelines`);
    expect(list.length).toBeGreaterThanOrEqual(200);
    expect(Date.now() - listStarted).toBeLessThan(5_000);

    await signIn(page);
    const pageStarted = Date.now();
    await page.goto('/p/default/pipelines');
    // The list pages (100 rows at a time here, the specs' choice) and stays quick on its last page.
    await expect(page.getByText(/^1–100 of \d+ pipelines$/)).toBeVisible();
    await page.getByRole('navigation', { name: 'Pages of pipelines' }).getByRole('button', { name: /^Page \d+$/ }).last().click();
    await expect(page.getByText(/^\d+–\d+ of \d+ pipelines$/)).toBeVisible();
    await page.getByLabel('Search pipelines').fill(`bulk-${tag}-199`.replace(/_/g, '-'));
    await expect(page.getByText(`bulk-${tag}-199`.replace(/_/g, '-'))).toBeVisible();
    expect(Date.now() - pageStarted).toBeLessThan(10_000);

    // The wizard's table picker copes with 1,000 tables: filter, select, continue.
    await page.goto('/p/default/pipelines/new');
    await page.getByRole('radio', { name: new RegExp(`big-${tag}`.replace(/_/g, '-')) }).click();
    await page.getByRole('button', { name: 'Continue to tables →' }).click();
    await page.getByLabel('Search tables').fill(`${schema}.t_999`);
    await page.getByRole('checkbox', { name: `${schema}.t_999`, exact: true }).check();
    await expect(page.getByText('1 selected')).toBeVisible();

    for (const id of ids) await api.send('DELETE', `${base}/pipelines/${id}`);
  } finally {
    await ownerSql(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
  }
});
