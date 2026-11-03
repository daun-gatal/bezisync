import { expect, test } from '@playwright/test';
import { Api, defaultProject, unique } from './helpers';
import { KAFKA, SOURCE_PG } from './fixtures';
import { sql, waitFor } from './data';

interface P { id: string; lifecycle: string; health: string; version: number; status: unknown }
type Events = { records: { value: Record<string, unknown> }[] };

test('day 2: add a table with backfill, read the position, skip to now', async () => {
  test.setTimeout(900_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('d2');
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `src-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: `dst-${tag}`, config: KAFKA }, 201);
  const p = await api.send<P>('POST', `${base}/pipelines`, {
    name: `d2-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, sink: { naming: `d2.${tag}.{table}` } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  const streaming = async (what: string) => waitFor(what, async () => {
    const cur = await api.get<P>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming' ? cur : undefined;
  }, 420_000);
  const events = (table: string) => api.get<Events>(`${url}/events?table=${table}&limit=500`);

  await api.send('POST', `${url}/deploy`, {});
  let cur = await streaming('first deploy streaming');

  // Add a table to the running pipeline: v2 deploys and an incremental snapshot backfills it.
  cur = await api.get<P>(url);
  await api.patch(url, { spec: { source: { tables: { include: ['shop.order_items', 'shop.orders'] } }, sink: { naming: `d2.${tag}.{table}` } } }, cur.version);
  const patched = await api.get<P & { current_version: number }>(url);
  expect(patched.current_version).toBe(2);
  const pre = await api.send<{ change: { tables_added: string[]; notes: string[] } }>('POST', `${url}/preflight`);
  expect(pre.change.tables_added).toEqual(['shop.order_items']);
  await api.send('POST', `${url}/deploy`, {});
  await streaming('v2 streaming');
  await waitFor('order_items backfilled by incremental snapshot', async () => (await events('shop.order_items')).records.length >= 300, 300_000);

  // The stored position is readable.
  const offsets = await waitFor('offsets stored', async () => {
    const o = await api.get<{ offsets: { position: string }[] }>(`${url}/offsets`);
    return o.offsets.length ? o : undefined;
  }, 120_000);
  expect(offsets.offsets[0]!.position).toMatch(/^LSN [0-9A-F]+\/[0-9A-F]+$/);

  // Skip to now: a change made while paused is never delivered; later changes are.
  await api.send('POST', `${url}/pause`);
  await waitFor('paused', async () => (await api.get<P>(url)).lifecycle === 'paused', 180_000);
  await sql('INSERT INTO shop.orders (customer_id, status, total) VALUES (3, $1, 1)', [`SKIPPED-${tag}`]);
  await waitFor('connector stopped', async () => {
    const res = await api.raw('POST', `${url}/offsets/reset`, { mode: 'skip_to_now', confirm: `d2-${tag}` });
    if (res.status() === 409 && (await res.json()).code === 'still_running') return false;
    expect(res.status(), await res.text()).toBe(200);
    return true;
  }, 120_000, 3_000);
  await api.send('POST', `${url}/resume`);
  await streaming('streaming after skip');
  await sql('INSERT INTO shop.orders (customer_id, status, total) VALUES (3, $1, 1)', [`AFTER-${tag}`]);
  const after = await waitFor('post-skip change delivered', async () => {
    const e = await events('shop.orders');
    return e.records.some((r) => r.value.status === `AFTER-${tag}`) ? e : undefined;
  }, 180_000);
  expect(after.records.some((r) => r.value.status === `SKIPPED-${tag}`)).toBe(false);

  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor('deleted', async () => (await api.get<P>(url)).lifecycle === 'deleted', 240_000);
});
