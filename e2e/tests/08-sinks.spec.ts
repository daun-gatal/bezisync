import { expect, test } from '@playwright/test';
import { Api, choose, defaultProject, signIn, unique } from './helpers';
import { EVENTHUBS, HTTP_ECHO, HTTP_ECHO_FROM_RUNNER, ICEBERG_REST, KINESIS, MOTO, MOTO_FROM_RUNNER, NATS, PUBSUB, PUBSUB_FROM_RUNNER, PULSAR, REDIS, RUSTFS_TABLES, SOURCE_PG, TARGET_MYSQL, TARGET_PG } from './fixtures';
import { sql, waitFor } from './data';
import { walkWizard } from './walk';

test.describe.configure({ mode: 'serial' });

interface Report { ok: boolean; checks: { code: string; status: string; message: string }[] }
type Rec = { value: Record<string, unknown> };

async function pipelineTo(
  label: string,
  sink: { type: string; config: Record<string, unknown>; secrets?: Record<string, unknown> },
  verify: (ctx: { api: Api; url: string; tag: string; marker: string; destination: string }) => Promise<void>,
  beforeInsert?: (destination: string) => Promise<void>,
  opts: { naming?: (tag: string) => string; options?: Record<string, unknown>; resources?: string; exclude?: string[] } = {},
): Promise<void> {
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique(label);
  const { password, ...pg } = SOURCE_PG;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: sink.type, name: `d-${tag}`, config: sink.config, secrets: sink.secrets ?? {} }, 201);
  const report = await api.send<Report>('POST', `${base}/connections/${dst.id}/check`, {});
  expect(report.ok, JSON.stringify(report.checks)).toBe(true);
  const naming = opts.naming ? opts.naming(tag) : `s-${tag}-{table}`;
  const destination = naming.replace('{table}', 'orders');
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: {
      source: { tables: { include: ['shop.orders'] }, ...(opts.exclude ? { columns: { 'shop.orders': { mask: [], exclude: opts.exclude } } } : {}) },
      sink: { naming, options: opts.options ?? {} },
      snapshot: { mode: 'no_data' },
      ...(opts.resources ? { policies: { resources: opts.resources } } : {}),
    },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  await api.send('POST', `${url}/deploy`, {});
  await waitFor(`${label} streaming`, async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 420_000);
  if (beforeInsert) await beforeInsert(destination);
  const marker = `SINK-${tag}`;
  await sql('INSERT INTO shop.orders (customer_id, status, total) VALUES (1, $1, 1)', [marker]);
  await verify({ api, url, tag, marker, destination });
  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor(`${label} deleted`, async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
}

async function viaEventsApi({ api, url, marker }: { api: Api; url: string; marker: string }) {
  await waitFor('event readable through BeziSync', async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=shop.orders&limit=100`);
    return e.records.some((r) => r.value.status === marker);
  }, 180_000);
}

test('Postgres → HTTP endpoint', async () => {
  test.setTimeout(600_000);
  const path = `/sink-${unique('h')}`;
  await pipelineTo('http', { type: 'http', config: { url: `${HTTP_ECHO}${path}` } }, async ({ marker }) => {
    await waitFor('HTTP POST received', async () => {
      const reqs = (await (await fetch(`${HTTP_ECHO_FROM_RUNNER}/requests${path}`)).json()) as { body: string }[];
      return reqs.some((r) => r.body.includes(marker));
    }, 180_000);
  });
});

test('Postgres → Redis Streams', async () => {
  test.setTimeout(600_000);
  const { password, ...config } = REDIS;
  await pipelineTo('redis', { type: 'redis', config, secrets: { password } }, viaEventsApi);
});

test('Postgres → NATS JetStream', async () => {
  test.setTimeout(600_000);
  await pipelineTo('nats', { type: 'nats', config: { ...NATS, stream: `CDC_${Date.now()}` } }, viaEventsApi);
});

test('Postgres → AWS Kinesis (kinesis-mock)', async () => {
  test.setTimeout(600_000);
  const { secret_access_key, ...config } = KINESIS;
  await pipelineTo('kinesis', { type: 'kinesis', config, secrets: { secret_access_key } }, viaEventsApi);
});

test('Postgres → Google Pub/Sub (emulator)', async () => {
  test.setTimeout(600_000);
  const sub = `sub-${unique('ps')}`;
  const subUrl = `${PUBSUB_FROM_RUNNER}/v1/projects/${PUBSUB.project_id}/subscriptions/${sub}`;
  await pipelineTo('pubsub', { type: 'pubsub', config: PUBSUB },
    async ({ marker }) => {
      await waitFor('Pub/Sub message pulled', async () => {
        const res = await fetch(`${subUrl}:pull`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ maxMessages: 100 }) });
        const body = (await res.json()) as { receivedMessages?: { message: { data: string } }[] };
        return (body.receivedMessages ?? []).some((m) => Buffer.from(m.message.data, 'base64').toString().includes(marker));
      }, 180_000);
    },
    async (destination) => {
      const res = await fetch(subUrl, { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ topic: `projects/${PUBSUB.project_id}/topics/${destination}` }) });
      expect(res.ok, await res.text()).toBe(true);
    });
});

// Apache Iceberg through the Debezium Server Iceberg build. Iceberg table names take no '-'.
const icebergNaming = (tag: string) => `s_${tag.replace(/-/g, '_')}_{table}`;

test('Postgres → Apache Iceberg (REST catalog): upserts, updates and deletes', async () => {
  test.setTimeout(900_000);
  const { secret_access_key, ...config } = ICEBERG_REST;
  await pipelineTo(
    'iceberg-rest',
    { type: 'iceberg', config, secrets: { secret_access_key } },
    async ({ api, url, marker }) => {
      await viaEventsApi({ api, url, marker });
      const [row] = await sql<{ id: number }>('SELECT id FROM shop.orders WHERE status = $1', [marker]);
      const paid = `${marker}-paid`;
      await sql('UPDATE shop.orders SET status = $1 WHERE id = $2', [paid, row!.id]);
      // Upserted by key: one row for the order, now with the new status.
      await waitFor('update upserted', async () => {
        const e = await api.get<{ records: Rec[] }>(`${url}/events?table=shop.orders&limit=100`);
        const mine = e.records.filter((r) => Number(r.value.id) === row!.id);
        return mine.length === 1 && mine[0]!.value.status === paid;
      }, 240_000);
      await sql('DELETE FROM shop.orders WHERE id = $1', [row!.id]);
      await waitFor('delete kept, marked __deleted', async () => {
        const e = await api.get<{ records: Rec[] }>(`${url}/events?table=shop.orders&limit=100`);
        const mine = e.records.filter((r) => Number(r.value.id) === row!.id);
        return mine.length === 1 && String(mine[0]!.value.__deleted) === 'true';
      }, 240_000);
    },
    undefined,
    { naming: icebergNaming, options: { write_mode: 'upsert_keep_deletes', commit_interval: 'now', partitions: { 'shop.orders': 'id' } } },
  );
});

// ADR 0005: Partition by on Debezium, picked in the wizard. Rows are upserted by key, so only the
// key partitions them; the build creates the table with that spec, and preflight blocks
// partitioning by a column outside the key.
test('Postgres → Apache Iceberg on Debezium: Partition by from the wizard, applied when the table is created', async ({ page }) => {
  test.setTimeout(900_000);
  const api = await Api.signIn();
  const project = await defaultProject(api);
  const base = `/api/v1/projects/${project.id}`;
  const tag = unique('icepart');
  const { password, ...pg } = SOURCE_PG;
  const { secret_access_key, ...config } = ICEBERG_REST;
  const rt = await api.send<{ id: string }>('POST', `${base}/runtimes`, { name: `rt-${tag}`, kind: 'kubernetes', config: { namespace: 'bezisync-e2e-runtime' } }, 201);
  const src = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'source', type: 'postgresql', name: `pg-${tag}`, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } }, 201);
  const dst = await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'iceberg', name: `d-${tag}`, config, secrets: { secret_access_key } }, 201);
  const p = await api.send<{ id: string }>('POST', `${base}/pipelines`, {
    name: `p-${tag}`, source_id: src.id, sink_id: dst.id, runtime_id: rt.id,
    spec: { source: { tables: { include: ['shop.orders'] } }, sink: { naming: icebergNaming(tag), options: { write_mode: 'upsert', commit_interval: 'now' } }, snapshot: { mode: 'no_data' } },
  }, 201);
  const url = `${base}/pipelines/${p.id}`;
  type Saved = { version: number; spec: { sink: { options: Record<string, unknown> } } & Record<string, unknown> };
  type Pre = { sections: { report: { checks: { code: string; status: string; message: string }[] } }[] };
  const check = (pre: Pre, code: string) => pre.sections.flatMap((s) => s.report.checks).find((c) => c.code === code);

  await signIn(page);
  await page.goto(`/p/${project.slug}/pipelines/new?draft=${p.id}&step=sink`);
  const part = page.getByRole('group', { name: 'Partition shop.orders' });
  // Without fields the table gets the automatic default.
  await expect(part.getByText('Automatic: 16 buckets of id')).toBeVisible();
  await part.getByRole('button', { name: 'Add a partition field' }).click();
  await part.getByLabel('shop.orders field 1 column').click();
  // The time of the change is a Debezium column, but it changes with every update: not a key.
  const changed = page.getByRole('listbox').getByRole('option', { name: '__source_ts_ms (time of the change)' });
  await expect(changed).toHaveAttribute('aria-disabled', 'true');
  await expect(page.getByRole('listbox').getByRole('option', { name: 'created_at', exact: true })).toHaveAttribute('title', /Not part of the key/);
  await page.getByRole('listbox').getByRole('option', { name: 'id', exact: true }).click();
  await choose(part.getByLabel('shop.orders field 1 transform'), 'Buckets of a hash');
  await part.getByLabel('shop.orders field 1 buckets').fill('8');
  await page.getByRole('button', { name: 'Continue to transform →' }).click();
  await expect(page.getByRole('heading', { name: 'How should events look?' })).toBeVisible();
  expect((await api.get<Saved>(url)).spec.sink.options.partition_by).toEqual({ 'shop.orders': [{ n: 8, column: 'id', transform: 'bucket' }] });
  await walkWizard(page, api, { projectId: project.id, slug: project.slug, pipelineId: p.id });

  const pre = await api.send<Pre>('POST', `${url}/preflight`, {});
  for (const code of ['iceberg_partition_column', 'iceberg_partition_key']) expect(check(pre, code)?.status, check(pre, code)?.message).toBe('pass');
  await api.send('POST', `${url}/deploy`, {});
  await waitFor('Debezium Iceberg with Partition by streaming', async () => {
    const cur = await api.get<{ lifecycle: string; status: unknown }>(url);
    if (cur.lifecycle === 'failed') throw new Error(JSON.stringify(cur.status));
    return cur.lifecycle === 'streaming';
  }, 420_000);
  const marker = `PART-${tag}`;
  await sql('INSERT INTO shop.orders (customer_id, status, total) VALUES (1, $1, 1)', [marker]);
  await viaEventsApi({ api, url, marker });
  const [row] = await sql<{ id: number }>('SELECT id FROM shop.orders WHERE status = $1', [marker]);
  await sql('UPDATE shop.orders SET status = $1 WHERE id = $2', [`${marker}-paid`, row!.id]);
  await waitFor('update upserted into its bucket', async () => {
    const e = await api.get<{ records: Rec[] }>(`${url}/events?table=shop.orders&limit=100`);
    const mine = e.records.filter((r) => Number(r.value.id) === row!.id);
    return mine.length === 1 && mine[0]!.value.status === `${marker}-paid`;
  }, 240_000);

  // The build created the table with that spec: the existing-table check now compares it.
  const again = await api.send<Pre>('POST', `${url}/preflight`, {});
  const existing = check(again, 'iceberg_partition_existing');
  expect(existing?.status, existing?.message).toBe('pass');
  expect(check(again, 'iceberg_partition_key')?.status).toBe('pass');
  // E5/E9: the day the order was made is outside the key (blocked), and the table keeps bucket[8](id).
  const cur = await api.get<Saved>(url);
  await api.patch(url, { spec: { ...cur.spec, sink: { ...cur.spec.sink, options: { ...cur.spec.sink.options, partition_by: { 'shop.orders': [{ n: 0, column: 'created_at', transform: 'day' }] } } } } }, cur.version);
  const unsafe = await api.send<Pre>('POST', `${url}/preflight`, {});
  expect(check(unsafe, 'iceberg_partition_existing')?.message).toContain('bucket[8](id)');
  expect(check(unsafe, 'iceberg_partition_existing')?.status).toBe('fail');
  await api.send('DELETE', `${url}?drop_destinations=true`);
  await waitFor('Debezium Iceberg with Partition by deleted', async () => (await api.get<{ lifecycle: string }>(url)).lifecycle === 'deleted', 240_000);
});

test('Postgres → Apache Iceberg (RustFS S3 Tables, preview): a change log', async () => {
  test.setTimeout(900_000);
  const { secret_access_key, ...config } = RUSTFS_TABLES;
  await pipelineTo('iceberg-s3tables', { type: 'iceberg', config, secrets: { secret_access_key } }, viaEventsApi, undefined, {
    naming: icebergNaming,
    options: { write_mode: 'append', commit_interval: 'now' },
  });
});

// Databases through Debezium's JDBC sink; table names take no '-'.
test('Postgres → PostgreSQL (JDBC): upserts, updates and deletes', async () => {
  test.setTimeout(900_000);
  const { password, ...config } = TARGET_PG;
  await pipelineTo(
    'jdbc-pg',
    { type: 'jdbc', config, secrets: { password } },
    async ({ api, url, marker }) => {
      await viaEventsApi({ api, url, marker });
      const [row] = await sql<{ id: number }>('SELECT id FROM shop.orders WHERE status = $1', [marker]);
      const paid = `${marker}-paid`;
      await sql('UPDATE shop.orders SET status = $1 WHERE id = $2', [paid, row!.id]);
      const mine = async () =>
        (await api.get<{ records: Rec[] }>(`${url}/events?table=shop.orders&limit=500`)).records.filter((r) => Number(r.value.id) === row!.id);
      await waitFor('update upserted', async () => {
        const rows = await mine();
        return rows.length === 1 && rows[0]!.value.status === paid;
      }, 240_000);
      await sql('DELETE FROM shop.orders WHERE id = $1', [row!.id]);
      await waitFor('row deleted', async () => (await mine()).length === 0, 240_000);
    },
    undefined,
    { naming: icebergNaming, options: { write_mode: 'upsert', deletes: 'delete' } },
  );
});

test('Postgres → MySQL (JDBC): insert only', async () => {
  test.setTimeout(900_000);
  const { password, ...config } = TARGET_MYSQL;
  // created_at (timestamptz DEFAULT now()) would become an invalid MySQL TIMESTAMP default: a
  // documented limit of Debezium's JDBC sink across databases.
  await pipelineTo('jdbc-mysql', { type: 'jdbc', config, secrets: { password } }, viaEventsApi, undefined, {
    naming: icebergNaming,
    options: { write_mode: 'insert' },
    exclude: ['created_at'],
  });
});

// Amazon SQS and SNS against moto (any access key works).
const AWS_KEYS = { region: MOTO.region, endpoint: MOTO.endpoint, access_key_id: 'test' };

test('Postgres → Amazon SQS (moto): one queue, peeked by the Events tab', async () => {
  test.setTimeout(600_000);
  const queue = `q-${unique('sqs')}`;
  // moto routes by the credential scope of the (unchecked) signature: unsigned requests go to S3.
  const res = await fetch(`${MOTO_FROM_RUNNER}/?Action=CreateQueue&QueueName=${queue}`, {
    method: 'POST',
    headers: { authorization: 'AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/sqs/aws4_request, SignedHeaders=host, Signature=0' },
  });
  expect(res.ok).toBe(true);
  await pipelineTo('sqs', { type: 'sqs', config: { ...AWS_KEYS, queue_url: `${MOTO.endpoint}/123456789012/${queue}` }, secrets: { secret_access_key: 'test' } }, viaEventsApi);
});

test('Postgres → Amazon SNS (moto): a topic per table, read through the preview queue', async () => {
  test.setTimeout(600_000);
  await pipelineTo('sns', { type: 'sns', config: { ...AWS_KEYS, account_id: '123456789012', create_topics: true }, secrets: { secret_access_key: 'test' } }, viaEventsApi);
});

test('Postgres → Azure Event Hubs (emulator): one hub, read back', async () => {
  test.setTimeout(600_000);
  const { connection_string, ...config } = EVENTHUBS;
  await pipelineTo('eventhubs', { type: 'eventhubs', config, secrets: { connection_string } }, viaEventsApi);
});

test('Postgres → Apache Pulsar (standalone): a topic per table, read through the admin API', async () => {
  test.setTimeout(600_000);
  await pipelineTo('pulsar', { type: 'pulsar', config: { ...PULSAR, create_topics: true } }, viaEventsApi);
});
