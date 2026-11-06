import { DEFAULT_SINK_CAPS, type SinkCaps } from '@/lib/connections';
import { deriveSinkOptions, destinationFor, fullSpec, namingError, optionsSchemaFor, parseAdvanced, pruneToTables, sinkDestinationFor, sinkNamingError, specForEngine, stageOf, withShape } from '../spec';

describe('pipeline spec helpers', () => {
  it('fills defaults that mirror the backend', () => {
    const s = fullSpec({ source: { tables: { include: ['a.b'] } } } as never);
    expect(s.sink.naming).toBe('cdc.{schema}.{table}');
    expect(s.snapshot.mode).toBe('initial');
    expect(s.transforms[0]?.type).toBe('unwrap');
    expect(s.policies.resources).toBe('small');
  });
  it('offers Flink CDC only the Iceberg options it writes, labelled as it writes them', () => {
    const schema = {
      type: 'object',
      properties: {
        write_mode: { type: 'string', enum: ['upsert_keep_deletes', 'upsert', 'append'], ui: { labels: { upsert_keep_deletes: 'Latest row; deletes kept with __deleted = true', upsert: 'Latest row; deletes removed' } } },
        commit_interval: { type: 'string' },
      },
    } as never;
    expect(optionsSchemaFor(schema, 'debezium')).toBe(schema);
    const flink = optionsSchemaFor(schema, 'flink-cdc');
    expect(Object.keys(flink?.properties ?? {})).toEqual(['write_mode']);
    expect(flink?.properties?.write_mode?.enum).toEqual(['upsert_keep_deletes', 'upsert']);
    expect(flink?.properties?.write_mode?.ui?.labels).toEqual({ upsert_keep_deletes: "Latest row; deletes kept with __op = '-D'", upsert: 'Latest row; deletes removed' });
  });
  it('previews destinations', () => {
    expect(destinationFor('cdc.{schema}.{table}', 'shop.orders', 'p')).toBe('cdc.shop.orders');
    expect(destinationFor('{pipeline}-{table}', 'shop.orders', 'orders-cdc')).toBe('orders-cdc-orders');
  });
  it('validates naming templates', () => {
    expect(namingError('cdc.{schema}.{table}')).toBeNull();
    expect(namingError('static')).toMatch(/\{table\}/);
    expect(namingError('x.{tbl}.{table}')).toMatch(/Unknown placeholder/);
    expect(namingError('a b {table}')).toMatch(/letters/);
  });
  it('applies the destination’s naming rules', () => {
    const sns: SinkCaps = { ...DEFAULT_SINK_CAPS, naming: { ...DEFAULT_SINK_CAPS.naming, pattern: '^[A-Za-z0-9_-]+$', max_length: 16, message: 'No dots.' } };
    expect(sinkNamingError('cdc.{schema}.{table}', sns, ['shop.orders'], 'p')).toBe('No dots.');
    expect(sinkNamingError('cdc_{schema}_{table}', sns, ['shop.orders'], 'p')).toBeNull();
    expect(sinkNamingError('cdc_{schema}_{table}', sns, ['shop.order_items_archive'], 'p')).toMatch(/longer than 16/);
    const lake: SinkCaps = { ...DEFAULT_SINK_CAPS, naming: { ...DEFAULT_SINK_CAPS.naming, pattern: '^[a-z0-9_]+$', flatten_dots: true } };
    expect(sinkDestinationFor('{schema}.{table}', 'shop.orders', 'p', lake)).toBe('shop_orders');
    expect(sinkNamingError('{schema}.{table}', lake, ['shop.orders'], 'p')).toBeNull();
    const queue: SinkCaps = { ...DEFAULT_SINK_CAPS, naming: { ...DEFAULT_SINK_CAPS.naming, single: true, pattern: '^$' } };
    expect(sinkNamingError('cdc.{schema}.{table}', queue, ['shop.orders'], 'p')).toBeNull();
  });
  it('derives table keys for the destination', () => {
    const schema = { properties: { write_mode: { type: 'string' }, partitions: { type: 'object', ui: { hidden: true, from: 'table_keys' } } } };
    const discovered = [
      { schema: 'shop', name: 'orders', primary_key: ['id', 'region'] },
      { schema: 'shop', name: 'events', primary_key: [] },
    ];
    const keys = { 'shop.events': ['event_id'] };
    expect(deriveSinkOptions({ write_mode: 'upsert' }, schema, ['shop.orders', 'shop.events', 'shop.gone'], keys, discovered)).toEqual({
      write_mode: 'upsert',
      partitions: { 'shop.orders': 'id', 'shop.events': 'event_id' },
    });
    expect(deriveSinkOptions({ a: 1 }, schema, ['shop.orders'], {}, undefined)).toEqual({ a: 1 });
    expect(deriveSinkOptions({}, schema, ['shop.events'], {}, discovered)).toEqual({ partitions: { 'shop.events': '' } });
  });
  it('parses advanced properties', () => {
    expect(parseAdvanced('# c\na.b=1\n\nc = x=y')).toEqual({ values: { 'a.b': '1', c: 'x=y' }, error: null });
    expect(parseAdvanced('nope').error).toMatch(/Line 1/);
  });
});

describe('fullSpec keeps what it does not edit', () => {
  it('preserves settings made through YAML or the API', () => {
    const spec = fullSpec({
      source: { tables: { include: ['shop.orders'] } },
      policies: { source_retained_limit_gb: 42, delivery_canary: true },
      debezium_image: 'registry/debezium:custom',
    } as never);
    expect(spec.policies.source_retained_limit_gb).toBe(42);
    expect(spec.policies.delivery_canary).toBe(true);
    expect(spec.debezium_image).toBe('registry/debezium:custom');
    expect(fullSpec(null).policies.delivery_canary).toBe(false);
  });
});

describe('withShape keeps the stage order', () => {
  const filter = { type: 'filter' as const, id: 'k7f2', operations: ['insert' as const] };
  const rename = { type: 'rename' as const, id: 'r9c1', renames: [{ from: 'a', to: 'b' }] };
  const unwrap = { type: 'unwrap' as const, delete_handling: 'drop' as const, add_fields: [], condition: null };
  it('adds the shape after keep-events filters', () => {
    expect(withShape([filter], unwrap).map((t) => t.type)).toEqual(['filter', 'unwrap']);
    expect(withShape([], unwrap).map((t) => t.type)).toEqual(['unwrap']);
    expect(withShape([{ type: 'outbox' }], unwrap).map((t) => t.type)).toEqual(['unwrap', 'outbox']);
  });
  it('replaces an existing shape in place and keeps its id', () => {
    const steps = withShape([filter, { type: 'unwrap', id: 'u1a0', delete_handling: 'rewrite' }, rename], unwrap);
    expect(steps.map((t) => t.type)).toEqual(['filter', 'unwrap', 'rename']);
    expect(steps[1]).toMatchObject({ id: 'u1a0', delete_handling: 'drop' });
  });
  it('removes the shape', () => {
    expect(withShape([filter, unwrap], null)).toEqual([filter]);
  });
  it('knows each stage', () => {
    expect([filter, unwrap, rename, { type: 'filter' as const, condition: 'x' }, { type: 'outbox' as const }].map(stageOf)).toEqual([0, 1, 2, -1, -1]);
  });
});

describe('pruneToTables', () => {
  it('drops references to tables no longer captured', () => {
    const spec = fullSpec({
      source: { tables: { include: ['a.x', 'a.y'] } },
      transforms: [
        { type: 'unwrap' },
        { type: 'rename', tables: ['a.x', 'a.y'], renames: [{ from: 'p', to: 'q' }] },
        { type: 'flatten', tables: ['a.y'] },
        { type: 'add_field', name: 'n', value: { kind: 'pipeline' } },
      ],
      keys: { 'a.y': ['id'], 'a.x': ['id'] },
    } as never);
    const out = pruneToTables(spec, ['a.x']);
    expect(out.transforms.map((t) => [t.type, t.tables])).toEqual([['unwrap', undefined], ['rename', ['a.x']], ['add_field', undefined]]);
    expect(out.keys).toEqual({ 'a.x': ['id'] });
    expect(pruneToTables(spec, ['a.z']).keys).toBeUndefined();
  });
});

describe('specForEngine', () => {
  const base = fullSpec({
    source: { tables: { include: ['s.t'] } },
    advanced: { 'debezium.source.x': '1' },
    values: { decimals: 'string' },
    debezium_image: 'quay.io/debezium/server:3.7.0.Final',
    snapshot: { mode: 'initial', delay_seconds: 30, window: '01:00-02:00' },
    flink_cdc: { new_columns: 'send' },
    tuning: { 'debezium.source.snapshot.max.threads': '4', 'source.connection.pool.size': '8' },
  } as never);
  it('keeps each engine’s own typed settings, and none when none are left', () => {
    expect(specForEngine(base, 'flink-cdc').tuning).toEqual({ 'source.connection.pool.size': '8' });
    expect(specForEngine(base, 'debezium').tuning).toEqual({ 'debezium.source.snapshot.max.threads': '4' });
    const noFlink = fullSpec({ source: { tables: { include: ['s.t'] } }, tuning: { 'debezium.source.poll.interval.ms': '250' } } as never);
    expect('tuning' in specForEngine(noFlink, 'flink-cdc')).toBe(false);
    expect('tuning' in specForEngine(fullSpec({ source: { tables: { include: ['s.t'] } } } as never), 'debezium')).toBe(false);
  });
  it('keeps only Flink CDC settings on Flink CDC', () => {
    const s = specForEngine(base, 'flink-cdc');
    expect(s.advanced).toEqual({});
    expect(s.values).toBeUndefined();
    expect(s.debezium_image).toBeNull();
    expect(s.snapshot.delay_seconds).toBe(0);
    expect(s.snapshot.window).toBeNull();
    expect(s.flink_cdc).toEqual({ new_columns: 'send' });
  });
  it('keeps only Debezium settings on Debezium', () => {
    const s = specForEngine(base, 'debezium');
    expect(s.flink_cdc).toBeUndefined();
    expect(s.advanced).toEqual({ 'debezium.source.x': '1' });
    expect(s.snapshot.window).toBe('01:00-02:00');
  });
});

describe('custom sizes', () => {
  it('offers CPU and memory the way the server stores them, and works out the heap like it', async () => {
    const { CUSTOM_CPUS, CUSTOM_MEMORIES, heapMiB, memoryMiB } = await import('../spec');
    expect(CUSTOM_CPUS.slice(0, 5)).toEqual(['0.25', '0.5', '0.75', '1', '1.25']);
    expect(CUSTOM_CPUS.at(-1)).toBe('8');
    expect(CUSTOM_MEMORIES[0]).toBe('512Mi');
    expect(CUSTOM_MEMORIES).toContain('2Gi');
    expect(CUSTOM_MEMORIES).not.toContain('2048Mi');
    expect(CUSTOM_MEMORIES.at(-1)).toBe('16Gi');
    expect(memoryMiB('1536Mi')).toBe(1536);
    expect(heapMiB(2048)).toBe(1433);
    expect(heapMiB(512)).toBe(256);
  });
});
