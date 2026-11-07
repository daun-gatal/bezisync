import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import type { FullSpec } from '../../spec';
import { specForEngine } from '../../spec';
import {
  automaticSummary,
  cleanPartitionBy,
  columnRefusal,
  equalityFields,
  PARTITION_TRANSFORMS,
  partitionByFor,
  partitionError,
  partitionFits,
  partitionKind,
  partitionSummary,
  withPartitionBy,
  writtenByTable,
  writtenColumns,
  type PartitionTransform,
} from '../PartitionBy';

// The vectors domain.PartitionKind and domain.PartitionTransforms are tested against too.
const vectors = JSON.parse(readFileSync(resolve(__dirname, '../../../../../../backend/testdata/partition_kinds.json'), 'utf8')) as {
  kinds: { engine: 'debezium' | 'flink-cdc'; source: string; type: string; decimals?: 'precise' | 'double' | 'string'; binary?: 'bytes' | 'base64' | 'hex'; kind: string }[];
  fits: Record<PartitionTransform, string[]>;
};

const spec = (over: Partial<FullSpec> = {}) =>
  ({ source: { tables: { include: ['shop.orders'] }, columns: {} }, transforms: [], keys: {}, ...over }) as Pick<FullSpec, 'source' | 'transforms' | 'keys' | 'values'>;
const orders = {
  schema: 'shop',
  name: 'orders',
  primary_key: ['id'],
  columns: [
    { name: 'id', type: 'integer' },
    { name: 'tenant_id', type: 'integer' },
    { name: 'created_at', type: 'timestamp with time zone' },
    { name: 'flag', type: 'boolean' },
  ],
};

describe('partition by', () => {
  it('matches the server on written kinds and transforms (shared vectors)', () => {
    for (const v of vectors.kinds) {
      expect(partitionKind(v.engine, v.source, v.type, { decimals: v.decimals, binary: v.binary }), `${v.engine} ${v.source} ${v.type}`).toBe(v.kind);
    }
    expect(PARTITION_TRANSFORMS).toEqual(vectors.fits);
    expect(partitionFits('bucket', 'bool')).toBe(false);
    expect(partitionFits('bucket', 'float')).toBe(false);
    expect(partitionFits('truncate', 'uuid')).toBe(false);
  });
  it('refuses what the server refuses (T19)', () => {
    expect(partitionError({ 'shop.orders': [{ n: 0, column: '', transform: 'identity' }] })).toMatch(/pick a column/);
    expect(partitionError({ 'shop.orders': [{ n: 1025, column: 'id', transform: 'bucket' }] })).toMatch(/1 to 1024/);
    expect(partitionError({ 'shop.orders': [{ n: 0, column: 'sku', transform: 'truncate' }] })).toMatch(/from 1 to 1,000,000/);
    expect(partitionError({ 'shop.orders': [{ n: 1_000_001, column: 'sku', transform: 'truncate' }] })).toMatch(/from 1 to 1,000,000/);
    expect(partitionError({ 'shop.orders': [{ n: 100_000, column: 'id', transform: 'truncate' }] })).toBeNull();
    expect(partitionError({ 'shop.orders': [{ n: 0, column: 'id', transform: 'identity' }, { n: 0, column: 'id', transform: 'identity' }] })).toMatch(/twice/);
    expect(partitionError({ 'shop.orders': [{ n: 0, column: 'ts', transform: 'day' }, { n: 0, column: 'ts', transform: 'hour' }] })).toMatch(/one time transform/);
    expect(partitionError({ 'shop.orders': [{ n: 16, column: 'id', transform: 'bucket' }] })).toBeNull();
  });
  it('stores as the server dumps it: no empty tables, keys in JSONB order, no stray numbers', () => {
    const out = cleanPartitionBy({ 'shop.order_items': [{ n: 9, column: 'sku', transform: 'day' }], 'shop.x': [], 'shop.orders': [{ n: 4, column: 'id', transform: 'bucket' }] });
    expect(JSON.stringify(out)).toBe('{"shop.orders":[{"n":4,"column":"id","transform":"bucket"}],"shop.order_items":[{"n":0,"column":"sku","transform":"day"}]}');
  });
});

describe('written columns and keys', () => {
  it('Debezium: the time of the change, its own fields, and key columns kept under their names', () => {
    const cols = writtenColumns(orders, spec(), 'debezium', 'postgresql', 'upsert');
    expect(cols.find((c) => c.name === '__source_ts_ms')).toEqual({ name: '__source_ts_ms', kind: 'timestamptz', key: false });
    expect(cols.find((c) => c.name === 'id')?.key).toBe(true);
    // A rename changes the row, not the key: the renamed key column drops out of the identity.
    const renamed = spec({ transforms: [{ type: 'rename', renames: [{ from: 'id', to: 'order_id' }] }] });
    expect(equalityFields(orders, renamed, 'debezium', 'upsert')).toEqual([]);
    expect(equalityFields(orders, spec(), 'debezium', 'append')).toEqual([]);
    expect(equalityFields(orders, spec({ keys: { 'shop.orders': ['nope'] } }), 'debezium', 'upsert')).toEqual(['id']);
    expect(equalityFields(orders, spec({ keys: { 'shop.orders': ['tenant_id', 'id'] } }), 'debezium', 'upsert')).toEqual(['tenant_id', 'id']);
  });
  it('Flink CDC: no time of the change, the key under its written name, one map of renames', () => {
    const renamed = spec({ transforms: [{ type: 'rename', renames: [{ from: 'id', to: 'b' }] }, { type: 'rename', renames: [{ from: 'b', to: 'c' }] }] });
    const cols = writtenColumns(orders, renamed, 'flink-cdc', 'postgresql', 'upsert');
    expect(cols.map((c) => c.name)).toContain('b');
    expect(cols.map((c) => c.name)).not.toContain('__source_ts_ms');
    expect(cols.find((c) => c.name === 'b')?.key).toBe(true);
    // Debezium chains them.
    expect(writtenColumns(orders, renamed, 'debezium', 'postgresql', 'upsert').map((c) => c.name)).toContain('c');
  });
  it('refuses non-key columns on an upserted table, with the reason', () => {
    const w = writtenByTable(['shop.orders'], [orders], spec(), 'flink-cdc', 'postgresql', 'upsert');
    const created = w['shop.orders']!.columns.find((c) => c.name === 'created_at')!;
    expect(columnRefusal(created, w['shop.orders']!.keys)).toMatch(/Not part of the key/);
    expect(partitionError({ 'shop.orders': [{ n: 0, column: 'created_at', transform: 'day' }] }, w)).toMatch(/Not part of the key/);
    expect(partitionError({ 'shop.orders': [{ n: 8, column: 'id', transform: 'bucket' }] }, w)).toBeNull();
    expect(partitionError({ 'shop.orders': [{ n: 4, column: 'flag', transform: 'bucket' }] }, writtenByTable(['shop.orders'], [orders], spec(), 'debezium', 'postgresql', 'append'))).toMatch(/doesn’t fit flag/);
    // A change log is appended: any column.
    const log = writtenByTable(['shop.orders'], [orders], spec(), 'debezium', 'postgresql', 'append');
    expect(partitionError({ 'shop.orders': [{ n: 0, column: '__source_ts_ms', transform: 'day' }, { n: 0, column: 'created_at', transform: 'month' }] }, log)).toBeNull();
    // X7: a column renamed away later is named.
    const later = writtenByTable(['shop.orders'], [orders], spec({ transforms: [{ type: 'rename', renames: [{ from: 'id', to: 'order_id' }] }] }), 'flink-cdc', 'postgresql', 'upsert');
    expect(partitionError({ 'shop.orders': [{ n: 8, column: 'id', transform: 'bucket' }] }, later)).toMatch(/id is no longer a column/);
  });
  it('says what an unlisted Debezium table gets, routed tables sharing their destination', () => {
    const none = new Map<string, string>();
    const one = ['shop.orders'];
    expect(automaticSummary('debezium', { partitions: { 'shop.orders': 'id' }, buckets: '64' }, 'shop.orders', one, none, {})).toBe('Automatic: 64 buckets of id');
    expect(automaticSummary('debezium', { partitions: { 'shop.orders': '' } }, 'shop.orders', one, none, {})).toBe('Automatic: by the day of the change');
    expect(automaticSummary('debezium', { write_mode: 'append' }, 'shop.orders', one, none, {})).toBe('Automatic: by the day of the change');
    expect(automaticSummary('debezium', { partitioning: 'none', partitions: { 'shop.orders': 'id' } }, 'shop.orders', one, none, {})).toBe('Not partitioned');
    expect(automaticSummary('flink-cdc', {}, 'shop.orders', one, none, {})).toBeNull();
    const two = ['shop.orders', 'shop.archive'];
    const merged = new Map([['shop.orders', 'all'], ['shop.archive', 'all']]);
    expect(automaticSummary('debezium', { partitions: { 'shop.orders': 'id', 'shop.archive': 'id' } }, 'shop.orders', two, merged, {})).toBe('Automatic: 16 buckets of id');
    expect(automaticSummary('debezium', { partitions: { 'shop.orders': 'id', 'shop.archive': '' } }, 'shop.orders', two, merged, {})).toBe('Not partitioned (the tables routed together differ)');
    expect(automaticSummary('debezium', { partitions: { 'shop.orders': 'id' } }, 'shop.orders', two, merged, { 'shop.archive': [{ n: 8, column: 'id', transform: 'bucket' }] })).toBe('Partitioned as shop.archive (routed together)');
  });
  it('X10: switching to Flink CDC drops the time of the change', () => {
    const by = { 'shop.orders': [{ n: 0, column: '__source_ts_ms', transform: 'day' as const }], 'shop.items': [{ n: 8, column: 'id', transform: 'bucket' as const }] };
    expect(partitionByFor(by, 'flink-cdc')).toEqual({ 'shop.items': by['shop.items'] });
    expect(partitionByFor(by, 'debezium')).toBe(by);
    const full = { source: { tables: { include: ['shop.orders'] }, columns: {} }, sink: { naming: '', format: {}, options: { write_mode: 'upsert', partition_by: by } }, snapshot: {}, transforms: [], policies: {}, advanced: {} } as unknown as FullSpec;
    expect(specForEngine(full, 'flink-cdc').sink.options).toEqual({ write_mode: 'upsert', partition_by: { 'shop.items': by['shop.items'] } });
    expect(specForEngine(full, 'debezium').sink.options.partition_by).toBe(by);
  });
});

describe('Partition by saved from the Transform step (ADR 0007)', () => {
  const bucket = { n: 16, column: 'id', transform: 'bucket' as const };
  it('P-1: an unchanged Partition by returns the same options object (no save)', () => {
    const options = { write_mode: 'upsert', partition_by: { 'shop.orders': [bucket] } };
    expect(withPartitionBy(options, { 'shop.orders': [bucket], 'shop.empty': [] }, 'flink-cdc')).toBe(options);
    const none = { write_mode: 'upsert' };
    expect(withPartitionBy(none, {}, 'debezium')).toBe(none);
  });
  it('stores what Destination stored: no empty tables, JSONB order, no time of the change on Flink CDC', () => {
    const out = withPartitionBy({ write_mode: 'upsert', format_version: '2' }, {
      'shop.orders': [{ n: 9, column: '__source_ts_ms', transform: 'day' }, { n: 7, column: 'id', transform: 'identity' }],
      'shop.items': [],
    }, 'flink-cdc');
    expect(JSON.stringify(out)).toBe(JSON.stringify({ write_mode: 'upsert', partition_by: { 'shop.orders': [{ n: 0, column: 'id', transform: 'identity' }] }, format_version: '2' }));
    expect(withPartitionBy({ write_mode: 'upsert', partition_by: { 'shop.orders': [bucket] } }, {}, 'debezium')).toEqual({ write_mode: 'upsert' });
  });
  it('P-14: summarises in Guided words, counts past two tables', () => {
    const tables = Array.from({ length: 30 }, (_, i) => `shop.t${i}`);
    expect(partitionSummary({}, tables, 'flink-cdc', {})).toBe('Not partitioned');
    expect(partitionSummary({}, tables, 'debezium', {})).toBe('Automatic');
    expect(partitionSummary({}, tables, 'debezium', { partitioning: 'none' })).toBe('Not partitioned');
    expect(partitionSummary({ 'shop.t0': [bucket], 'shop.t1': [{ n: 0, column: 'region', transform: 'identity' }] }, ['shop.t0', 'shop.t1'], 'flink-cdc', {}))
      .toBe('shop.t0: Buckets of id (16) · shop.t1: Each value of region');
    expect(partitionSummary({ 'shop.t0': [bucket] }, tables, 'debezium', {})).toBe('shop.t0: Buckets of id (16) · Automatic for the rest');
    const three = { 'shop.t0': [bucket], 'shop.t1': [bucket], 'shop.t2': [bucket] };
    expect(partitionSummary(three, tables, 'flink-cdc', {})).toBe('3 tables partitioned · 27 not');
    expect(partitionSummary(three, tables, 'debezium', {})).toBe('3 tables partitioned · Automatic for the rest');
  });
});
