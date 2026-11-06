import { describe, expect, it } from 'vitest';
import { cleanPartitionBy, partitionError } from '../PartitionBy';

describe('partition by', () => {
  it('refuses what the server refuses (T19)', () => {
    expect(partitionError({ 'shop.orders': [{ n: 0, column: '', transform: 'identity' }] })).toMatch(/pick a column/);
    expect(partitionError({ 'shop.orders': [{ n: 1025, column: 'id', transform: 'bucket' }] })).toMatch(/1 to 1024/);
    expect(partitionError({ 'shop.orders': [{ n: 0, column: 'sku', transform: 'truncate' }] })).toMatch(/at least 1/);
    expect(partitionError({ 'shop.orders': [{ n: 0, column: 'id', transform: 'identity' }, { n: 0, column: 'id', transform: 'identity' }] })).toMatch(/twice/);
    expect(partitionError({ 'shop.orders': [{ n: 16, column: 'id', transform: 'bucket' }] })).toBeNull();
  });
  it('stores as the server dumps it: no empty tables, keys in JSONB order, no stray numbers', () => {
    const out = cleanPartitionBy({ 'shop.order_items': [{ n: 9, column: 'sku', transform: 'day' }], 'shop.x': [], 'shop.orders': [{ n: 4, column: 'id', transform: 'bucket' }] });
    expect(JSON.stringify(out)).toBe('{"shop.orders":[{"n":4,"column":"id","transform":"bucket"}],"shop.order_items":[{"n":0,"column":"sku","transform":"day"}]}');
  });
});
