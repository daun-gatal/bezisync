import { ago, formatBytes, formatCount, formatDuration, slugify } from '../format';

describe('format helpers', () => {
  it('formats relative times', () => {
    const now = Date.parse('2026-10-26T10:00:00Z');
    expect(ago('2026-10-26T09:59:56Z', now)).toBe('4 s ago');
    expect(ago('2026-10-26T09:57:00Z', now)).toBe('3 min ago');
    expect(ago(null, now)).toBe('never');
    expect(ago('garbage', now)).toBe('unknown');
  });
  it('formats bytes, counts and durations', () => {
    expect(formatBytes(48 * 1024 * 1024)).toBe('48 MB');
    expect(formatBytes(18.4 * 1024 ** 3)).toBe('18 GB');
    expect(formatBytes(1536)).toBe('1.5 KB');
    expect(formatCount(1_210_000)).toBe('1.2 M');
    expect(formatCount(480_000)).toBe('480 k');
    expect(formatDuration(1200)).toBe('1.2 s');
    expect(formatDuration(42_000)).toBe('42 s');
    expect(formatDuration(-1)).toBe('—');
  });
  it('slugifies names', () => {
    expect(slugify('Orders → Kafka (prod)')).toBe('orders-kafka-prod');
  });
});
