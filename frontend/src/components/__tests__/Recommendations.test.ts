import { describe, expect, it } from 'vitest';
import { freshRecommendations } from '../Recommendations';
import type { Setting } from '@/lib/settings';

const threads = { key: 'debezium.source.snapshot.max.threads', title: 'Snapshot threads', type: 'int' } as Setting;

describe('freshRecommendations', () => {
  const now = Date.parse('2026-11-06T12:00:00Z');
  const rec = (at: string, key = threads.key) => ({ key, value: '3', why: 'big tables', evidence_at: at });
  it('shows fresh ones for settings the pipeline can use, once', () => {
    expect(freshRecommendations([rec('2026-11-06T11:55:00Z')], [threads], {}, now)).toHaveLength(1);
  });
  it('hides stale evidence (C13), settings it can’t use, and choices already made', () => {
    expect(freshRecommendations([rec('2026-11-06T11:49:00Z')], [threads], {}, now)).toEqual([]);
    expect(freshRecommendations([rec('2026-11-06T11:55:00Z', 'debezium.source.other')], [threads], {}, now)).toEqual([]);
    expect(freshRecommendations([rec('2026-11-06T11:55:00Z')], [threads], { [threads.key]: '2' }, now)).toEqual([]);
    expect(freshRecommendations(undefined, [threads], {}, now)).toEqual([]);
  });
});
