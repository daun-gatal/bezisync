import { blockedBy } from '../PoliciesSection';
import type { Pipeline } from '@/lib/pipelines';
import type { Policies } from '@/lib/policies';

const pipeline = (advanced: Record<string, string>) =>
  ({
    id: 'p1',
    source: { id: 's', type: 'postgresql', name: 'pg' },
    sink: { id: 'k', type: 'kafka', name: 'events' },
    runtime: null,
    spec: { source: { tables: { include: ['shop.orders'] } }, advanced },
  }) as unknown as Pipeline;

const policies = (extra: Partial<Policies> = {}) => ({ allow_force_deploy: true, require_approval: false, ...extra }) as Policies;

describe('blockedBy', () => {
  it('lists advanced properties when the project stops allowing them', () => {
    expect(blockedBy(pipeline({ 'debezium.source.poll.interval.ms': '250' }), policies(), () => '')).toContain('advanced Debezium properties');
    expect(blockedBy(pipeline({ 'debezium.source.poll.interval.ms': '250' }), policies({ allow_advanced_properties: true }), () => '')).not.toContain(
      'advanced Debezium properties',
    );
    expect(blockedBy(pipeline({}), policies(), () => '')).not.toContain('advanced Debezium properties');
  });
});

describe('blockedBy custom sizes', () => {
  it('lists a custom size above the largest size the project allows', () => {
    const custom = (cpu: string, memory: string) =>
      ({ ...pipeline({}), spec: { source: { tables: { include: ['shop.orders'] } }, policies: { resources: 'custom', custom_resources: { cpu, memory } } } }) as unknown as Pipeline;
    expect(blockedBy(custom('2', '1Gi'), policies({ max_resources: 'medium' }), () => '')).toContain('a custom size (2 CPU, 1Gi)');
    expect(blockedBy(custom('1', '1536Mi'), policies({ max_resources: 'medium' }), () => '')).toEqual([]);
  });
});
