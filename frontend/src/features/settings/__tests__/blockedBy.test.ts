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
