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

describe('blockedBy resource limits', () => {
  const withFootprint = (footprint: { cpu: number; memory_mib: number; readers: number }, engine = 'debezium', resources = 'medium') =>
    ({ ...pipeline({}), engine, footprint: { ...footprint, custom: resources === 'custom' }, spec: { source: { tables: { include: ['shop.orders'] } }, policies: { resources } } }) as unknown as Pipeline;
  it('counts the server’s peak footprint against each limit', () => {
    const p = withFootprint({ cpu: 2.5, memory_mib: 5120, readers: 4 }, 'flink-cdc');
    expect(blockedBy(p, policies({ resource_limits: { max_cpu: '2' } }), () => '')).toEqual(['2.5 CPU at its peak']);
    expect(blockedBy(p, policies({ resource_limits: { max_memory: '4Gi' } }), () => '')).toEqual(['5120 MiB at its peak']);
    expect(blockedBy(p, policies({ resource_limits: { max_parallelism: 2 } }), () => '')).toEqual(['4 readers']);
    expect(blockedBy(p, policies({ resource_limits: { max_cpu: '2.5', max_memory: '5Gi', max_parallelism: 4 } }), () => '')).toEqual([]);
    // Without a footprint (no spec in the list) nothing is guessed.
    expect(blockedBy({ ...p, footprint: undefined } as Pipeline, policies({ resource_limits: { max_cpu: '0.5' } }), () => '')).toEqual([]);
  });
  it('holds a custom Flink CDC cluster to the Flink preset max_resources names', () => {
    const sizes = { medium: { jobmanager_cpu: '500m', jobmanager_memory: '1Gi', taskmanagers: 2, taskmanager_cpu: '1', taskmanager_memory: '2Gi', parallelism: 4, total_memory_mib: 5120 } };
    const big = withFootprint({ cpu: 9, memory_mib: 17920, readers: 8 }, 'flink-cdc', 'custom');
    expect(blockedBy(big, policies({ max_resources: 'medium' }), () => '', sizes)).toEqual(['a custom cluster']);
    const small = withFootprint({ cpu: 2.5, memory_mib: 5120, readers: 4 }, 'flink-cdc', 'custom');
    // From the list (no spec): the footprint says it is custom.
    expect(blockedBy({ ...big, spec: undefined } as unknown as Pipeline, policies({ max_resources: 'medium' }), () => '', sizes)).toEqual(['a custom cluster']);
    expect(blockedBy(small, policies({ max_resources: 'medium' }), () => '', sizes)).toEqual([]);
  });
});
