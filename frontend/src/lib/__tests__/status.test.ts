import type { Pipeline } from '../pipelines';
import { fleetGroup, needsAttention, pipelineTone } from '../status';

const fresh = new Date().toISOString();
const old = new Date(Date.now() - 10 * 60_000).toISOString();

function p(lifecycle: string, health: string, asOf: string | null = fresh, deployed = 1): Pipeline {
  return { lifecycle, health, deployed_version: deployed, status: asOf ? { as_of: asOf } : {} } as unknown as Pipeline;
}

describe('fleet health: every pipeline is in exactly one group', () => {
  it.each([
    [p('draft', 'unknown', null, 0), 'draft'],
    [p('paused', 'unknown'), 'paused'],
    [p('failed', 'down'), 'down'],
    [p('cleanup_failed', 'unknown'), 'down'],
    [p('streaming', 'down'), 'down'],
    [p('deploying', 'unknown'), 'starting'],
    [p('deleting', 'unknown'), 'starting'],
    [p('streaming', 'healthy', old), 'nosignal'],
    [p('streaming', 'unknown'), 'nosignal'],
    [p('streaming', 'lagging'), 'lagging'],
    [p('snapshotting', 'at_risk'), 'lagging'],
    [p('streaming', 'healthy'), 'healthy'],
  ] as const)('%#: %o → %s', (pipeline, group) => {
    expect(fleetGroup(pipeline)).toBe(group);
  });
});

describe('row stripes mark only what needs attention', () => {
  it.each([
    [p('failed', 'down'), 'bad'],
    [p('streaming', 'at_risk'), 'risk'],
    [p('streaming', 'lagging'), 'warn'],
    [p('streaming', 'healthy', old), 'grey'],
    [p('streaming', 'healthy'), null],
    [p('draft', 'unknown', null, 0), null],
    [p('paused', 'unknown', old), null],
  ] as const)('%#: → %s', (pipeline, tone) => {
    expect(pipelineTone(pipeline)).toBe(tone);
    expect(needsAttention(pipeline)).toBe(tone !== null);
  });
});
