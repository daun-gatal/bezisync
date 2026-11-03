import type { Pipeline } from '@/lib/pipelines';
import { stepState } from '../wizard/shared';

function draft(tables: string[], sink: boolean, naming = '{schema}.{table}'): Pipeline {
  return {
    id: 'p',
    sink: sink ? { id: 's', name: 'k', type: 'kafka' } : null,
    spec: { source: { tables: { include: tables } }, sink: { naming } },
  } as unknown as Pipeline;
}

describe('wizard steps unlock in order', () => {
  it('before a draft exists, only Source is open', () => {
    expect(stepState('source', undefined)).toEqual({ done: false, locked: null });
    for (const s of ['tables', 'sink', 'transform', 'review'] as const) expect(stepState(s, undefined).locked).toBe('Choose a source first.');
  });

  it('a draft without tables opens Tables only', () => {
    const p = draft([], false);
    expect(stepState('source', p).done).toBe(true);
    expect(stepState('tables', p)).toEqual({ done: false, locked: null });
    expect(stepState('sink', p).locked).toBe('Choose tables first.');
    expect(stepState('review', p).locked).toBe('Choose tables first.');
  });

  it('tables chosen: Destination opens; Transform and Review wait for it', () => {
    const p = draft(['shop.orders'], false);
    expect(stepState('tables', p).done).toBe(true);
    expect(stepState('sink', p).locked).toBeNull();
    expect(stepState('transform', p).locked).toBe('Choose a destination first.');
    expect(stepState('review', p).locked).toBe('Choose a destination first.');
  });

  it('with a destination everything is open; transforms are optional', () => {
    const p = draft(['shop.orders'], true);
    expect(stepState('sink', p).done).toBe(true);
    expect(stepState('transform', p)).toEqual({ done: true, locked: null });
    expect(stepState('review', p).locked).toBeNull();
  });

  it('the step on screen counts as done when its unsaved choices are complete (leaving it saves them)', () => {
    expect(stepState('sink', draft([], false), 'tables').locked).toBeNull();
    expect(stepState('review', draft(['shop.orders'], false), 'sink').locked).toBeNull();
    expect(stepState('review', draft([], false), 'tables').locked).toBe('Choose a destination first.');
  });

  it('an invalid destination naming keeps the destination step not done', () => {
    expect(stepState('sink', draft(['shop.orders'], true, '')).done).toBe(false);
  });
});
