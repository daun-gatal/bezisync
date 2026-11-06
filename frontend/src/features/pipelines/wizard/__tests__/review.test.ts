import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Setting } from '@/lib/settings';
import { autosaveDecision, customErrors, focusReviewField, isPaused, problemsSummary, rawLabel, reviewProblems, useAutosave } from '../review';

describe('autosaveDecision', () => {
  const base = { dirty: true, saving: false, invalid: false, paused: false };
  it.each([
    [{ ...base, dirty: false }, 'idle'],
    [{ ...base, dirty: false, saving: true, invalid: true, paused: true }, 'idle'],
    [{ ...base, saving: true }, 'wait'],
    [{ ...base, saving: true, invalid: true, paused: true }, 'wait'],
    [{ ...base, invalid: true }, 'blocked'],
    [{ ...base, invalid: true, paused: true }, 'blocked'],
    [{ ...base, paused: true }, 'paused'],
    [base, 'schedule'],
  ] as const)('%o → %s (first match wins)', (s, want) => {
    expect(autosaveDecision(s)).toBe(want);
  });

  it('is paused only on the refused signature', () => {
    expect(isPaused('a', null)).toBe(false);
    expect(isPaused('a', 'a')).toBe(true);
    expect(isPaused('b', 'a')).toBe(false);
  });

  it('names the raw section for each engine', () => {
    expect(rawLabel(false)).toBe('Advanced Debezium properties');
    expect(rawLabel(true)).toBe('Advanced Flink CDC options');
  });
});

describe('useAutosave', () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  type P = { signature: string; dirty: boolean; invalid: boolean; saving: boolean; save: () => Promise<boolean> };
  const run = (props: P) => renderHook((p: P) => useAutosave(p), { initialProps: props });
  const tick = async (ms: number) => {
    await act(async () => {
      await vi.advanceTimersByTimeAsync(ms);
    });
  };

  it('saves a refused draft once, then waits for a change (no loop)', async () => {
    const save = vi.fn(async () => false);
    const h = run({ signature: 'a', dirty: true, invalid: false, saving: false, save });
    await tick(800);
    expect(save).toHaveBeenCalledTimes(1);
    expect(h.result.current.state).toBe('paused');
    await tick(10_000);
    expect(save).toHaveBeenCalledTimes(1);
    h.rerender({ signature: 'b', dirty: true, invalid: false, saving: false, save });
    await tick(800);
    expect(save).toHaveBeenCalledTimes(2);
    await tick(10_000);
    expect(save).toHaveBeenCalledTimes(2);
  });

  it('Try again saves the same draft once more', async () => {
    const save = vi.fn(async () => false);
    const h = run({ signature: 'a', dirty: true, invalid: false, saving: false, save });
    await tick(800);
    act(() => h.result.current.retry());
    await tick(800);
    expect(save).toHaveBeenCalledTimes(2);
    await tick(10_000);
    expect(save).toHaveBeenCalledTimes(2);
  });

  it('saves a draft that becomes valid without another edit', async () => {
    const save = vi.fn(async () => true);
    const h = run({ signature: 'a', dirty: true, invalid: true, saving: false, save });
    await tick(5_000);
    expect(save).not.toHaveBeenCalled();
    h.rerender({ signature: 'a', dirty: true, invalid: false, saving: false, save });
    await tick(800);
    expect(save).toHaveBeenCalledTimes(1);
  });

  it('attempt saves now even while paused, and a success clears the pause', async () => {
    let ok = false;
    const save = vi.fn(async () => ok);
    const h = run({ signature: 'a', dirty: true, invalid: false, saving: false, save });
    await tick(800);
    expect(h.result.current.paused).toBe(true);
    ok = true;
    await act(async () => {
      expect(await h.result.current.attempt()).toBe(true);
    });
    expect(h.result.current.paused).toBe(false);
  });

  it('waits while a save is in flight and never saves a clean draft', async () => {
    const save = vi.fn(async () => true);
    const h = run({ signature: 'a', dirty: true, invalid: false, saving: true, save });
    await tick(5_000);
    h.rerender({ signature: 'a', dirty: false, invalid: false, saving: false, save });
    await tick(5_000);
    expect(save).not.toHaveBeenCalled();
  });
});

describe('reviewProblems', () => {
  const none = { nameErr: null, custom: {}, streaming: {}, manual: {}, tuning: {}, tunable: [] as Setting[], raw: new Map<string, string>(), flink: false };
  const threads = { engine: 'debezium', scope: 'source', stage: 'snapshot', impact: 'restart', why: 'w', title: 'Snapshot threads', key: 'debezium.source.snapshot.max.threads', type: 'int', default: '1', min: '1', max: '16' } as Setting;

  it('is empty for a valid draft', () => {
    expect(reviewProblems(none)).toEqual([]);
  });

  it('lists every problem in page order, each on its own field', () => {
    const got = reviewProblems({
      ...none,
      nameErr: 'Give the pipeline a name.',
      custom: { memory: 'This project allows at most 768 MiB.' },
      streaming: { cpu: 'a', memory: 'b' },
      manual: { taskManagers: 'c', memory: 'd' },
      tuning: { 'debezium.source.snapshot.max.threads': '99', 'x.unknown': '1' },
      tunable: [threads],
      raw: new Map([['debezium.format.value', 'The event format is chosen on the Destination step.']]),
    });
    expect(got.map((p) => [p.field, p.label])).toEqual([
      ['name', 'Name'],
      ['memory', 'Memory'],
      ['streaming-cpu', 'Streaming reader CPU'],
      ['streaming-memory', 'Streaming reader memory'],
      ['task-managers', 'TaskManagers'],
      ['tm-memory', 'TaskManager memory'],
      ['tuning', 'Snapshot threads'],
      ['tuning', 'x.unknown'],
      ['advanced', 'Advanced Debezium properties'],
    ]);
    expect(got.find((p) => p.key === 'x.unknown')?.message).toBe('Not a setting this pipeline can use: remove it.');
    expect(got.find((p) => p.field === 'advanced')?.key).toBe('debezium.format.value');
  });

  it('labels raw properties by the engine section', () => {
    expect(reviewProblems({ ...none, flink: true, raw: new Map([['k', 'm']]) })[0]?.label).toBe('Advanced Flink CDC options');
  });

  it('checks each custom amount on its own (max before min)', () => {
    const max = { cpu: 0.5, mem: 768 };
    const min = { cpu: 1, mem: 1024 };
    expect(customErrors({ cpu: 0.5, mem: 1024 }, max, null, 'Kafka')).toEqual({ memory: 'This project allows at most 768 MiB.' });
    expect(customErrors({ cpu: 2, mem: 2048 }, max, null, 'Kafka')).toEqual({ cpu: 'This project allows at most 0.5 CPU.', memory: 'This project allows at most 768 MiB.' });
    expect(customErrors({ cpu: 0.5, mem: 512 }, null, min, 'Kafka')).toEqual({ cpu: 'Kafka needs at least 1 CPU.', memory: 'Kafka needs at least 1024 MiB.' });
    expect(customErrors({ cpu: 1, mem: 1024 }, null, min, 'Kafka')).toEqual({});
  });

  it('names the first problem and counts the rest', () => {
    const p = (label: string) => ({ field: 'cpu' as const, label, message: 'm' });
    expect(problemsSummary([])).toBe('');
    expect(problemsSummary([p('Memory')])).toBe('Memory');
    expect(problemsSummary([p('CPU'), p('Memory'), p('Name')])).toBe('CPU and 2 more');
  });
});

describe('focusReviewField', () => {
  afterEach(() => {
    document.body.innerHTML = '';
  });

  it('focuses the invalid control inside the field, without scrollIntoView (jsdom)', () => {
    document.body.innerHTML = '<div data-review-field="advanced"><input id="ok"><input id="bad" aria-invalid="true"></div>';
    expect(focusReviewField('advanced')).toBe(true);
    expect(document.activeElement?.id).toBe('bad');
  });

  it('falls back to the first focusable control, and matches space-separated fields', () => {
    document.body.innerHTML = '<div data-review-field="tuning advanced"><button id="remove">Remove</button></div>';
    expect(focusReviewField('tuning')).toBe(true);
    expect(document.activeElement?.id).toBe('remove');
  });

  it('says so when the field is not on the page', () => {
    expect(focusReviewField('memory')).toBe(false);
  });
});
