import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { autosaveDecision, isPaused, rawLabel, useAutosave } from '../review';

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
