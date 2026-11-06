import { useCallback, useEffect, useRef, useState } from 'react';

/**
 * The Review step's rules, kept out of the component so they can be tested on their own:
 * when autosave saves, and the label of the raw properties section.
 */

/** What the Review autosave does next. */
export type AutosaveState = 'idle' | 'wait' | 'blocked' | 'paused' | 'schedule';

/**
 * Checked top to bottom; the first match wins. A save the server refused pauses autosave until
 * the draft changes (its signature moves off the refused one), Try again is clicked, or the step
 * is left: it never retries by itself.
 */
export function autosaveDecision(s: { dirty: boolean; saving: boolean; invalid: boolean; paused: boolean }): AutosaveState {
  if (!s.dirty) return 'idle';
  if (s.saving) return 'wait';
  if (s.invalid) return 'blocked';
  if (s.paused) return 'paused';
  return 'schedule';
}

/** Whether the draft is the one the server last refused. */
export function isPaused(signature: string, failedSignature: string | null): boolean {
  return failedSignature !== null && signature === failedSignature;
}

/** The raw properties section's name on each engine. */
export function rawLabel(flink: boolean): string {
  return flink ? 'Advanced Flink CDC options' : 'Advanced Debezium properties';
}

/**
 * The Review autosave: saves `delay` ms after the last change while `autosaveDecision` says
 * `schedule`, one save at a time. A refused save remembers the draft's signature and pauses
 * until the signature changes or `retry` is called. `attempt` saves now (Try again, leaving
 * the step) and records the outcome the same way.
 */
export function useAutosave(o: { signature: string; dirty: boolean; invalid: boolean; saving: boolean; save: () => Promise<boolean>; delay?: number }) {
  const { signature, dirty, invalid, saving, delay = 800 } = o;
  const [failedSignature, setFailedSignature] = useState<string | null>(null);
  const paused = isPaused(signature, failedSignature);
  const state = autosaveDecision({ dirty, saving, invalid, paused });
  const latest = useRef({ ...o, paused });
  latest.current = { ...o, paused };
  const attempt = useCallback(async () => {
    const asked = latest.current.signature;
    const ok = await latest.current.save();
    setFailedSignature(ok ? null : asked);
    return ok;
  }, []);
  useEffect(() => {
    if (state !== 'schedule') return;
    const t = window.setTimeout(() => {
      if (autosaveDecision(latest.current) === 'schedule') void attempt();
    }, delay);
    return () => window.clearTimeout(t);
  }, [signature, state, delay, attempt]);
  const retry = useCallback(() => setFailedSignature(null), []);
  return { state, paused, attempt, retry };
}
