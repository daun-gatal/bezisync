import { useCallback, useEffect, useRef, useState } from 'react';
import { settingError, type Setting } from '@/lib/settings';

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
 * until the signature changes. `attempt` saves now (Try again, leaving the step) and records the
 * outcome the same way.
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
  return { state, paused, attempt };
}

/** A Review control that can hold a problem: its `data-review-field` wrapper. */
export type ReviewField = 'name' | 'cpu' | 'memory' | 'streaming-cpu' | 'streaming-memory' | 'task-managers' | 'tm-memory' | 'tuning' | 'advanced';

/** One reason the draft can't be saved, on the control it is about. */
export interface ReviewProblem {
  field: ReviewField;
  /** The control's visible label, as the badge names it. */
  label: string;
  message: string;
  /** The setting or property key, for tuning and advanced problems. */
  key?: string;
}

type Amount = { cpu: number; mem: number };

/** A Debezium custom size against the project's largest size and the destination's smallest, each amount on its own. */
export function customErrors(amount: Amount, max: Amount | null, min: Amount | null, sinkLabel: string): { cpu?: string; memory?: string } {
  const out: { cpu?: string; memory?: string } = {};
  if (max && amount.cpu > max.cpu) out.cpu = `This project allows at most ${max.cpu} CPU.`;
  else if (min && amount.cpu < min.cpu) out.cpu = `${sinkLabel} needs at least ${min.cpu} CPU.`;
  if (max && amount.mem > max.mem) out.memory = `This project allows at most ${max.mem} MiB.`;
  else if (min && amount.mem < min.mem) out.memory = `${sinkLabel} needs at least ${min.mem} MiB.`;
  return out;
}

/**
 * Every client-side reason the draft can't be saved, in page order. The badge, the Deploy reason
 * and each field's error read this list; `invalid` is a non-empty list. The server's own
 * refusals are not in it: they pause autosave instead (`useAutosave`).
 */
export function reviewProblems(i: {
  nameErr: string | null;
  custom: { cpu?: string; memory?: string };
  streaming: { cpu?: string; memory?: string };
  manual: { taskManagers?: string; memory?: string };
  tuning: Record<string, string>;
  tunable: Setting[];
  raw: Map<string, string>;
  flink: boolean;
}): ReviewProblem[] {
  const out: ReviewProblem[] = [];
  const add = (field: ReviewField, label: string, message: string | null | undefined, key?: string) => {
    if (message) out.push({ field, label, message, ...(key !== undefined ? { key } : {}) });
  };
  add('name', 'Name', i.nameErr);
  add('cpu', 'CPU', i.custom.cpu);
  add('memory', 'Memory', i.custom.memory);
  add('streaming-cpu', 'Streaming reader CPU', i.streaming.cpu);
  add('streaming-memory', 'Streaming reader memory', i.streaming.memory);
  add('task-managers', 'TaskManagers', i.manual.taskManagers);
  add('tm-memory', 'TaskManager memory', i.manual.memory);
  for (const [key, value] of Object.entries(i.tuning)) {
    const s = i.tunable.find((t) => t.key === key);
    if (!s) add('tuning', key, 'Not a setting this pipeline can use: remove it.', key);
    else {
      const err = settingError(s, value);
      if (err) add('tuning', s.title, `${s.title} ${err}.`, key);
    }
  }
  for (const [key, message] of i.raw) add('advanced', rawLabel(i.flink), message, key);
  return out;
}

/** The badge's words for the problems: "Memory", or "Memory and 2 more". */
export function problemsSummary(problems: ReviewProblem[]): string {
  if (!problems.length) return '';
  return problems.length === 1 ? problems[0]!.label : `${problems[0]!.label} and ${problems.length - 1} more`;
}

/**
 * Brings a problem's control into view and focuses it: the first invalid control inside the
 * field's wrapper, else its first focusable one. Wrappers list their fields space-separated.
 */
export function focusReviewField(field: ReviewField, root: ParentNode = document): boolean {
  const wrapper = root.querySelector<HTMLElement>(`[data-review-field~="${field}"]`);
  if (!wrapper) return false;
  wrapper.scrollIntoView?.({ block: 'center' });
  const target =
    wrapper.querySelector<HTMLElement>('[aria-invalid="true"]') ??
    wrapper.querySelector<HTMLElement>('input:not([disabled]), select:not([disabled]), textarea:not([disabled]), button:not([disabled]), [tabindex]:not([tabindex="-1"])');
  target?.focus();
  return Boolean(target);
}
