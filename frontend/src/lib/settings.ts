import type { Schemas } from './api';

/** A typed setting of the registry (GET /engines, per engine). */
export type Setting = Schemas['FlinkSetting'];
export type Impact = Setting['impact'];
export type Stage = Setting['stage'];

/** What changing a setting does to a running pipeline, lightest first, in words and a tone
 *  (Pill shows a glyph with it, never colour alone). */
export const IMPACT: Record<Impact, { label: string; long: string; tone: 'ok' | 'info' | 'warn' | 'risk' | 'bad' }> = {
  live: { label: 'Applies live', long: 'Applies without a restart', tone: 'ok' },
  restart: { label: 'Restart', long: 'Restarts the pipeline; it resumes where it stopped', tone: 'info' },
  first_copy: { label: 'First copy', long: 'Only matters while the pipeline first copies existing rows', tone: 'info' },
  next_copy: { label: 'Next copy', long: 'Takes effect the next time a table is copied', tone: 'warn' },
  breaks_positions: { label: 'Starts over', long: 'The saved position can’t be used: the pipeline starts over', tone: 'bad' },
};
export const IMPACT_ORDER: Impact[] = ['live', 'restart', 'first_copy', 'next_copy', 'breaks_positions'];

/** The heaviest of some impacts (undefined for none). */
export function heaviest(impacts: Impact[]): Impact | undefined {
  return impacts.reduce<Impact | undefined>((m, i) => (m === undefined || IMPACT_ORDER.indexOf(i) > IMPACT_ORDER.indexOf(m) ? i : m), undefined);
}

export const STAGES: { id: Stage; title: string }[] = [
  { id: 'snapshot', title: 'Snapshot' },
  { id: 'streaming', title: 'Streaming' },
  { id: 'delivery', title: 'Delivery' },
  { id: 'engine', title: 'Engine' },
];

const DURATION = /^(\d{1,7})\s*(ms|s|min)$/;
const MS = { ms: 1, s: 1000, min: 60_000 } as const;

function durationMs(v: string): number | null {
  const m = DURATION.exec(v.trim());
  return m ? Number(m[1]) * MS[m[2] as keyof typeof MS] : null;
}

/** Go durations as the registry writes bounds ("1m", "10s", "1h", "500ms"). */
function goDurationMs(v: string): number {
  const m = /^(\d+)(ms|s|m|h)$/.exec(v);
  if (!m) return NaN;
  return Number(m[1]) * { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[m[2] as 'ms' | 's' | 'm' | 'h'];
}

/** Why a value can't be used for a setting (null when it can): mirrors engine.CheckSetting. */
export function settingError(s: Setting, value: string): string | null {
  switch (s.type) {
    case 'duration': {
      const d = durationMs(value);
      if (d === null) return 'must be a duration such as 30 s, 5 min or 500 ms';
      if (d < goDurationMs(s.min ?? '0s') || d > goDurationMs(s.max ?? '0s')) return `must be between ${s.min} and ${s.max}`;
      return null;
    }
    case 'int': {
      const n = Number(value);
      if (!/^[+-]?\d+$/.test(value) || n < Number(s.min) || n > Number(s.max)) return `must be a whole number from ${s.min} to ${s.max}`;
      return null;
    }
    case 'fraction': {
      const f = Number(value);
      if (value.trim() === '' || /[eE]/.test(value) || !Number.isFinite(f) || f < Number(s.min) || f > Number(s.max)) return `must be a number from ${s.min} to ${s.max}`;
      return null;
    }
    case 'enum':
      return (s.choices ?? []).includes(value) ? null : `must be one of ${(s.choices ?? []).join(', ')}`;
    case 'bool':
      return value === 'true' || value === 'false' ? null : 'must be true or false';
  }
  return null;
}

/** The one way a spec stores a valid value (mirrors engine.CanonicalSetting); null when invalid. */
export function canonicalSetting(s: Setting, value: string): string | null {
  if (settingError(s, value)) return null;
  switch (s.type) {
    case 'int':
      return String(Number(value));
    case 'fraction':
      return String(Number(value));
    case 'duration': {
      const m = DURATION.exec(value.trim())!;
      return `${Number(m[1])} ${m[2]}`;
    }
  }
  return value;
}

/** An object's keys in PostgreSQL JSONB order (shorter first, then bytewise), the order the server
 *  returns every stored map in: a draft compared with the stored spec must use it too, or the
 *  wizard thinks it has changes forever and keeps saving. */
export function jsonbSorted<V>(o: Record<string, V>): Record<string, V> {
  const keys = Object.keys(o).sort((a, b) => a.length - b.length || (a < b ? -1 : a > b ? 1 : 0));
  return Object.fromEntries(keys.map((k) => [k, o[k]!]));
}
