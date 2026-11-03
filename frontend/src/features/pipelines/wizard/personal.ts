import type { FullSpec } from '../spec';

/**
 * Personal data, chosen per pipeline in the Tables step. Every column the source flags as
 * looking like personal data gets a choice: Mask (a salted hash, so joins still work), Remove
 * (left out of events) or Keep (sent as it is, on purpose). Any other column can be masked or
 * removed too. Mirrors backend domain/personal.go: a masked column that can't be hashed is a
 * blocking preflight check, an undecided personal-data column a warning.
 */

export type Choice = 'mask' | 'remove' | 'keep';
export type Rules = FullSpec['source']['columns'][string];
export type ColumnRulesMap = FullSpec['source']['columns'];

export interface DiscoveredColumn {
  name: string;
  type: string;
  pii?: boolean;
}

export interface PersonalRow {
  table: string;
  column: string;
  /** null: the column is not in the table any more (renamed or dropped). */
  type: string | null;
  detected: boolean;
  maskable: boolean;
  /** null: no choice yet (only possible for rules loaded from YAML or the API). */
  choice: Choice | null;
}

const EMPTY: Rules = { mask: [], exclude: [] };

/** Can this source hash a column of this type (descriptor transforms.maskable_types)? */
export function canMask(maskableTypes: string[] | undefined, type: string | null): boolean {
  return Boolean(type && maskableTypes?.includes(type.toLowerCase()));
}

/** One rule per column: removing wins over masking, masking over keeping (as the backend does). */
export function normalize(rules: Rules | undefined): Rules {
  const exclude = [...new Set(rules?.exclude ?? [])];
  const mask = [...new Set(rules?.mask ?? [])].filter((c) => !exclude.includes(c));
  const keep = [...new Set(rules?.keep ?? [])].filter((c) => !exclude.includes(c) && !mask.includes(c));
  return keep.length ? { mask, exclude, keep } : { mask, exclude };
}

export function choiceOf(rules: Rules | undefined, column: string): Choice | null {
  if (rules?.exclude.includes(column)) return 'remove';
  if (rules?.mask.includes(column)) return 'mask';
  if (rules?.keep?.includes(column)) return 'keep';
  return null;
}

/** The rules with one column's choice changed (null clears it). */
export function setChoice(rules: Rules | undefined, column: string, choice: Choice | null): Rules {
  const r = normalize(rules);
  const without = (l: string[] | undefined) => (l ?? []).filter((c) => c !== column);
  const next: Rules = { mask: without(r.mask), exclude: without(r.exclude), keep: without(r.keep) };
  if (choice === 'mask') next.mask.push(column);
  if (choice === 'remove') next.exclude.push(column);
  if (choice === 'keep') next.keep!.push(column);
  return normalize(next);
}

/** The safe choice for a column: mask what can be hashed, remove the rest. */
export function safeChoice(maskable: boolean): Choice {
  return maskable ? 'mask' : 'remove';
}

/** Choices for a table picked for the first time: every personal-data column protected. */
export function defaultsFor(columns: DiscoveredColumn[], maskableTypes: string[] | undefined): Rules {
  let rules: Rules = EMPTY;
  for (const c of columns) {
    if (c.pii) rules = setChoice(rules, c.name, safeChoice(canMask(maskableTypes, c.type)));
  }
  return rules;
}

/** The rows of the Personal data card: detected columns plus every column with a rule. */
export function personalRows(
  tables: string[],
  columnsOf: (table: string) => DiscoveredColumn[] | undefined,
  rules: ColumnRulesMap,
  maskableTypes: string[] | undefined,
): PersonalRow[] {
  const out: PersonalRow[] = [];
  for (const table of tables) {
    const cols = columnsOf(table) ?? [];
    const r = rules[table];
    const named = new Set([...(r?.mask ?? []), ...(r?.exclude ?? []), ...(r?.keep ?? [])]);
    for (const c of cols) {
      if (!c.pii && !named.has(c.name)) continue;
      out.push({ table, column: c.name, type: c.type, detected: Boolean(c.pii), maskable: canMask(maskableTypes, c.type), choice: choiceOf(r, c.name) });
      named.delete(c.name);
    }
    for (const gone of named) out.push({ table, column: gone, type: null, detected: false, maskable: false, choice: choiceOf(r, gone) });
  }
  return out;
}

/** What blocks or needs attention, mirroring preflight (blocking first). */
export function personalProblem(rows: PersonalRow[]): { blocking: boolean; text: string } | null {
  const unmaskable = rows.filter((r) => r.choice === 'mask' && r.type !== null && !r.maskable);
  if (unmaskable.length) {
    return { blocking: true, text: `${list(unmaskable)} can’t be masked: choose Remove or Keep as is.` };
  }
  const undecided = rows.filter((r) => r.detected && r.choice === null);
  if (undecided.length) return { blocking: false, text: `${list(undecided)} would be sent as is: choose what to do.` };
  return null;
}

function list(rows: PersonalRow[]): string {
  const names = rows.map((r) => `${r.table}.${r.column}`);
  return names.length > 3 ? `${names.slice(0, 3).join(', ')} and ${names.length - 3} more` : names.join(', ');
}

/** Counts for summaries (Review step, pipeline page). */
export function personalSummary(columns: ColumnRulesMap, tables: string[]): { masked: number; removed: number; kept: number } {
  let masked = 0;
  let removed = 0;
  let kept = 0;
  for (const t of tables) {
    const r = normalize(columns[t]);
    masked += r.mask.length;
    removed += r.exclude.length;
    kept += r.keep?.length ?? 0;
  }
  return { masked, removed, kept };
}

/** The column rules to save: only captured tables, tables without rules dropped. Rules
 *  nobody touched are kept exactly as saved, so visiting the step never makes a new version. */
export function rulesToSave(rules: ColumnRulesMap, tables: string[], saved: ColumnRulesMap): ColumnRulesMap {
  const out: ColumnRulesMap = {};
  for (const t of [...tables].sort()) {
    if (rules[t] !== undefined && rules[t] === saved[t]) {
      out[t] = saved[t];
      continue;
    }
    const r = normalize(rules[t]);
    if (r.mask.length || r.exclude.length || r.keep?.length) out[t] = r;
  }
  return out;
}
