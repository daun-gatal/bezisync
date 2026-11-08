import type { ComputedArg, ComputedFn, FilterOp, FilterOperation, FilterRule, FullSpec, RuleValue, TransformStep, TransformType, ValueFormats } from '../spec';
import { stageOf } from '../spec';

/** What the wizard knows about one captured table (from discovery). */
export interface TableInfo {
  name: string;
  columns: { name: string; type: string; nullable?: boolean; pii?: boolean }[];
  primaryKey: string[];
  replicaIdentity?: string | null;
}

/** Source capabilities (descriptor "transforms"). */
export interface Caps {
  value_formats: { decimals?: string[]; timestamps?: string[]; binary?: string[] };
  key_columns: boolean;
  row_filters: boolean;
}

export const NO_CAPS: Caps = { value_formats: {}, key_columns: false, row_filters: false };

/** Step titles on Flink CDC, which writes rows and columns into tables (the others as TYPE_TITLE). */
export const FLINK_TITLE: Partial<Record<TransformType, string>> = { filter: 'Keep only some rows', rename: 'Rename columns', add_field: 'Add a column', soft_delete: 'Keep deleted rows' };

/** Step types that run only on Debezium (the Flink CDC compiler refuses them). */
export const DEBEZIUM_ONLY_STEPS: readonly TransformType[] = ['outbox', 'flatten', 'custom'];
/** Steps only Flink CDC runs (T16: Debezium has no expressions short of scripting). */
export const FLINK_ONLY_STEPS: readonly TransformType[] = ['computed'];

/**
 * What the Transform step offers: the source's capabilities, without epoch timestamps for a
 * destination that can't read them (time_precision), and without value formats on Flink CDC,
 * which writes each column with its own type.
 */
export function stepCaps(source: Caps, timePrecision: boolean, flink: boolean): Caps {
  if (flink) return { ...source, value_formats: {} };
  if (!timePrecision) return source;
  return { ...source, value_formats: { ...source.value_formats, timestamps: source.value_formats.timestamps?.filter((m) => m !== 'epoch') } };
}

export const TYPE_TITLE: Record<TransformType, string> = {
  unwrap: 'Event shape',
  filter: 'Keep only some events',
  outbox: 'Outbox events',
  rename: 'Rename fields',
  add_field: 'Add a field',
  flatten: 'Flatten nested fields',
  custom: 'Custom transform',
  soft_delete: 'Keep deleted rows',
  truncate: 'Cut long text',
  route: 'Send tables elsewhere',
  computed: 'Computed column',
};

/** Sources whose Debezium connector truncates columns (domain.TruncateSources). */
export const TRUNCATE_SOURCES = ['postgresql', 'mysql'];

/** A column type a truncation cuts (text, not JSON). */
export function isTextType(type: string): boolean {
  const t = type.toLowerCase();
  return !t.includes('json') && ['char', 'text', 'string', 'clob'].some((s) => t.includes(s));
}

/** The field a soft delete marks rows with, per engine (domain.SoftDeleteField). */
export const SOFT_DELETE_FIELD = { debezium: '__deleted', 'flink-cdc': '__op' } as const;

export const DEBEZIUM_CLASS: Partial<Record<TransformType, string>> = {
  filter: 'io.debezium.transforms.Filter',
  rename: 'ReplaceField$Value',
  add_field: 'InsertField$Value',
  flatten: 'Flatten$Value',
  outbox: 'io.debezium.transforms.outbox.EventRouter',
};

export const OPERATIONS: { value: FilterOperation; label: string }[] = [
  { value: 'insert', label: 'insert' },
  { value: 'update', label: 'update' },
  { value: 'delete', label: 'delete' },
  { value: 'snapshot', label: 'snapshot' },
];

/** Event-shape metadata Debezium can add (unwrap add.fields) and the field each becomes. */
export const METADATA: { value: string; label: string; postgresOnly?: boolean }[] = [
  { value: 'op', label: 'Operation' },
  { value: 'source.ts_ms', label: 'Change time' },
  { value: 'source.table', label: 'Table' },
  { value: 'source.schema', label: 'Schema' },
  { value: 'source.db', label: 'Database' },
  { value: 'source.lsn', label: 'Log position', postgresOnly: true },
  { value: 'ts_ms', label: 'Processed time' },
];

export const metadataField = (f: string) => `__${f.replaceAll('.', '_')}`;

export const NAME_RE = /^[A-Za-z_][A-Za-z0-9_]{0,127}$/;

/** Error text for a new field name, or null. */
export function nameError(name: string, taken: string[] = []): string | null {
  if (!name) return 'Give the field a name.';
  if (!NAME_RE.test(name)) {
    const fixed = name.trim().replace(/[^A-Za-z0-9_]+/g, '_').replace(/^([0-9])/, '_$1').slice(0, 128);
    return `Use letters, digits and _, starting with a letter or _.${fixed && NAME_RE.test(fixed) ? ` Did you mean ${fixed}?` : ''}`;
  }
  if (taken.includes(name)) return `The event already has a field ${name}.`;
  return null;
}

/** Error text for a fixed value, or null (Debezium Server would expand "${…}"). */
export function textError(text: string): string | null {
  if (!text) return 'Enter the value.';
  if (text.length > 200) return 'At most 200 characters.';
  if (text.includes('${')) return 'Cannot contain "${" (Debezium Server would read it as a variable).';
  // eslint-disable-next-line no-control-regex
  if (/[\u0000-\u001f\u007f]/.test(text)) return 'Cannot contain line breaks or tabs.';
  return null;
}

export function newStepId(existing: TransformStep[]): string {
  const used = new Set(existing.map((s) => s.id));
  for (;;) {
    const id = Math.random().toString(36).slice(2, 8).padEnd(6, '0');
    if (!used.has(id) && /^[a-z0-9]{4,12}$/.test(id)) return id;
  }
}

export function defaultStep(type: TransformType, steps: TransformStep[]): TransformStep {
  const id = newStepId(steps);
  switch (type) {
    case 'unwrap':
      return { type, id, delete_handling: 'rewrite', add_fields: ['op', 'source.ts_ms'], condition: null };
    case 'filter':
      // No `match` until there are rules: the server refuses match without rules (domain decodeFilter).
      return { type, id, operations: ['insert', 'update', 'delete', 'snapshot'], rules: [] };
    case 'rename':
      return { type, id, renames: [{ from: '', to: '' }] };
    case 'add_field':
      return { type, id, name: '', value: { kind: 'static', text: '' } };
    case 'flatten':
      return { type, id, delimiter: '_' };
    case 'custom':
      return { type, id, class: '', properties: {} };
    case 'truncate':
      return { type, id, columns: [] };
    case 'route':
      return { type, id, routes: [{ match: '', to: '' }] };
    case 'computed':
      return { type, id, table: '', name: '', function: 'upper', args: [{ column: '' }] };
    default:
      return { type, id };
  }
}

const ROUTE_MATCH = /^[A-Za-z0-9_$*-]+\.[A-Za-z0-9_$*-]+$/;
const ROUTE_TO = /^[A-Za-z0-9_$][A-Za-z0-9_$.-]{0,199}$/;

/** Whether a route pattern matches a table ("schema.table"; * is any run) — domain.GlobMatch. */
export function globMatch(pattern: string, table: string): boolean {
  const parts = pattern.split('*');
  if (parts.length === 1) return pattern === table;
  // Start and end anchored; the middle parts found in order in what lies between.
  const first = parts[0] ?? '';
  const last = parts[parts.length - 1] ?? '';
  if (table.length < first.length + last.length || !table.startsWith(first) || !table.endsWith(last)) return false;
  let rest = table.slice(first.length, table.length - last.length);
  for (const p of parts.slice(1, -1)) {
    const at = rest.indexOf(p);
    if (at < 0) return false;
    rest = rest.slice(at + p.length);
  }
  return true;
}

/** The captured tables route steps send elsewhere: table → destination name (domain.RouteTargets). */
export function routeTargets(steps: TransformStep[], tables: string[]): Map<string, string> {
  const out = new Map<string, string>();
  for (const t of steps) {
    if (t.type !== 'route') continue;
    for (const r of t.routes ?? []) for (const table of tables) if (!out.has(table) && r.match && globMatch(r.match, table)) out.set(table, r.to);
  }
  return out;
}

/** Why a route step can't be saved (mirrors the decoder); with tables, also T10 across every
 *  route step (a table two routes match). Null when it can. */
export function routeError(steps: TransformStep[], step: TransformStep, tables: string[]): string | null {
  const routes = step.routes ?? [];
  if (!routes.length) return 'Add a route.';
  if (routes.length > 20) return 'At most 20 routes.';
  for (const r of routes) {
    if (!ROUTE_MATCH.test(r.match)) return 'Write each table pattern as schema.table, where * matches any run of characters.';
    if (!ROUTE_TO.test(r.to)) return `${r.match}: a destination name of letters, digits, _, $, . and -, at most 200.`;
  }
  const all = steps.filter((t) => t.type === 'route').flatMap((t) => t.routes ?? []);
  for (const table of tables) {
    const hits = all.filter((r) => globMatch(r.match, table)).map((r) => r.match);
    if (hits.length > 1) return `${table} matches more than one route (${hits.join(', ')}): give it one.`;
  }
  return null;
}

/**
 * Inserts a step where the chain stays valid: a keep-events filter just before the event shape
 * (`shapeIndex`), Keep deleted rows just after the unwrap (or at the end when there is none, so
 * the unwrap the compiler adds there lands where it always did), anything else at the end.
 */
export function addStep(steps: TransformStep[], step: TransformStep): TransformStep[] {
  let at = steps.length;
  if (stageOf(step) === 0) {
    const shape = shapeIndex(steps);
    if (shape >= 0) at = shape;
  } else if (step.type === 'soft_delete') {
    const unwrap = steps.findIndex((t) => t.type === 'unwrap');
    if (unwrap >= 0) at = unwrap + 1;
  }
  return [...steps.slice(0, at), step, ...steps.slice(at)];
}

/** Moves a field-change step up or down within its stage (other steps keep their place). */
export function moveStep(steps: TransformStep[], index: number, delta: -1 | 1): TransformStep[] {
  const j = index + delta;
  const a = steps[index];
  const b = steps[j];
  if (!a || !b || stageOf(a) !== 2 || stageOf(b) !== 2) return steps;
  const out = [...steps];
  out[index] = b;
  out[j] = a;
  return out;
}

export function canMove(steps: TransformStep[], index: number, delta: -1 | 1): boolean {
  return moveStep(steps, index, delta) !== steps;
}

/** Problems that make a step unsaveable (mirrors the backend decoder), keyed by step index. */
export function stepErrors(steps: TransformStep[], tableNames: string[] = []): Map<number, string> {
  const out = new Map<number, string>();
  // The server's boundary: the first unwrap or Keep deleted rows.
  const shapeAt = shapeIndex(steps);
  steps.forEach((t, i) => {
    const stage = stageOf(t);
    if (stage === 2 && (shapeAt < 0 || shapeAt > i)) out.set(i, 'Needs the "Row after change" event shape before it.');
    if (stage === 0 && shapeAt >= 0 && shapeAt < i) out.set(i, 'Must run before the event shape.');
    if (t.type === 'filter' && stage === 0) {
      t.rules?.forEach((r) => {
        if (!r.field) out.set(i, 'Pick a column for every rule.');
        else if (needsValue(r.op) && (r.value === undefined || r.value === '')) out.set(i, `Enter a value for ${r.field}.`);
        else if (needsValues(r.op) && !(r.values?.length)) out.set(i, `Add at least one value for ${r.field}.`);
      });
    }
    if (t.type === 'filter' && t.expression !== undefined && !t.expression.trim()) out.set(i, 'Write the condition, or switch back to rules.');
    else if (t.type === 'filter' && stage !== 0 && !t.condition) out.set(i, 'Choose at least one operation or add a rule.');
    if (t.type === 'rename') {
      const tos = (t.renames ?? []).map((r) => r.to);
      (t.renames ?? []).forEach((r) => {
        if (!r.from) out.set(i, 'Pick the field to rename.');
        else {
          const e = nameError(r.to);
          if (e) out.set(i, `${r.from}: ${e}`);
          else if (r.from === r.to) out.set(i, `${r.from} would keep its name.`);
          else if (tos.filter((x) => x === r.to).length > 1) out.set(i, `Two fields would be called ${r.to}.`);
        }
      });
      if (!(t.renames?.length)) out.set(i, 'Add a rename.');
    }
    if (t.type === 'add_field') {
      const e = nameError(t.name ?? '') ?? (t.value?.kind === 'static' ? textError(t.value.text ?? '') : null);
      if (e) out.set(i, e);
    }
    if (t.type === 'truncate') {
      if (!(t.columns?.length)) out.set(i, 'Add a column to cut.');
      else if (t.columns.some((c) => !c.column)) out.set(i, 'Pick a column for every row.');
    }
    if (t.type === 'computed') {
      const e = computedError(t);
      if (e) out.set(i, e);
    }
    if (t.type === 'route') {
      const e = routeError(steps, t, steps.findIndex((x) => x.type === 'route') === i ? tableNames : []);
      if (e) out.set(i, e);
    }
    if (t.type === 'custom' && !/^[A-Za-z_$][A-Za-z0-9_$]*(\.[A-Za-z_$][A-Za-z0-9_$]*)+$/.test(t.class ?? '')) out.set(i, 'Choose a class.');
  });
  return out;
}

export const needsValue = (op: FilterOp) => ['eq', 'ne', 'gt', 'gte', 'lt', 'lte'].includes(op);
export const needsValues = (op: FilterOp) => op === 'in' || op === 'not_in';

/** Column kind, like the backend's domain.ColumnKind. */
export function columnKind(sourceType: string, dbType: string): string {
  const t = dbType.trim().toLowerCase();
  if (sourceType === 'mongodb') return ({ int: 'int', float: 'float', bool: 'bool', dict: 'document', list: 'list', datetime: 'timestamp' } as Record<string, string>)[t] ?? 'text';
  if (sourceType === 'spanner') {
    if (t.startsWith('array')) return 'list';
    // TIMESTAMP is ISO-8601 text; DATE stays text.
    return ({ int64: 'int', float64: 'float', float32: 'float', numeric: 'decimal', bool: 'bool', timestamp: 'timestamptz', bytes: 'binary', json: 'json' } as Record<string, string>)[t] ?? 'text';
  }
  if (sourceType === 'oracle') {
    const oracle = (
      {
        number: 'decimal',
        binary_float: 'float',
        binary_double: 'float',
        date: 'timestamp', // an Oracle DATE has a time of day
        'timestamp with time zone': 'timestamptz',
        'timestamp with local time zone': 'timestamptz',
        raw: 'binary',
        'long raw': 'binary',
      } as Record<string, string>
    )[t];
    if (oracle) return oracle;
  }
  if (t === 'boolean' || t === 'bool' || (t === 'bit' && sourceType === 'sqlserver')) return 'bool';
  if (['integer', 'int', 'bigint', 'smallint', 'tinyint', 'mediumint', 'serial', 'bigserial'].includes(t)) return 'int';
  if (t.startsWith('numeric') || t.startsWith('decimal') || t === 'money' || t === 'smallmoney') return 'decimal';
  if (['real', 'double precision', 'float', 'double'].includes(t)) return 'float';
  if (t === 'timestamp with time zone' || t === 'datetimeoffset' || (t === 'timestamp' && (sourceType === 'mysql' || sourceType === 'vitess'))) return 'timestamptz';
  if (t.startsWith('timestamp') || ['datetime', 'datetime2', 'smalldatetime'].includes(t)) return 'timestamp';
  if (t === 'date') return 'date';
  if (t.startsWith('time')) return 'time';
  if (t === 'bytea' || t.includes('blob') || ['binary', 'varbinary', 'image'].includes(t)) return 'binary';
  if (t === 'json' || t === 'jsonb') return 'json';
  if (t === 'uuid' || t === 'uniqueidentifier') return 'uuid';
  return 'text';
}

/** Operators that make sense for a column kind, with plain labels. */
export function operatorsFor(kind: string): { value: FilterOp; label: string }[] {
  const base: { value: FilterOp; label: string }[] = [
    { value: 'is_null', label: 'is empty' },
    { value: 'is_not_null', label: 'is not empty' },
  ];
  if (kind === 'bool') return [{ value: 'is_true', label: 'is true' }, { value: 'is_false', label: 'is false' }, ...base];
  if (kind === 'int' || kind === 'decimal' || kind === 'float') {
    return [
      { value: 'eq', label: '=' }, { value: 'ne', label: '≠' }, { value: 'gt', label: '>' }, { value: 'gte', label: '≥' },
      { value: 'lt', label: '<' }, { value: 'lte', label: '≤' }, { value: 'in', label: 'is one of' }, { value: 'not_in', label: 'is not one of' }, ...base,
    ];
  }
  return [{ value: 'eq', label: 'is' }, { value: 'ne', label: 'is not' }, { value: 'in', label: 'is one of' }, { value: 'not_in', label: 'is not one of' }, ...base];
}

export const isNumericKind = (kind: string) => kind === 'int' || kind === 'decimal' || kind === 'float';

/** Parses what the user typed for a rule value, typed by the column kind. */
export function parseRuleValue(kind: string, raw: string): RuleValue | null {
  const s = raw.trim();
  if (!s) return null;
  if (isNumericKind(kind)) {
    if (!/^-?(\d+\.?\d*|\.\d+)([eE][-+]?\d+)?$/.test(s)) return null;
    return Number(s);
  }
  return s;
}

/** The field names an event has before step `index` runs, for one table. */
export function fieldsBefore(steps: TransformStep[], index: number, table: TableInfo | undefined, spec: Pick<FullSpec, 'source'>): string[] {
  if (!table) return [];
  const excluded = new Set(spec.source.columns[table.name]?.exclude ?? []);
  let fields = table.columns.map((c) => c.name).filter((c) => !excluded.has(c));
  for (const t of steps.slice(0, index)) {
    if (t.tables && !t.tables.includes(table.name)) continue;
    if (t.type === 'unwrap') {
      if (t.delete_handling === 'rewrite') fields = [...fields, '__deleted'];
      fields = [...fields, ...(t.add_fields ?? []).map(metadataField)];
    } else if (t.type === 'rename') {
      const map = new Map((t.renames ?? []).map((r) => [r.from, r.to]));
      fields = fields.map((f) => map.get(f) ?? f);
    } else if (t.type === 'add_field' && t.name) {
      fields = [...fields, t.name];
    } else if (t.type === 'computed' && t.table === table.name && t.name) {
      fields = [...fields, t.name];
    } else if (t.type === 'soft_delete' && !fields.includes('__deleted')) {
      fields = [...fields, '__deleted'];
    }
  }
  return fields;
}

const plural = (n: number, w: string) => `${n} ${w}${n === 1 ? '' : 's'}`;
const scope = (t: TransformStep) => (t.tables?.length ? t.tables.join(', ') : 'all tables');

function ruleText(r: FilterRule): string {
  const op: Record<FilterOp, string> = {
    eq: 'is', ne: 'is not', gt: '>', gte: '≥', lt: '<', lte: '≤', in: 'is one of', not_in: 'is not one of',
    is_null: 'is empty', is_not_null: 'is not empty', is_true: 'is true', is_false: 'is false',
  };
  const v = needsValues(r.op) ? (r.values ?? []).join(', ') : needsValue(r.op) ? String(r.value ?? '') : '';
  return `${r.field} ${op[r.op]}${v ? ` ${v}` : ''}`;
}

/** One-line plain-language summary of a step. */
export function summarize(t: TransformStep): string {
  switch (t.type) {
    case 'unwrap': {
      const deletes = { rewrite: 'deletes kept with __deleted = true', tombstone: 'deletes become tombstones', drop: 'deletes dropped' }[t.delete_handling ?? 'rewrite'];
      const meta = (t.add_fields ?? []).map(metadataField);
      return `Row after change · ${deletes}${meta.length ? ` · adds ${meta.join(', ')}` : ''}`;
    }
    case 'filter': {
      if (t.condition) return `Script: ${t.condition}`;
      if (t.expression !== undefined) return `${scope(t)}: where ${t.expression || '…'}`;
      const ops = t.operations?.length && t.operations.length < 4 ? t.operations.join(' or ') : 'every operation';
      const rules = (t.rules ?? []).map(ruleText);
      return `${scope(t)}: ${ops}${rules.length ? `, where ${rules.join(t.match === 'any' ? ' or ' : ' and ')}` : ''}`;
    }
    case 'rename':
      return `${scope(t)}: ${(t.renames ?? []).map((r) => `${r.from || '…'} → ${r.to || '…'}`).join(', ')}`;
    case 'add_field':
      return `${scope(t)}: ${t.name || '…'} = ${t.value?.kind === 'pipeline' ? 'the pipeline name' : `"${t.value?.text ?? ''}"`}`;
    case 'flatten':
      return `${scope(t)}: nested fields joined with ${t.delimiter === '.' ? '.' : '_'}`;
    case 'custom':
      return `${scope(t)}: ${t.class || 'no class yet'} · ${plural(Object.keys(t.properties ?? {}).length, 'property')}`;
    case 'outbox':
      return 'Outbox event router';
    case 'soft_delete':
      return 'A deleted row stays, marked deleted (__deleted on Debezium, __op on Flink CDC), instead of being removed: it keeps rows, it is not a filter';
    case 'truncate':
      return (t.columns ?? []).map((c) => `${c.table}.${c.column || '…'} to ${c.length}`).join(', ') || 'No columns yet';
    case 'route':
      return (t.routes ?? []).map((r) => `${r.match || '…'} → ${r.to || '…'}`).join(', ') || 'No routes yet';
    case 'computed':
      return `${t.table || '…'}: ${t.name || '…'} = ${formulaText(t)}`;
  }
}

export function summarizeValues(v: ValueFormats | undefined): string | null {
  if (!v) return null;
  const parts = [
    v.decimals && v.decimals !== 'precise' ? `decimals as ${v.decimals === 'double' ? 'numbers' : 'text'}` : null,
    v.timestamps === 'isostring' ? 'timestamps as ISO-8601 text' : null,
    v.binary && v.binary !== 'bytes' ? `binary as ${v.binary}` : null,
  ].filter(Boolean);
  return parts.length ? parts.join(' · ') : null;
}

export interface Impact {
  kind: 'breaking' | 'key' | 'additive';
  text: string;
}

const stepKey = (t: TransformStep, i: number) => t.id ?? `#${i}:${t.type}`;

/**
 * What changes for consumers between the deployed spec and a new one, as the compiler runs them
 * (`effectiveDeletes`): Keep deleted rows on a full-envelope pipeline also turns events into rows.
 */
export function outputImpact(before: FullSpec, after: FullSpec, ctx: ShapeCtx = {}): Impact[] {
  const out: Impact[] = [];
  const oldSteps = new Map(before.transforms.map((t, i) => [stepKey(t, i), t]));
  const newSteps = new Map(after.transforms.map((t, i) => [stepKey(t, i), t]));
  const same = (a: unknown, b: unknown) => JSON.stringify(a) === JSON.stringify(b);
  const oldDeletes = effectiveDeletes(before.transforms, before.sink.options, ctx);
  const newDeletes = effectiveDeletes(after.transforms, after.sink.options, ctx);
  const oldRows = oldDeletes.mode !== 'envelope';
  const newRows = newDeletes.mode !== 'envelope';
  const softChanged = before.transforms.some((t) => t.type === 'soft_delete') !== after.transforms.some((t) => t.type === 'soft_delete');
  if (oldRows !== newRows) {
    out.push({ kind: 'breaking', text: newRows ? 'Events become the row after the change instead of the full change envelope.' : 'Events become the full change envelope (before/after/source) instead of the row.' });
  } else if (oldRows && newRows && !ctx.flink) {
    // Adding or removing Keep deleted rows has its own line below.
    if (oldDeletes.mode !== newDeletes.mode && !softChanged) out.push({ kind: 'breaking', text: `Deletes change: ${DELETE_TEXT[newDeletes.mode] ?? newDeletes.mode}.` });
    const oldShape = before.transforms.find((t) => t.type === 'unwrap');
    const newShape = after.transforms.find((t) => t.type === 'unwrap');
    if (oldShape && newShape && !same(oldShape.add_fields, newShape.add_fields)) out.push({ kind: 'breaking', text: 'The metadata fields added to every event change.' });
  }
  for (const [k, t] of newSteps) {
    if (t.type === 'unwrap') continue;
    const old = oldSteps.get(k);
    if (old && same(old, t)) continue;
    const what = summarize(t);
    if (t.type === 'add_field' && !old) out.push({ kind: 'additive', text: `New field ${t.name} (${scope(t)}).` });
    else if (t.type === 'soft_delete' && !old) out.push({ kind: 'breaking', text: 'Deleted rows now stay, marked deleted, instead of being removed: consumers get a new field.' });
    else if (t.type === 'computed' && !old) out.push({ kind: 'additive', text: `New column ${t.name} (${t.table}).` });
    else if (t.type === 'route') out.push({ kind: 'breaking', text: `${old ? 'Changed' : 'New'} routes — ${what}. Matched tables arrive in another destination.` });
    else if (t.type === 'filter') out.push({ kind: 'breaking', text: `${old ? 'Changed' : 'New'} filter — ${what}. Events that do not match stop arriving.` });
    else if (t.type === 'rename') {
      const tos = (s: TransformStep) => (s.renames ?? []).map((r) => r.to).sort().join(',');
      if (old && tos(old) === tos(t) && same(old.tables, t.tables)) out.push({ kind: 'additive', text: `Rename now reads ${(t.renames ?? []).map((r) => r.from).join(', ')}; output names unchanged.` });
      else out.push({ kind: 'breaking', text: `${old ? 'Changed' : 'New'} rename — ${what}.` });
    }
    else out.push({ kind: 'breaking', text: `${old ? 'Changed' : 'New'} step: ${TYPE_TITLE[t.type]} — ${what}.` });
  }
  for (const [k, t] of oldSteps) {
    if (t.type === 'unwrap' || newSteps.has(k)) continue;
    out.push({ kind: t.type === 'filter' ? 'additive' : 'breaking', text: `Removed: ${TYPE_TITLE[t.type]} — ${summarize(t)}.${t.type === 'filter' ? ' Events it dropped arrive again.' : ''}` });
  }
  if (!same(before.values ?? {}, after.values ?? {})) out.push({ kind: 'breaking', text: `Value formats change: ${summarizeValues(after.values) ?? 'connector defaults'}.` });
  const tables = new Set([...Object.keys(before.keys ?? {}), ...Object.keys(after.keys ?? {})]);
  for (const t of [...tables].sort()) {
    const a = before.keys?.[t];
    const b = after.keys?.[t];
    if (!same(a, b)) out.push({ kind: 'key', text: `${t}: messages are keyed by ${b ? b.join(', ') : 'the primary key'} instead of ${a ? a.join(', ') : 'the primary key'}. Kafka routes them to different partitions.` });
  }
  return out;
}

/** SMT classes Debezium Server 3.7.0.Final ships (backend domain.KnownTransformClasses). */
export const KNOWN_CLASSES = [
  'io.debezium.transforms.ByLogicalTableRouter',
  'io.debezium.transforms.ContentBasedRouter',
  'io.debezium.transforms.EnforceRecordSize',
  'io.debezium.transforms.ExtractChangedRecordState',
  'io.debezium.transforms.ExtractSchemaToNewRecord',
  'io.debezium.transforms.Filter',
  'io.debezium.transforms.GeometryFormatTransformer',
  'io.debezium.transforms.HeaderToValue',
  'io.debezium.transforms.SchemaChangeEventFilter',
  'io.debezium.transforms.SwapGeometryCoordinates',
  'io.debezium.transforms.TimezoneConverter',
  'io.debezium.transforms.ToLogicalTopicRouter',
  'io.debezium.transforms.partitions.PartitionRouting',
  'org.apache.kafka.connect.transforms.Cast$Value',
  'org.apache.kafka.connect.transforms.DropHeaders',
  'org.apache.kafka.connect.transforms.ExtractField$Value',
  'org.apache.kafka.connect.transforms.HeaderFrom$Value',
  'org.apache.kafka.connect.transforms.HoistField$Value',
  'org.apache.kafka.connect.transforms.InsertHeader',
  'org.apache.kafka.connect.transforms.MaskField$Value',
  'org.apache.kafka.connect.transforms.SetSchemaMetadata$Value',
  'org.apache.kafka.connect.transforms.TimestampRouter',
  'org.apache.kafka.connect.transforms.ValueToKey',
] as const;

/** Classes that run code from their properties (need the scripted-transforms policy). */
export const SCRIPTED_CLASSES = ['io.debezium.transforms.Filter', 'io.debezium.transforms.ContentBasedRouter'];

/**
 * Flink CDC's filter operations: it can't tell rows from the first copy from new inserts, so
 * "new rows" stands for both (the compiler refuses one without the other).
 */
export const FLINK_OPERATIONS: { value: 'new' | 'update' | 'delete'; label: string; ops: FilterOperation[] }[] = [
  { value: 'new', label: 'new rows (first copy and inserts)', ops: ['insert', 'snapshot'] },
  { value: 'update', label: 'update', ops: ['update'] },
  { value: 'delete', label: 'delete', ops: ['delete'] },
];

/** Turns one Flink CDC operation on or off in a filter's operations (insert and snapshot together). */
export function toggleFlinkOperation(ops: FilterOperation[], value: 'new' | 'update' | 'delete', on: boolean): FilterOperation[] {
  const pick = FLINK_OPERATIONS.find((o) => o.value === value)!.ops;
  const rest = ops.filter((o) => !pick.includes(o));
  const next = on ? [...rest, ...pick] : rest;
  return OPERATIONS.map((o) => o.value).filter((o) => next.includes(o));
}

/** A destination column of a Flink CDC table, where it comes from, and its type: the source's
 *  type for a source column, the kind a step writes for the others (`KIND_NAMES`). */
export interface FlinkColumn {
  name: string;
  from: { kind: 'column'; column: string; renamed: boolean; cut?: number } | { kind: 'added'; value: string } | { kind: 'computed'; formula: string; expression?: string };
  type: string;
  key: boolean;
}

/** A kind (columnKind, a computed function's result, the expression check) as a type name. */
export const KIND_NAMES: Record<string, string> = {
  text: 'text',
  int: 'whole number',
  decimal: 'decimal number',
  float: 'floating-point number',
  bool: 'true or false',
  date: 'date',
  time: 'time of day',
  timestamp: 'timestamp',
  timestamptz: 'timestamp with time zone',
  binary: 'binary',
  json: 'JSON',
  unknown: 'decided when it runs',
};

/** The kind a typed computed column writes (domain.ComputedFunctions `out`; '' is the kind of its
 *  first column), or 'unknown' (an expression: the server's check knows it). */
export function computedKind(t: TransformStep, table: TableInfo, sourceType: string): string {
  if (t.expression !== undefined || !t.function) return 'unknown';
  const out = COMPUTED_FUNCTIONS[t.function]?.out;
  if (out) return out;
  const first = t.args?.find((a) => a.column)?.column;
  const col = table.columns.find((c) => c.name === first);
  return col ? columnKind(sourceType, col.type) : 'unknown';
}

/** The table's renames as Flink CDC's projection reads them (domain.FlinkRenames): one map of
 *  every rename step that applies; a later rename of the same column wins. */
export function flinkRenames(table: string, steps: TransformStep[]): Map<string, string> {
  const out = new Map<string, string>();
  for (const t of steps) if (t.type === 'rename' && (!t.tables || t.tables.includes(table))) for (const r of t.renames ?? []) out.set(r.from, r.to);
  return out;
}

/** The columns steps add to a table on Flink CDC (domain.StepColumns), name → step: added fields
 *  that apply to it and its computed columns. */
export function stepColumns(table: string, steps: TransformStep[]): Map<string, TransformStep> {
  const out = new Map<string, TransformStep>();
  for (const t of steps) {
    if (t.type === 'add_field' && t.name && (!t.tables || t.tables.includes(table))) out.set(t.name, t);
    if (t.type === 'computed' && t.table === table && t.name) out.set(t.name, t);
  }
  return out;
}

/** One chosen key column as the table writes it, or why it can't be (domain.FlinkKey). */
export type FlinkKeyColumn = { name: string; written: string; from: 'column' | 'added' | 'computed'; source?: string };

/**
 * The table's chosen key columns on Flink CDC, in their order (domain.FlinkKey, ADR 0010): a name is
 * a source column the table writes first (written under its renamed name), else a column a step
 * adds. An error names the first column the table doesn't write. Without the table's columns
 * (not read yet) every name is taken as it is, through the renames, as the server does.
 */
export function flinkKey(table: TableInfo, spec: Pick<FullSpec, 'source' | 'keys'>, steps: TransformStep[]): { columns: FlinkKeyColumn[]; error: string | null } {
  const chosen = spec.keys?.[table.name] ?? [];
  const renames = flinkRenames(table.name, steps);
  const added = stepColumns(table.name, steps);
  const excluded = spec.source.columns?.[table.name]?.exclude ?? [];
  const columns: FlinkKeyColumn[] = [];
  const known = table.columns.length > 0;
  for (const k of chosen) {
    const left = excluded.includes(k);
    const step = added.get(k);
    const source = !known || table.columns.some((c) => c.name === k);
    // The new name a rename gives a source column the table writes (compiled as it is before ADR 0010).
    const renamedFrom = known && !renames.has(k) ? [...renames].find(([from, to]) => to === k && table.columns.some((c) => c.name === from) && !excluded.includes(from))?.[0] : undefined;
    if (source && !left && !(!known && step && !renames.has(k))) columns.push({ name: k, written: renames.get(k) ?? k, from: 'column', source: k });
    else if (renamedFrom) columns.push({ name: k, written: k, from: 'column', source: renamedFrom });
    else if (step) columns.push({ name: k, written: k, from: step.type === 'computed' ? 'computed' : 'added' });
    else
      return {
        columns,
        error: left ? `${table.name}: the key column ${k} is left out of the table. Keep it, or pick another key column.` : `${table.name} writes no column ${k} to key its rows by: pick the key columns again.`,
      };
  }
  return { columns, error: null };
}

/**
 * The columns Flink CDC writes for a captured table (as the compiler builds its projection): the
 * source's columns minus those left out, renamed where a rename step applies, then the columns
 * steps add, each with its type. Key: the key columns chosen for the table (flinkKey), else its
 * primary key.
 */
export function flinkColumns(table: TableInfo, spec: FullSpec, steps: TransformStep[], pipelineName: string, sourceType = ''): FlinkColumn[] {
  const applies = (t: TransformStep) => !t.tables || t.tables.includes(table.name);
  const renames = flinkRenames(table.name, steps);
  const cut = new Map<string, number>();
  for (const t of steps) if (t.type === 'truncate' && applies(t)) for (const c of t.columns ?? []) if (c.table === table.name) cut.set(c.column, c.length);
  const excluded = spec.source.columns?.[table.name]?.exclude ?? [];
  const chosen = spec.keys?.[table.name]?.length ? flinkKey(table, spec, steps).columns.map((k) => k.written) : table.primaryKey.map((k) => renames.get(k) ?? k);
  const out: FlinkColumn[] = table.columns
    .filter((c) => !excluded.includes(c.name))
    .map((c) => {
      const name = renames.get(c.name) ?? c.name;
      return { name, from: { kind: 'column', column: c.name, renamed: renames.has(c.name), ...(cut.has(c.name) ? { cut: cut.get(c.name) } : {}) }, type: c.type, key: chosen.includes(name) };
    });
  for (const t of steps) {
    if (t.type === 'computed' && t.table === table.name && t.name)
      out.push({ name: t.name, from: { kind: 'computed', formula: formulaText(t), ...(t.expression !== undefined ? { expression: t.expression } : {}) }, type: computedKind(t, table, sourceType), key: chosen.includes(t.name) });
    if (t.type !== 'add_field' || !applies(t) || !t.name) continue;
    out.push({ name: t.name, from: { kind: 'added', value: t.value?.kind === 'pipeline' ? pipelineName : (t.value?.text ?? '') }, type: 'text', key: chosen.includes(t.name) });
  }
  return out;
}

/** Argument slots (domain.Arg*): a text column, a column of any plain kind, a timestamp or date
 *  column, a column or a text, a number 1–65535, a date pattern; added with ADR 0008: a text, a
 *  number column, digits 0–18, a number column or a number, a date pattern to read text with. */
export type ArgSlot = 'text' | 'any' | 'time' | 'any|lit' | 'number' | 'pattern' | 'lit' | 'num' | 'digits' | 'num|lit' | 'date_pattern';

/** The picker's groups, in order. */
export const COMPUTED_GROUPS = ['Text', 'Numbers', 'Dates', 'Conversions', 'Empty values'] as const;

type ComputedSpec = { title: string; args: ArgSlot[]; min?: number; max?: number; out: string; help: string; group: (typeof COMPUTED_GROUPS)[number] };

/** The functions a computed column offers (domain.ComputedFunctions; backend/testdata/computed_functions.json
 *  keeps the two equal). `out` '' is the kind of the arguments. */
export const COMPUTED_FUNCTIONS: Record<ComputedFn, ComputedSpec> = {
  upper: { title: 'Upper case', args: ['text'], out: 'text', group: 'Text', help: 'The text in capitals.' },
  lower: { title: 'Lower case', args: ['text'], out: 'text', group: 'Text', help: 'The text in small letters.' },
  trim: { title: 'Trim spaces', args: ['text'], out: 'text', group: 'Text', help: 'Without spaces at either end.' },
  length: { title: 'Length', args: ['text'], out: 'int', group: 'Text', help: 'How many characters the text has.' },
  concat: { title: 'Join', args: ['any|lit'], min: 2, max: 5, out: 'text', group: 'Text', help: 'Columns and texts one after another; empty when a column is.' },
  substr: { title: 'Part of text', args: ['text', 'number', 'number'], out: 'text', group: 'Text', help: 'From a position (1 is the first character), so many characters.' },
  replace: { title: 'Replace text', args: ['text', 'lit', 'lit'], out: 'text', group: 'Text', help: 'Every occurrence of a text replaced by another.' },
  left: { title: 'First characters', args: ['text', 'number'], out: 'text', group: 'Text', help: 'So many characters from the start.' },
  right: { title: 'Last characters', args: ['text', 'number'], out: 'text', group: 'Text', help: 'So many characters from the end.' },
  pad_left: { title: 'Pad on the left', args: ['text', 'number', 'lit'], out: 'text', group: 'Text', help: 'Filled on the left to a length (longer text is cut).' },
  pad_right: { title: 'Pad on the right', args: ['text', 'number', 'lit'], out: 'text', group: 'Text', help: 'Filled on the right to a length (longer text is cut).' },
  join_with: { title: 'Join with a separator', args: ['lit', 'any|lit'], min: 3, max: 6, out: 'text', group: 'Text', help: 'A separator, then columns and texts; empty ones are skipped.' },
  abs: { title: 'Absolute value', args: ['num'], out: '', group: 'Numbers', help: 'The number without its sign.' },
  ceil: { title: 'Round up', args: ['num'], out: '', group: 'Numbers', help: 'The smallest whole number not below it.' },
  floor: { title: 'Round down', args: ['num'], out: '', group: 'Numbers', help: 'The largest whole number not above it.' },
  round: { title: 'Round', args: ['num', 'digits'], out: '', group: 'Numbers', help: 'Rounded to so many decimal places.' },
  add: { title: 'Add', args: ['num', 'num|lit'], out: 'float', group: 'Numbers', help: 'A number plus a column or a number (floating point).' },
  subtract: { title: 'Subtract', args: ['num', 'num|lit'], out: 'float', group: 'Numbers', help: 'A number minus a column or a number (floating point).' },
  multiply: { title: 'Multiply', args: ['num', 'num|lit'], out: 'float', group: 'Numbers', help: 'A number times a column or a number (floating point).' },
  divide: { title: 'Divide', args: ['num', 'num|lit'], out: 'float', group: 'Numbers', help: 'A number divided by a column or a number; empty when dividing by zero.' },
  date_format: { title: 'Date as text', args: ['time', 'pattern'], out: 'text', group: 'Dates', help: 'A timestamp or date written as a pattern, e.g. 2026-10.' },
  year: { title: 'Year', args: ['time'], out: 'int', group: 'Dates', help: 'The year of a date or timestamp.' },
  month: { title: 'Month', args: ['time'], out: 'int', group: 'Dates', help: 'The month, 1 to 12.' },
  day: { title: 'Day of the month', args: ['time'], out: 'int', group: 'Dates', help: 'The day, 1 to 31.' },
  hour: { title: 'Hour', args: ['time'], out: 'int', group: 'Dates', help: 'The hour, 0 to 23.' },
  parse_date: { title: 'Date from text', args: ['text', 'date_pattern'], out: 'date', group: 'Dates', help: 'Text read as a date with a pattern; empty when it isn’t one.' },
  days_between: { title: 'Days between', args: ['time', 'time'], out: 'int', group: 'Dates', help: 'Whole days from the first to the second.' },
  to_text: { title: 'As text', args: ['any'], out: 'text', group: 'Conversions', help: 'Any plain value as text.' },
  to_integer: { title: 'As a whole number', args: ['text'], out: 'int', group: 'Conversions', help: 'Text that holds a whole number; empty when it doesn’t.' },
  to_decimal: { title: 'As a decimal number', args: ['any', 'digits'], out: 'decimal', group: 'Conversions', help: 'With so many decimal places; empty when it isn’t a number.' },
  to_float: { title: 'As a floating-point number', args: ['any'], out: 'float', group: 'Conversions', help: 'Empty when it isn’t a number.' },
  first_set: { title: 'First that isn’t empty', args: ['any'], min: 2, max: 5, out: '', group: 'Empty values', help: 'The first of the columns that has a value (all of one kind).' },
};

export const DATE_PATTERNS = ['yyyy', 'yyyy-MM', 'yyyy-MM-dd', 'yyyy-MM-dd HH', 'yyyy-MM-dd HH:mm:ss'];
/** The patterns "Date from text" reads (domain.ParseDatePatterns). */
export const PARSE_DATE_PATTERNS = ['yyyy-MM-dd', 'yyyyMMdd', 'dd/MM/yyyy', 'MM/dd/yyyy'];
const DECIMAL_RE = /^-?(0|[1-9][0-9]{0,17})(\.[0-9]{1,9})?$/;

/** The slot of argument i. */
export function argSlot(fn: ComputedFn, i: number): ArgSlot {
  const a = COMPUTED_FUNCTIONS[fn].args;
  return a[Math.min(i, a.length - 1)]!;
}

/** Whether a column of kind (columnKind) fits a slot (domain.KindFits). */
export function kindFits(slot: ArgSlot, kind: string): boolean {
  if (slot === 'text') return kind === 'text';
  if (slot === 'time') return kind === 'timestamp' || kind === 'date' || kind === 'timestamptz';
  if (slot === 'num' || slot === 'num|lit') return kind === 'int' || kind === 'decimal' || kind === 'float';
  if (slot === 'any' || slot === 'any|lit') return !['binary', 'document', 'list', 'json', 'unknown'].includes(kind);
  return false;
}

/** The arguments a function starts with when picked. */
export function defaultArgs(fn: ComputedFn): ComputedArg[] {
  const f = COMPUTED_FUNCTIONS[fn];
  const n = f.min ?? f.args.length;
  return Array.from({ length: n }, (_, i): ComputedArg => {
    const slot = argSlot(fn, i);
    if (slot === 'number') return { number: 1 };
    if (slot === 'pattern') return { text: 'yyyy-MM-dd' };
    if (slot === 'date_pattern') return { text: 'yyyy-MM-dd' };
    if (slot === 'lit') return { text: '' };
    if (slot === 'digits') return { integer: 2 };
    return { column: '' };
  });
}

/** The formula in words, e.g. Upper case(status); an expression as it is stored. */
export function formulaText(t: TransformStep): string {
  if (t.expression !== undefined) return t.expression || '…';
  const fn = t.function ? COMPUTED_FUNCTIONS[t.function] : undefined;
  if (!fn) return '…';
  const args = (t.args ?? []).map((a) =>
    a.column !== undefined ? a.column || '…' : a.text !== undefined ? `"${a.text}"` : a.decimal !== undefined ? a.decimal : String(a.number ?? a.integer ?? '…'),
  );
  return `${fn.title}(${args.join(', ')})`;
}

/** Why a computed step can't be saved as it is (the decoder's rules); null when it can. */
export function computedError(t: TransformStep): string | null {
  if (!t.table) return 'Choose the table.';
  const e = nameError(t.name ?? '');
  if (e) return e;
  if (t.expression !== undefined) return t.expression.trim() ? null : 'Write the expression, or switch back to a function.';
  const fn = t.function ? COMPUTED_FUNCTIONS[t.function] : undefined;
  if (!fn) return 'Choose a function.';
  const args = t.args ?? [];
  for (const [i, a] of args.entries()) {
    const slot = argSlot(t.function!, i);
    if (slot === 'number' && !(Number.isInteger(a.number) && a.number! >= 1 && a.number! <= 65535)) return 'Numbers are whole, from 1 to 65535.';
    if (slot === 'digits' && !(Number.isInteger(a.integer) && a.integer! >= 0 && a.integer! <= 18)) return 'Decimal places are whole, from 0 to 18.';
    if ((slot === 'text' || slot === 'any' || slot === 'time' || slot === 'num') && !a.column) return 'Pick a column for every argument.';
    if ((slot === 'any|lit' || slot === 'num|lit') && a.column === '') return 'Pick a column for every argument.';
    if (slot === 'num|lit' && a.decimal !== undefined && !DECIMAL_RE.test(a.decimal)) return 'Write the number in full, e.g. 2.5 or -10 (no leading zeros).';
    if (a.text !== undefined && (slot === 'any|lit' || slot === 'lit')) {
      const te = textError(a.text);
      if (te) return te;
    }
  }
  return null;
}

/**
 * Where the event shape starts for ordering: the first stage-1 step, `unwrap` or `soft_delete`
 * (mirrors `unwrapAt` in domain.validateTransforms). Keep-events filters must come before it and
 * field changes after it. -1 when there is none.
 */
export function shapeIndex(steps: TransformStep[]): number {
  return steps.findIndex((t) => stageOf(t) === 1);
}

/** Where the Transform step draws a step. On Flink CDC the event shape isn't drawn (rows are always written). */
export type Section = 'keep' | 'shape' | 'fields' | 'other' | 'hidden';

export function sectionOf(t: TransformStep, flink: boolean): Section {
  if (t.type === 'unwrap') return flink ? 'hidden' : 'shape';
  if (t.type === 'soft_delete') return flink ? 'keep' : 'shape';
  const stage = stageOf(t);
  return stage === 0 ? 'keep' : stage === 2 ? 'fields' : 'other';
}

const SECTION_ORDER: Section[] = ['keep', 'shape', 'fields', 'other'];

/**
 * The chain indexes in the order the Transform step draws them: by section (keep, shape, field
 * changes, other), in chain order inside each, the shape row before Keep deleted rows. A step's
 * number on screen is its position here; hidden steps aren't drawn. Nothing reorders the spec.
 */
export function drawOrder(steps: TransformStep[], flink: boolean): number[] {
  const rank = (t: TransformStep) => (t.type === 'unwrap' ? 0 : t.type === 'soft_delete' ? 1 : 0);
  return SECTION_ORDER.flatMap((section) =>
    steps
      .map((t, i) => [t, i] as const)
      .filter(([t]) => sectionOf(t, flink) === section)
      .sort(([a, i], [b, j]) => (section === 'shape' ? rank(a) - rank(b) : 0) || i - j)
      .map(([, i]) => i),
  );
}

/** What the destination says about the event shape (its connector type's `unwrap`), and its option defaults. */
export interface ShapeCtx {
  flink?: boolean;
  unwrap?: { required: boolean; add_fields: string[]; delete_handling: string[]; delete_handling_from: { option: string; map: Record<string, string> } | null } | null;
  /** The destination options' schema defaults: the compiler reads options with them filled in. */
  optionDefaults?: Record<string, unknown>;
}

export type DeleteMode = 'rewrite' | 'tombstone' | 'drop' | 'envelope' | 'op_marked' | 'flink_default';

export interface Deletes {
  mode: DeleteMode;
  /** What decides it: Keep deleted rows, the destination's Deletes option, or the shape step. */
  by: 'keep_deleted' | 'destination' | 'shape';
  /** The drawn number of the first Keep deleted rows step, when it decides. */
  step?: number;
}

/**
 * What a delete really becomes once compiled: mirrors compiler.withSinkUnwrap then
 * withSoftDelete (compiler.go). Keep deleted rows wins over the destination's Deletes option,
 * which wins over the shape step's own choice; the destination's mapping applies only to an
 * unwrap the spec has (one the compiler adds takes the destination's first allowed mode).
 */
export function effectiveDeletes(steps: TransformStep[], options: Record<string, unknown> | undefined, ctx: ShapeCtx = {}): Deletes {
  const soft = steps.findIndex((t) => t.type === 'soft_delete');
  if (ctx.flink) return soft >= 0 ? { mode: 'op_marked', by: 'keep_deleted' } : { mode: 'flink_default', by: 'shape' };
  if (soft >= 0) return { mode: 'rewrite', by: 'keep_deleted', step: drawOrder(steps, false).indexOf(soft) + 1 };
  const unwrap = steps.find((t) => t.type === 'unwrap');
  const rule = ctx.unwrap;
  if (unwrap) {
    const from = rule?.delete_handling_from;
    if (from) {
      const value = { ...(ctx.optionDefaults ?? {}), ...(options ?? {}) }[from.option];
      const mapped = value === undefined || value === null ? undefined : from.map[String(value)];
      if (mapped) return { mode: mapped as DeleteMode, by: 'destination' };
    }
    return { mode: unwrap.delete_handling ?? 'rewrite', by: 'shape' };
  }
  if (rule?.required) return { mode: (rule.delete_handling[0] as DeleteMode | undefined) ?? 'rewrite', by: 'shape' };
  return { mode: 'envelope', by: 'shape' };
}

/**
 * The metadata fields the compiled row shape adds: the spec's unwrap with the destination's
 * required ones merged in (withSinkUnwrap), the operation and source time on an unwrap the
 * compiler adds for Keep deleted rows (withSoftDelete), else the destination's own.
 */
export function shapeFields(steps: TransformStep[], ctx: ShapeCtx = {}): string[] {
  const unwrap = steps.find((t) => t.type === 'unwrap');
  const required = ctx.unwrap?.add_fields ?? [];
  if (unwrap) return [...(unwrap.add_fields ?? []), ...required.filter((f) => !(unwrap.add_fields ?? []).includes(f))];
  if (steps.some((t) => t.type === 'soft_delete')) return ['op', 'source.ts_ms'];
  return required;
}

const DELETE_TEXT: Partial<Record<DeleteMode, string>> = { rewrite: 'deletes kept with __deleted = true', tombstone: 'deletes become tombstones', drop: 'deletes dropped' };

/**
 * The event shape in one line, as it runs: "Row after change · deletes kept with __deleted =
 * true (Keep deleted rows) · adds __op", or null for the full change envelope (and on Flink CDC).
 */
export function summarizeShape(steps: TransformStep[], options: Record<string, unknown> | undefined, ctx: ShapeCtx = {}): string | null {
  const d = effectiveDeletes(steps, options, ctx);
  const text = DELETE_TEXT[d.mode];
  if (!text) return null;
  const fields = shapeFields(steps, ctx);
  const why = d.by === 'keep_deleted' ? ' (Keep deleted rows)' : d.by === 'destination' ? ' (the destination’s Deletes setting)' : '';
  const meta = fields.map(metadataField);
  return `Row after change · ${text}${why}${meta.length ? ` · adds ${meta.join(', ')}` : ''}`;
}

/** A column the key editor offers: `value` is what spec.keys stores (a source column's own name, a
 *  step's column by its name), `label` the column the table writes; `why` when it can't be picked. */
export type KeyChoice = { value: string; label: string; type: string; from: 'column' | 'added' | 'computed'; renamedFrom?: string; why?: string; expression?: string; checking?: boolean };

/** The kinds a computed key column can have (domain.KeyKinds): BeziSync makes it NOT NULL with
 *  IFNULL and a fallback of that kind (ADR 0010). */
export const KEY_KINDS = ['text', 'int'];
/** Functions that can give no value for a value (domain: NeverNull of their tree). */
const EMPTY_ON_VALUES: ComputedFn[] = ['to_integer', 'to_float', 'to_decimal', 'parse_date', 'divide'];

/** Whether a typed computed column always has a value, given which of its columns are NOT NULL
 *  (expr.NeverNull of domain.ComputedTree; backend/testdata/key_columns.json holds the two equal). */
export function computedNeverEmpty(t: TransformStep, notNull: (column: string) => boolean): boolean {
  const fn = t.function;
  if (!fn || EMPTY_ON_VALUES.includes(fn)) return false;
  if (fn === 'join_with') return true; // CONCAT_WS skips empty values; its separator is a text
  const cols = (t.args ?? []).flatMap((a) => (a.column ? [a.column] : []));
  if (fn === 'first_set') return cols.some(notNull);
  return cols.every(notNull);
}

/** Why a typed computed column can't be in a Flink CDC key (domain.FlinkKeyProblems), or null. An
 *  expression is asked of the server (expressions:check: key_problem). */
export function computedKeyProblem(t: TransformStep, table: TableInfo): string | null {
  const out = t.function ? COMPUTED_FUNCTIONS[t.function]?.out : undefined;
  if (out === 'date') return 'A computed key column is text or a whole number. To partition by a date, put the timestamp column itself in the key and partition it by its day under Partition by.';
  if (!out || !KEY_KINDS.includes(out)) return 'A computed key column is text or a whole number.';
  const notNull = (c: string) => table.primaryKey.includes(c) || table.columns.some((x) => x.name === c && x.nullable === false);
  // Every column it reads must be NOT NULL: a value standing in for an empty one would give those
  // rows one key (domain.ComputedKeyProblem).
  const reads = (t.args ?? []).flatMap((a) => (a.column ? [a.column] : []));
  if (!reads.every(notNull) || !computedNeverEmpty(t, notNull)) {
    const empty = t.function && EMPTY_ON_VALUES.includes(t.function);
    return empty ? 'Can be empty: it gives no value for some rows.' : 'Can be empty: it reads a column the source allows to be empty.';
  }
  return null;
}

/**
 * What a table's key can be made of (ADR 0010). On Flink CDC every column the table writes: its
 * source columns under their written names (those left out are offered disabled), then the columns
 * steps add. On Debezium the message key is built in the source connector before any step runs: the
 * source's columns only, and the fields steps add are shown disabled with that reason.
 */
export function keyChoices(table: TableInfo, spec: Pick<FullSpec, 'source'>, steps: TransformStep[], flink: boolean, sourceType: string): KeyChoice[] {
  const excluded = spec.source.columns?.[table.name]?.exclude ?? [];
  const renames = flink ? flinkRenames(table.name, steps) : new Map<string, string>();
  const out: KeyChoice[] = table.columns.map((c) => {
    const to = renames.get(c.name);
    return {
      value: c.name,
      label: to ?? c.name,
      type: c.type,
      from: 'column',
      ...(to ? { renamedFrom: c.name } : {}),
      ...(flink && excluded.includes(c.name) ? { why: 'Left out of the table.' } : {}),
    };
  });
  for (const [name, t] of stepColumns(table.name, steps)) {
    if (flink && t.type === 'computed') {
      const why = t.expression !== undefined ? null : computedKeyProblem(t, table);
      out.push({ value: name, label: name, type: computedKind(t, table, sourceType), from: 'computed', ...(why ? { why } : {}), ...(t.expression !== undefined ? { expression: t.expression } : {}) });
    }
    else if (t.type === 'add_field')
      out.push({
        value: name,
        label: name,
        type: 'text',
        from: 'added',
        ...(flink ? {} : { why: 'Added by a step: Debezium builds the message key at the source, before any step runs.' }),
      });
  }
  // One choice per name, the one a key of that name resolves to (flinkKey): a source column the
  // table writes, else the step's column.
  const picked = (c: KeyChoice) => (c.from !== 'column' ? 1 : c.why ? 2 : 0);
  return out.filter((c) => !out.some((d) => d !== c && d.value === c.value && picked(d) < picked(c)));
}

/** What the server is asked about an expression column that could key its table: its columns
 *  with whether each can be empty (in the primary key or NOT NULL in the source: not). */
export function expressionKeyBody(t: TransformStep, table: TableInfo): { use: 'column'; table: string; expression: string; columns: { name: string; type: string; nullable: boolean }[] | null } {
  const columns = table.columns.map((c) => ({ name: c.name, type: c.type, nullable: !table.primaryKey.includes(c.name) && c.nullable !== false }));
  return { use: 'column', table: table.name, expression: t.expression ?? '', columns: columns.length ? columns : null };
}

/** Why an expression column can't be in a Flink CDC key, from the server's check (the rule
 *  preflight applies, domain.ComputedKeyProblem). While it is checked, or when the check failed, the
 *  answer blocks too: Continue waits for it. */
export function expressionKeyProblem(check: { error?: unknown; key_problem?: string | null } | undefined, failed = false): string | null {
  if (failed) return 'BeziSync couldn’t check its expression; try again.';
  if (!check) return 'Checking its expression…';
  if (check.error) return 'Its expression has an error: fix it in its step.';
  return check.key_problem ?? null;
}

/** The first reason a table's chosen Flink CDC key can't key its rows, or null (domain.FlinkKey,
 *  then domain.FlinkKeyProblems): a column it doesn't write, a cut source column, a computed column
 *  that can be empty or isn't text or a whole number. `expressionWhy` holds the server's answer for
 *  expression columns (by name). Checked only once the table's columns are known. */
export function flinkKeyProblem(table: TableInfo, spec: Pick<FullSpec, 'source' | 'keys'>, steps: TransformStep[], expressionWhy: Record<string, string | null | undefined> = {}): string | null {
  if (!spec.keys?.[table.name]?.length || !table.columns.length) return null;
  const key = flinkKey(table, spec, steps);
  if (key.error) return key.error;
  const added = stepColumns(table.name, steps);
  for (const k of key.columns) {
    if (k.from === 'column') {
      const cut = steps.flatMap((t) => (t.type === 'truncate' ? (t.columns ?? []) : [])).find((c) => c.table === table.name && c.column === (k.source ?? k.name));
      if (cut) return `${table.name}: the key column ${k.written} is cut to ${cut.length} characters, so rows that start alike would replace each other. Don't cut a key column.`;
    }
    if (k.from !== 'computed') continue;
    const t = added.get(k.name)!;
    const why = t.expression !== undefined ? (t.expression.trim() ? (k.name in expressionWhy ? expressionWhy[k.name]! : 'Checking its expression…') : 'Its expression is empty.') : computedKeyProblem(t, table);
    if (why) return `${table.name}: the key column ${k.written}: ${why}`;
  }
  // A key with a column a step adds keeps the whole primary key, or rows would share a key.
  if (table.primaryKey.length && key.columns.some((k) => k.from !== 'column')) {
    const missing = table.primaryKey.filter((c) => !key.columns.some((k) => k.from === 'column' && k.source === c));
    if (missing.length) return `${table.name}: a key with a column a step adds must keep the whole primary key (${missing.join(', ')}), or rows would share a key and replace each other.`;
  }
  return null;
}
