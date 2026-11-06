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
      return { type, id, operations: ['insert', 'update', 'delete', 'snapshot'], match: 'all', rules: [] };
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
    if (t.type === 'filter' && stage !== 0 && !t.condition) out.set(i, 'Choose at least one operation or add a rule.');
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

/** A destination column of a Flink CDC table, and where it comes from. */
export interface FlinkColumn {
  name: string;
  from: { kind: 'column'; column: string; renamed: boolean } | { kind: 'added'; value: string } | { kind: 'computed'; formula: string };
  key: boolean;
}

/**
 * The columns Flink CDC writes for a captured table (as the compiler builds its projection): the
 * source's columns minus those left out, renamed where a rename step applies, then added fields.
 * Key: the key columns chosen for the table, else its primary key.
 */
export function flinkColumns(table: TableInfo, spec: FullSpec, steps: TransformStep[], pipelineName: string): FlinkColumn[] {
  const applies = (t: TransformStep) => !t.tables || t.tables.includes(table.name);
  const renames = new Map<string, string>();
  for (const t of steps) if (t.type === 'rename' && applies(t)) for (const r of t.renames ?? []) renames.set(r.from, r.to);
  const excluded = spec.source.columns?.[table.name]?.exclude ?? [];
  const keys = spec.keys?.[table.name]?.length ? spec.keys[table.name]! : table.primaryKey;
  const out: FlinkColumn[] = table.columns
    .filter((c) => !excluded.includes(c.name))
    .map((c) => ({ name: renames.get(c.name) ?? c.name, from: { kind: 'column', column: c.name, renamed: renames.has(c.name) }, key: keys.includes(c.name) }));
  for (const t of steps) {
    if (t.type === 'computed' && t.table === table.name && t.name) out.push({ name: t.name, from: { kind: 'computed', formula: formulaText(t) }, key: false });
    if (t.type !== 'add_field' || !applies(t) || !t.name) continue;
    out.push({ name: t.name, from: { kind: 'added', value: t.value?.kind === 'pipeline' ? pipelineName : (t.value?.text ?? '') }, key: false });
  }
  return out;
}

/** Argument slots (domain.Arg*): a text column, a column of any plain kind, a timestamp or date
 *  column, a column or a text, a number 1–65535, a date pattern. */
export type ArgSlot = 'text' | 'any' | 'time' | 'any|lit' | 'number' | 'pattern';

/** The functions a computed column offers (domain.ComputedFunctions). */
export const COMPUTED_FUNCTIONS: Record<ComputedFn, { title: string; args: ArgSlot[]; min?: number; max?: number; out: string; help: string }> = {
  upper: { title: 'Upper case', args: ['text'], out: 'text', help: 'The text in capitals.' },
  lower: { title: 'Lower case', args: ['text'], out: 'text', help: 'The text in small letters.' },
  trim: { title: 'Trim spaces', args: ['text'], out: 'text', help: 'Without spaces at either end.' },
  length: { title: 'Length', args: ['text'], out: 'int', help: 'How many characters the text has.' },
  concat: { title: 'Join', args: ['any|lit'], min: 2, max: 5, out: 'text', help: 'Columns and texts one after another; empty when a column is.' },
  substr: { title: 'Part of text', args: ['text', 'number', 'number'], out: 'text', help: 'From a position (1 is the first character), so many characters.' },
  date_format: { title: 'Date as text', args: ['time', 'pattern'], out: 'text', help: 'A timestamp or date written as a pattern, e.g. 2026-10.' },
  to_text: { title: 'As text', args: ['any'], out: 'text', help: 'Any plain value as text.' },
  to_integer: { title: 'As a whole number', args: ['text'], out: 'int', help: 'Text that holds a whole number; empty when it doesn’t.' },
};

export const DATE_PATTERNS = ['yyyy', 'yyyy-MM', 'yyyy-MM-dd', 'yyyy-MM-dd HH', 'yyyy-MM-dd HH:mm:ss'];

/** The slot of argument i. */
export function argSlot(fn: ComputedFn, i: number): ArgSlot {
  const a = COMPUTED_FUNCTIONS[fn].args;
  return a[Math.min(i, a.length - 1)]!;
}

/** Whether a column of kind (columnKind) fits a slot (domain.KindFits). */
export function kindFits(slot: ArgSlot, kind: string): boolean {
  if (slot === 'text') return kind === 'text';
  if (slot === 'time') return kind === 'timestamp' || kind === 'date';
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
    return { column: '' };
  });
}

/** The formula in words, e.g. Upper case(status). */
export function formulaText(t: TransformStep): string {
  const fn = t.function ? COMPUTED_FUNCTIONS[t.function] : undefined;
  if (!fn) return '…';
  const args = (t.args ?? []).map((a) => (a.column !== undefined ? a.column || '…' : a.text !== undefined ? `"${a.text}"` : String(a.number ?? '…')));
  return `${fn.title}(${args.join(', ')})`;
}

/** Why a computed step can't be saved as it is (the decoder's rules); null when it can. */
export function computedError(t: TransformStep): string | null {
  if (!t.table) return 'Choose the table.';
  const e = nameError(t.name ?? '');
  if (e) return e;
  const fn = t.function ? COMPUTED_FUNCTIONS[t.function] : undefined;
  if (!fn) return 'Choose a function.';
  const args = t.args ?? [];
  for (const [i, a] of args.entries()) {
    const slot = argSlot(t.function!, i);
    if (slot === 'number' && !(Number.isInteger(a.number) && a.number! >= 1 && a.number! <= 65535)) return 'Numbers are whole, from 1 to 65535.';
    if ((slot === 'text' || slot === 'any' || slot === 'time') && !a.column) return 'Pick a column for every argument.';
    if (slot === 'any|lit' && a.column === '') return 'Pick a column for every argument.';
    if (a.text !== undefined && slot === 'any|lit') {
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

const DELETE_TEXT: Partial<Record<DeleteMode, string>> = { rewrite: 'deletes kept with __deleted = true', tombstone: 'deletes become tombstones', drop: 'deletes dropped' };

/**
 * The event shape in one line, as it runs: "Row after change · deletes kept with __deleted =
 * true (Keep deleted rows) · adds __op", or null for the full change envelope (and on Flink CDC).
 */
export function summarizeShape(steps: TransformStep[], options: Record<string, unknown> | undefined, ctx: ShapeCtx = {}): string | null {
  const d = effectiveDeletes(steps, options, ctx);
  const text = DELETE_TEXT[d.mode];
  if (!text) return null;
  const unwrap = steps.find((t) => t.type === 'unwrap');
  // An unwrap the compiler adds for Keep deleted rows carries the operation and source time.
  const fields = unwrap ? (unwrap.add_fields ?? []) : steps.some((t) => t.type === 'soft_delete') ? ['op', 'source.ts_ms'] : (ctx.unwrap?.add_fields ?? []);
  const why = d.by === 'keep_deleted' ? ' (Keep deleted rows)' : d.by === 'destination' ? ' (the destination’s Deletes setting)' : '';
  const meta = fields.map(metadataField);
  return `Row after change · ${text}${why}${meta.length ? ` · adds ${meta.join(', ')}` : ''}`;
}
