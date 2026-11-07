import type { PipelineSpec } from '@/lib/pipelines';
import type { SinkCaps } from '@/lib/connections';
import { cleanValues, type FormValues, type JsonSchema } from '@/components/SchemaForm';
import { jsonbSorted } from '@/lib/settings';
import type { EngineId, FlinkChoices } from '@/lib/engines';

/** The spec with every default filled in (mirrors backend defaults in domain/spec.py).
 *  Fields this file does not know are kept, so saving from the wizard never drops settings
 *  made through YAML or the API. */
export interface FullSpec {
  /** columns: per table, masked and removed columns, and personal data kept as is on purpose. */
  source: { tables: { include: string[] }; columns: Record<string, { mask: string[]; exclude: string[]; keep?: string[] }> };
  sink: {
    naming: string;
    format: { type: 'json' | 'avro' | 'protobuf' | 'cloudevents'; include_schema: boolean; registry_url: string | null; registry_kind: 'apicurio' | 'confluent' };
    options: Record<string, unknown>;
  };
  snapshot: { mode: SnapshotMode; chunk_size: number; fetch_size: number; delay_seconds: number; window: string | null };
  transforms: TransformStep[];
  policies: {
    heartbeat_seconds: number;
    resources: 'small' | 'medium' | 'large' | 'custom';
    /** The size when resources is 'custom' (Debezium only). */
    custom_resources?: { cpu: string; memory: string };
    errors: { retries: number; on_poison_event: 'fail' | 'warn' | 'skip' };
    latency_target_seconds: number;
    source_retained_limit_gb: number;
    delivery_canary: boolean;
  };
  debezium_image: string | null;
  advanced: Record<string, string>;
  /** Typed settings from the registry (GET /engines), key → canonical value; absent = none. */
  tuning?: Record<string, string>;
  /** The runtime's placement profile the pods run with (Kubernetes); dumped only when set. */
  placement?: string;
  /** Connector value encodings; absent = connector defaults. */
  values?: ValueFormats;
  /** Message key columns per captured table; absent = primary keys. On Flink CDC: the
   *  destination's primary key for a table that has none. */
  keys?: Record<string, string[]>;
  /** Flink CDC pipelines only: their choices and image pin. */
  flink_cdc?: Partial<FlinkChoices>;
  flink_cdc_image?: string | null;
}

/** The spec with only the settings of its engine: Flink CDC and Debezium refuse each other's. */
export function specForEngine(spec: FullSpec, engine: EngineId): FullSpec {
  const tuning = tuningFor(spec.tuning, engine);
  if (engine === 'flink-cdc') {
    const { values: _values, tuning: _t, ...rest } = spec;
    void _values;
    void _t;
    return {
      ...rest,
      sink: { ...spec.sink, options: withoutSourceTimestamp(spec.sink.options) },
      debezium_image: null,
      advanced: {},
      snapshot: { ...spec.snapshot, delay_seconds: 0, window: null },
      ...(tuning ? { tuning } : {}),
    };
  }
  const { flink_cdc: _fc, flink_cdc_image: _img, tuning: _t, ...rest } = spec;
  void _fc;
  void _img;
  void _t;
  return { ...rest, ...(tuning ? { tuning } : {}) };
}

/** Iceberg Partition by without Debezium's time of the change (__source_ts_ms), which Flink CDC
 *  doesn't write; a table left without fields is dropped, and so is an empty Partition by. */
function withoutSourceTimestamp(options: Record<string, unknown>): Record<string, unknown> {
  const by = options.partition_by as Record<string, { column: string }[]> | undefined;
  if (!by) return options;
  if (!Object.values(by).some((fields) => fields.some((f) => f.column === '__source_ts_ms'))) return options;
  const kept = Object.fromEntries(Object.entries(by).map(([t, f]) => [t, f.filter((x) => x.column !== '__source_ts_ms')] as const).filter(([, f]) => f.length));
  const { partition_by: _pb, ...rest } = options;
  void _pb;
  return Object.keys(kept).length ? { ...rest, partition_by: kept } : rest;
}

/** The settings of one engine: Debezium keys start with "debezium.", Flink CDC keys with the YAML
 *  block they go in ("source.", "sink.", "pipeline."). Undefined when none are left. */
export function tuningFor(tuning: Record<string, string> | null | undefined, engine: EngineId): Record<string, string> | undefined {
  const kept = Object.entries(tuning ?? {}).filter(([k]) => (engine === 'debezium') === k.startsWith('debezium.'));
  return kept.length ? Object.fromEntries(kept) : undefined;
}

export type TransformType = 'unwrap' | 'filter' | 'outbox' | 'rename' | 'add_field' | 'flatten' | 'custom' | 'soft_delete' | 'truncate' | 'route' | 'computed';
export type FilterOperation = 'insert' | 'update' | 'delete' | 'snapshot';
export type FilterOp = 'eq' | 'ne' | 'in' | 'not_in' | 'gt' | 'gte' | 'lt' | 'lte' | 'is_null' | 'is_not_null' | 'is_true' | 'is_false';
export type RuleValue = string | number | boolean;
export interface FilterRule { field: string; op: FilterOp; value?: RuleValue; values?: RuleValue[] }
export interface ValueFormats { decimals?: 'precise' | 'double' | 'string'; timestamps?: 'epoch' | 'isostring'; binary?: 'bytes' | 'base64' | 'hex' }

/** One step of spec.transforms (backend domain/transforms.go). Python-era types (unwrap, filter
 *  with a raw condition, outbox) carry delete_handling / add_fields / condition; the rest only
 *  their own fields. */
export interface TransformStep {
  type: TransformType;
  id?: string;
  tables?: string[];
  delete_handling?: 'rewrite' | 'tombstone' | 'drop';
  add_fields?: string[];
  condition?: string | null;
  operations?: FilterOperation[];
  match?: 'all' | 'any';
  rules?: FilterRule[];
  renames?: { from: string; to: string }[];
  name?: string;
  value?: { kind: 'static' | 'pipeline'; text?: string };
  delimiter?: '_' | '.';
  class?: string;
  properties?: Record<string, string>;
  /** truncate: text columns cut to at most length characters. */
  columns?: { table: string; column: string; length: number }[];
  routes?: { match: string; to: string }[];
  /** computed: the table, the function and its arguments (name is the new column). */
  table?: string;
  function?: ComputedFn;
  args?: ComputedArg[];
}

export type ComputedFn = 'upper' | 'lower' | 'trim' | 'length' | 'concat' | 'substr' | 'date_format' | 'to_text' | 'to_integer';
/** One argument: a column, a text or a number (exactly one). */
export interface ComputedArg { column?: string; text?: string; number?: number }

/** Stage of a step: 0 keep events, 1 event shape, 2 field changes, -1 order as written. */
export function stageOf(t: TransformStep): number {
  if (t.type === 'filter' && ((t.operations?.length ?? 0) > 0 || (t.rules?.length ?? 0) > 0)) return 0;
  if (t.type === 'unwrap' || t.type === 'soft_delete') return 1;
  if (t.type === 'rename' || t.type === 'add_field' || t.type === 'flatten' || t.type === 'custom') return 2;
  return -1;
}

/** Keeps only references to captured tables: scoped steps lose removed tables (a step left
 *  with none is dropped), and key columns of removed tables go. */
export function pruneToTables(spec: FullSpec, include: string[]): Pick<FullSpec, 'transforms' | 'keys'> {
  const keep = new Set(include);
  const transforms = spec.transforms.flatMap((t) => {
    if (!t.tables) return [t];
    const tables = t.tables.filter((x) => keep.has(x));
    return tables.length ? [{ ...t, tables }] : [];
  });
  const keys = spec.keys ? Object.fromEntries(Object.entries(spec.keys).filter(([t]) => keep.has(t))) : undefined;
  return { transforms, ...(keys && Object.keys(keys).length ? { keys } : { keys: undefined }) };
}

/** Puts (or removes, with null) the event-shape step without breaking the stage order: an
 *  existing one is replaced in place, a new one goes after the keep-events filters. */
export function withShape(steps: TransformStep[], shape: TransformStep | null): TransformStep[] {
  const at = steps.findIndex((t) => t.type === 'unwrap');
  if (!shape) return steps.filter((t) => t.type !== 'unwrap');
  if (at >= 0) return steps.map((t, i) => (i === at ? { ...t, ...shape } : t));
  const after = steps.reduce((last, t, i) => (stageOf(t) === 0 ? i : last), -1);
  return [...steps.slice(0, after + 1), shape, ...steps.slice(after + 1)];
}

export type SnapshotMode = 'initial' | 'no_data' | 'when_needed' | 'initial_only' | 'always';

export const SNAPSHOT_MODES: { value: SnapshotMode; title: string; help: string }[] = [
  { value: 'initial', title: 'Copy existing rows, then stream changes', help: 'Recommended. Debezium: snapshot.mode=initial' },
  { value: 'no_data', title: 'Only new changes', help: 'Skip existing rows. Debezium: no_data' },
  { value: 'when_needed', title: 'Copy existing rows only if needed', help: 'Re-copies when the stored position is missing or invalid. Debezium: when_needed' },
  { value: 'initial_only', title: 'Copy existing rows once, no streaming', help: 'A one-off export. Debezium: initial_only' },
  { value: 'always', title: 'Copy existing rows on every start', help: 'Rarely useful; expensive. Debezium: always' },
];

/** Custom size choices, as the spec stores them (domain.ParseCPU, ParseMemory). */
export const CUSTOM_CPUS = Array.from({ length: 32 }, (_, i) => String(((i + 1) * 250) / 1000));
export const CUSTOM_MEMORIES = Array.from({ length: 63 }, (_, i) => {
  const mib = 512 + i * 256;
  return mib % 1024 === 0 ? `${mib / 1024}Gi` : `${mib}Mi`;
});
/** MiB of a memory choice. */
export const memoryMiB = (m: string) => (m.endsWith('Gi') ? Number(m.slice(0, -2)) * 1024 : Number(m.slice(0, -2)));
/** The Java heap BeziSync works out from memory (domain.HeapMiB): 70%, at least 256 MiB left to the JVM. */
export const heapMiB = (mem: number) => Math.min(Math.floor((mem * 7) / 10), mem - 256);
/** Preset sizes in numbers, for comparing a custom size with a limit. */
export const PRESET_AMOUNTS = { small: { cpu: 0.5, mem: 768 }, medium: { cpu: 1, mem: 1536 }, large: { cpu: 2, mem: 3072 } } as const;

export const RESOURCES = [
  { value: 'small', title: 'Small', help: '0.5 CPU · 768 MiB · up to ~2k changes/s' },
  { value: 'medium', title: 'Medium', help: '1 CPU · 1.5 GiB · up to ~10k changes/s' },
  { value: 'large', title: 'Large', help: '2 CPU · 3 GiB · bulk snapshots and wide tables' },
] as const;

export function fullSpec(spec: PipelineSpec | null | undefined): FullSpec {
  const s = (spec ?? {}) as Partial<FullSpec>;
  return {
    ...s,
    source: { ...s.source, tables: { ...s.source?.tables, include: s.source?.tables?.include ?? [] }, columns: s.source?.columns ?? {} },
    sink: {
      ...s.sink,
      naming: s.sink?.naming ?? 'cdc.{schema}.{table}',
      format: {
        type: s.sink?.format?.type ?? 'json',
        include_schema: s.sink?.format?.include_schema ?? false,
        registry_url: s.sink?.format?.registry_url ?? null,
        registry_kind: s.sink?.format?.registry_kind ?? 'apicurio',
      },
      options: s.sink?.options ?? {},
    },
    snapshot: {
      ...s.snapshot,
      mode: s.snapshot?.mode ?? 'initial',
      chunk_size: s.snapshot?.chunk_size ?? 1024,
      fetch_size: s.snapshot?.fetch_size ?? 10000,
      delay_seconds: s.snapshot?.delay_seconds ?? 0,
      window: s.snapshot?.window ?? null,
    },
    transforms: s.transforms ?? [{ type: 'unwrap', delete_handling: 'rewrite', add_fields: ['op', 'source.ts_ms'], condition: null }],
    policies: {
      ...s.policies,
      heartbeat_seconds: s.policies?.heartbeat_seconds ?? 10,
      resources: s.policies?.resources ?? 'small',
      errors: { retries: s.policies?.errors?.retries ?? -1, on_poison_event: s.policies?.errors?.on_poison_event ?? 'fail' },
      latency_target_seconds: s.policies?.latency_target_seconds ?? 60,
      source_retained_limit_gb: s.policies?.source_retained_limit_gb ?? 10,
      delivery_canary: s.policies?.delivery_canary ?? false,
    },
    debezium_image: s.debezium_image ?? null,
    advanced: s.advanced ?? {},
  };
}

/** Preview the destination name of one table for a naming template. */
export function destinationFor(naming: string, table: string, pipeline: string): string {
  const [schema = '', name = ''] = table.split('.', 2);
  return naming.replaceAll('{schema}', schema).replaceAll('{table}', name).replaceAll('{pipeline}', pipeline);
}

export function namingError(naming: string): string | null {
  if (!naming.includes('{table}')) return 'Must contain {table} so each table gets its own destination.';
  const unknown = (naming.match(/\{[^}]*\}/g) ?? []).filter((p) => !['{schema}', '{table}', '{pipeline}'].includes(p));
  if (unknown.length) return `Unknown placeholder ${unknown.join(', ')}. Use {schema}, {table} or {pipeline}.`;
  if (!/^[A-Za-z0-9._\-{}]+$/.test(naming)) return "Use letters, digits, '.', '_', '-' and placeholders only.";
  return null;
}

/** The destination name a sink really uses (some turn dots into underscores). */
export function sinkDestinationFor(naming: string, table: string, pipeline: string, caps: SinkCaps): string {
  const name = destinationFor(naming, table, pipeline);
  return caps.naming.flatten_dots ? name.replaceAll('.', '_') : name;
}

/** namingError plus the sink's own rules (characters, length of the names it would create). */
export function sinkNamingError(naming: string, caps: SinkCaps, tables: string[], pipeline: string): string | null {
  const generic = namingError(naming);
  if (generic || caps.naming.single) return generic;
  const pattern = new RegExp(caps.naming.pattern);
  const literal = naming.replace(/\{(schema|table|pipeline)\}/g, 'x');
  if (!pattern.test(caps.naming.flatten_dots ? literal.replaceAll('.', '_') : literal)) return caps.naming.message;
  for (const t of tables) {
    const name = sinkDestinationFor(naming, t, pipeline, caps);
    if (!pattern.test(name)) return `“${name}” (for ${t}) is not allowed here. ${caps.naming.message}`;
    if (name.length > caps.naming.max_length) return `“${name}” (for ${t}) is longer than ${caps.naming.max_length} characters.`;
  }
  return null;
}

export function parseAdvanced(text: string): { values: Record<string, string>; error: string | null } {
  const values: Record<string, string> = {};
  for (const [i, raw] of text.split('\n').entries()) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq <= 0) return { values, error: `Line ${i + 1}: expected key=value` };
    values[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  }
  return { values, error: null };
}

interface DiscoveredKey {
  schema: string;
  name: string;
  primary_key?: string[] | null;
}

/** Fills the sink options the wizard derives from discovery: a hidden field with `ui.from:
 *  "table_keys"` maps each captured table to its first key column (the chosen message key, else
 *  the primary key; "" without one). Tables discovery does not know are left out. */
export function deriveSinkOptions(
  options: Record<string, unknown>,
  schema: JsonSchema | null | undefined,
  include: string[],
  keys: Record<string, string[]>,
  discovered: DiscoveredKey[] | undefined,
): Record<string, unknown> {
  if (!schema?.properties || !discovered) return options;
  let out = options;
  for (const [name, prop] of Object.entries(schema.properties)) {
    if (prop.ui?.from !== 'table_keys') continue;
    const map: Record<string, string> = {};
    for (const t of include) {
      const found = discovered.find((d) => `${d.schema}.${d.name}` === t);
      if (!found && !keys[t]) continue;
      map[t] = keys[t]?.[0] ?? found?.primary_key?.[0] ?? '';
    }
    out = { ...out, [name]: map };
  }
  return out;
}

/** The recorded table keys (`ui.from: table_keys`, Debezium Iceberg's automatic partitioning) again
 *  for the tables whose chosen key changed from `before` to `after`, by deriveSinkOptions' rule
 *  (ADR 0007 P3). Other tables keep what was recorded, so saving without a key change saves the
 *  same options (the same object). */
export function rederiveTableKeys(
  options: Record<string, unknown>,
  schema: JsonSchema | null | undefined,
  include: string[],
  before: Record<string, string[]>,
  after: Record<string, string[]>,
  discovered: DiscoveredKey[] | undefined,
): Record<string, unknown> {
  const changed = include.filter((t) => JSON.stringify(before[t] ?? []) !== JSON.stringify(after[t] ?? []));
  if (!schema?.properties || !discovered || !changed.length) return options;
  let out = options;
  for (const [name, prop] of Object.entries(schema.properties)) {
    if (prop.ui?.from !== 'table_keys') continue;
    const recorded = options[name];
    if (!recorded || typeof recorded !== 'object') continue;
    const map = { ...(recorded as Record<string, string>) };
    for (const t of changed) {
      const found = discovered.find((d) => `${d.schema}.${d.name}` === t);
      if (!found && !after[t]) continue;
      map[t] = after[t]?.[0] ?? found?.primary_key?.[0] ?? '';
    }
    if (JSON.stringify(map) !== JSON.stringify(recorded)) out = jsonbSorted({ ...out, [name]: jsonbSorted(map) });
  }
  return out;
}

/** Destination options a Flink CDC pipeline has: Iceberg keeps how rows are written (upserts by
 *  key: no append) and the table format; commit timing and bucket partitioning are Debezium's. */
const FLINK_OPTIONS = ['write_mode', 'format_version'];

/** A Flink CDC pipeline's destination options: its engine's form fields, plus Partition by (which
 *  its own card edits, outside the form), in the server's JSONB key order. */
export function flinkSinkOptions(schema: JsonSchema, options: Record<string, unknown>): Record<string, unknown> {
  const out = cleanValues(schema, options as FormValues);
  const by = options.partition_by;
  return by && typeof by === 'object' && Object.keys(by).length ? jsonbSorted({ ...out, partition_by: by }) : out;
}

/** Why Flink CDC can't write an Iceberg change log (ADR 0007 P4): its writer always upserts by key. */
export const FLINK_NO_APPEND = "Flink CDC's Iceberg writer always writes rows by the table's key, so it can't keep every change as its own row.";

/** The destination options an engine has. On Flink CDC, Iceberg's change log is shown unavailable
 *  with the reason, naming Debezium when it is a real choice (`debeziumOffered`). */
export function optionsSchemaFor(schema: JsonSchema | null | undefined, engine: EngineId, opts: { debeziumOffered?: boolean } = {}): JsonSchema | null {
  if (!schema) return null;
  if (engine !== 'flink-cdc' || !schema.properties) return schema;
  const properties: NonNullable<JsonSchema['properties']> = {};
  for (const [name, prop] of Object.entries(schema.properties)) {
    if (!FLINK_OPTIONS.includes(name)) continue;
    properties[name] =
      name === 'write_mode' && prop.enum
        ? {
            ...prop,
            // Flink CDC marks a kept delete with its change type, not Debezium's __deleted; a change log
            // is shown, unavailable, with the reason.
            ui: {
              ...prop.ui,
              labels: { ...prop.ui?.labels, upsert_keep_deletes: "Latest row; deletes kept with __op = '-D'" },
              ...(prop.enum.includes('append')
                ? { unavailable: { append: FLINK_NO_APPEND + (opts.debeziumOffered ? ' Choose Debezium above for a change log.' : '') } }
                : {}),
            },
          }
        : prop;
  }
  return Object.keys(properties).length ? { ...schema, properties } : null;
}

/** The Destination step's options with the stored Partition by carried over unchanged (ADR 0007
 *  P2: the Transform step writes it), or without it once the destination isn't Iceberg. */
export function keepPartitionBy(options: Record<string, unknown>, stored: unknown, iceberg: boolean): Record<string, unknown> {
  const { partition_by: _pb, ...rest } = options;
  void _pb;
  return iceberg && stored && typeof stored === 'object' && Object.keys(stored).length ? jsonbSorted({ ...rest, partition_by: stored }) : rest;
}
