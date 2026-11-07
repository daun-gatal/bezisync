import { Plus, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/form';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import { jsonbSorted } from '@/lib/settings';
import type { EngineId } from '@/lib/engines';
import type { FullSpec, ValueFormats } from '../spec';
import { COMPUTED_FUNCTIONS, columnKind, routeTargets } from '../transforms/model';

/** One partition field, in the order the server's JSONB keeps its keys. */
export interface PartitionField {
  n: number;
  column: string;
  transform: PartitionTransform;
}
export type PartitionTransform = 'identity' | 'year' | 'month' | 'day' | 'hour' | 'bucket' | 'truncate';
export type PartitionBy = Record<string, PartitionField[]>;

/** Iceberg's transform table on BeziSync's written kinds (domain.PartitionTransforms; the shared
 *  vectors in backend/testdata/partition_kinds.json keep the two equal). */
export const PARTITION_TRANSFORMS: Record<PartitionTransform, string[]> = {
  identity: ['int', 'decimal', 'float', 'bool', 'text', 'uuid', 'date', 'time', 'timestamp', 'timestamptz', 'binary'],
  bucket: ['int', 'decimal', 'text', 'uuid', 'date', 'time', 'timestamp', 'timestamptz', 'binary'],
  truncate: ['int', 'decimal', 'text', 'binary'],
  year: ['date', 'timestamp', 'timestamptz'],
  month: ['date', 'timestamp', 'timestamptz'],
  day: ['date', 'timestamp', 'timestamptz'],
  hour: ['timestamp', 'timestamptz'],
};

export const partitionFits = (transform: PartitionTransform, kind: string) => PARTITION_TRANSFORMS[transform].includes(kind);

const TIME_TRANSFORMS: PartitionTransform[] = ['year', 'month', 'day', 'hour'];
/** truncate's widest width (descriptor maximum). */
export const MAX_TRUNCATE = 1_000_000;
/** Debezium's time of the change in the table (domain.SourceTimestampField). */
export const SOURCE_TS = '__source_ts_ms';

const LABELS: Record<PartitionTransform, string> = {
  identity: 'Each value',
  year: 'Year',
  month: 'Month',
  day: 'Day',
  hour: 'Hour',
  bucket: 'Buckets of a hash',
  truncate: 'Leading part',
};

/** The kind an engine writes into the Iceberg table for a source column (domain.PartitionKind);
 *  '' for a value no transform takes. */
export function partitionKind(engine: EngineId, sourceType: string, dbType: string, values: ValueFormats | undefined): string {
  const t = dbType.trim().toLowerCase();
  const kind = columnKind(sourceType, dbType);
  if (kind === 'document' || kind === 'list') return '';
  if (kind === 'json') return 'text';
  if (sourceType === 'postgresql' && (t === 'array' || t === 'user-defined' || t.startsWith('_') || t.endsWith('[]'))) return '';
  if (engine === 'flink-cdc') {
    if (kind === 'uuid') return 'text';
    if (sourceType === 'mysql' && t.startsWith('tinyint')) return 'bool';
    return kind;
  }
  if (kind === 'decimal' && ['cockroachdb', 'vitess', 'spanner'].includes(sourceType)) return 'text';
  if (kind === 'decimal' && values?.decimals === 'double') return 'float';
  if (kind === 'decimal' && values?.decimals === 'string') return 'text';
  if (kind === 'decimal' && sourceType === 'postgresql') return '';
  if (kind === 'binary' && values?.binary && values.binary !== 'bytes') return 'text';
  if (kind === 'time') return 'int';
  if (kind === 'timestamptz' && sourceType === 'spanner') return 'text';
  return kind;
}

interface Discovered {
  schema: string;
  name: string;
  columns?: { name: string; type: string }[] | null;
  primary_key?: string[] | null;
}

/** A column the table writes, with its kind and whether upserts delete by it. */
export interface WrittenColumn {
  name: string;
  kind: string;
  key: boolean;
}

type SpecParts = Pick<FullSpec, 'source' | 'transforms' | 'keys' | 'values'>;

/** A column's name in the written row after the table's renames (iceberg_partition.go
 *  renamedTo): one step after another on Debezium, one map of every rename on Flink CDC. */
export function renamedTo(spec: SpecParts, table: string, name: string, engine: EngineId): string {
  const flat = new Map<string, string>();
  let current = name;
  for (const t of spec.transforms) {
    if (t.type !== 'rename' || (t.tables && !t.tables.includes(table))) continue;
    for (const r of t.renames ?? []) {
      if (engine === 'flink-cdc') flat.set(r.from, r.to);
      else if (r.from === current) {
        current = r.to;
        break;
      }
    }
  }
  return flat.get(current) ?? current;
}

/** The columns upserts delete by (iceberg_partition.go equalityFields); empty when the table is
 *  appended. */
export function equalityFields(found: Discovered, spec: SpecParts, engine: EngineId, writeMode: unknown): string[] {
  const id = `${found.schema}.${found.name}`;
  const cols = found.columns ?? [];
  const pk = found.primary_key ?? [];
  if (engine === 'flink-cdc') {
    const keys = spec.keys?.[id]?.length ? spec.keys[id]! : pk;
    return keys.map((k) => renamedTo(spec, id, k, engine));
  }
  if (writeMode === 'append') return [];
  const chosen = (spec.keys?.[id] ?? []).filter((k) => cols.some((c) => c.name === k));
  const key = chosen.length ? chosen : pk;
  const excluded = spec.source.columns[id]?.exclude ?? [];
  // A key column renamed or left out drops out of the table's identity.
  return key.filter((k) => !excluded.includes(k) && renamedTo(spec, id, k, engine) === k);
}

/** The columns a table writes into Iceberg on an engine (iceberg_partition.go writtenColumns). */
export function writtenColumns(found: Discovered, spec: SpecParts, engine: EngineId, sourceType: string, writeMode: unknown): WrittenColumn[] {
  const id = `${found.schema}.${found.name}`;
  const excluded = spec.source.columns[id]?.exclude ?? [];
  const keys = equalityFields(found, spec, engine, writeMode);
  const out = new Map<string, string>();
  for (const c of found.columns ?? []) {
    if (excluded.includes(c.name)) continue;
    out.set(renamedTo(spec, id, c.name, engine), partitionKind(engine, sourceType, c.type, spec.values));
  }
  for (const t of spec.transforms) {
    if (t.type === 'add_field' && t.name && (!t.tables || t.tables.includes(id))) out.set(t.name, 'text');
    if (t.type === 'computed' && t.table === id && t.name && engine === 'flink-cdc' && t.function) out.set(t.name, COMPUTED_FUNCTIONS[t.function].out);
    if (t.type === 'soft_delete') out.set(engine === 'flink-cdc' ? '__op' : '__deleted', 'text');
  }
  if (engine !== 'flink-cdc') {
    out.set('__op', 'text');
    out.set('__deleted', 'text');
    out.set(SOURCE_TS, 'timestamptz');
  }
  return [...out].map(([name, kind]) => ({ name, kind, key: keys.includes(name) }));
}

/** What a Debezium table Partition by doesn't list gets (iceberg_partition.go debeziumAutomatic),
 *  in words; null on Flink CDC. */
export function automaticSummary(engine: EngineId, options: Record<string, unknown>, table: string, routed: boolean): string | null {
  if (engine === 'flink-cdc') return null;
  if (options.partitioning === 'none' || routed) return 'Not partitioned';
  if (options.write_mode === 'append') return 'Automatic: by the day of the change';
  const recorded = (options.partitions as Record<string, string> | undefined)?.[table];
  if (recorded === undefined) return 'Not partitioned';
  if (recorded === '') return 'Automatic: by the day of the change';
  return `Automatic: ${String(options.buckets ?? '16')} buckets of ${recorded}`;
}

/** Why a column can't be picked for a field (null when it can): upserts delete by the key only
 *  within a partition (ADR 0005 R5). */
export function columnRefusal(col: WrittenColumn, keys: string[]): string | null {
  if (!col.kind) return 'Written as a list, a document or a structure: it can’t be partitioned.';
  if (keys.length && !col.key) return 'Not part of the key: an update that changes it would leave the old row behind.';
  return null;
}

/** Why partitioning can't be saved as it is (IcebergPartitionBy's rules, and with the tables'
 *  written columns the preflight rules); null when it can. */
export function partitionError(by: PartitionBy, written?: Record<string, { columns: WrittenColumn[]; keys: string[] }>): string | null {
  for (const [table, fields] of Object.entries(by)) {
    for (const f of fields) {
      if (!f.column) return `${table}: pick a column for every field.`;
      if (f.transform === 'bucket' && !(f.n >= 1 && f.n <= 1024)) return `${table}: a bucket count from 1 to 1024.`;
      if (f.transform === 'truncate' && !(f.n >= 1 && f.n <= MAX_TRUNCATE)) return `${table}: a width from 1 to 1,000,000.`;
      const w = written?.[table];
      const col = w?.columns.find((c) => c.name === f.column);
      if (w && !col) return `${table}: ${f.column} is no longer a column the table writes.`;
      if (w && col) {
        const refusal = columnRefusal(col, w.keys);
        if (refusal) return `${table}: ${f.column}: ${refusal}`;
        if (!partitionFits(f.transform, col.kind)) return `${table}: ${LABELS[f.transform]} doesn’t fit ${f.column}.`;
      }
    }
    const exprs = fields.map((f) => `${f.transform}${f.n}${f.column}`);
    if (new Set(exprs).size !== exprs.length) return `${table}: a field is listed twice.`;
    const timed = fields.filter((f) => TIME_TRANSFORMS.includes(f.transform)).map((f) => f.column);
    if (new Set(timed).size !== timed.length) return `${table}: one time transform per column (Year, Month, Day or Hour).`;
  }
  return null;
}

/** partition_by as stored: tables without fields left out, keys in JSONB order. */
export function cleanPartitionBy(by: PartitionBy): PartitionBy {
  const out: PartitionBy = {};
  for (const [t, fields] of Object.entries(by)) {
    if (fields.length) out[t] = fields.map((f) => ({ n: f.transform === 'bucket' || f.transform === 'truncate' ? f.n : 0, column: f.column, transform: f.transform }));
  }
  return jsonbSorted(out);
}

/** Partition by without fields an engine doesn't have (Debezium's time of the change on Flink CDC). */
export function partitionByFor(by: PartitionBy, engine: EngineId): PartitionBy {
  if (engine !== 'flink-cdc') return by;
  const out: PartitionBy = {};
  for (const [t, fields] of Object.entries(by)) {
    const kept = fields.filter((f) => f.column !== SOURCE_TS);
    if (kept.length) out[t] = kept;
  }
  return out;
}

/** Each captured table's written columns and the key its upserts delete by, from discovery. */
export function writtenByTable(tables: string[], discovered: Discovered[] | undefined, spec: SpecParts, engine: EngineId, sourceType: string, writeMode: unknown) {
  const out: Record<string, { columns: WrittenColumn[]; keys: string[] }> = {};
  for (const t of tables) {
    const found = discovered?.find((d) => `${d.schema}.${d.name}` === t);
    if (found) out[t] = { columns: writtenColumns(found, spec, engine, sourceType, writeMode), keys: equalityFields(found, spec, engine, writeMode) };
  }
  return out;
}

/**
 * Partition by (Iceberg, both engines): per captured table, up to four fields, each a column the
 * table writes and how it partitions. The transforms offered fit the column's written kind; on a
 * table upserted by key only key columns can be picked (a delete reaches only its own partition).
 * The engine applies it when it creates the table; an existing table keeps its own (preflight
 * compares them). A Debezium table without fields shows what it gets automatically.
 */
export function PartitionByEditor({ value, onChange, tables, discovered, spec, engine, sourceType, options, disabled }: {
  value: PartitionBy;
  onChange: (v: PartitionBy) => void;
  tables: string[];
  discovered: Discovered[] | undefined;
  spec: SpecParts;
  engine: EngineId;
  sourceType: string;
  options: Record<string, unknown>;
  disabled?: boolean;
}) {
  const put = (table: string, fields: PartitionField[]) => onChange({ ...value, [table]: fields });
  const written = writtenByTable(tables, discovered, spec, engine, sourceType, options.write_mode);
  const routed = routeTargets(spec.transforms, tables);
  return (
    <section aria-label="Partition by" className="flex min-w-0 flex-col gap-3">
      <div className="flex flex-col gap-0.5">
        <strong className="text-sm font-semibold">Partition by</strong>
        <Meta>
          How Iceberg splits each table’s files. Applied when the table is created; a table that already exists keeps its partitioning. Rows upserted by key can be
          partitioned only by key columns.
        </Meta>
      </div>
      {tables.map((table) => {
        const fields = value[table] ?? [];
        const w = written[table];
        const cols = w?.columns ?? [];
        const keys = w?.keys ?? [];
        const kindOf = (name: string) => cols.find((c) => c.name === name)?.kind ?? '';
        const automatic = automaticSummary(engine, options, table, routed.has(table));
        return (
          <div key={table} role="group" aria-label={`Partition ${table}`} className="flex min-w-0 flex-col gap-2 rounded-md border border-line p-3">
            <div className="flex min-w-0 flex-wrap items-center justify-between gap-2">
              <span className="min-w-0 break-all font-mono text-sm">{table}</span>
              {fields.length ? null : <span className="text-xs text-muted">{automatic ?? 'Not partitioned'}</span>}
            </div>
            {fields.map((f, i) => {
              const known = cols.some((c) => c.name === f.column);
              const options = (Object.keys(LABELS) as PartitionTransform[]).filter((t) => !f.column || !known || partitionFits(t, kindOf(f.column)) || t === f.transform);
              const set = (g: Partial<PartitionField>) => put(table, fields.map((x, j) => (j === i ? { ...x, ...g } : x)));
              const columnOptions = cols.map((c) => {
                const why = columnRefusal(c, keys);
                return { value: c.name, label: c.name === SOURCE_TS ? `${c.name} (time of the change)` : c.name, description: c.key ? 'Key column' : undefined, disabled: Boolean(why), why: why ?? undefined };
              });
              if (f.column && !known) columnOptions.push({ value: f.column, label: `${f.column} (no longer written)`, description: undefined, disabled: true, why: 'The table no longer writes this column.' });
              return (
                <div key={i} className="grid min-w-0 grid-cols-1 items-end gap-2 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_7rem_auto]">
                  <Select label={`${table} field ${i + 1} column`} value={f.column} disabled={disabled}
                    options={columnOptions}
                    onChange={(column) => {
                      const keep = partitionFits(f.transform, kindOf(column));
                      set({ column, ...(keep ? {} : { transform: 'identity', n: 0 }) });
                    }} />
                  <Select label={`${table} field ${i + 1} transform`} value={f.transform} disabled={disabled}
                    options={options.map((t) => ({ value: t, label: LABELS[t] }))}
                    onChange={(transform) => set({ transform: transform as PartitionTransform, n: transform === 'bucket' ? 16 : transform === 'truncate' ? 10 : 0 })} />
                  {f.transform === 'bucket' || f.transform === 'truncate' ? (
                    <Input aria-label={`${table} field ${i + 1} ${f.transform === 'bucket' ? 'buckets' : 'width'}`} type="number" min={1} max={f.transform === 'bucket' ? 1024 : MAX_TRUNCATE}
                      value={f.n} disabled={disabled} onChange={(e) => set({ n: Math.trunc(Number(e.target.value)) || 0 })} />
                  ) : <span className="hidden sm:block" />}
                  <Button size="sm" variant="ghost" icon={<X />} aria-label={`Remove ${table} field ${i + 1}`} disabled={disabled} onClick={() => put(table, fields.filter((_, j) => j !== i))} />
                </div>
              );
            })}
            {disabled || fields.length >= 4 ? null : (
              <div>
                <Button size="sm" variant="ghost" icon={<Plus />} disabled={!cols.length} onClick={() => put(table, [...fields, { n: 0, column: '', transform: 'identity' }])}>
                  Add a partition field
                </Button>
              </div>
            )}
          </div>
        );
      })}
    </section>
  );
}
