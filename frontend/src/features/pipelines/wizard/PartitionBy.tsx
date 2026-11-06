import { Plus, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/form';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import { jsonbSorted } from '@/lib/settings';
import { columnKind } from '../transforms/model';

/** One partition field, in the order the server's JSONB keeps its keys. */
export interface PartitionField {
  n: number;
  column: string;
  transform: PartitionTransform;
}
export type PartitionTransform = 'identity' | 'year' | 'month' | 'day' | 'hour' | 'bucket' | 'truncate';
export type PartitionBy = Record<string, PartitionField[]>;

const TRANSFORMS: { value: PartitionTransform; label: string; fits: (kind: string) => boolean }[] = [
  { value: 'identity', label: 'Each value', fits: (k) => !['binary', 'document', 'list', 'json'].includes(k) },
  { value: 'year', label: 'Year', fits: (k) => ['timestamp', 'timestamptz', 'date'].includes(k) },
  { value: 'month', label: 'Month', fits: (k) => ['timestamp', 'timestamptz', 'date'].includes(k) },
  { value: 'day', label: 'Day', fits: (k) => ['timestamp', 'timestamptz', 'date'].includes(k) },
  { value: 'hour', label: 'Hour', fits: (k) => ['timestamp', 'timestamptz'].includes(k) },
  { value: 'bucket', label: 'Buckets of a hash', fits: (k) => !['document', 'list', 'json'].includes(k) },
  { value: 'truncate', label: 'Leading part', fits: (k) => ['text', 'int', 'decimal'].includes(k) },
];

/** Why partitioning can't be saved as it is (IcebergPartitionBy's rules); null when it can. */
export function partitionError(by: PartitionBy): string | null {
  for (const [table, fields] of Object.entries(by)) {
    for (const f of fields) {
      if (!f.column) return `${table}: pick a column for every field.`;
      if (f.transform === 'bucket' && !(f.n >= 1 && f.n <= 1024)) return `${table}: a bucket count from 1 to 1024.`;
      if (f.transform === 'truncate' && !(f.n >= 1 && f.n <= 65535)) return `${table}: a width of at least 1.`;
    }
    const exprs = fields.map((f) => `${f.transform}${f.n}${f.column}`);
    if (new Set(exprs).size !== exprs.length) return `${table}: a field is listed twice.`;
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

interface Discovered {
  schema: string;
  name: string;
  columns?: { name: string; type: string }[] | null;
}

/**
 * Partition by (Flink CDC on Iceberg): per captured table, up to four fields, each a column and how
 * it partitions — the transforms offered fit the column's type. Flink CDC applies it when it
 * creates the table; an existing table keeps its own (preflight compares them).
 */
export function PartitionByEditor({ value, onChange, tables, discovered, excluded, sourceType, disabled }: {
  value: PartitionBy;
  onChange: (v: PartitionBy) => void;
  tables: string[];
  discovered: Discovered[] | undefined;
  excluded: Record<string, string[]>;
  sourceType: string;
  disabled?: boolean;
}) {
  const put = (table: string, fields: PartitionField[]) => onChange({ ...value, [table]: fields });
  return (
    <section aria-label="Partition by" className="flex min-w-0 flex-col gap-3">
      <div className="flex flex-col gap-0.5">
        <strong className="text-sm font-semibold">Partition by</strong>
        <Meta>How Iceberg splits each table’s files. Applied when Flink CDC creates the table; a table that already exists keeps its partitioning.</Meta>
      </div>
      {tables.map((table) => {
        const fields = value[table] ?? [];
        const found = discovered?.find((d) => `${d.schema}.${d.name}` === table);
        const cols = (found?.columns ?? []).filter((c) => !(excluded[table] ?? []).includes(c.name));
        const kindOf = (name: string) => columnKind(sourceType, cols.find((c) => c.name === name)?.type ?? '');
        return (
          <div key={table} role="group" aria-label={`Partition ${table}`} className="flex min-w-0 flex-col gap-2 rounded-md border border-line p-3">
            <div className="flex items-center justify-between gap-2">
              <span className="font-mono text-sm">{table}</span>
              {fields.length ? null : <span className="text-xs text-muted">Not partitioned</span>}
            </div>
            {fields.map((f, i) => {
              const options = TRANSFORMS.filter((t) => !f.column || t.fits(kindOf(f.column)));
              const set = (g: Partial<PartitionField>) => put(table, fields.map((x, j) => (j === i ? { ...x, ...g } : x)));
              return (
                <div key={i} className="grid min-w-0 grid-cols-[minmax(0,1fr)_minmax(0,1fr)_6rem_auto] items-end gap-2">
                  <Select label={`${table} field ${i + 1} column`} value={f.column} disabled={disabled}
                    options={cols.map((c) => ({ value: c.name, label: c.name, description: c.type }))}
                    onChange={(column) => {
                      const keep = TRANSFORMS.find((t) => t.value === f.transform)?.fits(kindOf(column));
                      set({ column, ...(keep ? {} : { transform: 'identity', n: 0 }) });
                    }} />
                  <Select label={`${table} field ${i + 1} transform`} value={f.transform} disabled={disabled}
                    options={options.map((t) => ({ value: t.value, label: t.label }))}
                    onChange={(transform) => set({ transform: transform as PartitionTransform, n: transform === 'bucket' ? 16 : transform === 'truncate' ? 10 : 0 })} />
                  {f.transform === 'bucket' || f.transform === 'truncate' ? (
                    <Input aria-label={`${table} field ${i + 1} ${f.transform === 'bucket' ? 'buckets' : 'width'}`} type="number" min={1} max={f.transform === 'bucket' ? 1024 : 65535}
                      value={f.n} disabled={disabled} onChange={(e) => set({ n: Math.trunc(Number(e.target.value)) || 0 })} />
                  ) : <span />}
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
