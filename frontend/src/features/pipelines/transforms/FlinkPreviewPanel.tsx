import { useState } from 'react';
import { Pill } from '@/components/ui/feedback';
import { Card } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import type { SinkCaps } from '@/lib/connections';
import { sinkDestinationFor, type FullSpec, type TransformStep } from '../spec';
import { flinkColumns, summarize, type TableInfo } from './model';

/**
 * What a Flink CDC pipeline writes: for each captured table, the destination table and its
 * columns (renamed, added, the key), and the filters that decide which rows reach it. Flink CDC
 * writes rows into tables, so there is no event to preview.
 */
export function FlinkPreviewPanel({
  spec,
  steps,
  tables,
  pipelineName,
  sinkType,
  sinkCaps,
}: {
  spec: FullSpec;
  steps: TransformStep[];
  tables: TableInfo[];
  pipelineName: string;
  sinkType: string | undefined;
  sinkCaps: SinkCaps;
}) {
  const [tableName, setTableName] = useState(tables[0]?.name);
  const table = tables.find((t) => t.name === tableName) ?? tables[0];
  if (!table) {
    return (
      <Card title="Destination table">
        <p className="m-0 text-sm text-muted">Choose tables first.</p>
      </Card>
    );
  }
  const columns = flinkColumns(table, spec, steps, pipelineName);
  const destination = sinkDestinationFor(spec.sink.naming, table.name, pipelineName, sinkCaps).toLowerCase();
  const filters = steps.filter((t) => t.type === 'filter' && (!t.tables || t.tables.includes(table.name)));
  // Iceberg keeps deleted rows by default, marked with their change type (connectors/iceberg_flink.go).
  const keepsDeletes = sinkType === 'iceberg' && (spec.sink.options?.write_mode ?? 'upsert_keep_deletes') === 'upsert_keep_deletes';
  const noKey = !columns.some((c) => c.key);
  return (
    <Card title="Destination table">
      <div className="flex min-w-0 flex-col gap-3">
        {tables.length > 1 ? (
          <Select
            label="Table"
            value={table.name}
            onChange={setTableName}
            options={tables.map((t) => ({ value: t.name, label: t.name }))}
          />
        ) : null}
        <p className="m-0 text-sm">
          <span className="text-muted">{table.name} →</span> <code className="font-mono wrap-anywhere">{destination}</code>
        </p>
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs text-muted">
              <th className="py-1 font-medium">Column</th>
              <th className="py-1 font-medium">From</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((c) => (
              <tr key={c.name} className="border-t border-line align-top">
                <td className="py-1.5 pr-3">
                  <span className="font-mono wrap-anywhere">{c.name}</span> {c.key ? <Pill tone="info">key</Pill> : null}
                </td>
                <td className="py-1.5 text-muted wrap-anywhere">
                  {c.from.kind === 'added' ? `added: "${c.from.value}"` : c.from.renamed ? `renamed from ${c.from.column}` : 'source column'}
                </td>
              </tr>
            ))}
            {keepsDeletes ? (
              <tr className="border-t border-line align-top">
                <td className="py-1.5 pr-3 font-mono">__op</td>
                <td className="py-1.5 text-muted">change type: deleted rows stay, marked -D</td>
              </tr>
            ) : null}
          </tbody>
        </table>
        {noKey ? <p className="m-0 text-xs text-bad">This table has no key: choose key columns, Flink CDC writes each row by its key.</p> : null}
        <p className="m-0 text-xs text-muted">
          {filters.length ? `Rows kept: ${filters.map((f) => summarize(f)).join('; ')}.` : 'Every row is kept.'} New columns in the source wait for the next
          deploy unless the review step sends them as they appear.
        </p>
      </div>
    </Card>
  );
}
