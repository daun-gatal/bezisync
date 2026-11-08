import { useState } from 'react';
import { Pill } from '@/components/ui/feedback';
import { Card } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import type { SinkCaps } from '@/lib/connections';
import { sinkDestinationFor, type FullSpec, type TransformStep } from '../spec';
import { useExpressionCheck } from '@/lib/transforms';
import { flinkColumns, KIND_NAMES, summarize, type FlinkColumn, type TableInfo } from './model';

/**
 * What a Flink CDC pipeline writes: for each captured table, the destination table and its
 * columns (renamed, added, computed, the key) with their types, and the filters that decide which
 * rows reach it. Flink CDC writes rows into tables, so there is no event to preview. A source
 * column shows the source's type (the destination maps it to its own); a computed column the kind
 * it writes, asked of the server for an expression.
 */
export function FlinkPreviewPanel({
  spec,
  steps,
  tables,
  pipelineName,
  sinkType,
  sinkCaps,
  sourceType,
  projectId,
  pipelineId,
}: {
  spec: FullSpec;
  steps: TransformStep[];
  tables: TableInfo[];
  pipelineName: string;
  sinkType: string | undefined;
  sinkCaps: SinkCaps;
  sourceType: string;
  projectId: string;
  pipelineId: string;
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
  const columns = flinkColumns(table, spec, steps, pipelineName, sourceType);
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
              <th className="py-1 pr-3 font-medium">Type</th>
              <th className="py-1 font-medium">From</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((c) => (
              <tr key={c.name} className="border-t border-line align-top">
                <td className="py-1.5 pr-3">
                  <span className="font-mono wrap-anywhere">{c.name}</span> {c.key ? <Pill tone="info">key</Pill> : null}
                </td>
                <td className="py-1.5 pr-3 wrap-anywhere">
                  <ColumnType column={c} table={table} projectId={projectId} pipelineId={pipelineId} />
                </td>
                <td className="py-1.5 text-muted wrap-anywhere">{fromText(c)}</td>
              </tr>
            ))}
            {keepsDeletes ? (
              <tr className="border-t border-line align-top">
                <td className="py-1.5 pr-3 font-mono">__op</td>
                <td className="py-1.5 pr-3">text</td>
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

function fromText(c: FlinkColumn): string {
  if (c.from.kind === 'added') return `added: "${c.from.value}"`;
  if (c.from.kind === 'computed') return `computed: ${c.from.formula}`;
  const base = c.from.renamed ? `renamed from ${c.from.column}` : 'source column';
  return c.from.cut ? `${base}, cut to ${c.from.cut} characters` : base;
}

/** A column's type: the source's for a source column, the kind a step writes for the others; an
 *  expression's kind comes from the server's check (the same request its editor makes). */
function ColumnType({ column, table, projectId, pipelineId }: { column: FlinkColumn; table: TableInfo; projectId: string; pipelineId: string }) {
  const expression = column.from.kind === 'computed' && column.from.expression?.trim() ? column.from.expression : null;
  const check = useExpressionCheck(
    projectId,
    pipelineId,
    expression ? { use: 'column', table: table.name, expression, columns: table.columns.length ? table.columns.map((c) => ({ name: c.name, type: c.type })) : null } : null,
  );
  if (column.from.kind === 'column') return <span className="font-mono">{column.type}</span>;
  const kind = expression ? (check.data && !check.data.error ? (check.data.kind ?? 'unknown') : 'unknown') : column.type;
  return <span className="text-muted">{KIND_NAMES[kind] ?? kind}</span>;
}
