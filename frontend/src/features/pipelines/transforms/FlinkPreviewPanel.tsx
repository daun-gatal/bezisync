import { useState } from 'react';
import { Pill } from '@/components/ui/feedback';
import { Card } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import type { SinkCaps } from '@/lib/connections';
import { sinkDestinationFor, type FullSpec, type TransformStep } from '../spec';
import { useExpressionCheck } from '@/lib/transforms';
import { flinkColumns, flinkSoftDeleteField, KIND_NAMES, summarize, type FlinkColumn, type FlinkWords, type TableInfo } from './model';

/**
 * What a Flink CDC pipeline writes: for each captured table, the destination table and its
 * columns (renamed, added, computed, the key) with their types, and the filters that decide which
 * rows reach it. There is no sample event to preview (the server renders Debezium's only). A source
 * column shows the source's type (the destination maps it to its own); a computed column the kind
 * it writes, asked of the server for an expression. On a destination Flink CDC writes as change
 * events (Kafka, ADR 0011) the same columns are the fields of each event's before and after, and the
 * panel says what the topic, the key and the value hold.
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
  words,
}: {
  words: FlinkWords;
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
      <Card title={words.previewTitle}>
        <p className="m-0 text-sm text-muted">Choose tables first.</p>
      </Card>
    );
  }
  const columns = flinkColumns(table, spec, steps, pipelineName, sourceType);
  const destination = sinkDestinationFor(spec.sink.naming, table.name, pipelineName, sinkCaps).toLowerCase();
  const filters = steps.filter((t) => t.type === 'filter' && (!t.tables || t.tables.includes(table.name)));
  // Iceberg keeps deleted rows by default, marked with their change type (connectors/iceberg_flink.go).
  // Keep deleted rows adds the change type too, on every destination (compiler: SOFT_DELETE).
  const softDelete = steps.some((t) => t.type === 'soft_delete');
  const keepsDeletes = softDelete || (sinkType === 'iceberg' && (spec.sink.options?.write_mode ?? 'upsert_keep_deletes') === 'upsert_keep_deletes');
  const noKey = !columns.some((c) => c.key);
  const keyNames = columns.filter((c) => c.key).map((c) => c.name);
  const events = words.events;
  return (
    <Card title={words.previewTitle}>
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
        {events ? (
          <dl className="m-0 grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1 text-sm" aria-label="Each event">
            <dt className="text-muted">Key</dt>
            <dd className="m-0 wrap-anywhere">
              JSON: the table's id (<code className="font-mono">TableId</code>) and {keyNames.length ? <code className="font-mono">{keyNames.join(', ')}</code> : 'no key column'}; events are spread over partitions by it
            </dd>
            <dt className="text-muted">Value</dt>
            <dd className="m-0 wrap-anywhere">
              Debezium-style JSON: <code className="font-mono">before</code> and <code className="font-mono">after</code> with the fields below,{' '}
              <code className="font-mono">op</code> (c, u, d) and a short <code className="font-mono">source</code>; no schema
            </dd>
          </dl>
        ) : null}
        <table className="w-full text-sm">
          <thead>
            <tr className="text-left text-xs text-muted">
              <th className="py-1 font-medium">{events ? 'Field' : 'Column'}</th>
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
                <td className="py-1.5 pr-3 font-mono">{flinkSoftDeleteField(sinkType)}</td>
                <td className="py-1.5 pr-3">text</td>
                <td className="py-1.5 text-muted">{events ? 'change type: a delete is sent as an insert marked -D' : 'change type: deleted rows stay, marked -D'}</td>
              </tr>
            ) : null}
          </tbody>
        </table>
        {noKey ? <p className="m-0 text-xs text-bad">{words.noKey}</p> : null}
        <p className="m-0 text-xs text-muted">
          {filters.length ? `${events ? 'Events' : 'Rows'} kept: ${filters.map((f) => summarize(f)).join('; ')}.` : events ? 'Every change is sent.' : 'Every row is kept.'} New columns in the source wait for the next
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
