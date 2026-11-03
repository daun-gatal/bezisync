import { useMemo, useState } from 'react';
import { Pill as Badge, Skeleton } from '@/components/ui/feedback';
import { Card } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import { QueryError } from '@/app/ErrorPanel';
import { useTransformPreview } from '@/lib/transforms';
import type { FullSpec, TransformStep } from '../spec';
import { Segmented } from './editors';
import { TYPE_TITLE, type TableInfo } from './model';

const OPS = [
  { value: 'insert', label: 'Insert' },
  { value: 'update', label: 'Update' },
  { value: 'delete', label: 'Delete' },
  { value: 'snapshot', label: 'Snapshot' },
] as const;
type Op = (typeof OPS)[number]['value'];

const OUTCOME: Record<string, { tone: 'ok' | 'grey' | 'bad'; label: string }> = {
  delivered: { tone: 'ok', label: 'Delivered' },
  dropped: { tone: 'grey', label: 'Not delivered' },
  unknown: { tone: 'grey', label: 'Unknown' },
  error: { tone: 'bad', label: 'Would stop the connector' },
};

/** The event a sample row becomes, step by step (server-side simulator; never reads data). */
export function PreviewPanel({
  projectId,
  pipelineId,
  spec,
  steps,
  tables,
  blocked,
}: {
  projectId: string;
  pipelineId: string;
  spec: FullSpec;
  steps: TransformStep[];
  tables: TableInfo[];
  blocked: string | null;
}) {
  const [tableName, setTableName] = useState(tables[0]?.name);
  const [op, setOp] = useState<Op>('insert');
  const [after, setAfter] = useState<number | 'final'>('final');
  const table = tables.find((t) => t.name === tableName) ?? tables[0];
  const body = useMemo(
    () =>
      blocked || !table
        ? null
        : {
            spec: spec as never,
            table: table.name,
            operation: op,
            columns: table.columns.map((c) => ({ name: c.name, type: c.type, nullable: c.nullable ?? false, pii: c.pii ?? false })),
            primary_key: table.primaryKey,
            replica_identity: table.replicaIdentity ?? null,
          },
    [blocked, table, spec, op],
  );
  const preview = useTransformPreview(projectId, pipelineId, body);
  const res = preview.data;
  const point = after === 'final' || after >= steps.length ? 'final' : after;
  let event = res?.output ?? null;
  let status: string | undefined = res?.outcome;
  if (res && point !== 'final') {
    const upTo = res.steps.slice(0, point + 1);
    const last = [...upTo].reverse().find((s) => s.event);
    event = last?.event ?? res.input;
    const halted = upTo.find((s) => s.status === 'dropped' || s.status === 'unknown' || s.status === 'error');
    status = halted ? { dropped: 'dropped', unknown: 'unknown', error: 'error' }[halted.status as 'dropped' | 'unknown' | 'error'] : 'delivered';
    if (halted) event = null;
  }
  const outcome = status ? OUTCOME[status] : undefined;
  const reason = res?.steps.find((s) => ['dropped', 'unknown', 'error'].includes(s.status) && (point === 'final' || s.index <= point));

  return (
    <Card title="Preview" className="lg:sticky lg:top-4">
      {tables.length > 1 ? (
        <Select
          label="Preview table"
          className="font-mono"
          value={table?.name}
          onChange={setTableName}
          options={tables.map((t) => ({ value: t.name, label: t.name }))}
        />
      ) : (
        <code className="font-mono text-xs wrap-anywhere text-muted">{table?.name}</code>
      )}
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <Segmented label="Operation" value={op} options={[...OPS]} onChange={setOp} />
        <div className="min-w-0 flex-[1_1_200px]">
          <Select
            label="After step"
            value={String(point)}
            onChange={(v) => setAfter(v === 'final' ? 'final' : Number(v))}
            options={[
              { value: 'final', label: 'Final event' },
              ...steps.map((s, i) => ({ value: String(i), label: `After step ${i + 1} · ${TYPE_TITLE[s.type]}` })),
            ]}
          />
        </div>
      </div>
      {blocked ? (
        <p className="m-0 text-sm text-muted">{blocked}</p>
      ) : !table ? (
        <p className="m-0 text-sm text-muted">BeziSync has not read the tables’ columns yet.</p>
      ) : preview.isPending ? (
        <Skeleton lines={6} />
      ) : preview.error ? (
        <QueryError error={preview.error} onRetry={() => void preview.refetch()} />
      ) : res ? (
        <>
          <div className="flex flex-wrap items-center gap-2 text-sm">
            {outcome ? <Badge tone={outcome.tone}>{outcome.label}</Badge> : null}
            {reason && status !== 'delivered' ? <span className="text-muted">{reason.note || `Step ${reason.index + 1}`}</span> : null}
          </div>
          {event ? (
            <pre
              aria-label="Preview event"
              tabIndex={0}
              data-scroll-x
              className="m-0 max-h-[420px] overflow-auto rounded-md bg-code-bg p-3 font-mono text-xs leading-relaxed text-code-ink"
            >
              {JSON.stringify(event.value, null, 2) ?? 'null'}
            </pre>
          ) : (
            <div className="rounded-lg border border-dashed border-line-strong p-5 text-center text-sm text-muted">
              {status === 'dropped'
                ? 'This event is not delivered.'
                : status === 'error'
                  ? 'The connector would stop at this step.'
                  : 'BeziSync cannot tell what this step produces; check the Events tab after deploying.'}
            </div>
          )}
          <dl className="m-0 grid grid-cols-[max-content_minmax(0,1fr)] gap-x-3 gap-y-1 text-sm">
            <dt className="text-muted">Key</dt>
            <dd className="m-0 break-all font-mono">{event ? JSON.stringify(event.key) : '—'}</dd>
            <dt className="text-muted">Destination</dt>
            <dd className="m-0 break-all font-mono">{res.destination}</dd>
          </dl>
          {res.notes.length ? (
            <ul className="m-0 list-disc pl-5 text-xs text-muted">
              {res.notes.map((n) => (
                <li key={n}>{n}</li>
              ))}
            </ul>
          ) : null}
          <p className="m-0 text-xs text-muted">Sample row built from the table’s columns; no data is read.</p>
        </>
      ) : null}
    </Card>
  );
}
