import { useMemo, useState } from 'react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Notice } from '@/components/ui/feedback';
import { Check, Search } from '@/components/ui/form';
import { Card, QueryState, Split } from '@/components/ui/layout';
import { Confirm } from '@/components/ui/overlays';
import { DataTable } from '@/components/ui/table';
import { Ident, Meta, plural, Short } from '@/components/ui/text';
import { captureOf, TYPE_LABEL, useConnectorTypes, useDiscovery } from '@/lib/connections';
import { ago, formatBytes, formatCount, formatDateTime } from '@/lib/format';
import { FlinkPositions } from './FlinkPositions';
import { useOffsets, useResetOffsets, useSnapshot, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { fullSpec } from '../spec';

/** Copies above these sizes load the source noticeably: they warn and ask to confirm. */
export const LARGE_TABLES = 10;
export const LARGE_ROWS = 5_000_000;
/** A deliberately rough guide for the time estimate; real speed depends on the source and destination. */
const ROWS_PER_SECOND = 10_000;

interface Row {
  table: string;
  rows: number | null;
  bytes: number | null;
}

export function copyEstimate(rows: Row[]): { rows: number; bytes: number; unknown: number; minutes: number | null; large: boolean } {
  const known = rows.filter((r) => r.rows !== null);
  const total = known.reduce((n, r) => n + (r.rows ?? 0), 0);
  const bytes = rows.reduce((n, r) => n + (r.bytes ?? 0), 0);
  return {
    rows: total,
    bytes,
    unknown: rows.length - known.length,
    minutes: known.length ? Math.max(1, Math.round(total / ROWS_PER_SECOND / 60)) : null,
    large: rows.length > LARGE_TABLES || total > LARGE_ROWS,
  };
}

export function SnapshotsTab({ pipeline: p }: { pipeline: Pipeline }) {
  if (p.engine === 'flink-cdc') {
    return (
      <Split ratio="main">
        <FlinkPositions pipeline={p} />
        <Card title="Copy tables again">
          <p className="m-0 text-base text-ink-2">
            {p.caps.why?.recopy_table ?? 'Flink CDC can’t copy one table again while the others stream.'} Tables added in the Tables step are copied at the
            next deploy; the others keep streaming.
          </p>
        </Card>
      </Split>
    );
  }
  return (
    <Split ratio="main">
      <Recopy pipeline={p} />
      <Position pipeline={p} />
    </Split>
  );
}

function Recopy({ pipeline: p }: { pipeline: Pipeline }) {
  const { project, can } = useProject();
  const capture = captureOf(useConnectorTypes(project.id).data, p.source.type);
  const sourceLabel = TYPE_LABEL[p.source.type] ?? 'This source';
  const snapshot = useSnapshot(project.id);
  const canOperate = can('pipeline.operate');
  const discovery = useDiscovery(project.id, canOperate ? p.source.id : undefined);
  const spec = fullSpec(p.spec);
  const window = spec.snapshot.window;
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [q, setQ] = useState('');
  const [now, setNow] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const scheduled = (p.status as { scheduled_snapshot?: { tables: string[]; at: string; window: string } }).scheduled_snapshot;
  const stats = useMemo(() => {
    const m = new Map<string, { rows: number | null; bytes: number | null }>();
    for (const t of discovery.data?.tables ?? []) m.set(`${t.schema}.${t.name}`, { rows: t.rows_estimate ?? null, bytes: t.bytes_estimate ?? null });
    return m;
  }, [discovery.data]);
  const rows: Row[] = spec.source.tables.include.map((t) => ({ table: t, rows: stats.get(t)?.rows ?? null, bytes: stats.get(t)?.bytes ?? null }));
  const shown = rows.filter((r) => !q || r.table.toLowerCase().includes(q.toLowerCase()));
  const picked = rows.filter((r) => selected.has(r.table));
  const est = copyEstimate(picked);
  const why = !canOperate
    ? 'Only operators can re-copy rows.'
    : p.desired_state !== 'running'
      ? 'The pipeline must be running: resume it first.'
      : !p.deployed_version
        ? 'Deploy the pipeline first.'
        : picked.length === 0
          ? 'Choose at least one table.'
          : null;

  const start = () =>
    snapshot.mutate(
      { id: p.id, tables: picked.map((r) => r.table), ignoreWindow: now },
      {
        onSuccess: () => {
          toast.success(window && !now ? `Snapshot scheduled for the ${window} UTC window` : 'Snapshot requested');
          setSelected(new Set());
          setConfirming(false);
        },
      },
    );

  if (!capture.incremental) {
    return (
      <Card title="Re-copy existing rows">
        <p className="m-0 text-base text-ink-2">
          {capture.snapshot_modes.length
            ? `${sourceLabel} can’t copy tables again while it streams. To copy every captured table again, pause the pipeline, reset its position from scratch and resume it.`
            : `${sourceLabel} can’t copy the rows a table already has: the pipeline only streams changes.`}
        </p>
      </Card>
    );
  }

  return (
    <Card title="Re-copy existing rows" pad={false}>
      <div className="flex flex-col gap-3 px-card pt-card pb-3">
        {scheduled ? (
          <Notice tone="info" title="Snapshot waiting for the maintenance window">
            {plural(scheduled.tables.length, 'table')} will be copied at {formatDateTime(scheduled.at)} (window {scheduled.window} UTC).
          </Notice>
        ) : null}
        <p className="m-0 text-base text-ink-2">
          Runs an <strong>incremental snapshot</strong>: existing rows are copied in chunks while live changes keep streaming. Use it after data loss in the
          destination, or to backfill.
        </p>
        {capture.incremental_note ? (
          <Notice tone="warn" title={`Re-copies from ${sourceLabel}`}>
            {capture.incremental_note}
          </Notice>
        ) : null}
        {rows.length > 10 ? <Search label="Search captured tables" placeholder="Search tables…" value={q} onChange={setQ} /> : null}
        {discovery.isError ? <Meta as="p">Row counts aren’t available: BeziSync couldn’t read the source’s table sizes just now.</Meta> : null}
      </div>
      <DataTable
        label="Tables to re-copy"
        rows={shown}
        rowKey={(r) => r.table}
        rowLabel={(r) => r.table}
        paging="card"
        selection={canOperate ? { selected, onChange: setSelected } : undefined}
        empty={<Empty title={`No table matches “${q}”`} />}
        columns={[
          {
            id: 'table',
            header: 'Table',
            min: 180,
            cell: (r) => (
              <Short max={44} mono>
                {r.table}
              </Short>
            ),
          },
          {
            id: 'rows',
            header: 'Rows',
            fit: true,
            align: 'right',
            cell: (r) => <span className="font-mono text-sm">{discovery.isPending && canOperate ? '…' : formatCount(r.rows)}</span>,
          },
          {
            id: 'size',
            header: 'Size',
            priority: 2,
            fit: true,
            align: 'right',
            cell: (r) => <span className="font-mono text-sm">{formatBytes(r.bytes)}</span>,
          },
        ]}
      />
      <div className="flex flex-col gap-3 border-t border-line p-card">
        {picked.length ? (
          <p className="m-0 flex flex-wrap gap-x-4 gap-y-1 text-sm text-ink-2" aria-live="polite">
            <span>
              <b className="font-mono font-medium">{plural(picked.length, 'table')}</b> chosen
            </span>
            <span>
              about <b className="font-mono font-medium">{formatCount(est.rows)}</b> rows{est.unknown ? ` (+${est.unknown} of unknown size)` : ''}
            </span>
            {est.bytes ? (
              <span>
                <b className="font-mono font-medium">{formatBytes(est.bytes)}</b>
              </span>
            ) : null}
            {est.minutes !== null ? (
              <span>roughly {est.minutes < 60 ? `${est.minutes} min` : `${(est.minutes / 60).toFixed(1)} h`} at 10,000 rows a second</span>
            ) : null}
          </p>
        ) : null}
        {est.large ? (
          <Notice tone="warn" title="A large copy">
            Copying {picked.length > LARGE_TABLES ? `${picked.length} tables` : `about ${formatCount(est.rows)} rows`} adds read load on the source for a while,
            and consumers receive every row again.{' '}
            {window ? `It waits for the ${window} UTC window unless you run it now.` : 'Consider a maintenance window (pipeline settings).'}
          </Notice>
        ) : null}
        {window ? (
          <Check
            checked={now}
            onChange={setNow}
            label="Run now, outside the maintenance window"
            description={`Snapshots of this pipeline otherwise wait for ${window} UTC to protect the source.`}
          />
        ) : null}
        <InlineError error={snapshot.error} doing="Couldn’t start the snapshot" />
        <div className="flex flex-wrap items-center gap-3">
          <Button variant="primary" disabled={Boolean(why)} why={why} loading={snapshot.isPending} onClick={() => (est.large ? setConfirming(true) : start())}>
            Start snapshot
          </Button>
          {why && picked.length === 0 && canOperate ? <Meta>{why}</Meta> : null}
        </div>
      </div>
      <Confirm
        open={confirming}
        onOpenChange={setConfirming}
        tone="primary"
        title={`Re-copy ${plural(picked.length, 'table')}?`}
        effects={[
          `About ${formatCount(est.rows)} rows${est.bytes ? ` (${formatBytes(est.bytes)})` : ''} are read from the source in chunks, alongside live changes.`,
          'Consumers receive every copied row again, as a read event.',
          window && !now ? `It starts in the next ${window} UTC window.` : 'It starts now.',
        ]}
        confirmLabel="Start snapshot"
        pending={snapshot.isPending}
        error={snapshot.error}
        doing="Couldn’t start the snapshot"
        onConfirm={start}
      />
    </Card>
  );
}

function Position({ pipeline: p }: { pipeline: Pipeline }) {
  const { project, can } = useProject();
  const capture = captureOf(useConnectorTypes(project.id).data, p.source.type);
  const canCopy = capture.snapshot_modes.length > 0;
  const offsets = useOffsets(project.id, p.id, Boolean(p.deployed_version));
  const reset = useResetOffsets(project.id);
  const [mode, setMode] = useState<'resnapshot' | 'skip_to_now' | null>(null);
  const paused = p.desired_state === 'paused';
  const why = paused ? null : 'Pause the pipeline to reset its position.';
  return (
    <Card title="Stored position">
      {!p.deployed_version ? (
        <Meta as="p">Nothing stored yet: the pipeline hasn’t run.</Meta>
      ) : (
        <QueryState query={offsets} doing="Couldn’t load the stored position" lines={3}>
          {(data) => {
            const current = data.offsets[0];
            return current ? (
              <div className="flex min-w-0 flex-col gap-1">
                <Ident>{current.position}</Ident>
                <Meta>
                  {current.source_time ? `source time ${formatDateTime(current.source_time)} · ` : ''}stored {ago(current.stored_at)}
                  {current.snapshot_in_progress ? ' · snapshot in progress' : ''}
                </Meta>
              </div>
            ) : (
              <Meta as="p">
                No stored position
                {data.snapshot_override === 'no_data' ? ': the next start streams only new changes.' : ': the next start begins from the snapshot mode.'}
              </Meta>
            );
          }}
        </QueryState>
      )}
      <Meta as="p">Kept in the BeziSync state database, so the connector resumes exactly where it stopped.</Meta>
      {can('offsets.reset') && p.deployed_version ? (
        <div className="flex flex-col gap-2 border-t border-line pt-4">
          <strong className="text-sm font-semibold">Reset the position</strong>
          <div className="flex flex-wrap gap-2">
            {canCopy ? (
              <Button
                disabled={!paused}
                why={why}
                onClick={() => {
                  reset.reset();
                  setMode('resnapshot');
                }}
              >
                Re-snapshot from scratch…
              </Button>
            ) : null}
            <Button
              disabled={!paused}
              why={why}
              variant="danger"
              onClick={() => {
                reset.reset();
                setMode('skip_to_now');
              }}
            >
              Skip to now…
            </Button>
          </div>
          {!paused ? <Meta>{why}</Meta> : null}
        </div>
      ) : null}
      <Confirm
        open={mode !== null}
        onOpenChange={(o) => !o && setMode(null)}
        title={mode === 'skip_to_now' ? 'Skip to now?' : 'Re-snapshot from scratch?'}
        effects={
          mode === 'skip_to_now'
            ? [
                'Changes made on the source since the pipeline stopped are never delivered.',
                ...(p.source.type === 'postgresql' ? ['The replication slot is dropped and recreated at the current position.'] : []),
                'Existing rows are not copied.',
              ]
            : [
                'On resume, every existing row of the captured tables is copied again, then streaming continues.',
                'Consumers may see rows they already received.',
              ]
        }
        confirmLabel={mode === 'skip_to_now' ? 'Skip changes' : 'Reset position'}
        typeToConfirm={p.name}
        pending={reset.isPending}
        error={reset.error}
        doing="Couldn’t reset the position"
        onConfirm={() =>
          mode &&
          reset.mutate(
            { id: p.id, mode, confirm: p.name },
            {
              onSuccess: () => {
                toast.success('Position reset. Resume the pipeline to continue.');
                setMode(null);
              },
            },
          )
        }
      />
    </Card>
  );
}
