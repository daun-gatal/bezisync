import { useState, type ComponentType } from 'react';
import { ArrowRightLeft, Hash, Inbox, KeyRound, Layers, PenLine, Plus, Puzzle, Rows3, SquarePlus, Trash, Funnel, Split as SplitIcon } from 'lucide-react';
import { Button, ButtonLink } from '@/components/ui/button';
import { toast } from 'sonner';
import { Empty, Meter, Notice, Pill, Skeleton, TONE_TEXT } from '@/components/ui/feedback';
import { Segmented } from '@/components/ui/form';
import { Card, KeyValue, QueryState, Split, Stack } from '@/components/ui/layout';
import { List, ListRow } from '@/components/ui/table';
import { Ident, Meta, plural, Short } from '@/components/ui/text';
import { TimeSeries } from '@/components/TimeSeries';
import { ago, formatCount, formatDuration } from '@/lib/format';
import { useMetrics, useShrinkCluster, useVersions, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { isStale, statusOf, type FlinkStatus } from '@/lib/status';
import { fullSpec } from '../spec';
import { Flow } from './Flow';
import { flinkView, formatLatency, latest, protectionView, shapeView, transformView, type Fact, type Row, type RowIcon } from './overview';

/** Overview: the flow strip is the summary; the cards below only add what it doesn't show. */
export function Overview({ pipeline: p }: { pipeline: Pipeline }) {
  const { can } = useProject();
  const s = statusOf(p);
  const stale = isStale(p);
  const reasons = (s.reasons ?? []).filter((r) => r.severity !== 'info');
  const cleanup = (p.artifacts.cleanup ?? []) as { kind: string; name: string; outcome: string; detail?: string }[];
  const gone = ['deleting', 'deleted', 'cleanup_failed'].includes(p.lifecycle);
  return (
    <>
      {gone ? null : <Flow pipeline={p} />}
      {stale ? (
        <Notice tone="warn" title="Status is stale">
          The last observation is from {ago(s.as_of)}. The controller or runtime may be unreachable, so nothing here is current.
        </Notice>
      ) : null}
      {reasons.map((r) => (
        <Notice key={r.code} tone={r.severity === 'error' ? 'bad' : 'warn'} title={r.message}>
          {r.fix || undefined}
        </Notice>
      ))}
      {p.engine === 'flink-cdc' && s.flink && !gone ? <FlinkCluster pipeline={p} status={s.flink} /> : null}
      {p.lifecycle === 'draft' ? (
        <Card>
          <Empty title="Not deployed yet">
            {can('pipeline.edit')
              ? 'Continue the setup and deploy it to start streaming changes.'
              : 'Someone who can edit pipelines finishes the setup and deploys it.'}
          </Empty>
        </Card>
      ) : null}
      {cleanup.length ? (
        <Card title="Cleanup report" pad={false}>
          <List label="Cleanup report">
            {cleanup.map((r) => (
              <ListRow key={`${r.kind}:${r.name}`} tone={r.outcome === 'failed' ? 'bad' : null}>
                <Pill tone={r.outcome === 'failed' ? 'bad' : r.outcome === 'removed' ? 'ok' : 'grey'}>{r.outcome}</Pill>
                <span className="min-w-0 flex-[1_1_240px] text-base">
                  {r.kind.replace('_', ' ')} <Ident>{r.name}</Ident>
                  {r.detail ? <span className="text-muted"> · {r.detail}</span> : null}
                </span>
              </ListRow>
            ))}
          </List>
        </Card>
      ) : null}
      {p.deployed_version && !gone ? (
        <>
          <ChartCard pipeline={p} />
          <Split>
            <Stack>
              <SinceStart pipeline={p} grow={!statusOf(p).source_impact} />
              <SourceProtection pipeline={p} />
            </Stack>
            {/* Side by side, the left column sets the row's height (at least 320 px) and Transforms
                scrolls inside it; stacked on a narrow screen, it takes its natural height. */}
            <div className="relative min-w-0 @4xl:min-h-80">
              <Stack className="@4xl:absolute @4xl:inset-0">
                <TransformsCard pipeline={p} />
                {fullSpec(p.spec).policies.delivery_canary ? <DeliveryCard pipeline={p} /> : null}
              </Stack>
            </div>
          </Split>
        </>
      ) : null}
    </>
  );
}

const RANGES = ['1h', '6h', '24h', '7d', '30d'] as const;

function ChartCard({ pipeline: p }: { pipeline: Pipeline }) {
  const { project } = useProject();
  const [range, setRange] = useState<(typeof RANGES)[number]>('6h');
  const metrics = useMetrics(project.id, p.id, range, Boolean(p.deployed_version));
  const points = metrics.data?.points ?? [];
  const times = points.map((pt) => Date.parse(pt.at) / 1000);
  const eps = points.map((pt) => pt.events_per_second ?? null);
  const lag = points.map((pt) => (pt.lag_ms === null || pt.lag_ms === undefined ? null : pt.lag_ms));
  const maxEps = Math.max(0, ...eps.filter((v): v is number => v !== null));
  const lastLag = latest(lag);
  return (
    <Card
      title="Throughput & latency"
      actions={<Segmented label="Time range" value={range} onChange={setRange} options={RANGES.map((r) => ({ value: r, label: r }))} />}
    >
      {metrics.isPending ? (
        <Skeleton lines={5} />
      ) : points.length < 2 ? (
        <Meta as="p" className="py-6 text-center">
          Collecting data. The chart appears after a minute of observations.
        </Meta>
      ) : (
        <TimeSeries
          times={times}
          height={300}
          peakLabel={`Peak in ${range}`}
          summary={`Over the last ${range}: peak ${maxEps.toFixed(1)} changes per second; latest latency ${lastLag !== null ? formatLatency(lastLag) : 'unknown'}.`}
          series={[
            { label: 'Changes per second', values: eps, color: '--bz-accent', format: (v) => v.toFixed(v < 10 ? 1 : 0) },
            { label: 'Latency', values: lag, color: '--bz-risk', format: formatLatency, dashed: true },
          ]}
        />
      )}
    </Card>
  );
}

/** `grow`: the last card in its column, so it stretches to the row's height. */
function SinceStart({ pipeline: p, grow }: { pipeline: Pipeline; grow?: boolean }) {
  const s = statusOf(p);
  const m = s.metrics ?? undefined;
  return (
    <Card title="Since the connector started" className={grow ? 'flex-1' : undefined}>
      <KeyValue
        items={[
          ['Changes streamed', <span className="font-mono">{formatCount(m?.events_seen_streaming)}</span>],
          [
            'Existing rows copied',
            <span>
              <span className="font-mono">{formatCount(m?.events_seen_snapshot ?? m?.snapshot_rows_scanned)}</span>
              <Meta className="block">
                {m?.snapshot_running
                  ? `copying · ${(m.snapshot_tables_total ?? 0) - (m.snapshot_tables_remaining ?? 0)} of ${m.snapshot_tables_total ?? '?'} tables`
                  : m?.snapshot_completed
                    ? 'copy complete'
                    : 'no copy this run'}
              </Meta>
            </span>,
          ],
          ['Restarts', <span className="font-mono">{s.workload?.restarts ?? 0}</span>],
          ['Queue headroom', <span className="font-mono">{m?.queue_total ? `${Math.round(((m.queue_remaining ?? 0) / m.queue_total) * 100)}%` : '—'}</span>],
          ['Started', s.workload?.started_at ? ago(s.workload.started_at) : 'unknown'],
          ['Connected to source', m?.connected === undefined ? 'unknown' : m.connected ? 'yes' : 'no'],
        ]}
      />
    </Card>
  );
}

function FactValue({ fact: f }: { fact: Fact }) {
  return (
    <span className="flex min-w-0 flex-col gap-0.5">
      <span className={f.tone ? TONE_TEXT[f.tone] : undefined}>{f.mono ? <Short mono max={28}>{f.value}</Short> : f.value}</span>
      {f.note ? <Meta>{f.note}</Meta> : null}
    </span>
  );
}

/** Source protection in the source's own terms; anything not measured says so. */
function SourceProtection({ pipeline: p }: { pipeline: Pipeline }) {
  const impact = statusOf(p).source_impact;
  if (!impact) return null;
  const v = protectionView(p.source.type, impact);
  return (
    <Card title="Source protection" className="flex-1" actions={<Pill tone={v.pill.tone}>{v.pill.label}</Pill>}>
      {v.facts.length ? <KeyValue items={v.facts.map((f) => [f.label, <FactValue fact={f} />])} /> : null}
      {v.note ? <Meta as="p">{v.note}</Meta> : null}
      {v.meter ? <Meter {...v.meter} /> : null}
      {impact.risk ? (
        <Notice tone={impact.lost ? 'bad' : 'risk'} title={impact.risk}>
          {impact.risk_fix}
        </Notice>
      ) : null}
      <Meta>Checked {ago(impact.at)}</Meta>
    </Card>
  );
}

const ICON: Record<RowIcon, ComponentType<{ className?: string; 'aria-hidden'?: boolean }>> = {
  shape: Rows3,
  delete: Trash,
  fields: Plus,
  format: Hash,
  key: KeyRound,
  unwrap: Rows3,
  outbox: Inbox,
  soft_delete: Trash,
  truncate: Funnel,
  route: SplitIcon,
  filter: Funnel,
  rename: PenLine,
  add_field: SquarePlus,
  flatten: Layers,
  custom: Puzzle,
};

function Rows({ rows, label }: { rows: Row[]; label: string }) {
  return (
    <ul aria-label={label} className="m-0 flex list-none flex-col p-0">
      {rows.map((r, i) => {
        const Icon = ICON[r.icon] ?? ArrowRightLeft;
        return (
          <li key={`${i}:${r.title}`} className="grid grid-cols-[auto_minmax(0,1fr)] items-start gap-3 border-t border-line py-2 first:border-t-0 first:pt-0 last:pb-0">
            <span className="grid size-7 place-items-center rounded-md border border-line bg-subtle text-ink-2">
              <Icon className="size-4" aria-hidden />
            </span>
            <span className="flex min-w-0 flex-col gap-0.5">
              <span className="text-base font-medium wrap-anywhere">{r.title}</span>
              <span className="text-xs text-muted wrap-anywhere">
                {r.detail}
                {r.code?.length ? (
                  <>
                    {' '}
                    {r.code.map((c, i) => (
                      <span key={c}>
                        {i ? ', ' : ''}
                        <code className="font-mono text-ink-2">{c}</code>
                      </span>
                    ))}
                  </>
                ) : null}
              </span>
            </span>
          </li>
        );
      })}
    </ul>
  );
}

/** The deployed transforms as what consumers receive and the steps on the way, plus what a schema change broke. */
function TransformsCard({ pipeline: p }: { pipeline: Pipeline }) {
  const { project, can } = useProject();
  const versions = useVersions(project.id, p.id);
  const drift = ((p.status as Record<string, unknown>).transform_drift ?? []) as { code: string; fail: boolean; message: string }[];
  return (
    <Card
      title="Transforms"
      pad={false}
      className="min-h-0 flex-1"
      actions={
        <>
          {drift.length ? <Pill tone={drift.some((d) => d.fail) ? 'bad' : 'warn'}>{`${drift.length} to fix`}</Pill> : null}
          {can('pipeline.edit') ? (
            <ButtonLink
              size="sm"
              variant={drift.length ? 'primary' : 'ghost'}
              to="/p/$projectSlug/pipelines/new"
              params={{ projectSlug: project.slug }}
              search={{ draft: p.id, step: 'transform' }}
            >
              {drift.length ? 'Fix' : 'Edit'}
            </ButtonLink>
          ) : null}
        </>
      }
    >
      {/* Scrolls when the steps outgrow the height the left column gives it. Focusable so the keyboard can scroll it. */}
      <div tabIndex={0} aria-label="Transform details" role="group" className="flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto p-card">
      <QueryState query={versions} doing="load the deployed version" lines={4}>
        {(list) => {
          const deployed = list.find((v) => v.number === p.deployed_version);
          if (!deployed) return <Meta as="p">The deployed version is not available.</Meta>;
          const { output, steps } = transformView(fullSpec(deployed.spec), p.engine === 'flink-cdc' ? { sinkType: p.sink?.type } : undefined);
          return (
            <>
              {drift.length ? (
                <ul className="m-0 flex list-none flex-col gap-1.5 p-0 text-sm">
                  {drift.map((d) => (
                    <li key={d.message} className={d.fail ? 'text-bad' : 'text-warn'}>
                      {d.message}
                    </li>
                  ))}
                  <li className="text-xs text-muted">
                    Found after the table’s columns changed. Events keep flowing{drift.some((d) => d.fail) ? ', but the next deploy would fail' : ''}.
                  </li>
                </ul>
              ) : null}
              <section className="flex flex-col gap-2">
                <h3 className="m-0 text-xs font-semibold tracking-wide text-muted uppercase">What consumers receive</h3>
                <Rows rows={output} label="What consumers receive" />
              </section>
              <section className="flex flex-col gap-2">
                <h3 className="m-0 text-xs font-semibold tracking-wide text-muted uppercase">Steps</h3>
                {steps.length ? (
                  <Rows rows={steps} label="Steps" />
                ) : (
                  <Meta as="p">No filters, renames or other steps.</Meta>
                )}
              </section>
            </>
          );
        }}
      </QueryState>
      </div>
    </Card>
  );
}

function DeliveryCard({ pipeline: p }: { pipeline: Pipeline }) {
  const c = statusOf(p).canary;
  return (
    <Card
      title="End-to-end delivery"
      className="shrink-0"
      actions={<Pill tone={c?.ok === false ? 'bad' : c?.ok ? 'ok' : 'grey'}>{c?.ok === false ? 'Not verified' : c?.ok ? 'Verified' : 'Pending'}</Pill>}
    >
      <p className="m-0 text-base text-ink-2">
        {c?.ok === false
          ? `A canary written ${c.pending ? ago(c.pending.sent_at) : 'earlier'} has not reached the destination.`
          : c?.verified_at
            ? `A canary row reached the destination ${ago(c.verified_at)}, ${formatDuration(c.latency_ms)} after it was written.`
            : 'Waiting for the first canary to arrive.'}
      </p>
      {c?.error ? <Meta as="p">{c.error}</Meta> : null}
    </Card>
  );
}

/** A Flink CDC pipeline's cluster: the job, its TaskManagers, its last save and the first copy. */
function FlinkCluster({ pipeline: p, status }: { pipeline: Pipeline; status: FlinkStatus }) {
  const { project, can } = useProject();
  const shrink = useShrinkCluster(project.id);
  const v = flinkView(status, ago);
  const shape = shapeView(p.artifacts as Record<string, unknown>, ago);
  if (!v) return null;
  return (
    <Card
      title="Flink CDC cluster"
      actions={
        shape?.ready && can('pipeline.operate') ? (
          <Button
            size="sm"
            loading={shrink.isPending}
            onClick={() => shrink.mutate(p.id, { onSuccess: () => toast.success('Shrinking: the cluster restarts from its saved position.') })}
          >
            Shrink now
          </Button>
        ) : undefined
      }
    >
      {shape?.problem ? (
        <Notice tone="warn" title="It stopped shrinking">
          {shape.problem}
        </Notice>
      ) : null}
      {v.problem ? (
        <Notice tone="warn" title="Positions aren’t being saved">
          {v.problem}
        </Notice>
      ) : null}
      <KeyValue
        items={[
          ['Job', v.job],
          shape ? ['Size', shape.text] : null,
          v.taskmanagers ? ['TaskManagers', v.taskmanagers] : null,
          ['Last saved', v.saved ?? 'Not yet'],
          v.restored ? ['Started from', v.restored] : null,
        ]}
      />
      {v.copy?.length ? (
        <div className="flex flex-col gap-1">
          <strong className="text-sm font-semibold">First copy</strong>
          {v.copy.map((c) => (
            <Meta key={c.table}>
              {c.table}: {plural(c.chunks, 'chunk')} copied so far
            </Meta>
          ))}
        </div>
      ) : null}
      {status.components?.length ? (
        <List label="Cluster components">
          {status.components.map((c) => (
            <ListRow key={c.name} tone={c.state === 'running' ? null : 'warn'}>
              <span className="flex min-w-0 flex-[1_1_240px] flex-col gap-0.5">
                <Short max={40} mono>
                  {c.name}
                </Short>
                <Meta>
                  {c.role}
                  {c.node ? ` · ${c.node}` : ''}
                  {c.restarts ? ` · ${plural(c.restarts, 'restart')}` : ''}
                </Meta>
              </span>
              <Pill tone={c.state === 'running' ? 'ok' : 'warn'}>{c.state}</Pill>
            </ListRow>
          ))}
        </List>
      ) : null}
    </Card>
  );
}
