import { useState, type ReactNode } from 'react';
import clsx from 'clsx';
import { Button } from '@/components/ui/button';
import { plural, Short, Title } from '@/components/ui/text';
import { TYPE_LABEL } from '@/lib/connections';
import { imageByPipeline, shortImage, SOURCE_LABEL, useDebeziumImages } from '@/lib/debezium';
import { formatBytes, formatDuration } from '@/lib/format';
import type { Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { HEALTH, isStale, LIFECYCLE, statusOf } from '@/lib/status';
import { ChangeDebeziumDialog } from '../ChangeDebeziumDialog';
import { formatWindow } from './overview';

function Node({ kicker, title, children, tone }: { kicker: string; title: ReactNode; children?: ReactNode; tone?: string }) {
  return (
    <div
      data-tone={tone}
      className={clsx(
        'flex min-w-0 flex-col gap-1 rounded-[10px] border border-line bg-surface px-4 py-3',
        tone && 'border-[var(--bz-stripe)] shadow-[inset_0_3px_0_var(--bz-stripe)]',
      )}
    >
      <span className="text-xs font-semibold tracking-wide text-muted uppercase">{kicker}</span>
      <div className="min-w-0 font-semibold">{title}</div>
      {children ? <div className="flex min-w-0 flex-col gap-0.5 text-xs text-muted">{children}</div> : null}
    </div>
  );
}

function Link({ state, label }: { state: 'idle' | 'live' | 'down'; label?: string }) {
  return (
    <div data-state={state} className="bz-flow-link flex min-h-8 items-center justify-center text-xs text-muted" aria-hidden>
      {label ? <span className="relative z-10 bg-bg px-1 whitespace-nowrap @2xl/flow:-translate-y-3.5">{label}</span> : null}
    </div>
  );
}

/**
 * The pipeline at a glance and the one summary on its page: source → connector → destination,
 * with the numbers that matter on each part. The links are solid while changes stream and red
 * when the connector is down. Below 672 px of width it stacks vertically.
 */
export function Flow({ pipeline: p }: { pipeline: Pipeline }) {
  const { project, can } = useProject();
  const images = useDebeziumImages(project.id);
  const img = imageByPipeline(images.data).get(p.id);
  const [changing, setChanging] = useState(false);
  const s = statusOf(p);
  const stale = isStale(p);
  const deployed = Boolean(p.deployed_version);
  const down = ['failed', 'cleanup_failed'].includes(p.lifecycle) || p.health === 'down';
  const live = !down && !stale && ['streaming', 'snapshotting'].includes(p.lifecycle);
  const link = down ? 'down' : live ? 'live' : 'idle';
  const eps = s.events_per_second;
  const lag = s.metrics?.ms_behind_source;
  const health = stale ? { label: 'Stale', tone: 'grey' } : (HEALTH[p.health] ?? HEALTH.unknown!);
  const tone = !deployed
    ? undefined
    : down
      ? 'bad'
      : stale
        ? 'grey'
        : p.health === 'healthy'
          ? 'ok'
          : p.health === 'lagging'
            ? 'warn'
            : p.health === 'at_risk'
              ? 'risk'
              : 'info';
  const topics = Object.keys((p.artifacts.destinations ?? {}) as Record<string, string>).length;
  const impact = s.source_impact;
  // MongoDB's oplog is shared by every reader, so its size is not "this pipeline's"; its window is what matters.
  const sourceLog =
    p.source.type === 'mongodb'
      ? impact?.window_seconds !== null && impact?.window_seconds !== undefined
        ? `Oplog keeps ${formatWindow(impact.window_seconds)}`
        : impact?.log_status === 'filling'
          ? 'Oplog still filling'
          : null
      : impact?.retained_bytes !== null && impact?.retained_bytes !== undefined
        ? `Log kept for this pipeline: ${formatBytes(impact.retained_bytes)}`
        : null;
  return (
    <section aria-label="Data flow" className="@container/flow min-w-0">
      <div className="grid min-w-0 grid-cols-[minmax(0,1fr)] grid-rows-[auto_32px_auto_32px_auto] @2xl/flow:grid-cols-[minmax(0,1fr)_56px_minmax(0,1.15fr)_56px_minmax(0,1fr)] @2xl/flow:grid-rows-1">
        <Node
          kicker="Source"
          title={
            <Title as="span" lines={2}>
              {p.source.name}
            </Title>
          }
        >
          <span>{TYPE_LABEL[p.source.type] ?? p.source.type}</span>
          {deployed && sourceLog ? <span className={s.source_impact?.risk ? 'font-semibold text-risk' : undefined}>{sourceLog}</span> : null}
        </Node>
        <Link state={link} />
        <Node kicker="Connector" tone={tone} title={deployed ? `${LIFECYCLE[p.lifecycle]?.label ?? p.lifecycle} · ${health.label}` : 'Not deployed yet'}>
          {deployed ? (
            <>
              <span>
                {stale ? 'No recent signal' : `Behind the source: ${formatDuration(lag)}`}
                {eps !== null && eps !== undefined && !stale ? ` · ${eps.toFixed(eps < 10 ? 1 : 0)} changes/s` : ''}
              </span>
              {img ? (
                <span className="flex min-w-0 flex-wrap items-center gap-x-1.5">
                  <span className="min-w-0">
                    Debezium{' '}
                    <Short max={26} mono className="text-xs text-ink-2">
                      {shortImage(img.image)}
                    </Short>{' '}
                    · {img.source === 'runtime' && img.runtime ? `default of ${img.runtime}` : SOURCE_LABEL[img.source]}
                    {img.standing === 'deprecated' ? ' · deprecated' : img.standing === 'unlisted' ? ' · not offered by your Admins' : ''}
                    {img.pending_image ? ` · next deploy ${shortImage(img.pending_image)}` : ''}
                  </span>
                  {can('pipeline.edit') && !['deleting', 'deleted'].includes(p.lifecycle) ? (
                    <Button size="sm" variant="ghost" className="-my-1 h-6 px-1.5 text-xs" onClick={() => setChanging(true)}>
                      Change<span className="sr-only"> Debezium version</span>
                    </Button>
                  ) : null}
                </span>
              ) : null}
            </>
          ) : null}
          {p.runtime ? <span className="min-w-0 truncate">Runtime {p.runtime.name}</span> : <span>No runtime chosen yet</span>}
        </Node>
        <Link state={link} />
        <Node
          kicker="Destination"
          title={
            p.sink ? (
              <Title as="span" lines={2}>
                {p.sink.name}
              </Title>
            ) : (
              <span className="text-muted">Not chosen yet</span>
            )
          }
        >
          {p.sink ? <span>{TYPE_LABEL[p.sink.type] ?? p.sink.type}</span> : null}
          {topics ? <span>{plural(topics, 'destination')} written</span> : null}
        </Node>
      </div>
      <ChangeDebeziumDialog pipelines={[{ id: p.id, name: p.name }]} open={changing} onOpenChange={setChanging} />
    </section>
  );
}
