import clsx from 'clsx';
import type { ReactNode } from 'react';
import { Meta } from '@/components/ui/text';
import { memoryLabel } from '@/lib/cluster';

export interface ClusterShape {
  task_managers: number;
  /** Slots per TaskManager. */
  slots: number;
  readers: number;
  task_manager_cpu: string;
  task_manager_memory: string;
  job_manager_cpu: string;
  job_manager_memory: string;
}

/** How many readers each TaskManager runs: spread as evenly as Flink spreads them. */
export function readersPerTaskManager(s: ClusterShape): number[] {
  const out: number[] = [];
  let left = s.readers;
  for (let i = 0; i < s.task_managers; i++) {
    const n = Math.min(s.slots, Math.ceil(left / (s.task_managers - i)));
    out.push(n);
    left -= n;
  }
  return out;
}

/** The words the diagram stands for (its accessible name). */
export function clusterSummary(s: ClusterShape): string {
  const tms = `${s.task_managers} TaskManager${s.task_managers === 1 ? '' : 's'} × ${s.slots} slot${s.slots === 1 ? '' : 's'}`;
  const idle = s.task_managers * s.slots - s.readers;
  return `A JobManager (${s.job_manager_cpu} CPU, ${memoryLabel(s.job_manager_memory)}) and ${tms}, ${s.task_manager_cpu} CPU and ${memoryLabel(s.task_manager_memory)} each: ${s.readers} reader${s.readers === 1 ? '' : 's'}${idle > 0 ? `, ${idle} slot${idle === 1 ? '' : 's'} idle` : ''}.`;
}

/**
 * A Flink CDC cluster as boxes: the JobManager, then each TaskManager with its slots (filled: a
 * reader; hollow: idle). It wraps on narrow screens and never scrolls sideways; the words say
 * the same for screen readers.
 */
export function ClusterDiagram({ shape, title }: { shape: ClusterShape; title: string }) {
  const per = readersPerTaskManager(shape);
  return (
    <figure className="m-0 flex min-w-0 flex-col gap-2">
      <Meta className="font-semibold tracking-wide uppercase">{title}</Meta>
      <div role="img" aria-label={`${title}: ${clusterSummary(shape)}`} className="flex min-w-0 flex-wrap gap-2">
        <Box name="JobManager" detail={`${shape.job_manager_cpu} CPU · ${memoryLabel(shape.job_manager_memory)}`} />
        {per.map((busy, i) => (
          <Box key={i} name={`TaskManager ${i + 1}`} detail={`${shape.task_manager_cpu} CPU · ${memoryLabel(shape.task_manager_memory)}`}>
            <div className="flex flex-wrap gap-1" aria-hidden>
              {Array.from({ length: shape.slots }, (_, j) => (
                <span key={j} className={clsx('size-3.5 rounded-sm border', j < busy ? 'border-accent bg-accent' : 'border-line-strong bg-surface')} />
              ))}
            </div>
          </Box>
        ))}
      </div>
      <figcaption className="text-sm text-muted">{clusterSummary(shape)}</figcaption>
    </figure>
  );
}

function Box({ name, detail, children }: { name: string; detail: string; children?: ReactNode }) {
  return (
    <div className="flex min-w-36 flex-col gap-1.5 rounded-md border border-line bg-subtle px-3 py-2" aria-hidden>
      <span className="text-sm font-semibold text-ink">{name}</span>
      <span className="font-mono text-xs text-muted">{detail}</span>
      {children}
    </div>
  );
}
