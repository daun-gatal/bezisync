import clsx from 'clsx';
import { FLEET, fleetGroup, type FleetGroup } from '@/lib/status';
import type { Pipeline } from '@/lib/pipelines';

/**
 * The whole fleet at a glance: one bar split by health group, and a legend whose entries
 * filter the list. The bar is decoration; the legend carries the numbers in text.
 */
export function FleetHealth({ pipelines, active, onPick }: { pipelines: Pipeline[]; active?: string; onPick: (g: FleetGroup | undefined) => void }) {
  const counts = new Map<FleetGroup, number>();
  for (const p of pipelines) counts.set(fleetGroup(p), (counts.get(fleetGroup(p)) ?? 0) + 1);
  const total = pipelines.length || 1;
  const groups = FLEET.filter((g) => counts.get(g.id));
  return (
    <section aria-label="Fleet health" className="flex min-w-0 flex-col gap-3 rounded-[10px] border border-line bg-surface p-card">
      <div aria-hidden className="flex h-2.5 w-full gap-0.5 overflow-hidden rounded-full">
        {groups.map((g) => (
          <i key={g.id} className={clsx('block h-full', g.color)} style={{ width: `${((counts.get(g.id) ?? 0) / total) * 100}%` }} />
        ))}
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1.5">
        {groups.map((g) => (
          <button
            key={g.id}
            type="button"
            aria-pressed={active === g.id}
            onClick={() => onPick(active === g.id ? undefined : g.id)}
            className={clsx(
              'inline-flex items-center gap-1.5 rounded py-0.5 text-sm hover:text-ink hover:underline',
              active === g.id ? 'font-semibold text-ink underline' : 'text-ink-2',
            )}
          >
            <i aria-hidden className={clsx('inline-block size-2.5 rounded-sm', g.color, g.id === 'draft' && 'ring-1 ring-line-strong')} />
            {g.label}
            <b className="font-mono font-medium">{counts.get(g.id)}</b>
          </button>
        ))}
      </div>
    </section>
  );
}
