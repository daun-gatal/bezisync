import type { ReactNode } from 'react';
import { Check, Lock } from 'lucide-react';
import clsx from 'clsx';
import { RadioCards } from '@/components/ui/form';
import { reportBadge } from '@/components/CheckList';
import { TYPE_LABEL, type Connection } from '@/lib/connections';
import type { Pipeline } from '@/lib/pipelines';
import { fullSpec, namingError } from '../spec';

export type Step = 'source' | 'tables' | 'sink' | 'transform' | 'review';

export const STEPS: { id: Step; label: string }[] = [
  { id: 'source', label: 'Source' },
  { id: 'tables', label: 'Tables' },
  { id: 'sink', label: 'Destination' },
  { id: 'transform', label: 'Transform' },
  { id: 'review', label: 'Review & deploy' },
];

/**
 * A step unlocks only when the ones before it are done; a locked step says what it needs.
 * Transforms are optional, so Review needs only a destination. `ready` is the step on screen
 * when its unsaved choices are complete: leaving it saves them, so the steps after it open.
 */
export function stepState(step: Step, pipeline: Pipeline | undefined, ready: Step | null = null): { done: boolean; locked: string | null } {
  const spec = pipeline ? fullSpec(pipeline.spec) : null;
  const hasTables = Boolean(spec?.source.tables.include.length) || ready === 'tables';
  const hasSink = (Boolean(pipeline?.sink) && !namingError(spec?.sink.naming ?? '')) || ready === 'sink';
  switch (step) {
    case 'source':
      return { done: Boolean(pipeline), locked: null };
    case 'tables':
      return { done: hasTables, locked: pipeline ? null : 'Choose a source first.' };
    case 'sink':
      return { done: hasSink, locked: !pipeline ? 'Choose a source first.' : hasTables ? null : 'Choose tables first.' };
    case 'transform':
      return {
        done: hasSink,
        locked: !pipeline ? 'Choose a source first.' : !hasTables ? 'Choose tables first.' : hasSink ? null : 'Choose a destination first.',
      };
    case 'review':
      return {
        done: false,
        locked: !pipeline ? 'Choose a source first.' : !hasTables ? 'Choose tables first.' : hasSink ? null : 'Choose a destination first.',
      };
  }
}

export function StepNav({ current, pipeline, ready, onGo }: { current: Step; pipeline: Pipeline | undefined; ready: boolean; onGo: (s: Step) => void }) {
  return (
    <nav aria-label="Steps" className="min-w-0">
      <ol className="m-0 flex list-none gap-1 overflow-x-auto p-0 md:flex-col md:overflow-visible" data-scroll-x>
        {STEPS.map((s, i) => {
          const { done, locked } = stepState(s.id, pipeline, ready ? current : null);
          const isCurrent = s.id === current;
          return (
            <li key={s.id} className="shrink-0">
              <button
                type="button"
                aria-current={isCurrent ? 'step' : undefined}
                aria-disabled={locked ? true : undefined}
                title={locked ?? undefined}
                onClick={() => !locked && !isCurrent && onGo(s.id)}
                className={clsx(
                  'flex h-10 w-full items-center gap-2.5 rounded-md px-2 text-left text-base whitespace-nowrap',
                  isCurrent ? 'font-semibold text-ink' : locked ? 'cursor-not-allowed text-muted opacity-70' : 'text-ink-2 hover:bg-hover hover:text-ink',
                )}
              >
                <span
                  className={clsx(
                    'grid size-7 shrink-0 place-items-center rounded-full border-[1.5px] text-xs font-semibold',
                    isCurrent ? 'border-accent bg-accent text-on-accent' : done ? 'border-ok bg-ok-soft text-ok' : 'border-line-strong bg-surface',
                  )}
                >
                  {locked ? <Lock className="size-3.5" aria-hidden /> : done && !isCurrent ? <Check className="size-3.5" aria-hidden /> : i + 1}
                </span>
                {s.label}
                {locked ? <span className="sr-only"> (locked: {locked})</span> : done && !isCurrent ? <span className="sr-only"> (done)</span> : null}
              </button>
            </li>
          );
        })}
      </ol>
    </nav>
  );
}

export function StepHeader({ title, description }: { title: string; description: ReactNode }) {
  return (
    <div className="flex flex-col gap-1">
      <h2 className="m-0 text-xl font-semibold">{title}</h2>
      <p className="m-0 text-base text-muted">{description}</p>
    </div>
  );
}

/** Connections as radio cards: the name wraps, the last check is the badge. */
export function ConnectionCards({
  items,
  selected,
  onSelect,
  label,
  disabled,
}: {
  items: Connection[];
  selected?: string;
  onSelect: (c: Connection) => void;
  label: string;
  disabled?: boolean;
}) {
  return (
    <RadioCards
      label={label}
      value={selected}
      disabled={disabled}
      onChange={(id) => {
        const c = items.find((x) => x.id === id);
        if (c) onSelect(c);
      }}
      options={items.map((c) => ({
        value: c.id,
        label: c.name,
        badge: reportBadge(c.last_check),
        description: [TYPE_LABEL[c.type] ?? c.type, c.managed ? 'managed' : '', c.description].filter(Boolean).join(' · '),
      }))}
    />
  );
}

/** Drafts created in this tab: closing the wizard asks whether to keep them. */
export const createdHere = new Set<string>();
