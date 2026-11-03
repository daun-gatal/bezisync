import type { ReactNode } from 'react';
import clsx from 'clsx';
import { RefreshCw } from 'lucide-react';
import { describeError, isRetryable } from '@/lib/errors';
import { Button } from './button';

export type Tone = 'ok' | 'warn' | 'risk' | 'bad' | 'grey' | 'info';

export const TONE_BG: Record<Tone, string> = {
  ok: 'bg-ok-soft text-ok',
  warn: 'bg-warn-soft text-warn',
  risk: 'bg-risk-soft text-risk',
  bad: 'bg-bad-soft text-bad',
  grey: 'bg-grey-soft text-grey',
  info: 'bg-accent-soft text-accent-ink',
};

export const TONE_TEXT: Record<Tone, string> = {
  ok: 'text-ok',
  warn: 'text-warn',
  risk: 'text-risk',
  bad: 'text-bad',
  grey: 'text-muted',
  info: 'text-accent-ink',
};

const GLYPH: Record<Tone, string> = { ok: '●', warn: '▲', risk: '◆', bad: '■', grey: '○', info: '◐' };

/** Status is a glyph and text, never colour alone. Long text truncates inside its box, with the full text as a tooltip. */
export function Pill({ tone = 'grey', children, title, className }: { tone?: Tone; children: ReactNode; title?: string; className?: string }) {
  return (
    <span
      title={title ?? (typeof children === 'string' ? children : undefined)}
      className={clsx('inline-flex h-6 max-w-full min-w-0 items-center gap-1.5 rounded-full px-2 text-xs font-semibold', TONE_BG[tone], className)}
    >
      <span aria-hidden className="shrink-0">
        {GLYPH[tone]}
      </span>
      <span className="min-w-0 truncate">{children}</span>
    </span>
  );
}

export function Notice({
  tone = 'info',
  title,
  children,
  action,
  className,
}: {
  tone?: Tone;
  title?: ReactNode;
  children?: ReactNode;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <div
      role={tone === 'bad' ? 'alert' : 'status'}
      className={clsx('flex flex-wrap items-start gap-x-4 gap-y-2 rounded-md px-3.5 py-3 text-sm wrap-break-word', TONE_BG[tone], className)}
    >
      <div className="flex min-w-0 flex-[1_1_240px] flex-col gap-0.5">
        {title ? <strong className="font-semibold">{title}</strong> : null}
        {children ? <div className="opacity-95">{children}</div> : null}
      </div>
      {action ? <div className="flex shrink-0 flex-wrap gap-2">{action}</div> : null}
    </div>
  );
}

/** Errors appear where they happened: what happened, why, what to do, and the request ID. Never a toast. */
export function InlineError({
  error,
  doing,
  onRetry,
  fix,
  className,
}: {
  error: unknown;
  /** The action that failed ("Couldn’t save the pipeline"). */
  doing?: string;
  onRetry?: () => void;
  /** Replaces the generic "what to do" when the screen knows better. */
  fix?: string;
  className?: string;
}) {
  if (!error) return null;
  const described = describeError(error, doing);
  const e = fix === undefined ? described : { ...described, fix };
  return (
    <div role="alert" className={clsx('flex flex-col gap-1.5 rounded-md bg-bad-soft px-3.5 py-3 text-sm text-bad wrap-break-word', className)}>
      <strong className="font-semibold">{e.title}</strong>
      <span className="text-ink-2">
        {e.detail}
        {e.fix ? ` ${e.fix}` : ''}
      </span>
      {e.fields.length ? (
        <ul className="m-0 flex list-disc flex-col gap-0.5 pl-5 text-ink-2">
          {e.fields.map((f) => (
            <li key={f.field + f.message}>
              {f.field ? <code className="font-mono text-xs">{f.field}</code> : null} {f.message}
            </li>
          ))}
        </ul>
      ) : null}
      {(onRetry && isRetryable(error)) || e.requestId ? (
        <div className="flex flex-wrap items-center gap-3">
          {onRetry && isRetryable(error) ? (
            <Button size="sm" icon={<RefreshCw />} onClick={onRetry}>
              Try again
            </Button>
          ) : null}
          {e.requestId ? <span className="font-mono text-xs text-muted">Request ID {e.requestId}</span> : null}
        </div>
      ) : null}
    </div>
  );
}

export function Skeleton({ lines = 3, className }: { lines?: number; className?: string }) {
  return (
    <div role="status" aria-label="Loading" className={clsx('flex flex-col gap-2.5 py-1', className)}>
      {Array.from({ length: lines }, (_, i) => (
        <div key={i} className="h-4 animate-pulse rounded bg-grey-soft" style={{ width: `${92 - i * 14}%` }} />
      ))}
    </div>
  );
}

export function Empty({ title, children, action, icon }: { title: string; children?: ReactNode; action?: ReactNode; icon?: ReactNode }) {
  return (
    <div className="flex flex-col items-center gap-3 px-6 py-12 text-center">
      {icon ? <div className="grid size-11 place-items-center rounded-full bg-grey-soft text-ink-2 [&>svg]:size-5">{icon}</div> : null}
      <div className="flex max-w-md flex-col gap-1">
        <p className="m-0 text-lg font-semibold wrap-anywhere">{title}</p>
        {children ? <p className="m-0 text-base text-muted">{children}</p> : null}
      </div>
      {action}
    </div>
  );
}

const METER_FILL: Record<Tone, string> = { ok: 'bg-ok', warn: 'bg-warn', risk: 'bg-risk', bad: 'bg-bad', grey: 'bg-grey', info: 'bg-accent' };

/**
 * How full something is against a limit: a label, the amount in words (`text`) and a bar. Past
 * the limit the bar stays full; the words and the tone say by how much.
 */
export function Meter({ label, ratio, text, tone = 'ok' }: { label: string; ratio: number; text: string; tone?: Tone }) {
  const pct = Math.round(Math.min(Math.max(Number.isFinite(ratio) ? ratio : 0, 0), 1) * 100);
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <div className="flex min-w-0 flex-wrap items-baseline justify-between gap-x-3 text-xs text-muted">
        <span className="min-w-0">{label}</span>
        <span className={clsx('font-mono', tone === 'ok' ? 'text-ink-2' : TONE_TEXT[tone])}>{text}</span>
      </div>
      <div
        role="meter"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={pct}
        aria-valuetext={text}
        className="h-1.5 overflow-hidden rounded-full border border-line bg-subtle"
      >
        <div className={clsx('h-full rounded-full', METER_FILL[tone])} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}
