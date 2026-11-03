import { createElement, type ReactNode } from 'react';
import clsx from 'clsx';

/**
 * Every text slot declares how it overflows:
 * - Title: user names and headings wrap anywhere and clamp to `lines`, with the full text as a tooltip;
 * - Short: identifiers in dense places shorten in the middle, keeping the start and the
 *   distinguishing end; the full value is the tooltip and what screen readers hear;
 * - Ident: identifiers on detail pages, in mono, wrapping anywhere.
 */

const CLAMP = { 1: 'line-clamp-1', 2: 'line-clamp-2', 3: 'line-clamp-3' } as const;

export function Title({
  as = 'h1',
  lines = 2,
  className,
  children,
  id,
}: {
  as?: 'h1' | 'h2' | 'h3' | 'span' | 'strong' | 'div';
  lines?: 1 | 2 | 3 | 'none';
  className?: string;
  children: ReactNode;
  id?: string;
}) {
  return createElement(
    as,
    {
      id,
      className: clsx('m-0 min-w-0 wrap-anywhere', lines !== 'none' && CLAMP[lines], className),
      title: typeof children === 'string' ? children : undefined,
    },
    children,
  );
}

/** "orders_postgres_primary_cdc_masking_v2" → "orders_postgres…masking_v2" (max characters, ellipsis included). */
export function shorten(value: string, max = 32): string {
  if (value.length <= max) return value;
  const keep = max - 1;
  const head = Math.ceil(keep * 0.6);
  return `${value.slice(0, head)}…${value.slice(value.length - (keep - head))}`;
}

export function Short({ children, max = 32, mono, className }: { children: string | null | undefined; max?: number; mono?: boolean; className?: string }) {
  const value = children ?? '';
  const short = shorten(value, max);
  const cls = clsx('inline-block max-w-full truncate align-bottom', mono && 'font-mono text-sm', className);
  if (short === value) {
    return (
      <span className={cls} title={value}>
        {value}
      </span>
    );
  }
  return (
    <span className={cls} title={value}>
      <span aria-hidden>{short}</span>
      <span className="sr-only">{value}</span>
    </span>
  );
}

export function Ident({ children, className }: { children: ReactNode; className?: string }) {
  return <code className={clsx('font-mono text-sm wrap-anywhere text-ink', className)}>{children}</code>;
}

/** Secondary text: 12 px, muted. */
export function Meta({ children, className, as = 'span' }: { children: ReactNode; className?: string; as?: 'span' | 'p' | 'div' }) {
  return createElement(as, { className: clsx('m-0 text-xs text-muted', className) }, children);
}

export function plural(n: number, word: string, many = `${word}s`): string {
  return `${n.toLocaleString()} ${n === 1 ? word : many}`;
}
