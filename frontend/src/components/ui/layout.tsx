import { Fragment, useId, type ReactNode } from 'react';
import type { UseQueryResult } from '@tanstack/react-query';
import clsx from 'clsx';
import { ChevronRight } from 'lucide-react';
import { InlineError, Skeleton } from './feedback';
import { Title } from './text';

/**
 * Layout pieces. Sizes come from the rem scale: card padding (p-card, 18 px), card header (h-head, 56 px),
 * table rows (h-row, 48 px). Columns always shrink (minmax(0, …)); splits stack on narrow screens
 * by container width, not the window's.
 */

export function Card({
  title,
  actions,
  children,
  footer,
  pad = true,
  className,
  id,
  labelledBy,
}: {
  title?: ReactNode;
  actions?: ReactNode;
  children?: ReactNode;
  footer?: ReactNode;
  /** false: the body draws its own edges (tables, lists). */
  pad?: boolean;
  className?: string;
  id?: string;
  labelledBy?: string;
}) {
  const auto = useId();
  const headingId = `${id ?? auto}-title`;
  return (
    <section
      id={id}
      aria-labelledby={labelledBy ?? (title ? headingId : undefined)}
      className={clsx('flex min-w-0 flex-col rounded-[10px] border border-line bg-surface', className)}
    >
      {title ? (
        <header data-card-head className="flex h-head shrink-0 items-center gap-3 border-b border-line px-card">
          <Title as="h2" lines={1} id={headingId} className="flex-1 text-lg font-semibold">
            {title}
          </Title>
          {actions ? <div className="flex shrink-0 items-center gap-2">{actions}</div> : null}
        </header>
      ) : null}
      {pad ? <div className="flex min-w-0 flex-1 flex-col gap-4 p-card">{children}</div> : <div className="flex min-h-0 min-w-0 flex-1 flex-col">{children}</div>}
      {footer ? <footer className="flex flex-wrap items-center justify-end gap-2 border-t border-line px-card py-3">{footer}</footer> : null}
    </section>
  );
}

export interface Crumb {
  label: string;
  /** A rendered link (router Link); the last crumb is the current page and has none. */
  link?: (children: ReactNode, className: string) => ReactNode;
}

export function PageHeader({
  title,
  crumbs,
  status,
  description,
  actions,
}: {
  title: ReactNode;
  crumbs?: Crumb[];
  /** Pills and short facts under the title (state, version, owner). */
  status?: ReactNode;
  description?: ReactNode;
  /** One primary action plus secondary actions and menus, primary last. */
  actions?: ReactNode;
}) {
  return (
    <header className="flex min-w-0 flex-col gap-2">
      {crumbs?.length ? (
        <nav aria-label="Breadcrumb" className="min-w-0">
          <ol className="m-0 flex min-w-0 list-none items-center gap-1 p-0 text-sm text-muted">
            {crumbs.map((c, i) => (
              <Fragment key={`${i}-${c.label}`}>
                {i ? (
                  <li aria-hidden className="flex shrink-0">
                    <ChevronRight className="size-3.5" />
                  </li>
                ) : null}
                <li className={i === crumbs.length - 1 ? 'min-w-0 truncate' : 'max-w-[40%] shrink-0 truncate'} title={c.label}>
                  {c.link ? c.link(c.label, 'text-muted no-underline hover:text-ink hover:underline') : <span aria-current="page">{c.label}</span>}
                </li>
              </Fragment>
            ))}
          </ol>
        </nav>
      ) : null}
      <div className="flex flex-wrap items-start justify-between gap-x-6 gap-y-3">
        <div className="flex min-w-0 flex-[1_1_320px] flex-col gap-2">
          <Title as="h1" lines="none" className="text-2xl font-semibold tracking-tight">
            {title}
          </Title>
          {status ? <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1.5 text-sm text-muted">{status}</div> : null}
          {description ? <p className="m-0 max-w-3xl text-base text-muted wrap-break-word">{description}</p> : null}
        </div>
        {actions ? <div className="flex max-w-full flex-wrap items-center gap-2">{actions}</div> : null}
      </div>
    </header>
  );
}

/** Two columns that stack when their container is narrow. */
export function Split({ children, ratio = 'even', className }: { children: ReactNode; ratio?: 'even' | 'main' | 'side'; className?: string }) {
  const cols = {
    even: '@4xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]',
    main: '@4xl:grid-cols-[minmax(0,1.6fr)_minmax(0,1fr)]',
    side: '@4xl:grid-cols-[minmax(0,320px)_minmax(0,1fr)]',
  }[ratio];
  return (
    <div className="@container min-w-0">
      <div className={clsx('grid min-w-0 grid-cols-[minmax(0,1fr)] items-stretch gap-4', cols, className)}>{children}</div>
    </div>
  );
}

/** A column of cards inside a Split cell. */
export function Stack({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={clsx('flex min-w-0 flex-col gap-4', className)}>{children}</div>;
}

export function KeyValue({ items, className }: { items: ([ReactNode, ReactNode] | null | false | undefined)[]; className?: string }) {
  return (
    <dl className={clsx('m-0 grid grid-cols-[repeat(auto-fill,minmax(min(100%,140px),1fr))] gap-x-6 gap-y-3', className)}>
      {items
        .filter((i): i is [ReactNode, ReactNode] => Boolean(i))
        .map(([k, v], i) => (
          <div key={i} className="flex min-w-0 flex-col gap-0.5">
            <dt className="text-xs text-muted">{k}</dt>
            <dd className="m-0 min-w-0 text-base wrap-anywhere">{v}</dd>
          </div>
        ))}
    </dl>
  );
}

export function Metric({ label, value, foot, tone }: { label: string; value: ReactNode; foot?: ReactNode; tone?: 'warn' | 'risk' | 'bad' | 'grey' }) {
  const color = tone === 'warn' ? 'text-warn' : tone === 'risk' ? 'text-risk' : tone === 'bad' ? 'text-bad' : tone === 'grey' ? 'text-muted' : 'text-ink';
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <span className="text-xs text-muted">{label}</span>
      <span className={clsx('font-mono text-xl font-medium tabular-nums', color)}>{value}</span>
      {foot ? (
        <span className="truncate text-xs text-muted" title={typeof foot === 'string' ? foot : undefined}>
          {foot}
        </span>
      ) : null}
    </div>
  );
}

/** A query renders exactly one of its states: loading, error (with Try again and the request ID), or its data. */
export function QueryState<T>({
  query,
  children,
  lines = 4,
  doing,
}: {
  query: Pick<UseQueryResult<T>, 'isPending' | 'isError' | 'error' | 'data' | 'refetch'>;
  children: (data: T) => ReactNode;
  lines?: number;
  doing?: string;
}) {
  if (query.isPending) return <Skeleton lines={lines} />;
  if (query.isError) return <InlineError error={query.error} doing={doing} onRetry={() => void query.refetch()} />;
  return <>{children(query.data as T)}</>;
}

/** A row of filters and search above a list. Wraps instead of squeezing. */
export function Toolbar({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={clsx('flex min-w-0 flex-wrap items-center gap-2', className)}>{children}</div>;
}

/** A page body: the gap between header, notices and cards. */
export function Page({ children }: { children: ReactNode }) {
  return <div className="flex min-w-0 flex-col gap-5">{children}</div>;
}
