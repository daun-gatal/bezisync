import { useEffect, useRef, useState, type ReactNode, type RefObject } from 'react';
import clsx from 'clsx';
import { Check, ChevronLeft, ChevronRight } from 'lucide-react';
import { Button } from './button';
import { readStored, writeStored } from './hooks';
import { Menu } from './overlays';

/**
 * Paging for every list. One rows-per-page choice per viewer (10, 20, 50 or 100; 20 by default),
 * remembered in this browser and shared by all lists. Three pagers:
 *
 * - `Pager`: the list that is a page's subject (Pipelines, Connections, People…): the range,
 *   rows per page, page numbers and Previous / Next.
 * - `CursorPager`: lists the server pages by cursor with no total (audit log): Newer / Older.
 * - `CompactPager`: a list inside a card that shares the page (a pipeline's versions, a wizard
 *   card): "1–20 of 64" and two arrows, nothing at all when everything fits.
 *
 * Lists scroll inside their own box (`scrollBox`) so the header row and the pager stay in view.
 */

export const PAGE_SIZES = [10, 20, 50, 100] as const;
export const DEFAULT_PAGE_SIZE = 20;
export const PAGE_SIZE_KEY = 'bz.pageSize';
const CHANGED = 'bz:page-size';

export function readPageSize(): number {
  const n = Number(readStored(PAGE_SIZE_KEY));
  return (PAGE_SIZES as readonly number[]).includes(n) ? n : DEFAULT_PAGE_SIZE;
}

/** The viewer's rows per page, kept in step across lists and tabs. */
export function usePageSize(): [number, (n: number) => void] {
  const [size, set] = useState(readPageSize);
  useEffect(() => {
    const sync = () => set(readPageSize());
    const onStorage = (e: StorageEvent) => {
      if (e.key === PAGE_SIZE_KEY) sync();
    };
    window.addEventListener(CHANGED, sync);
    window.addEventListener('storage', onStorage);
    return () => {
      window.removeEventListener(CHANGED, sync);
      window.removeEventListener('storage', onStorage);
    };
  }, []);
  const change = (n: number) => {
    writeStored(PAGE_SIZE_KEY, String(n));
    window.dispatchEvent(new Event(CHANGED));
  };
  return [size, change];
}

export function pageCount(total: number, size: number): number {
  return Math.max(1, Math.ceil(total / size));
}

/** The page clamped into range (a filter or a delete can leave fewer pages). */
export function clampPage(page: number, total: number, size: number): number {
  return Math.min(Math.max(1, Math.floor(page) || 1), pageCount(total, size));
}

/** Page numbers to show around the current one: 1 … 4 5 6 … 12. */
export function pageItems(current: number, pages: number): (number | 'gap')[] {
  if (pages <= 7) return Array.from({ length: pages }, (_, i) => i + 1);
  const out: (number | 'gap')[] = [1];
  const from = Math.max(2, Math.min(current - 1, pages - 4));
  const to = Math.min(pages - 1, Math.max(current + 1, 5));
  if (from > 2) out.push('gap');
  for (let i = from; i <= to; i++) out.push(i);
  if (to < pages - 1) out.push('gap');
  out.push(pages);
  return out;
}

/** "21–40 of 312" */
export function rangeLabel(page: number, size: number, total: number): string {
  const a = (page - 1) * size + 1;
  const b = Math.min(total, page * size);
  return `${a.toLocaleString()}–${b.toLocaleString()} of ${total.toLocaleString()}`;
}

/**
 * Pages a client-side list. `page`/`onPage` may come from the URL (shareable links); without
 * them the page is local state. The page is clamped when the list shrinks.
 */
export function usePaged<T>(rows: T[], control?: { page?: number; onPage?: (p: number) => void }) {
  const [size] = usePageSize();
  const [local, setLocal] = useState(1);
  const page = clampPage(control?.onPage ? (control.page ?? 1) : local, rows.length, size);
  const setPage = control?.onPage ?? setLocal;
  return {
    page,
    setPage,
    size,
    total: rows.length,
    shown: rows.slice((page - 1) * size, page * size),
  };
}

/** Scrolls a list's own box back to the top when its page changes. */
export function useScrollTopOn(ref: RefObject<HTMLElement | null>, key: unknown): void {
  useEffect(() => {
    if (ref.current) ref.current.scrollTop = 0;
  }, [ref, key]);
}

/**
 * Classes for a list's own scroll box: `page` lists grow with the window, lists in `card`s stop at
 * a fixed height. Pair with `tabIndex={0}` and a label so keyboards can scroll it.
 */
export function scrollBox(kind: 'page' | 'card'): string {
  return clsx('overflow-auto overscroll-contain', kind === 'page' ? 'max-h-[max(20rem,calc(100dvh-16rem))]' : 'max-h-[28rem]');
}

function PageSizeMenu() {
  const [size, setSize] = usePageSize();
  return (
    <Menu
      label={`${size} per page`}
      variant="ghost"
      size="sm"
      align="start"
      items={[
        { heading: 'Rows per page, for every list' },
        ...PAGE_SIZES.map((n) => ({
          label: `${n} per page`,
          icon: n === size ? <Check /> : undefined,
          onSelect: () => setSize(n),
        })),
      ]}
    />
  );
}

const bar = 'flex flex-wrap items-center gap-x-4 gap-y-2 border-t border-line px-card py-2.5 text-sm';

/** The full pager. Shows the count (and the size choice) even when everything fits. */
export function Pager({ total, page, onPage, noun }: { total: number; page: number; onPage: (p: number) => void; noun: string }) {
  const [size] = usePageSize();
  if (total === 0) return null;
  const pages = pageCount(total, size);
  const current = clampPage(page, total, size);
  if (pages === 1) {
    return (
      <div className={bar}>
        <span className="mr-auto text-muted tabular-nums">
          {total.toLocaleString()} {noun}
        </span>
        {total > PAGE_SIZES[0] ? <PageSizeMenu /> : null}
      </div>
    );
  }
  return (
    <div className={bar}>
      <span className="mr-auto text-muted tabular-nums">
        {rangeLabel(current, size, total)} {noun}
      </span>
      <PageSizeMenu />
      <nav aria-label={`Pages of ${noun}`} className="flex flex-wrap items-center gap-1">
        <Button size="sm" variant="ghost" icon={<ChevronLeft />} disabled={current === 1} onClick={() => onPage(current - 1)}>
          Previous
        </Button>
        {pageItems(current, pages).map((n, i) =>
          n === 'gap' ? (
            <span key={`gap-${i}`} className="px-1 text-muted" aria-hidden>
              …
            </span>
          ) : (
            <button
              key={n}
              type="button"
              aria-label={`Page ${n}`}
              aria-current={n === current ? 'page' : undefined}
              onClick={() => onPage(n)}
              className={clsx(
                'h-7 min-w-7 rounded-md border px-1.5 text-sm tabular-nums',
                n === current ? 'border-accent bg-accent-soft font-semibold text-accent-ink' : 'border-transparent hover:bg-hover',
              )}
            >
              {n}
            </button>
          ),
        )}
        <Button size="sm" variant="ghost" disabled={current === pages} onClick={() => onPage(current + 1)}>
          <span className="inline-flex items-center gap-1">
            Next <ChevronRight className="size-4" aria-hidden />
          </span>
        </Button>
      </nav>
    </div>
  );
}

/** Newer / Older for cursor-paged lists (no total). `page` counts from 1 for the range label. */
export function CursorPager({
  page,
  count,
  hasOlder,
  onNewer,
  onOlder,
  noun,
  loading,
}: {
  page: number;
  count: number;
  hasOlder: boolean;
  onNewer: () => void;
  onOlder: () => void;
  noun: string;
  loading?: boolean;
}) {
  const [size] = usePageSize();
  if (page === 1 && !hasOlder && count === 0) return null;
  const a = (page - 1) * size + 1;
  return (
    <div className={bar}>
      <span className="mr-auto text-muted tabular-nums">
        {page === 1 ? `Newest ${count.toLocaleString()}` : `${a.toLocaleString()}–${(a + count - 1).toLocaleString()}`} {noun}
      </span>
      <PageSizeMenu />
      <nav aria-label={`Pages of ${noun}`} className="flex items-center gap-1">
        <Button size="sm" variant="ghost" icon={<ChevronLeft />} disabled={page === 1} onClick={onNewer}>
          Newer
        </Button>
        <Button size="sm" variant="ghost" disabled={!hasOlder} loading={loading} onClick={onOlder}>
          <span className="inline-flex items-center gap-1">
            Older <ChevronRight className="size-4" aria-hidden />
          </span>
        </Button>
      </nav>
    </div>
  );
}

/** The quiet pager for a list inside a card. Renders nothing when everything fits. */
export function CompactPager({
  total,
  page,
  onPage,
  noun,
  hasOlder,
}: {
  /** The total, or for a cursor-paged list the rows on this page (pass `hasOlder`). */
  total: number;
  page: number;
  onPage: (p: number) => void;
  noun: string;
  hasOlder?: boolean;
}) {
  const [size] = usePageSize();
  const cursor = hasOlder !== undefined;
  if (cursor ? page === 1 && !hasOlder : total <= size) return null;
  const pages = cursor ? page + (hasOlder ? 1 : 0) : pageCount(total, size);
  const current = cursor ? page : clampPage(page, total, size);
  const a = (current - 1) * size + 1;
  const label: ReactNode = cursor ? (current === 1 ? `Newest ${total}` : `${a}–${a + total - 1}`) : rangeLabel(current, size, total);
  const arrow =
    'grid size-7 place-items-center rounded-md border border-line bg-surface text-ink hover:bg-hover disabled:cursor-not-allowed disabled:opacity-40';
  return (
    <div className="flex items-center gap-2 border-t border-line py-1.5 pr-2.5 pl-card text-sm">
      <span className="mr-auto text-muted tabular-nums">{label}</span>
      <nav aria-label={`Pages of ${noun}`} className="flex items-center gap-1">
        <button
          type="button"
          className={arrow}
          disabled={current === 1}
          onClick={() => onPage(current - 1)}
          aria-label={cursor ? `Newer ${noun}` : `Previous page of ${noun}`}
        >
          <ChevronLeft className="size-4" aria-hidden />
        </button>
        <button
          type="button"
          className={arrow}
          disabled={current >= pages}
          onClick={() => onPage(current + 1)}
          aria-label={cursor ? `Older ${noun}` : `Next page of ${noun}`}
        >
          <ChevronRight className="size-4" aria-hidden />
        </button>
      </nav>
    </div>
  );
}

/**
 * A paged list that isn't a table (versions, alert cards): its own scroll box plus the pager for
 * its place, `card` (compact) or `page` (full). `children` renders the rows of the current page.
 */
export function PagedBox<T>({
  rows,
  label,
  noun,
  kind = 'card',
  page,
  onPage,
  className,
  children,
}: {
  rows: T[];
  /** Names the scroll region for screen readers ("Versions"). */
  label: string;
  noun: string;
  kind?: 'page' | 'card';
  page?: number;
  onPage?: (p: number) => void;
  className?: string;
  children: (shown: T[]) => ReactNode;
}) {
  const paged = usePaged(rows, { page, onPage });
  const box = useRef<HTMLDivElement>(null);
  useScrollTopOn(box, paged.page);
  return (
    <>
      <div ref={box} role="region" aria-label={`${label}, scrolls`} tabIndex={0} className={clsx('min-w-0', scrollBox(kind), className)}>
        {children(paged.shown)}
      </div>
      {kind === 'card' ? (
        <CompactPager total={paged.total} page={paged.page} onPage={paged.setPage} noun={noun} />
      ) : (
        <Pager total={paged.total} page={paged.page} onPage={paged.setPage} noun={noun} />
      )}
    </>
  );
}
