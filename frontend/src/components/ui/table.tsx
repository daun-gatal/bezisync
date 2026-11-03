import { useMemo, useRef, type ReactNode } from 'react';
import clsx from 'clsx';
import { Button } from './button';
import type { Tone } from './feedback';
import { RowCheck } from './form';
import { useElementWidth } from './hooks';
import { CompactPager, Pager, scrollBox, usePaged, useScrollTopOn } from './pager';
import { plural } from './text';

export interface Column<R> {
  id: string;
  header: ReactNode;
  cell: (row: R) => ReactNode;
  /** 1 always shows; 2 from 600 px, 3 from 860 px, 4 from 1080 px of table width. */
  priority?: 1 | 2 | 3 | 4;
  /** Minimum width in px, so one long value can't squeeze this column. */
  min?: number;
  /** Shrink to content (checkboxes, actions, short numbers). */
  fit?: boolean;
  align?: 'left' | 'right';
  /** In card rows (narrow screens), take the full width instead of one grid cell. */
  wide?: boolean;
}

const SHOW_FROM = { 1: 0, 2: 600, 3: 860, 4: 1080 } as const;
const CARDS_BELOW = 560;

export interface Selection<R> {
  selected: Set<string>;
  onChange: (next: Set<string>) => void;
  /** Why a row can't be selected (it still shows a checkbox, disabled). */
  why?: (row: R) => string | null;
}

/**
 * The one way to show rows. Columns hide by priority as the table narrows, and below 560 px
 * rows become cards. Rows page by the viewer's rows-per-page choice (pager.tsx): `paging="page"`
 * for the list a page is about (full pager, scrolls within the window), `"card"` for a list in a
 * card that shares the page (compact pager, fixed height). The header row stays in view. Rows that need attention carry a
 * status stripe. The first column should hold the row's link; `onRowClick` is a mouse shortcut.
 */
export function DataTable<R>({
  label,
  columns,
  rows,
  rowKey,
  rowLabel,
  checkLabel,
  rowTone,
  onRowClick,
  selection,
  empty,
  paging = 'page',
  page,
  onPage,
  noun,
  bulk,
}: {
  /** What the table lists, for screen readers ("Pipelines"). */
  label: string;
  columns: Column<R>[];
  rows: R[];
  rowKey: (row: R) => string;
  /** Names the row's checkbox ("Select orders-cdc"). */
  rowLabel?: (row: R) => string;
  /** The checkbox's whole accessible name, when the row is itself the thing chosen ("shop.orders"). */
  checkLabel?: (row: R) => string;
  rowTone?: (row: R) => Tone | null | undefined;
  onRowClick?: (row: R) => void;
  selection?: Selection<R>;
  empty?: ReactNode;
  /** `page`: the page's main list; `card`: a list inside a card beside others. */
  paging?: 'page' | 'card';
  /** The page from the URL, with `onPage` to change it; local state without them. */
  page?: number;
  onPage?: (page: number) => void;
  /** What the pager counts ("pipelines"); the label in lower case by default. */
  noun?: string;
  /** Actions for the selected rows; shown in a bar while any are selected. */
  bulk?: (selected: R[]) => ReactNode;
}) {
  const box = useRef<HTMLDivElement>(null);
  const width = useElementWidth(box);
  const scroller = useRef<HTMLDivElement>(null);
  const paged = usePaged(rows, { page, onPage });
  const shown = paged.shown;
  useScrollTopOn(scroller, paged.page);
  const counted = noun ?? label.toLowerCase();

  // 0 = not measured yet (tests, first paint): render the full table.
  const cards = width > 0 && width < CARDS_BELOW;
  const cols = columns.filter((c) => !width || width >= SHOW_FROM[c.priority ?? 1]);

  const pickable = useMemo(() => (selection ? rows.filter((r) => !selection.why?.(r)) : []), [rows, selection]);
  const selectedRows = selection ? rows.filter((r) => selection.selected.has(rowKey(r))) : [];
  const allOn = pickable.length > 0 && pickable.every((r) => selection!.selected.has(rowKey(r)));
  const someOn = !allOn && pickable.some((r) => selection!.selected.has(rowKey(r)));
  const toggleAll = () => {
    const next = new Set(selection!.selected);
    for (const r of pickable) {
      if (allOn) next.delete(rowKey(r));
      else next.add(rowKey(r));
    }
    selection!.onChange(next);
  };
  const toggle = (r: R, on: boolean) => {
    const next = new Set(selection!.selected);
    if (on) next.add(rowKey(r));
    else next.delete(rowKey(r));
    selection!.onChange(next);
  };
  const rowCheck = (r: R) => {
    const why = selection!.why?.(r) ?? null;
    return (
      <RowCheck
        checked={selection!.selected.has(rowKey(r))}
        onChange={(on) => toggle(r, on)}
        label={checkLabel ? checkLabel(r) : `Select ${rowLabel ? rowLabel(r) : rowKey(r)}`}
        disabled={Boolean(why)}
        why={why}
      />
    );
  };
  const actions = columns.find((c) => c.id === 'actions');

  return (
    <div ref={box} className="flex min-w-0 flex-col">
      {selection && selectedRows.length ? (
        <div
          role="region"
          aria-label="Selected rows"
          className="sticky top-0 z-10 flex min-h-12 flex-wrap items-center gap-3 border-b border-line bg-accent-soft px-card py-2 text-sm text-accent-ink"
        >
          <strong className="font-semibold">{plural(selectedRows.length, 'selected', 'selected')}</strong>
          <Button size="sm" variant="ghost" onClick={() => selection.onChange(new Set())}>
            Clear
          </Button>
          <div className="ml-auto flex flex-wrap gap-2">{bulk?.(selectedRows)}</div>
        </div>
      ) : null}
      {rows.length === 0 ? (
        (empty ?? null)
      ) : cards ? (
        <ul aria-label={label} className="m-0 flex list-none flex-col p-0">
          {shown.map((r) => {
            const tone = rowTone?.(r) ?? undefined;
            return (
              <li
                key={rowKey(r)}
                data-tone={tone}
                onClick={onRowClick ? () => onRowClick(r) : undefined}
                className={clsx(
                  'bz-striped flex min-w-0 flex-col gap-2 border-b border-line px-card py-3 last:border-b-0',
                  onRowClick && 'cursor-pointer hover:bg-hover',
                )}
              >
                <div className="flex min-w-0 items-start gap-3">
                  {selection ? <span className="pt-0.5">{rowCheck(r)}</span> : null}
                  <div className="min-w-0 flex-1">{columns[0]!.cell(r)}</div>
                </div>
                <dl className="m-0 grid grid-cols-[repeat(auto-fill,minmax(140px,1fr))] gap-x-4 gap-y-2">
                  {columns
                    .slice(1)
                    .filter((c) => c.id !== 'actions')
                    .slice(0, 4)
                    .map((c) => (
                      <div key={c.id} className={clsx('flex min-w-0 flex-col gap-0.5', c.wide && 'col-span-full')}>
                        <dt className="text-xs text-muted">{c.header}</dt>
                        <dd className="m-0 min-w-0 text-sm">{c.cell(r)}</dd>
                      </div>
                    ))}
                </dl>
                {actions ? (
                  <div className="min-w-0" onClick={(e) => e.stopPropagation()}>
                    {actions.cell(r)}
                  </div>
                ) : null}
              </li>
            );
          })}
        </ul>
      ) : (
        <div ref={scroller} className={clsx('relative min-w-0', scrollBox(paging))} data-scroll-x tabIndex={0} role="region" aria-label={`${label}, scrolls`}>
          <table aria-label={label} data-table className="w-full border-collapse text-left text-base">
            <thead>
              <tr className="border-b border-line">
                {selection ? (
                  <th scope="col" className="sticky top-0 z-[1] h-10 w-px bg-surface px-card align-middle shadow-[inset_0_-1px_0_var(--bz-line)]">
                    <RowCheck
                      checked={allOn}
                      indeterminate={someOn}
                      onChange={toggleAll}
                      label={allOn ? 'Clear the selection' : `Select all ${pickable.length}`}
                      disabled={!pickable.length}
                    />
                  </th>
                ) : null}
                {cols.map((c, i) => (
                  <th
                    key={c.id}
                    scope="col"
                    style={{ minWidth: c.min }}
                    className={clsx(
                      'sticky top-0 z-[1] h-10 bg-surface text-xs font-semibold tracking-wide whitespace-nowrap text-muted uppercase shadow-[inset_0_-1px_0_var(--bz-line)]',
                      i === 0 && !selection ? 'pl-card' : 'pl-3',
                      i === cols.length - 1 ? 'pr-card' : 'pr-3',
                      c.fit && 'w-px',
                      c.align === 'right' && 'text-right',
                    )}
                  >
                    {c.header}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {shown.map((r) => {
                const tone = rowTone?.(r) ?? undefined;
                return (
                  <tr
                    key={rowKey(r)}
                    data-tone={tone}
                    aria-selected={selection ? selection.selected.has(rowKey(r)) : undefined}
                    onClick={onRowClick ? () => onRowClick(r) : undefined}
                    className={clsx(
                      'h-row border-b border-line last:border-b-0',
                      onRowClick && 'cursor-pointer hover:bg-hover',
                      selection?.selected.has(rowKey(r)) && 'bg-accent-soft/40',
                    )}
                  >
                    {selection ? (
                      <td className="w-px px-card py-2 align-middle" onClick={(e) => e.stopPropagation()}>
                        {rowCheck(r)}
                      </td>
                    ) : null}
                    {cols.map((c, i) => (
                      <td
                        key={c.id}
                        style={{ minWidth: c.min }}
                        onClick={c.id === 'actions' ? (e) => e.stopPropagation() : undefined}
                        className={clsx(
                          'py-2 align-middle',
                          i === 0 && !selection ? 'pl-card' : 'pl-3',
                          i === cols.length - 1 ? 'pr-card' : 'pr-3',
                          c.fit && 'w-px whitespace-nowrap',
                          c.align === 'right' && 'text-right tabular-nums',
                        )}
                      >
                        <div className="min-w-0 wrap-anywhere">{c.cell(r)}</div>
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {rows.length === 0 ? null : paging === 'card' ? (
        <CompactPager total={rows.length} page={paged.page} onPage={paged.setPage} noun={counted} />
      ) : (
        <Pager total={rows.length} page={paged.page} onPage={paged.setPage} noun={counted} />
      )}
    </div>
  );
}

/** A cell's main line plus a muted second line. */
export function Cell({ main, sub }: { main: ReactNode; sub?: ReactNode }) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      <div className="line-clamp-2 min-w-0 wrap-anywhere">{main}</div>
      {sub ? <div className="line-clamp-1 min-w-0 text-xs wrap-anywhere text-muted">{sub}</div> : null}
    </div>
  );
}

/** A list row (non-tabular lists inside cards): one height, the card's padding. */
export function ListRow({ children, tone, className }: { children: ReactNode; tone?: Tone | null; className?: string }) {
  return (
    <li
      data-tone={tone ?? undefined}
      className={clsx(
        'bz-striped flex min-h-row min-w-0 flex-wrap items-center gap-x-4 gap-y-2 border-b border-line px-card py-2.5 last:border-b-0',
        className,
      )}
    >
      {children}
    </li>
  );
}

export function List({ children, label }: { children: ReactNode; label: string }) {
  return (
    <ul aria-label={label} className="m-0 flex list-none flex-col p-0">
      {children}
    </ul>
  );
}
