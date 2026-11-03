import { useLayoutEffect, useRef, useState, type KeyboardEvent, type ReactNode } from 'react';
import clsx from 'clsx';
import { useElementWidth } from './hooks';
import { Menu } from './overlays';

export interface TabItem<V extends string> {
  id: V;
  label: string;
  count?: number | null;
}

/**
 * Tabs never wrap: the ones that don't fit fold into "More", and the active tab always stays
 * visible. Arrow keys move between tabs. Panels use TabPanel with the same `idBase`.
 */
export function Tabs<V extends string>({
  tabs,
  value,
  onChange,
  label,
  idBase,
}: {
  tabs: TabItem<V>[];
  value: V;
  onChange: (v: V) => void;
  label: string;
  idBase: string;
}) {
  const wrap = useRef<HTMLDivElement>(null);
  const measure = useRef<HTMLDivElement>(null);
  const width = useElementWidth(wrap);
  const [fit, setFit] = useState(tabs.length);
  const signature = tabs.map((t) => `${t.label}${t.count ?? ''}`).join('|');
  useLayoutEffect(() => {
    const el = measure.current;
    if (!el || !width) {
      setFit(tabs.length);
      return;
    }
    const widths = [...el.children].map((c) => c.getBoundingClientRect().width + 4);
    let used = 0;
    let n = 0;
    for (let i = 0; i < widths.length; i++) {
      const reserve = i < widths.length - 1 ? 96 : 0; // room for "More"
      if (used + widths[i]! + reserve > width) break;
      used += widths[i]!;
      n += 1;
    }
    setFit(n === tabs.length ? n : Math.max(1, n));
  }, [width, signature]); // eslint-disable-line react-hooks/exhaustive-deps

  let visible = tabs.slice(0, fit);
  const activeIndex = tabs.findIndex((t) => t.id === value);
  if (activeIndex >= fit && fit > 0) visible = [...tabs.slice(0, fit - 1), tabs[activeIndex]!];
  const hidden = tabs.filter((t) => !visible.includes(t));

  const onKey = (e: KeyboardEvent<HTMLButtonElement>, i: number) => {
    const dir = e.key === 'ArrowRight' ? 1 : e.key === 'ArrowLeft' ? -1 : 0;
    if (!dir) return;
    e.preventDefault();
    const next = visible[(i + dir + visible.length) % visible.length]!;
    onChange(next.id);
    wrap.current?.querySelector<HTMLButtonElement>(`#${CSS.escape(`${idBase}-tab-${next.id}`)}`)?.focus();
  };

  const body = (t: TabItem<V>) => (
    <>
      {t.label}
      {t.count !== undefined && t.count !== null ? (
        <span className="rounded-full bg-grey-soft px-1.5 text-xs font-semibold text-ink-2 tabular-nums">{t.count}</span>
      ) : null}
    </>
  );

  return (
    <div ref={wrap} className="relative flex min-w-0 items-end gap-1 border-b border-line">
      {/* Measures every tab at full width; clipped so it never widens the page. */}
      <div aria-hidden className="pointer-events-none invisible absolute inset-0 overflow-hidden">
        <div ref={measure} className="flex w-max gap-1 whitespace-nowrap">
          {tabs.map((t) => (
            <span key={t.id} className="inline-flex h-10 items-center gap-1.5 px-3 text-base font-medium">
              {body(t)}
            </span>
          ))}
        </div>
      </div>
      <div role="tablist" aria-label={label} className="flex min-w-0 items-end gap-1">
        {visible.map((t, i) => (
          <button
            key={t.id}
            id={`${idBase}-tab-${t.id}`}
            type="button"
            role="tab"
            aria-selected={t.id === value}
            aria-controls={`${idBase}-panel`}
            tabIndex={t.id === value ? 0 : -1}
            onClick={() => onChange(t.id)}
            onKeyDown={(e) => onKey(e, i)}
            className={clsx(
              '-mb-px inline-flex h-10 shrink-0 items-center gap-1.5 border-b-2 px-3 text-base font-medium whitespace-nowrap',
              t.id === value ? 'border-accent text-ink' : 'border-transparent text-muted hover:text-ink',
            )}
          >
            {body(t)}
          </button>
        ))}
      </div>
      {hidden.length ? (
        <div className="mb-1 shrink-0">
          <Menu
            label="More"
            variant="ghost"
            size="sm"
            items={hidden.map((t) => ({
              label: t.count !== undefined && t.count !== null ? `${t.label} (${t.count})` : t.label,
              onSelect: () => onChange(t.id),
            }))}
          />
        </div>
      ) : null}
    </div>
  );
}

export function TabPanel({ idBase, value, children }: { idBase: string; value: string; children: ReactNode }) {
  return (
    <div id={`${idBase}-panel`} role="tabpanel" aria-labelledby={`${idBase}-tab-${value}`} tabIndex={-1} className="flex min-w-0 flex-col gap-4 outline-none">
      {children}
    </div>
  );
}
