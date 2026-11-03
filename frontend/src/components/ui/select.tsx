import { useEffect, useId, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from 'react';
import * as Popover from '@radix-ui/react-popover';
import clsx from 'clsx';
import { Check as CheckIcon, ChevronsUpDown, Search as SearchIcon } from 'lucide-react';
import { controlClass, Switch } from './form';

export interface Option<V extends string = string> {
  value: V;
  label: ReactNode;
  /** Plain text for search and the closed control when `label` is not a string. */
  text?: string;
  /** A second line of help. */
  description?: ReactNode;
  disabled?: boolean;
  /** Why the option is disabled, shown in place of the description. */
  why?: string | null;
}

const textOf = (o: Option) => o.text ?? (typeof o.label === 'string' ? o.label : String(o.value));

/** The list shared by Select and Combobox: arrow keys move, Enter picks, disabled options say why. */
function OptionList<V extends string>({
  id,
  options,
  value,
  onPick,
  active,
  setActive,
  empty,
}: {
  id: string;
  options: Option<V>[];
  value: V | null | undefined;
  onPick: (v: V) => void;
  active: number;
  setActive: (i: number) => void;
  empty: string;
}) {
  const list = useRef<HTMLDivElement>(null);
  useEffect(() => {
    list.current?.querySelector(`[data-index="${active}"]`)?.scrollIntoView?.({ block: 'nearest' });
  }, [active]);
  if (!options.length) return <p className="m-0 px-3 py-4 text-center text-sm text-muted">{empty}</p>;
  return (
    <div ref={list} id={id} role="listbox" className="max-h-72 overflow-y-auto p-1">
      {options.map((o, i) => {
        const selected = o.value === value;
        const note = o.disabled && o.why ? o.why : o.description;
        return (
          <div
            key={o.value}
            id={`${id}-${i}`}
            data-index={i}
            role="option"
            aria-selected={selected}
            aria-disabled={o.disabled || undefined}
            aria-labelledby={`${id}-${i}-l`}
            aria-describedby={note ? `${id}-${i}-d` : undefined}
            title={o.disabled && o.why ? o.why : undefined}
            onMouseEnter={() => setActive(i)}
            onMouseDown={(e) => e.preventDefault()}
            onClick={() => !o.disabled && onPick(o.value)}
            className={clsx(
              'flex min-w-0 cursor-pointer items-start gap-2 rounded-md px-2 py-1.5',
              i === active && 'bg-hover',
              o.disabled && 'cursor-not-allowed opacity-55',
            )}
          >
            <span className="mt-0.5 flex size-4 shrink-0 text-accent">{selected ? <CheckIcon className="size-4" aria-hidden /> : null}</span>
            <span className="flex min-w-0 flex-col gap-0.5">
              <span id={`${id}-${i}-l`} className="text-base wrap-anywhere">
                {o.label}
              </span>
              {note ? (
                <span id={`${id}-${i}-d`} className="text-xs text-muted wrap-break-word">
                  {note}
                </span>
              ) : null}
            </span>
          </div>
        );
      })}
    </div>
  );
}

function useListKeys<V extends string>(
  options: Option<V>[],
  onPick: (v: V) => void,
  open: boolean,
  setOpen: (o: boolean) => void,
  value: V | null | undefined,
) {
  const [active, setActive] = useState(-1);
  const enabled = options.map((o, i) => (o.disabled ? -1 : i)).filter((i) => i >= 0);
  useEffect(() => {
    if (open)
      setActive(
        Math.max(
          0,
          options.findIndex((o) => o.value === value),
        ),
      );
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps
  const onKeyDown = (e: KeyboardEvent) => {
    if (!open) {
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp' || e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        setOpen(true);
      }
      return;
    }
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      if (!enabled.length) return;
      const pos = enabled.indexOf(active);
      const step = e.key === 'ArrowDown' ? 1 : -1;
      setActive(enabled[pos < 0 ? 0 : (pos + step + enabled.length) % enabled.length]!);
    } else if (e.key === 'Home' || e.key === 'End') {
      e.preventDefault();
      if (enabled.length) setActive(e.key === 'Home' ? enabled[0]! : enabled[enabled.length - 1]!);
    } else if (e.key === 'Enter' || (e.key === ' ' && e.currentTarget.tagName === 'BUTTON')) {
      e.preventDefault();
      const o = options[active];
      if (o && !o.disabled) onPick(o.value);
    }
  };
  return { active, setActive, onKeyDown };
}

const popClass =
  'z-50 w-[max(var(--radix-popover-trigger-width),240px)] max-w-[calc(100vw-24px)] rounded-lg border border-line bg-surface shadow-lg outline-none';

/** Pick one of a known set. Each option can carry a second line; disabled options explain why. */
export function Select<V extends string>({
  id,
  value,
  options,
  onChange,
  placeholder = 'Choose…',
  disabled,
  invalid,
  className,
  label,
}: {
  id?: string;
  value: V | null | undefined;
  options: Option<V>[];
  onChange: (v: V) => void;
  placeholder?: string;
  disabled?: boolean;
  invalid?: boolean;
  className?: string;
  /** Accessible name when there is no visible <label> (controls inside table rows). */
  label?: string;
}) {
  const auto = useId();
  const listId = `${id ?? auto}-list`;
  const [open, setOpen] = useState(false);
  const current = options.find((o) => o.value === value);
  const pick = (v: V) => {
    onChange(v);
    setOpen(false);
  };
  const keys = useListKeys(options, pick, open, setOpen, value);
  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Trigger asChild disabled={disabled}>
        <button
          id={id}
          type="button"
          aria-haspopup="listbox"
          aria-expanded={open}
          aria-controls={open ? listId : undefined}
          aria-activedescendant={open && keys.active >= 0 ? `${listId}-${keys.active}` : undefined}
          aria-invalid={invalid || undefined}
          aria-label={label ? `${label}: ${current ? textOf(current) : placeholder}` : undefined}
          onKeyDown={keys.onKeyDown}
          className={clsx(controlClass, 'flex items-center justify-between gap-2 text-left', className)}
        >
          <span className={clsx('min-w-0 truncate', !current && 'text-muted')}>{current ? current.label : placeholder}</span>
          <ChevronsUpDown className="size-4 shrink-0 text-muted" aria-hidden />
        </button>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content
          align="start"
          sideOffset={4}
          collisionPadding={12}
          className={popClass}
          onOpenAutoFocus={(e) => e.preventDefault()}
          onKeyDown={keys.onKeyDown}
        >
          <OptionList
            id={listId}
            options={options}
            value={value}
            onPick={pick}
            active={keys.active}
            setActive={keys.setActive}
            empty="Nothing to choose from."
          />
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}

/** Select for long lists (tables, owners, columns): a search box filters the options. */
export function Combobox<V extends string>({
  id,
  value,
  options,
  onChange,
  placeholder = 'Choose…',
  searchLabel = 'Search',
  disabled,
  footer,
}: {
  id?: string;
  value: V | null | undefined;
  options: Option<V>[];
  onChange: (v: V) => void;
  placeholder?: string;
  searchLabel?: string;
  disabled?: boolean;
  /** Extra content under the list, e.g. "Add a new team". */
  footer?: (close: () => void, query: string) => ReactNode;
}) {
  const auto = useId();
  const listId = `${id ?? auto}-list`;
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const current = options.find((o) => o.value === value);
  const shown = useMemo(() => {
    const q = query.trim().toLowerCase();
    return (q ? options.filter((o) => `${textOf(o)} ${typeof o.description === 'string' ? o.description : ''}`.toLowerCase().includes(q)) : options).slice(
      0,
      100,
    );
  }, [options, query]);
  const pick = (v: V) => {
    onChange(v);
    setOpen(false);
  };
  const keys = useListKeys(shown, pick, open, setOpen, value);
  useEffect(() => {
    if (!open) setQuery('');
  }, [open]);
  useEffect(() => keys.setActive(0), [query]); // eslint-disable-line react-hooks/exhaustive-deps
  return (
    <Popover.Root open={open} onOpenChange={setOpen}>
      <Popover.Trigger asChild disabled={disabled}>
        <button
          id={id}
          type="button"
          aria-haspopup="listbox"
          aria-expanded={open}
          className={clsx(controlClass, 'flex items-center justify-between gap-2 text-left')}
        >
          <span className={clsx('min-w-0 truncate', !current && 'text-muted')}>{current ? current.label : placeholder}</span>
          <SearchIcon className="size-4 shrink-0 text-muted" aria-hidden />
        </button>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content align="start" sideOffset={4} collisionPadding={12} className={clsx(popClass, 'w-[max(var(--radix-popover-trigger-width),320px)]')}>
          <div className="border-b border-line p-2">
            <input
              autoFocus
              type="search"
              role="combobox"
              aria-expanded
              aria-controls={listId}
              aria-activedescendant={keys.active >= 0 && shown[keys.active] ? `${listId}-${keys.active}` : undefined}
              aria-label={searchLabel}
              placeholder={`${searchLabel}…`}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={keys.onKeyDown}
              className={controlClass}
            />
          </div>
          <OptionList
            id={listId}
            options={shown}
            value={value}
            onPick={pick}
            active={keys.active}
            setActive={keys.setActive}
            empty={`Nothing matches “${query}”.`}
          />
          {footer ? <div className="border-t border-line p-1">{footer(() => setOpen(false), query)}</div> : null}
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}

const TIMES = Array.from({ length: 48 }, (_, i) => `${String(Math.floor(i / 2)).padStart(2, '0')}:${i % 2 ? '30' : '00'}`);
const TIME_OPTIONS: Option[] = TIMES.map((t) => ({ value: t, label: t }));

/** A daily window as two time pickers ("HH:MM-HH:MM" in UTC), or none. */
export function TimeRange({
  value,
  onChange,
  label,
  description,
  fallback = '22:00-06:00',
}: {
  value: string | null | undefined;
  onChange: (v: string | null) => void;
  label: string;
  description?: ReactNode;
  fallback?: string;
}) {
  const [from = '', to = ''] = value ? value.split('-') : [];
  const fromId = useId();
  const toId = useId();
  const withHalfHours = (t: string) => (TIMES.includes(t) || !t ? TIME_OPTIONS : [{ value: t, label: t }, ...TIME_OPTIONS]);
  return (
    <div className="flex min-w-0 flex-col gap-3">
      <Switch checked={Boolean(value)} onChange={(on) => onChange(on ? fallback : null)} label={label} description={description} />
      {value ? (
        <div className="flex flex-wrap items-end gap-3 pl-12">
          <div className="flex w-32 flex-col gap-1">
            <label htmlFor={fromId} className="text-xs text-muted">
              From (UTC)
            </label>
            <Select id={fromId} value={from} options={withHalfHours(from)} onChange={(v) => onChange(`${v}-${to}`)} />
          </div>
          <div className="flex w-32 flex-col gap-1">
            <label htmlFor={toId} className="text-xs text-muted">
              To (UTC)
            </label>
            <Select id={toId} value={to} options={withHalfHours(to)} onChange={(v) => onChange(`${from}-${v}`)} />
          </div>
          {from > to ? <span className="pb-2 text-sm text-muted">Runs overnight</span> : null}
        </div>
      ) : null}
    </div>
  );
}
