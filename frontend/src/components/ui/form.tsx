import {
  forwardRef,
  useEffect,
  useId,
  useRef,
  useState,
  type InputHTMLAttributes,
  type KeyboardEvent,
  type ReactNode,
  type TextareaHTMLAttributes,
} from 'react';
import clsx from 'clsx';
import { Minus, Plus, Search as SearchIcon, X } from 'lucide-react';
import { slugify } from '@/lib/format';
import { Button } from './button';

/** Field: a label, the control, then the error or the hint. `children` gets the control's id. */
export function Field({
  label,
  hint,
  error,
  optional,
  children,
  className,
  group,
}: {
  label: ReactNode;
  hint?: ReactNode;
  error?: string | null;
  optional?: boolean;
  children: (id: string) => ReactNode;
  className?: string;
  /** The control is a group (radio cards, switches): label it with aria-labelledby, not <label>. */
  group?: boolean;
}) {
  const id = useId();
  const labelId = `${id}-label`;
  const msgId = `${id}-msg`;
  const text = (
    <>
      {label}
      {optional ? <span className="font-normal text-muted"> (optional)</span> : null}
    </>
  );
  return (
    <div
      className={clsx('flex min-w-0 flex-col gap-1.5', className)}
      role={group ? 'group' : undefined}
      aria-labelledby={group ? labelId : undefined}
      aria-describedby={group && (error || hint) ? msgId : undefined}
    >
      {group ? (
        <span id={labelId} className="text-sm font-medium text-ink-2">
          {text}
        </span>
      ) : (
        <label htmlFor={id} id={labelId} className="text-sm font-medium text-ink-2">
          {text}
        </label>
      )}
      {children(id)}
      {error ? (
        <p id={msgId} className="m-0 text-xs text-bad wrap-break-word" role="alert">
          {error}
        </p>
      ) : hint ? (
        <p id={msgId} className="m-0 text-xs text-muted wrap-break-word">
          {hint}
        </p>
      ) : null}
    </div>
  );
}

export const controlClass =
  'h-9 w-full min-w-0 rounded-md border border-line-strong bg-surface px-2.5 text-base text-ink placeholder:text-muted disabled:cursor-not-allowed disabled:opacity-60 aria-invalid:border-bad';

export const Input = forwardRef<HTMLInputElement, InputHTMLAttributes<HTMLInputElement> & { invalid?: boolean }>(function Input(
  { className, invalid, ...rest },
  ref,
) {
  return <input ref={ref} aria-invalid={invalid || undefined} className={clsx(controlClass, className)} {...rest} />;
});

export const Textarea = forwardRef<HTMLTextAreaElement, TextareaHTMLAttributes<HTMLTextAreaElement> & { mono?: boolean }>(function Textarea(
  { className, mono, ...rest },
  ref,
) {
  return <textarea ref={ref} className={clsx(controlClass, 'h-auto min-h-24 py-2', mono && 'font-mono text-sm', className)} {...rest} />;
});

export function Search({
  value,
  onChange,
  label,
  placeholder,
  className,
}: {
  value: string;
  onChange: (v: string) => void;
  label: string;
  placeholder?: string;
  className?: string;
}) {
  // max-h-fit: in a column (a page or card body) the 220px basis would otherwise become its height.
  return (
    <div className={clsx('relative max-h-fit min-w-0 flex-[1_1_220px] sm:max-w-sm', className)}>
      <SearchIcon className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted" aria-hidden />
      <input
        type="search"
        aria-label={label}
        placeholder={placeholder ?? `${label}…`}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className={clsx(controlClass, 'pl-8')}
      />
    </div>
  );
}

/**
 * Type a name freely; the saved form (a slug) is shown under the box and never rewritten under
 * the cursor. `taken` are slugs already used in this scope.
 */
export function SlugInput({
  id,
  value,
  onChange,
  placeholder,
  disabled,
  taken = [],
  autoFocus,
}: {
  id: string;
  value: string;
  onChange: (raw: string) => void;
  placeholder?: string;
  disabled?: boolean;
  taken?: string[];
  autoFocus?: boolean;
}) {
  const slug = slugify(value);
  const error = slugError(value, taken);
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <Input
        id={id}
        value={value}
        placeholder={placeholder}
        disabled={disabled}
        invalid={Boolean(error)}
        autoFocus={autoFocus}
        spellCheck={false}
        autoComplete="off"
        onChange={(e) => onChange(e.target.value)}
      />
      {error ? (
        <span className="text-xs text-bad" role="alert">
          {error}
        </span>
      ) : value && slug !== value ? (
        <span className="text-xs text-muted wrap-anywhere">
          Saved as <code className="font-mono text-ink-2">{slug}</code>
        </span>
      ) : null}
    </div>
  );
}

export function slugError(value: string, taken: string[] = []): string | null {
  if (!value) return null;
  const slug = slugify(value);
  if (!slug) return 'Use at least one letter or digit.';
  if (taken.includes(slug)) return `“${slug}” is already used here. Choose another name.`;
  return null;
}

/** A bounded number. Clearing the box never saves 0: it puts back the last value. */
export function Stepper({
  id,
  value,
  onChange,
  min = 1,
  max = 3600,
  step = 1,
  unit,
  disabled,
}: {
  id: string;
  value: number;
  onChange: (v: number) => void;
  min?: number;
  max?: number;
  step?: number;
  unit?: string;
  disabled?: boolean;
}) {
  const [text, setText] = useState(String(value));
  useEffect(() => setText(String(value)), [value]);
  const commit = (t: string) => {
    const n = Number(t);
    if (t.trim() === '' || !Number.isFinite(n)) {
      setText(String(value));
      return;
    }
    const clamped = Math.min(max, Math.max(min, Math.round(n)));
    setText(String(clamped));
    if (clamped !== value) onChange(clamped);
  };
  return (
    <div className="flex items-center gap-1.5">
      <Button size="sm" icon={<Minus />} label="Decrease" disabled={disabled || value <= min} onClick={() => onChange(Math.max(min, value - step))} />
      <input
        id={id}
        inputMode="numeric"
        disabled={disabled}
        value={text}
        onChange={(e) => setText(e.target.value.replace(/[^\d]/g, ''))}
        onBlur={(e) => commit(e.target.value)}
        onKeyDown={(e) => e.key === 'Enter' && commit((e.target as HTMLInputElement).value)}
        className={clsx(controlClass, 'w-20 text-center tabular-nums')}
      />
      <Button size="sm" icon={<Plus />} label="Increase" disabled={disabled || value >= max} onClick={() => onChange(Math.min(max, value + step))} />
      {unit ? <span className="text-sm text-muted">{unit}</span> : null}
    </div>
  );
}

export function Switch({
  checked,
  onChange,
  label,
  description,
  disabled,
  why,
  id,
}: {
  checked: boolean;
  onChange: (on: boolean) => void;
  label: ReactNode;
  description?: ReactNode;
  disabled?: boolean;
  why?: string | null;
  id?: string;
}) {
  const auto = useId();
  const switchId = id ?? auto;
  const note = disabled && why ? why : description;
  return (
    <div className="flex min-w-0 items-start gap-3">
      <button
        id={switchId}
        type="button"
        role="switch"
        aria-checked={checked}
        aria-labelledby={`${switchId}-label`}
        aria-describedby={note ? `${switchId}-note` : undefined}
        disabled={disabled}
        onClick={() => onChange(!checked)}
        className={clsx(
          'relative mt-px inline-flex h-5 w-9 shrink-0 items-center rounded-full border transition-colors disabled:cursor-not-allowed disabled:opacity-55',
          checked ? 'border-accent bg-accent' : 'border-line-strong bg-grey-soft',
        )}
      >
        <span
          className={clsx('block size-3.5 rounded-full shadow-sm transition-transform', checked ? 'translate-x-4 bg-on-accent' : 'translate-x-0.5 bg-surface')}
          aria-hidden
        />
      </button>
      <span className="flex min-w-0 flex-col gap-0.5">
        <label
          htmlFor={switchId}
          id={`${switchId}-label`}
          className={clsx('text-base wrap-break-word', disabled ? 'cursor-not-allowed text-muted' : 'cursor-pointer')}
        >
          {label}
        </label>
        {note ? (
          <span id={`${switchId}-note`} className="text-xs text-muted wrap-break-word">
            {note}
          </span>
        ) : null}
      </span>
    </div>
  );
}

export function Check({
  checked,
  onChange,
  label,
  description,
  disabled,
  why,
  indeterminate,
  className,
}: {
  checked: boolean;
  onChange: (on: boolean) => void;
  label: ReactNode;
  description?: ReactNode;
  disabled?: boolean;
  why?: string | null;
  indeterminate?: boolean;
  className?: string;
}) {
  const ref = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (ref.current) ref.current.indeterminate = Boolean(indeterminate);
  }, [indeterminate]);
  const note = disabled && why ? why : description;
  return (
    <label
      title={disabled && why ? why : undefined}
      className={clsx('flex min-w-0 items-start gap-2.5 text-base', disabled ? 'cursor-not-allowed text-muted' : 'cursor-pointer', className)}
    >
      <input
        ref={ref}
        type="checkbox"
        checked={checked}
        disabled={disabled}
        onChange={(e) => onChange(e.target.checked)}
        className="mt-0.5 size-4 shrink-0 accent-accent"
      />
      <span className="flex min-w-0 flex-col gap-0.5 wrap-anywhere">
        <span>{label}</span>
        {note ? <span className="text-xs text-muted">{note}</span> : null}
      </span>
    </label>
  );
}

/** A bare checkbox for table rows (the row text is its label). */
export function RowCheck({
  checked,
  onChange,
  label,
  disabled,
  why,
  indeterminate,
}: {
  checked: boolean;
  onChange: (on: boolean) => void;
  label: string;
  disabled?: boolean;
  why?: string | null;
  indeterminate?: boolean;
}) {
  const ref = useRef<HTMLInputElement>(null);
  useEffect(() => {
    if (ref.current) ref.current.indeterminate = Boolean(indeterminate);
  }, [indeterminate]);
  return (
    <input
      ref={ref}
      type="checkbox"
      aria-label={label}
      title={disabled && why ? why : label}
      checked={checked}
      disabled={disabled}
      onClick={(e) => e.stopPropagation()}
      onChange={(e) => onChange(e.target.checked)}
      className="size-4 accent-accent disabled:cursor-not-allowed"
    />
  );
}

export interface CardOption<V extends string> {
  value: V;
  label: ReactNode;
  /** Plain-text label for tooltips when `label` is not a string. */
  text?: string;
  description?: ReactNode;
  badge?: ReactNode;
  disabled?: boolean;
  why?: string | null;
}

/** A choice between a few options, each with a second line of help. Disabled options say why. */
export function RadioCards<V extends string>({
  value,
  options,
  onChange,
  label,
  columns = 'auto',
  disabled,
}: {
  value: V | null | undefined;
  options: CardOption<V>[];
  onChange: (v: V) => void;
  label: string;
  columns?: 'auto' | 1 | 2;
  disabled?: boolean;
}) {
  const refs = useRef<(HTMLButtonElement | null)[]>([]);
  const enabled = options.map((o, i) => (o.disabled || disabled ? -1 : i)).filter((i) => i >= 0);
  const current = options.findIndex((o) => o.value === value);
  const focusable = current >= 0 && !options[current]?.disabled ? current : (enabled[0] ?? -1);
  const move = (e: KeyboardEvent, from: number) => {
    const dir = e.key === 'ArrowDown' || e.key === 'ArrowRight' ? 1 : e.key === 'ArrowUp' || e.key === 'ArrowLeft' ? -1 : 0;
    if (!dir || !enabled.length) return;
    e.preventDefault();
    const pos = enabled.indexOf(from);
    const next = enabled[(pos + dir + enabled.length) % enabled.length]!;
    refs.current[next]?.focus();
    onChange(options[next]!.value);
  };
  return (
    <div
      role="radiogroup"
      aria-label={label}
      className={clsx(
        'grid min-w-0 gap-2.5',
        columns === 1
          ? 'grid-cols-[minmax(0,1fr)]'
          : columns === 2
            ? 'grid-cols-[repeat(auto-fill,minmax(min(100%,260px),1fr))]'
            : 'grid-cols-[repeat(auto-fill,minmax(min(100%,220px),1fr))]',
      )}
    >
      {options.map((o, i) => {
        const checked = o.value === value;
        const off = disabled || o.disabled;
        const text = o.text ?? (typeof o.label === 'string' ? o.label : undefined);
        return (
          <button
            key={o.value}
            ref={(el) => {
              refs.current[i] = el;
            }}
            type="button"
            role="radio"
            aria-checked={checked}
            aria-disabled={off || undefined}
            tabIndex={i === focusable ? 0 : -1}
            title={off && o.why ? o.why : undefined}
            onClick={() => !off && onChange(o.value)}
            onKeyDown={(e) => move(e, i)}
            className={clsx(
              'flex min-w-0 flex-col items-start gap-1 rounded-lg border bg-surface px-3.5 py-3 text-left transition-colors',
              checked ? 'border-accent shadow-[inset_0_0_0_1px_var(--bz-accent)]' : 'border-line-strong',
              off ? 'cursor-not-allowed opacity-60' : 'hover:bg-hover',
            )}
          >
            <span className="flex w-full min-w-0 flex-wrap items-start justify-between gap-x-2 gap-y-1">
              <span className="line-clamp-2 min-w-0 flex-[1_1_120px] text-base font-semibold wrap-anywhere" title={text}>
                {o.label}
              </span>
              {o.badge ? <span className="flex shrink-0">{o.badge}</span> : null}
            </span>
            {(off && o.why) || o.description ? (
              <span className="line-clamp-3 min-w-0 text-sm text-muted wrap-break-word">{off && o.why ? o.why : o.description}</span>
            ) : null}
          </button>
        );
      })}
    </div>
  );
}

export function Segmented<V extends string>({
  value,
  options,
  onChange,
  label,
  disabled,
}: {
  value: V;
  options: { value: V; label: ReactNode }[];
  onChange: (v: V) => void;
  label: string;
  disabled?: boolean;
}) {
  return (
    <div role="group" aria-label={label} className="inline-flex max-w-full flex-wrap gap-0.5 rounded-md border border-line-strong bg-subtle p-0.5">
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          aria-pressed={value === o.value}
          disabled={disabled}
          onClick={() => onChange(o.value)}
          className={clsx(
            'h-7 rounded-[5px] px-2.5 text-sm font-medium whitespace-nowrap disabled:cursor-not-allowed',
            value === o.value ? 'bg-surface text-ink shadow-sm' : 'text-muted enabled:hover:text-ink',
          )}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

/** A list of values typed one at a time (email recipients), each validated, shown as chips. */
export function ChipsInput({
  id,
  values,
  onChange,
  placeholder,
  validate,
  hint = 'Press Enter after each one.',
}: {
  id: string;
  values: string[];
  onChange: (v: string[]) => void;
  placeholder?: string;
  validate?: (v: string) => string | null;
  hint?: string;
}) {
  const [text, setText] = useState('');
  const [error, setError] = useState<string | null>(null);
  const add = () => {
    const v = text.trim();
    if (!v) return;
    const problem = validate?.(v) ?? null;
    if (problem) {
      setError(problem);
      return;
    }
    if (!values.includes(v)) onChange([...values, v]);
    setText('');
    setError(null);
  };
  return (
    <div className="flex min-w-0 flex-col gap-1">
      <div className={clsx(controlClass, 'flex h-auto min-h-9 flex-wrap items-center gap-1.5 py-1')}>
        {values.map((v) => (
          <span key={v} className="inline-flex max-w-full min-w-0 items-center gap-1 rounded bg-grey-soft py-0.5 pr-0.5 pl-2 text-sm">
            <span className="min-w-0 truncate" title={v}>
              {v}
            </span>
            <button
              type="button"
              aria-label={`Remove ${v}`}
              onClick={() => onChange(values.filter((x) => x !== v))}
              className="grid size-5 place-items-center rounded hover:bg-hover"
            >
              <X className="size-3" aria-hidden />
            </button>
          </span>
        ))}
        <input
          id={id}
          value={text}
          placeholder={values.length ? '' : placeholder}
          aria-invalid={Boolean(error) || undefined}
          onChange={(e) => {
            setText(e.target.value);
            setError(null);
          }}
          onKeyDown={(e) => {
            if (e.key === 'Enter' || e.key === ',') {
              e.preventDefault();
              add();
            } else if (e.key === 'Backspace' && !text && values.length) onChange(values.slice(0, -1));
          }}
          onBlur={add}
          className="min-w-32 flex-1 border-0 bg-transparent text-base text-ink outline-none placeholder:text-muted"
        />
      </div>
      {error ? (
        <span className="text-xs text-bad" role="alert">
          {error}
        </span>
      ) : (
        <span className="text-xs text-muted">{hint}</span>
      )}
    </div>
  );
}
