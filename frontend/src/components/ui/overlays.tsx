import { useEffect, useId, useState, type ReactNode } from 'react';
import * as Dialog from '@radix-ui/react-dialog';
import * as Dropdown from '@radix-ui/react-dropdown-menu';
import { Check, ChevronDown, Copy, MoreHorizontal, X } from 'lucide-react';
import clsx from 'clsx';
import { Button, type ButtonVariant } from './button';
import { InlineError } from './feedback';
import { Check as CheckBox, Field, Input } from './form';
import { Title } from './text';

const SIZES = { sm: 'max-w-md', md: 'max-w-lg', lg: 'max-w-3xl', xl: 'max-w-5xl' } as const;

/**
 * Modal: the header grid lets long titles wrap beside the close button, the body scrolls and
 * the footer stays. With `dirty`, closing (Esc, outside click, ×) asks before discarding.
 * `why` explains a disabled primary action in the footer.
 */
export function Modal({
  open,
  onOpenChange,
  title,
  description,
  children,
  footer,
  size = 'md',
  dirty,
  why,
  closable = true,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: ReactNode;
  description?: ReactNode;
  children?: ReactNode;
  footer?: ReactNode;
  size?: keyof typeof SIZES;
  dirty?: boolean;
  why?: string | null;
  /** false: only the footer can close it (one-time secrets). */
  closable?: boolean;
}) {
  const [asking, setAsking] = useState(false);
  useEffect(() => {
    if (!open) setAsking(false);
  }, [open]);
  const request = (next: boolean) => {
    if (next) return onOpenChange(true);
    if (!closable) return;
    if (dirty) setAsking(true);
    else onOpenChange(false);
  };
  return (
    <Dialog.Root open={open} onOpenChange={request}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-40 bg-scrim" />
        <Dialog.Content
          {...(description ? {} : { 'aria-describedby': undefined })}
          onEscapeKeyDown={(e) => !closable && e.preventDefault()}
          onPointerDownOutside={(e) => !closable && e.preventDefault()}
          className={clsx(
            'fixed top-1/2 left-1/2 z-50 flex max-h-[min(88vh,900px)] w-[calc(100vw-24px)] -translate-x-1/2 -translate-y-1/2 flex-col rounded-xl border border-line bg-surface shadow-xl outline-none',
            SIZES[size],
          )}
        >
          <div className="grid shrink-0 grid-cols-[minmax(0,1fr)_auto] items-start gap-4 border-b border-line px-5 py-4">
            <div className="flex min-w-0 flex-col gap-1">
              <Dialog.Title asChild>
                <Title as="h2" lines={3} className="text-lg font-semibold">
                  {title}
                </Title>
              </Dialog.Title>
              {description ? <Dialog.Description className="m-0 text-sm text-muted wrap-break-word">{description}</Dialog.Description> : null}
            </div>
            {closable ? (
              <Dialog.Close asChild>
                <Button variant="ghost" size="sm" icon={<X />} label="Close" />
              </Dialog.Close>
            ) : null}
          </div>
          <div className="flex min-h-0 min-w-0 flex-col gap-4 overflow-y-auto px-5 py-4">{children}</div>
          {footer ? (
            <div className="flex shrink-0 flex-wrap items-center justify-end gap-2 border-t border-line px-5 py-3">
              {why ? <span className="mr-auto min-w-0 flex-[1_1_200px] text-xs text-muted">{why}</span> : null}
              {footer}
            </div>
          ) : null}
          <Dialog.Root open={asking} onOpenChange={setAsking}>
            <Dialog.Portal>
              <Dialog.Overlay className="fixed inset-0 z-50 bg-scrim" />
              <Dialog.Content
                role="alertdialog"
                className="fixed top-1/2 left-1/2 z-50 flex w-[calc(100vw-24px)] max-w-md -translate-x-1/2 -translate-y-1/2 flex-col gap-4 rounded-xl border border-line bg-surface p-5 shadow-xl"
              >
                <Dialog.Title className="m-0 text-lg font-semibold">Discard your changes?</Dialog.Title>
                <Dialog.Description className="m-0 text-base text-muted">What you entered here hasn’t been saved.</Dialog.Description>
                <div className="flex flex-wrap justify-end gap-2">
                  <Button onClick={() => setAsking(false)}>Keep editing</Button>
                  <Button
                    variant="danger"
                    onClick={() => {
                      setAsking(false);
                      onOpenChange(false);
                    }}
                  >
                    Discard
                  </Button>
                </div>
              </Dialog.Content>
            </Dialog.Portal>
          </Dialog.Root>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

/**
 * Confirmation for destructive and powerful actions. It lists what will happen; irreversible
 * actions need the name typed. The typed text clears every time it opens.
 */
export function Confirm({
  open,
  onOpenChange,
  title,
  effects,
  children,
  confirmLabel,
  typeToConfirm,
  tone = 'danger',
  onConfirm,
  pending,
  error,
  doing,
  disabled,
  why,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: ReactNode;
  effects?: ReactNode[];
  children?: ReactNode;
  confirmLabel: string;
  typeToConfirm?: string;
  tone?: 'danger' | 'primary';
  onConfirm: () => void;
  pending?: boolean;
  error?: unknown;
  doing?: string;
  disabled?: boolean;
  why?: string | null;
}) {
  const [typed, setTyped] = useState('');
  useEffect(() => {
    if (open) setTyped('');
  }, [open]);
  const typedOk = !typeToConfirm || typed === typeToConfirm;
  const blockedWhy = !typedOk ? 'Type the name to confirm.' : disabled ? (why ?? null) : null;
  return (
    <Modal
      open={open}
      onOpenChange={(next) => !pending && onOpenChange(next)}
      title={title}
      size="sm"
      why={blockedWhy}
      footer={
        <>
          <Button onClick={() => onOpenChange(false)} disabled={pending}>
            Cancel
          </Button>
          <Button variant={tone as ButtonVariant} disabled={!typedOk || disabled} loading={pending} onClick={onConfirm}>
            {confirmLabel}
          </Button>
        </>
      }
    >
      {effects?.length ? (
        <div className="flex flex-col gap-2">
          <strong className="text-sm font-semibold text-ink">What happens</strong>
          <ul className="m-0 flex list-disc flex-col gap-1 pl-5 text-base text-ink-2 wrap-break-word">
            {effects.map((e, i) => (
              <li key={i}>{e}</li>
            ))}
          </ul>
        </div>
      ) : null}
      {children ? <div className="flex flex-col gap-3 text-base text-ink-2 wrap-break-word">{children}</div> : null}
      {typeToConfirm ? (
        <Field
          label={
            <>
              Type <code className="font-mono text-ink wrap-anywhere">{typeToConfirm}</code> to confirm
            </>
          }
        >
          {(id) => <Input id={id} value={typed} autoComplete="off" spellCheck={false} onChange={(e) => setTyped(e.target.value)} />}
        </Field>
      ) : null}
      <InlineError error={error} doing={doing} />
    </Modal>
  );
}

/**
 * Shows a secret exactly once (API tokens, agent tokens). It can't be dismissed by accident:
 * Done needs "I've copied it", and Esc or an outside click do nothing.
 */
export function OneTimeSecret({
  open,
  onDone,
  title,
  secret,
  label,
  children,
}: {
  open: boolean;
  onDone: () => void;
  title: ReactNode;
  secret: string;
  label: string;
  children?: ReactNode;
}) {
  const [copied, setCopied] = useState(false);
  useEffect(() => {
    if (open) setCopied(false);
  }, [open]);
  return (
    <Modal
      open={open}
      onOpenChange={() => undefined}
      closable={false}
      title={title}
      size="md"
      why={copied ? null : 'Copy it first: it won’t be shown again.'}
      footer={
        <Button variant="primary" disabled={!copied} onClick={onDone}>
          Done
        </Button>
      }
    >
      {children}
      <CodeBlock code={secret} label={label} wrap />
      <CheckBox checked={copied} onChange={setCopied} label="I’ve copied it and stored it somewhere safe" />
    </Modal>
  );
}

export function CodeBlock({ code, label, dark = true, wrap, className }: { code: string; label?: string; dark?: boolean; wrap?: boolean; className?: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className={clsx('relative min-w-0', className)}>
      <pre
        aria-label={label}
        tabIndex={0}
        data-scroll-x
        className={clsx(
          'relative m-0 max-h-[60vh] overflow-auto rounded-md p-3.5 pr-12 font-mono text-sm leading-relaxed',
          wrap ? 'break-all whitespace-pre-wrap' : 'whitespace-pre',
          dark ? 'bg-code-bg text-code-ink' : 'border border-line bg-subtle text-ink',
        )}
      >
        {code}
      </pre>
      <button
        type="button"
        aria-label={copied ? 'Copied' : 'Copy to clipboard'}
        title={copied ? 'Copied' : 'Copy to clipboard'}
        onClick={() => {
          const done = () => {
            setCopied(true);
            window.setTimeout(() => setCopied(false), 1500);
          };
          void navigator.clipboard?.writeText(code).then(done, done);
        }}
        className={clsx(
          'absolute top-2 right-2 grid size-8 place-items-center rounded-md',
          dark ? 'text-code-ink hover:bg-white/10' : 'text-muted hover:bg-hover',
        )}
      >
        {copied ? <Check className="size-4" aria-hidden /> : <Copy className="size-4" aria-hidden />}
      </button>
    </div>
  );
}

export type MenuEntry =
  | {
      label: string;
      onSelect: () => void;
      icon?: ReactNode;
      description?: string;
      disabled?: boolean;
      /** Why it is disabled, shown under the label. */
      why?: string | null;
      danger?: boolean;
    }
  | { heading: string }
  | '-'
  | null
  | false
  | undefined;

/** An action menu. It flips to stay on screen; disabled items stay visible and say why. */
export function Menu({
  label,
  items,
  variant = 'secondary',
  size = 'md',
  icon,
  align = 'end',
  trigger,
}: {
  label: string;
  items: MenuEntry[];
  variant?: ButtonVariant;
  size?: 'sm' | 'md';
  /** Icon-only trigger (⋯) when set to "more". */
  icon?: 'more' | ReactNode;
  align?: 'start' | 'end';
  /** A custom trigger element (must forward refs). */
  trigger?: ReactNode;
}) {
  const id = useId();
  const entries = items.filter(Boolean) as Exclude<MenuEntry, null | false | undefined>[];
  return (
    <Dropdown.Root>
      <Dropdown.Trigger asChild>
        {trigger ??
          (icon === 'more' ? (
            <Button variant={variant} size={size} icon={<MoreHorizontal />} label={label} />
          ) : (
            <Button variant={variant} size={size} icon={icon}>
              <span className="inline-flex items-center gap-1">
                {label}
                <ChevronDown className="size-3.5" aria-hidden />
              </span>
            </Button>
          ))}
      </Dropdown.Trigger>
      <Dropdown.Portal>
        <Dropdown.Content
          align={align}
          sideOffset={6}
          collisionPadding={12}
          className="z-50 max-h-[var(--radix-dropdown-menu-content-available-height)] w-max max-w-[min(340px,calc(100vw-24px))] min-w-56 overflow-y-auto rounded-lg border border-line bg-surface p-1 shadow-lg"
        >
          {entries.map((it, i) =>
            it === '-' ? (
              <Dropdown.Separator key={`${id}-${i}`} className="my-1 h-px bg-line" />
            ) : 'heading' in it ? (
              <Dropdown.Label key={`${id}-${i}`} className="px-2 pt-2 pb-1 text-xs font-semibold text-muted">
                {it.heading}
              </Dropdown.Label>
            ) : (
              <Dropdown.Item
                key={`${id}-${i}`}
                disabled={it.disabled}
                onSelect={it.onSelect}
                className={clsx(
                  'flex min-w-0 cursor-pointer items-start gap-2 rounded-md px-2 py-1.5 outline-none data-disabled:cursor-not-allowed data-highlighted:bg-hover',
                  it.danger ? 'text-bad' : 'text-ink',
                )}
              >
                <span
                  className={clsx('mt-0.5 flex size-4 shrink-0 [&>svg]:size-4', it.danger ? 'text-bad' : 'text-muted', it.disabled && 'opacity-55')}
                  aria-hidden
                >
                  {it.icon}
                </span>
                <span className="flex min-w-0 flex-col gap-0.5">
                  <span className={clsx('text-base wrap-break-word', it.disabled && 'opacity-55')}>{it.label}</span>
                  {(it.disabled && it.why) || it.description ? (
                    <span className="text-xs text-muted wrap-break-word">{it.disabled && it.why ? it.why : it.description}</span>
                  ) : null}
                </span>
              </Dropdown.Item>
            ),
          )}
        </Dropdown.Content>
      </Dropdown.Portal>
    </Dropdown.Root>
  );
}
