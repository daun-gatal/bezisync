import { forwardRef, useId, type AnchorHTMLAttributes, type ButtonHTMLAttributes, type MouseEvent, type ReactNode } from 'react';
import { createLink } from '@tanstack/react-router';
import clsx from 'clsx';
import { Loader2 } from 'lucide-react';

export type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger';

const VARIANTS: Record<ButtonVariant, string> = {
  primary: 'bg-accent text-on-accent border-accent hover:bg-accent-hover hover:border-accent-hover',
  secondary: 'bg-surface text-ink border-line-strong hover:bg-hover',
  ghost: 'bg-transparent text-ink-2 border-transparent hover:bg-hover hover:text-ink',
  danger: 'bg-bad text-on-bad border-bad hover:opacity-90',
};

/** Shared look: buttons and links that act like buttons render the same. Labels truncate, never overflow. */
export function buttonClass(variant: ButtonVariant = 'secondary', size: 'sm' | 'md' = 'md', iconOnly = false) {
  return clsx(
    'inline-flex max-w-full min-w-0 shrink-0 items-center justify-center gap-1.5 rounded-md border font-medium whitespace-nowrap no-underline transition-colors select-none',
    'aria-disabled:cursor-not-allowed aria-disabled:opacity-55 disabled:cursor-not-allowed disabled:opacity-55',
    size === 'sm' ? 'h-8 text-sm' : 'h-9 text-base',
    iconOnly ? (size === 'sm' ? 'w-8' : 'w-9') : size === 'sm' ? 'px-2.5' : 'px-3.5',
    VARIANTS[variant],
  );
}

export interface ButtonProps extends Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'children'> {
  variant?: ButtonVariant;
  size?: 'sm' | 'md';
  loading?: boolean;
  icon?: ReactNode;
  /** Why the button is disabled: shown as its tooltip and read by screen readers. */
  why?: string | null;
  /** Accessible name for an icon-only button (also its tooltip). */
  label?: string;
  children?: ReactNode;
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { variant = 'secondary', size = 'md', loading, icon, why, label, className, children, disabled, type = 'button', onClick, title, ...rest },
  ref,
) {
  const reasonId = useId();
  // A disabled button with a reason stays focusable (aria-disabled) so the reason can be read and hovered.
  const explained = Boolean(disabled && why);
  const blocked = Boolean(disabled || loading);
  return (
    <>
      <button
        ref={ref}
        type={type}
        disabled={blocked && !explained}
        aria-disabled={explained || undefined}
        aria-busy={loading || undefined}
        aria-label={children ? undefined : label}
        aria-describedby={explained ? reasonId : undefined}
        title={explained ? why! : (title ?? (children ? undefined : label))}
        className={clsx(buttonClass(variant, size, !children && Boolean(icon)), className)}
        onClick={(e: MouseEvent<HTMLButtonElement>) => {
          if (blocked) {
            e.preventDefault();
            return;
          }
          onClick?.(e);
        }}
        {...rest}
      >
        {loading ? (
          <Loader2 className="size-4 shrink-0 animate-spin" aria-hidden />
        ) : icon ? (
          <span className="flex shrink-0 [&>svg]:size-4" aria-hidden>
            {icon}
          </span>
        ) : null}
        {children ? <span className="min-w-0 truncate">{children}</span> : null}
      </button>
      {explained ? (
        <span id={reasonId} className="sr-only">
          {why}
        </span>
      ) : null}
    </>
  );
});

interface AnchorButtonProps extends AnchorHTMLAttributes<HTMLAnchorElement> {
  variant?: ButtonVariant;
  size?: 'sm' | 'md';
  icon?: ReactNode;
}

const AnchorButton = forwardRef<HTMLAnchorElement, AnchorButtonProps>(function AnchorButton(
  { variant = 'secondary', size = 'md', icon, className, children, ...rest },
  ref,
) {
  return (
    <a ref={ref} className={clsx(buttonClass(variant, size, !children && Boolean(icon)), className)} {...rest}>
      {icon ? (
        <span className="flex shrink-0 [&>svg]:size-4" aria-hidden>
          {icon}
        </span>
      ) : null}
      {children ? <span className="min-w-0 truncate">{children}</span> : null}
    </a>
  );
});

/** A router link that looks like a Button (one button style for both). */
export const ButtonLink = createLink(AnchorButton);

/** An external or full-page link that looks like a Button (sign-in with SSO, downloads). */
export const ButtonAnchor = AnchorButton;
