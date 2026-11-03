import { ButtonAnchor } from '@/components/ui/button';
import type { Schemas } from '@/lib/api';

export type SignInOption = Schemas['SignInOption'];

/** The start of single sign-on with one provider; the server returns to `next` afterwards. */
export function providerHref(option: SignInOption, next: string): string {
  return `/api/v1/auth/oidc/login?provider=${encodeURIComponent(option.id)}&next=${encodeURIComponent(next)}`;
}

/** One button per sign-in provider an Admin turned on (Settings → Sign-in). */
export function ProviderButtons({
  options,
  next,
  primary,
  suffix = '',
}: {
  options: SignInOption[];
  next: string;
  /** The first button is the page's primary action. */
  primary?: boolean;
  suffix?: string;
}) {
  return (
    <div className="flex flex-col gap-2">
      {options.map((o, i) => (
        <ButtonAnchor key={o.id} variant={primary && i === 0 ? 'primary' : 'secondary'} href={providerHref(o, next)}>
          Sign in with {o.label}
          {suffix}
        </ButtonAnchor>
      ))}
    </div>
  );
}
