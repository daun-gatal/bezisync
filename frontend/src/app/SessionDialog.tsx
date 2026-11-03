import { useState, type FormEvent } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useNavigate } from '@tanstack/react-router';
import { toast } from 'sonner';
import { wrongPassword } from '@/lib/errors';
import { Button } from '@/components/ui/button';
import { InlineError } from '@/components/ui/feedback';
import { Field, Input } from '@/components/ui/form';
import { Modal } from '@/components/ui/overlays';
import { meKey, useLogin, useProviders, type Me } from '@/lib/session';
import { ProviderButtons } from '@/features/auth/ProviderButtons';
import { sessionExpiry, useSessionExpired } from './guard';

/**
 * When the session ends mid-work (a 401 on any request), sign in again over the page: what
 * you entered stays where it is. Signing in as someone else starts afresh.
 */
export function SessionDialog() {
  const open = useSessionExpired();
  const qc = useQueryClient();
  const navigate = useNavigate();
  const providers = useProviders();
  const previous = qc.getQueryData<Me>(meKey);
  const login = useLogin();
  const [email, setEmail] = useState(previous?.user.email ?? '');
  const [password, setPassword] = useState('');

  const submit = (e: FormEvent) => {
    e.preventDefault();
    login.mutate(
      { email, password },
      {
        onSuccess: (me) => {
          setPassword('');
          sessionExpiry.set(false);
          if (previous && me.user.id !== previous.user.id) {
            qc.clear();
            qc.setQueryData(meKey, me);
            void navigate({ to: '/' });
            return;
          }
          void qc.invalidateQueries();
          toast.success('Signed in again. What you entered is still here: repeat your last action to finish it.');
        },
      },
    );
  };

  const passwordOn = providers.data?.password ?? true;
  const here = typeof window === 'undefined' ? '/' : window.location.pathname + window.location.search;
  return (
    <Modal
      open={open}
      onOpenChange={(next) => !next && sessionExpiry.set(false)}
      title="Your session ended"
      description="Sign in again to keep working. Nothing on this page is lost."
      size="sm"
    >
      <form id="session-signin" onSubmit={submit} className="flex flex-col gap-4">
        <InlineError
          error={login.error}
          doing="Couldn’t sign you in"
          fix={wrongPassword(login.error) ? 'Check the email and password, then try again.' : undefined}
        />
        {passwordOn ? (
          <>
            <Field label="Email">
              {(id) => <Input id={id} type="email" autoComplete="username" required value={email} onChange={(e) => setEmail(e.target.value)} />}
            </Field>
            <Field label="Password">
              {(id) => (
                <Input id={id} type="password" autoComplete="current-password" required autoFocus value={password} onChange={(e) => setPassword(e.target.value)} />
              )}
            </Field>
            <Button type="submit" variant="primary" loading={login.isPending}>
              Sign in
            </Button>
          </>
        ) : null}
        <ProviderButtons options={providers.data?.providers ?? []} next={here} primary={!passwordOn} suffix=" (leaves this page)" />
      </form>
    </Modal>
  );
}
