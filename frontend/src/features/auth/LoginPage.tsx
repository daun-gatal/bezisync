import { useState, type FormEvent } from 'react';
import { Link, useSearch } from '@tanstack/react-router';
import { FlaskConical } from 'lucide-react';
import { wrongPassword } from '@/lib/errors';
import { Button } from '@/components/ui/button';
import { InlineError, Notice } from '@/components/ui/feedback';
import { Field, Input } from '@/components/ui/form';
import { Title } from '@/components/ui/text';
import { safeNext } from '@/app/guard';
import { useLogin, useProviders } from '@/lib/session';
import { ProviderButtons } from './ProviderButtons';

/**
 * Sign-in. Only signed-out people see it: the route guard sends anyone with a session on to
 * where they were going, which is also what happens right after a successful sign-in.
 */
export function LoginPage() {
  const { next, error } = useSearch({ from: '/login' });
  const providers = useProviders();
  const lab = providers.data?.lab ?? false;
  const options = providers.data?.providers ?? [];
  // Password sign-in is on unless an Admin turned it off (and a provider replaces it).
  const password = providers.data?.password ?? true;
  const login = useLogin();
  const [email, setEmail] = useState('');
  const [secret, setSecret] = useState('');

  function submit(event: FormEvent) {
    event.preventDefault();
    login.mutate({ email, password: secret });
  }

  return (
    <main className="flex min-h-screen flex-col items-center justify-center gap-4 bg-bg px-4 py-12">
      {lab ? (
        <div className="flex w-full max-w-sm items-start gap-2.5 rounded-xl bg-accent-soft px-3.5 py-3 text-sm text-accent-ink">
          <FlaskConical className="mt-px size-4 shrink-0" aria-hidden />
          <span>
            <strong className="font-semibold">BeziSync Lab is open.</strong>{' '}
            {options.length === 1 ? `Sign in with any ${options[0]?.label ?? ''} account` : 'Sign in'} to try change data capture on live sample databases. Nothing to
            install.
          </span>
        </div>
      ) : null}
      <form onSubmit={submit} className="flex w-full max-w-sm flex-col gap-5 rounded-2xl border border-line bg-surface p-7">
        <div className="flex items-center gap-3">
          <img src="/favicon.svg" alt="" className="size-9" />
          <div className="flex min-w-0 flex-col">
            <Title as="h1" className="text-xl font-semibold">
              Sign in to BeziSync
            </Title>
            <p className="m-0 text-sm text-muted">Change data capture, made safe.</p>
          </div>
        </div>
        {login.error ? (
          <InlineError
            error={login.error}
            doing="Couldn’t sign you in"
            fix={wrongPassword(login.error) ? 'Check the email and password, then try again.' : undefined}
          />
        ) : error ? (
          <Notice tone="bad">{error}</Notice>
        ) : null}
        {options.length ? <ProviderButtons options={options} next={safeNext(next)} primary /> : null}
        {options.length && password ? (
          <div className="flex items-center gap-3 text-xs text-muted" aria-hidden>
            <span className="h-px flex-1 bg-line" /> or with a password <span className="h-px flex-1 bg-line" />
          </div>
        ) : null}
        {password ? (
          <>
            <Field label="Email">
              {(id) => <Input id={id} type="email" autoComplete="username" required value={email} onChange={(e) => setEmail(e.target.value)} autoFocus />}
            </Field>
            <Field label="Password">
              {(id) => (
                <Input id={id} type="password" autoComplete="current-password" required value={secret} onChange={(e) => setSecret(e.target.value)} />
              )}
            </Field>
            <Button type="submit" variant={options.length ? 'secondary' : 'primary'} loading={login.isPending}>
              Sign in
            </Button>
          </>
        ) : null}
      </form>
      {lab ? (
        <p className="m-0 w-full max-w-sm text-center text-sm text-muted">
          By signing in you agree to the{' '}
          <Link to="/testing-policy" className="font-semibold text-accent hover:underline">
            testing policy
          </Link>
          . Found a problem? Use <strong className="font-semibold text-ink-2">Feedback</strong> in the app.
        </p>
      ) : null}
    </main>
  );
}
