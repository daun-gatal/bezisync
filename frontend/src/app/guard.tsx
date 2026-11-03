import { useRef, useSyncExternalStore, type ReactNode } from 'react';
import { Navigate, useRouter } from '@tanstack/react-router';
import { ApiError } from '@/lib/api';
import { useMeQuery, type Me } from '@/lib/session';
import { readStored, writeStored } from '@/components/ui/hooks';
import { FullPageError, FullPageLoading } from './ErrorPanel';

/**
 * Who may see a page, declared once per route:
 * - public: anyone (the testing policy);
 * - signed-out: only people who are not signed in (the sign-in page sends others on);
 * - signed-in: everything inside a project.
 * Admin-only screens check `canOpenSettings` and explain instead of redirecting.
 */
export type Access = 'public' | 'signed-out' | 'signed-in';

export type SessionState = 'loading' | 'signed-in' | 'signed-out' | 'unreachable';

/** Only a 401 means signed out. A network failure or a 500 is "unreachable", never a trip to sign-in. */
export function sessionState(q: { isPending: boolean; error: unknown; data?: unknown }): SessionState {
  if (q.data) return 'signed-in';
  if (q.isPending) return 'loading';
  if (q.error instanceof ApiError && q.error.status === 401) return 'signed-out';
  return 'unreachable';
}

export type Decision = 'render' | 'loading' | 'to-login' | 'to-next' | 'unreachable';

export function decide(access: Access, state: SessionState): Decision {
  if (access === 'public') return 'render';
  if (state === 'loading') return 'loading';
  if (access === 'signed-out') return state === 'signed-in' ? 'to-next' : 'render';
  if (state === 'signed-in') return 'render';
  return state === 'signed-out' ? 'to-login' : 'unreachable';
}

/** A same-site path to continue to after signing in; anything else goes home. */
export function safeNext(next: string | undefined | null): string {
  return next && next.startsWith('/') && !next.startsWith('//') && !next.startsWith('/\\') && !next.startsWith('/login') ? next : '/';
}

export function Guard({ access, children, next }: { access: Access; children: ReactNode; next?: string }) {
  const me = useMeQuery();
  const router = useRouter();
  const decision = decide(access, sessionState(me));
  // The address to come back to, frozen when the redirect starts: re-reading it while the
  // redirect is under way would nest ?next= inside itself.
  const back = useRef<string | null>(null);
  if (decision === 'to-login') back.current ??= router.state.location.href;
  else back.current = null;
  switch (decision) {
    case 'loading':
      return <FullPageLoading />;
    case 'to-login':
      return <Navigate to="/login" search={{ next: back.current! }} replace />;
    case 'to-next':
      return <Navigate to={safeNext(next)} replace />;
    case 'unreachable':
      return (
        <FullPageError
          title="Can’t reach BeziSync"
          detail="Your browser couldn’t get an answer from the BeziSync server. It may be restarting, or your connection dropped."
          error={me.error}
          onRetry={() => void me.refetch()}
        />
      );
    default:
      return <>{children}</>;
  }
}

const LAST_PROJECT = 'bz.lastProject';

/** Home opens the project you used last, if you still have it; otherwise the first one. */
export function homeProject(me: Me, last = readStored(LAST_PROJECT)): string | null {
  const projects = me.projects;
  return (projects.find((p) => p.slug === last) ?? projects[0])?.slug ?? null;
}

export function rememberProject(slug: string) {
  if (readStored(LAST_PROJECT) !== slug) writeStored(LAST_PROJECT, slug);
}

/* ── Session expiry: a 401 mid-session opens a sign-in dialog over the page instead of navigating away. ── */

let expired = false;
const listeners = new Set<() => void>();
export const sessionExpiry = {
  set(value: boolean) {
    if (expired === value) return;
    expired = value;
    listeners.forEach((l) => l());
  },
  get: () => expired,
  subscribe(l: () => void) {
    listeners.add(l);
    return () => listeners.delete(l);
  },
};

export function useSessionExpired(): boolean {
  return useSyncExternalStore(sessionExpiry.subscribe, sessionExpiry.get, sessionExpiry.get);
}
