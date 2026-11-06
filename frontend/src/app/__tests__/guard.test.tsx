import { screen, waitFor } from '@testing-library/react';
import { ApiError, client } from '@/lib/api';
import type { Me } from '@/lib/session';
import { LoginPage } from '@/features/auth/LoginPage';
import { accountsPhrase, TestingPolicyPage } from '@/features/auth/TestingPolicyPage';
import { renderAt } from '@/test/render';
import { decide, Guard, homeProject, safeNext, sessionState, type Access, type Decision, type SessionState } from '../guard';

const net = new ApiError({ status: 0, code: 'network_error', detail: 'Cannot reach the BeziSync server.' });
const e401 = new ApiError({ status: 401, code: 'unauthenticated', detail: 'Sign in' });
const e500 = new ApiError({ status: 500, code: 'internal', detail: 'Boom' });

describe('sessionState: only a 401 means signed out', () => {
  it.each([
    [{ isPending: true, error: null }, 'loading'],
    [{ isPending: false, error: null, data: { user: {} } }, 'signed-in'],
    [{ isPending: false, error: e401 }, 'signed-out'],
    [{ isPending: false, error: net }, 'unreachable'],
    [{ isPending: false, error: e500 }, 'unreachable'],
    [{ isPending: false, error: new Error('x') }, 'unreachable'],
  ] as const)('%o → %s', (q, want) => {
    expect(sessionState(q)).toBe(want);
  });
});

describe('decide: one table for every page', () => {
  const cases: [Access, SessionState, Decision][] = [
    ['public', 'loading', 'render'],
    ['public', 'signed-in', 'render'],
    ['public', 'signed-out', 'render'],
    ['public', 'unreachable', 'render'],
    ['signed-out', 'loading', 'loading'],
    ['signed-out', 'signed-in', 'to-next'],
    ['signed-out', 'signed-out', 'render'],
    ['signed-out', 'unreachable', 'render'],
    ['signed-in', 'loading', 'loading'],
    ['signed-in', 'signed-in', 'render'],
    ['signed-in', 'signed-out', 'to-login'],
    ['signed-in', 'unreachable', 'unreachable'],
  ];
  it.each(cases)('%s page, %s → %s', (access, state, want) => {
    expect(decide(access, state)).toBe(want);
  });
});

describe('safeNext', () => {
  it.each([
    ['/p/default/alerts?tab=rules', '/p/default/alerts?tab=rules'],
    [undefined, '/'],
    ['https://evil.example', '/'],
    ['//evil.example', '/'],
    ['/\\evil.example', '/'],
    ['/login?next=/x', '/'],
  ])('%s → %s', (next, want) => {
    expect(safeNext(next)).toBe(want);
  });
});

describe('homeProject', () => {
  const me = { user: {}, projects: [{ slug: 'default' }, { slug: 'playground' }] } as unknown as Me;
  it('opens the last project you used, if you still have it', () => {
    expect(homeProject(me, 'playground')).toBe('playground');
    expect(homeProject(me, 'gone')).toBe('default');
    expect(homeProject(me, null)).toBe('default');
    expect(homeProject({ ...me, projects: [] } as unknown as Me, null)).toBeNull();
  });
});

const ME = {
  user: { id: 'u', email: 'sam@example.com', name: 'Sam', is_superuser: false, ui_mode: 'guided', muted_notifications: [] },
  projects: [{ id: 'p', slug: 'playground', name: 'Playground', role: 'editor', permissions: ['view'], is_sandbox: false }],
};

function api(routes: Record<string, { status: number; body: unknown } | 'offline'>) {
  vi.spyOn(client, 'GET').mockImplementation(((path: string) => {
    const hit = routes[path] ?? { status: 404, body: { code: 'not_found', detail: 'Not found' } };
    if (hit === 'offline') return Promise.reject(new TypeError('Failed to fetch'));
    const response = new Response(JSON.stringify(hit.body), { status: hit.status });
    return Promise.resolve(hit.status < 400 ? { data: hit.body, response } : { error: hit.body, response });
  }) as never);
}

afterEach(() => vi.restoreAllMocks());

describe('pages know who is looking', () => {
  it('the testing policy offers "Back to BeziSync" to someone signed in', async () => {
    api({ '/api/v1/auth/providers': { status: 200, body: { password: true, providers: [{ id: 'p1', label: 'Google' }], lab: true } }, '/api/v1/auth/me': { status: 200, body: ME } });
    renderAt(TestingPolicyPage, { path: '/testing-policy' });
    expect(await screen.findByRole('link', { name: 'Back to BeziSync' }, { timeout: 5000 })).toHaveAttribute('href', '/');
    expect(screen.queryByRole('link', { name: 'Sign in to try it' })).toBeNull();
  });

  it('the testing policy names every way the lab lets testers sign in', async () => {
    api({
      '/api/v1/auth/providers': {
        status: 200,
        body: { password: false, providers: [{ id: 'p1', label: 'Google' }, { id: 'p2', label: 'GitHub' }], lab: true },
      },
      '/api/v1/auth/me': { status: 401, body: { code: 'unauthenticated', detail: 'Sign in' } },
    });
    renderAt(TestingPolicyPage, { path: '/testing-policy' });
    expect(await screen.findByText(/Anyone with a Google or GitHub account can try it here/)).toBeInTheDocument();
    expect(accountsPhrase(['Google'])).toBe('a Google account');
    expect(accountsPhrase(['Google', 'GitHub', 'Okta', 'GitHub'])).toBe('a Google, GitHub or Okta account');
    expect(accountsPhrase(['Okta'])).toBe('an Okta account');
    expect(accountsPhrase([])).toBe('an account');
  });

  it('the testing policy points at the open source and the private GitHub report', async () => {
    api({
      '/api/v1/auth/providers': { status: 200, body: { password: true, providers: [{ id: 'p1', label: 'Google' }], lab: true } },
      '/api/v1/auth/me': { status: 401, body: { code: 'unauthenticated', detail: 'Sign in' } },
    });
    renderAt(TestingPolicyPage, { path: '/testing-policy' });
    expect(await screen.findByText(/open source \(Apache 2\.0\)/)).toBeInTheDocument();
    expect(screen.queryByText(/source code isn.t public/)).toBeNull();
    expect(screen.getByRole('link', { name: 'GitHub' })).toHaveAttribute('href', 'https://github.com/daun-gatal/bezisync');
    expect(screen.getByRole('link', { name: 'Security → Report a vulnerability' })).toHaveAttribute(
      'href',
      'https://github.com/daun-gatal/bezisync/security/advisories/new',
    );
  });

  it('the testing policy offers "Sign in to try it" to someone signed out', async () => {
    api({
      '/api/v1/auth/providers': { status: 200, body: { password: true, providers: [{ id: 'p1', label: 'Google' }], lab: true } },
      '/api/v1/auth/me': { status: 401, body: { code: 'unauthenticated', detail: 'Sign in' } },
    });
    renderAt(TestingPolicyPage, { path: '/testing-policy' });
    expect(await screen.findByRole('link', { name: 'Sign in to try it' })).toHaveAttribute('href', '/login');
  });

  it('the sign-in page sends someone already signed in on to where they were going', async () => {
    api({ '/api/v1/auth/providers': { status: 200, body: { password: true, providers: [], lab: false } }, '/api/v1/auth/me': { status: 200, body: ME } });
    renderAt(
      () => (
        <Guard access="signed-out" next="/p/playground/alerts">
          <LoginPage />
        </Guard>
      ),
      { path: '/login' },
    );
    await waitFor(() => expect(screen.getByTestId('elsewhere')).toHaveTextContent('/p/playground/alerts'));
  });

  it('a signed-in page that cannot reach the server says so, instead of sending you to sign in', async () => {
    api({ '/api/v1/auth/me': 'offline' });
    renderAt(
      () => (
        <Guard access="signed-in">
          <p>secret</p>
        </Guard>
      ),
      { path: '/p/x/pipelines' },
    );
    expect(await screen.findByRole('heading', { name: 'Can’t reach BeziSync' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Try again' })).toBeInTheDocument();
    expect(screen.queryByText('secret')).toBeNull();
    expect(screen.queryByTestId('elsewhere')).toBeNull();
  });

  it('a signed-in page sends someone signed out to sign in, keeping the address to come back to', async () => {
    api({ '/api/v1/auth/me': { status: 401, body: { code: 'unauthenticated', detail: 'Sign in' } } });
    renderAt(
      () => (
        <Guard access="signed-in">
          <p>secret</p>
        </Guard>
      ),
      { path: '/p/x/pipelines', at: '/p/x/pipelines?q=orders' },
    );
    await waitFor(() => expect(screen.getByTestId('elsewhere')).toHaveTextContent('/login?next=%2Fp%2Fx%2Fpipelines%3Fq%3Dorders'));
  });
});
