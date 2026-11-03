import { Navigate, Outlet, createRootRouteWithContext, createRoute, createRouter, lazyRouteComponent, redirect, useParams } from '@tanstack/react-router';
import type { QueryClient } from '@tanstack/react-query';
import { Toaster } from 'sonner';
import { useEffect } from 'react';
import { AppShell } from './AppShell';
import { ErrorPanel, NotFound, RouteError } from './ErrorPanel';
import { Guard, homeProject, rememberProject } from './guard';
import { SessionDialog } from './SessionDialog';
import { ProjectProvider, useMeQuery } from '@/lib/session';
import { LoginPage } from '@/features/auth/LoginPage';
import { TestingPolicyPage } from '@/features/auth/TestingPolicyPage';
import { PipelinesPage } from '@/features/pipelines/PipelinesPage';
import { ConsentPage } from '@/features/mcp/ConsentPage';
import { LEGACY_ADMIN_SECTIONS, LEGACY_SECTIONS, SettingsPage } from '@/features/settings/SettingsPage';
import { AccountPage } from '@/features/account/AccountPage';
import { ConnectionsPage } from '@/features/connections/ConnectionsPage';
import { AlertsPage } from '@/features/alerts/AlertsPage';
import { PipelinePage } from '@/features/pipelines/PipelinePage';

export interface RouterContext {
  queryClient: QueryClient;
}

const rootRoute = createRootRouteWithContext<RouterContext>()({
  component: () => (
    <>
      <Outlet />
      <SessionDialog />
      <Toaster position="bottom-right" richColors closeButton />
    </>
  ),
  notFoundComponent: NotFound,
});

const loginRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/login',
  validateSearch: (s: Record<string, unknown>): { next?: string; error?: string } => ({
    ...(typeof s.next === 'string' ? { next: s.next } : {}),
    ...(typeof s.error === 'string' ? { error: s.error } : {}),
  }),
  component: function Login() {
    const { next } = loginRoute.useSearch();
    return (
      <Guard access="signed-out" next={next}>
        <LoginPage />
      </Guard>
    );
  },
});

/** OAuth consent for MCP agents: the API sends people here from /api/v1/oauth/authorize. */
const consentRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/oauth/consent',
  validateSearch: (s: Record<string, unknown>): { request?: string; error?: string } => ({
    ...(typeof s.request === 'string' ? { request: s.request } : {}),
    ...(typeof s.error === 'string' ? { error: s.error } : {}),
  }),
  component: function Consent() {
    const { request, error } = consentRoute.useSearch();
    return (
      <Guard access="signed-in">
        <ConsentPage request={request} error={error} />
      </Guard>
    );
  },
});

const testingPolicyRoute = createRoute({ getParentRoute: () => rootRoute, path: '/testing-policy', component: TestingPolicyPage });

/** The component gallery: development builds only (import.meta.env.DEV is false in production, so it is never bundled). */
const galleryRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: '/__ui',
  component: import.meta.env.DEV ? lazyRouteComponent(() => import('./Gallery'), 'Gallery') : NotFound,
});

function Home() {
  return (
    <Guard access="signed-in">
      <HomeRedirect />
    </Guard>
  );
}

function HomeRedirect() {
  const me = useMeQuery().data!;
  const slug = homeProject(me);
  if (!slug) return <ErrorPanel title="No projects yet" detail="Ask a BeziSync Admin to add you to a project." home={false} />;
  return <Navigate to="/p/$projectSlug/pipelines" params={{ projectSlug: slug }} replace />;
}

const indexRoute = createRoute({ getParentRoute: () => rootRoute, path: '/', component: Home });

function ProjectLayout() {
  return (
    <Guard access="signed-in">
      <ProjectScope />
    </Guard>
  );
}

function ProjectScope() {
  const { projectSlug } = useParams({ from: '/p/$projectSlug' });
  const me = useMeQuery().data!;
  const project = me.projects.find((p) => p.slug === projectSlug);
  useEffect(() => {
    if (project) rememberProject(project.slug);
  }, [project]);
  if (!project) return <ErrorPanel title="Project not found" detail="It doesn’t exist, or you don’t have access to it. Ask a project Admin to add you." />;
  return (
    <ProjectProvider me={me} project={project}>
      <AppShell>
        <Outlet />
      </AppShell>
    </ProjectProvider>
  );
}

const projectRoute = createRoute({ getParentRoute: () => rootRoute, path: '/p/$projectSlug', component: ProjectLayout });

const projectIndex = createRoute({
  getParentRoute: () => projectRoute,
  path: '/',
  component: () => {
    const { projectSlug } = useParams({ from: '/p/$projectSlug/' });
    return <Navigate to="/p/$projectSlug/pipelines" params={{ projectSlug }} />;
  },
});

/** `?page=` on paged lists: a whole number above 1 (page 1 is the default and leaves the URL clean). */
export const pageSearch = (s: Record<string, unknown>): { page?: number } => {
  const n = Number(s.page);
  return Number.isInteger(n) && n > 1 ? { page: n } : {};
};

const FILTERS = ['attention', 'healthy', 'lagging', 'down', 'nosignal', 'starting', 'paused', 'draft'] as const;

export interface PipelinesSearch {
  /** "attention", or one group of the fleet health bar. */
  filter?: (typeof FILTERS)[number];
  q?: string;
  /** Only pipelines deployed on this Debezium Server image. */
  debezium?: string;
  page?: number;
}

const pipelinesRoute = createRoute({
  getParentRoute: () => projectRoute,
  path: '/pipelines',
  validateSearch: (s: Record<string, unknown>): PipelinesSearch => ({
    ...(FILTERS.includes(s.filter as (typeof FILTERS)[number]) ? { filter: s.filter as (typeof FILTERS)[number] } : {}),
    ...(typeof s.q === 'string' && s.q ? { q: s.q } : {}),
    ...(typeof s.debezium === 'string' && s.debezium ? { debezium: s.debezium } : {}),
    ...pageSearch(s),
  }),
  component: PipelinesPage,
});

export interface NewPipelineSearch {
  step?: 'source' | 'tables' | 'sink' | 'transform' | 'review';
  draft?: string;
  /** Start a new pipeline from this template. */
  template?: string;
}

const STEPS = ['source', 'tables', 'sink', 'transform', 'review'] as const;

const newPipelineRoute = createRoute({
  getParentRoute: () => projectRoute,
  path: '/pipelines/new',
  validateSearch: (s: Record<string, unknown>): NewPipelineSearch => ({
    ...(STEPS.includes(s.step as (typeof STEPS)[number]) ? { step: s.step as NewPipelineSearch['step'] } : {}),
    ...(typeof s.draft === 'string' ? { draft: s.draft } : {}),
    ...(typeof s.template === 'string' ? { template: s.template } : {}),
  }),
  // Loaded when first opened, like the Tour, Learn and Welcome pages: they are not needed to start.
  component: lazyRouteComponent(() => import('@/features/pipelines/NewPipelinePage'), 'NewPipelinePage'),
});

export const PIPELINE_TABS = ['overview', 'data', 'events', 'logs', 'snapshots', 'history'] as const;
export type PipelineTab = (typeof PIPELINE_TABS)[number];
/** Tabs that were merged: old links keep working. */
const LEGACY_PIPELINE_TABS: Record<string, PipelineTab> = { tables: 'data', schema: 'data', offsets: 'snapshots', config: 'history', activity: 'history' };

const pipelineRoute = createRoute({
  getParentRoute: () => projectRoute,
  path: '/pipelines/$pipelineId',
  validateSearch: (s: Record<string, unknown>): { tab?: PipelineTab } => {
    const tab = typeof s.tab === 'string' ? (LEGACY_PIPELINE_TABS[s.tab] ?? s.tab) : undefined;
    return PIPELINE_TABS.includes(tab as PipelineTab) && tab !== 'overview' ? { tab: tab as PipelineTab } : {};
  },
  component: PipelinePage,
});

const connectionsRoute = createRoute({
  getParentRoute: () => projectRoute,
  path: '/connections',
  validateSearch: (s: Record<string, unknown>): { kind?: 'sink'; page?: number } => ({ ...(s.kind === 'sink' ? { kind: 'sink' as const } : {}), ...pageSearch(s) }),
  component: ConnectionsPage,
});

const alertsRoute = createRoute({
  getParentRoute: () => projectRoute,
  path: '/alerts',
  validateSearch: (s: Record<string, unknown>): { tab?: 'rules' | 'channels'; page?: number } => ({
    ...(s.tab === 'rules' || s.tab === 'channels' ? { tab: s.tab } : {}),
    ...pageSearch(s),
  }),
  component: AlertsPage,
});

const welcomeRoute = createRoute({ getParentRoute: () => projectRoute, path: '/welcome', component: lazyRouteComponent(() => import('@/features/onboarding/WelcomePage'), 'WelcomePage') });
const learnRoute = createRoute({ getParentRoute: () => projectRoute, path: '/learn', component: lazyRouteComponent(() => import('@/features/learn/LearnPage'), 'LearnPage') });
const tourRoute = createRoute({ getParentRoute: () => projectRoute, path: '/tour', component: lazyRouteComponent(() => import('@/features/tour/TourPage'), 'TourPage') });

/** q: search; in: People's project filter; scope: the project a per-project section shows (default: the current one); page: a paged section's page. */
const sectionSearch = (s: Record<string, unknown>): { q?: string; in?: string; scope?: string; page?: number } => ({
  ...pageSearch(s),
  ...(typeof s.q === 'string' && s.q ? { q: s.q } : {}),
  ...(typeof s.in === 'string' && s.in ? { in: s.in } : {}),
  ...(typeof s.scope === 'string' && s.scope ? { scope: s.scope } : {}),
});

/** Admin pages moved into Settings; old links redirect. */
const adminRoute = createRoute({
  getParentRoute: () => projectRoute,
  path: '/admin/$section',
  validateSearch: sectionSearch,
  beforeLoad: ({ params, search }) => {
    throw redirect({
      to: '/p/$projectSlug/settings/$section',
      params: { projectSlug: params.projectSlug, section: LEGACY_ADMIN_SECTIONS[params.section] ?? 'people' },
      search,
      replace: true,
    });
  },
});

const accountRoute = createRoute({ getParentRoute: () => projectRoute, path: '/account', component: AccountPage });

const settingsRoute = createRoute({
  getParentRoute: () => projectRoute,
  path: '/settings/$section',
  validateSearch: sectionSearch,
  beforeLoad: ({ params, search }) => {
    const moved = LEGACY_SECTIONS[params.section];
    if (moved) throw redirect({ to: '/p/$projectSlug/settings/$section', params: { ...params, section: moved }, search, replace: true });
  },
  component: SettingsPage,
});

const routeTree = rootRoute.addChildren([
  loginRoute,
  consentRoute,
  testingPolicyRoute,
  galleryRoute,
  indexRoute,
  projectRoute.addChildren([
    projectIndex,
    pipelinesRoute,
    newPipelineRoute,
    pipelineRoute,
    connectionsRoute,
    alertsRoute,
    welcomeRoute,
    learnRoute,
    tourRoute,
    accountRoute,
    adminRoute,
    settingsRoute,
  ]),
]);

export function makeRouter(queryClient: QueryClient) {
  return createRouter({ routeTree, context: { queryClient }, defaultPreload: 'intent', scrollRestoration: true, defaultErrorComponent: RouteError });
}

declare module '@tanstack/react-router' {
  interface Register {
    router: ReturnType<typeof makeRouter>;
  }
}
