import { Link, useNavigate, useParams, useSearch } from '@tanstack/react-router';
import clsx from 'clsx';
import { ButtonLink } from '@/components/ui/button';
import { Empty } from '@/components/ui/feedback';
import { Card, Page, PageHeader } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import { ProjectProvider, useProject } from '@/lib/session';
import { AuditSection } from './AuditSection';
import { EngineVersionsSection } from './EngineVersionsSection';
import { FeedbackSection, useOpenFeedbackCount } from './FeedbackSection';
import { PeopleSection } from './PeopleSection';
import { PoliciesSection } from './PoliciesSection';
import { ProjectsSection } from './ProjectsSection';
import { RuntimesSection } from './RuntimesSection';
import { SecretStoresSection } from './SecretStoresSection';
import { McpSection } from './McpSection';
import { SignInSection } from './SignInSection';
import { TemplatesSection } from './TemplatesSection';

type Ctx = ReturnType<typeof useProject>;

interface Section {
  id: string;
  label: string;
  group: string;
  /** Applies to one project: the header shows which, with a picker to switch. */
  perProject: boolean;
  render: () => React.ReactNode;
}

/** Grouped by what an Admin is doing, not by where the data lives. */
export const SETTINGS_SECTIONS: Section[] = [
  { id: 'people', label: 'People', group: 'People & projects', perProject: false, render: () => <PeopleSection /> },
  { id: 'projects', label: 'Projects', group: 'People & projects', perProject: false, render: () => <ProjectsSection /> },
  { id: 'feedback', label: 'Feedback', group: 'People & projects', perProject: false, render: () => <FeedbackSection /> },
  { id: 'sign-in', label: 'Sign-in', group: 'Access', perProject: false, render: () => <SignInSection /> },
  { id: 'mcp', label: 'MCP', group: 'Access', perProject: false, render: () => <McpSection /> },
  { id: 'runtimes', label: 'Runtimes', group: 'Running pipelines', perProject: true, render: () => <RuntimesSection /> },
  { id: 'secret-stores', label: 'Secret stores', group: 'Running pipelines', perProject: true, render: () => <SecretStoresSection /> },
  { id: 'engines', label: 'Engine versions', group: 'Running pipelines', perProject: false, render: () => <EngineVersionsSection /> },
  { id: 'policies', label: 'Policies', group: 'Guardrails', perProject: true, render: () => <PoliciesSection /> },
  { id: 'templates', label: 'Templates', group: 'Guardrails', perProject: true, render: () => <TemplatesSection /> },
  { id: 'audit', label: 'Audit log', group: 'Guardrails', perProject: true, render: () => <AuditSection /> },
];

/** Section ids that moved, so old links still land: Members, Users & access → People; images and Debezium versions → Engine versions. */
export const LEGACY_SECTIONS: Record<string, string> = { members: 'people', users: 'people', images: 'engines', 'debezium-catalog': 'engines', debezium: 'engines' };
/** The former /admin/$section pages. */
export const LEGACY_ADMIN_SECTIONS: Record<string, string> = { users: 'people', projects: 'projects', debezium: 'engines' };

/** Settings are for Admins only (account-wide); project roles never manage the project itself. */
export function canOpenSettings(ctx: Ctx): boolean {
  return ctx.me.user.is_superuser;
}

export function SettingsPage() {
  const ctx = useProject();
  const navigate = useNavigate({ from: '/p/$projectSlug/settings/$section' });
  const { section } = useParams({ from: '/p/$projectSlug/settings/$section' });
  const search = useSearch({ from: '/p/$projectSlug/settings/$section' });
  const openFeedback = useOpenFeedbackCount(canOpenSettings(ctx));
  if (!canOpenSettings(ctx)) {
    return (
      <Page>
        <PageHeader title="Settings" />
        <Card>
          <Empty title="Settings are for Admins">
            Ask an Admin to change runtimes, people or policies. Your API tokens, notifications and mode are under{' '}
            <Link to="/p/$projectSlug/account" params={{ projectSlug: ctx.project.slug }} className="font-semibold text-accent underline">
              Account
            </Link>
            .
          </Empty>
        </Card>
      </Page>
    );
  }
  const active = SETTINGS_SECTIONS.find((s) => s.id === section);
  // A per-project section shows the project picked here; you stay in your own project.
  const scope = ctx.me.projects.find((p) => p.slug === search.scope) ?? ctx.project;
  const groups = [...new Set(SETTINGS_SECTIONS.map((s) => s.group))];
  const go = (id: string) =>
    void navigate({ params: { projectSlug: ctx.project.slug, section: id }, search: (s) => ({ ...(s.scope ? { scope: s.scope } : {}) }) });
  return (
    <Page>
      <PageHeader
        title="Settings"
        description={active ? (active.perProject ? `For the ${scope.name} project.` : 'Applies to every project.') : undefined}
        actions={
          active?.perProject ? (
            <div className="flex min-w-0 flex-[0_1_280px] items-center gap-2">
              <label htmlFor="settings-scope" className="shrink-0 text-sm text-muted">
                Project
              </label>
              <Select
                id="settings-scope"
                value={scope.slug}
                onChange={(slug) => void navigate({ search: (s) => ({ ...s, scope: slug === ctx.project.slug ? undefined : slug }), replace: true })}
                options={ctx.me.projects.map((p) => ({
                  value: p.slug,
                  label: p.name,
                  description: p.slug === ctx.project.slug ? 'The project you’re in' : undefined,
                }))}
              />
            </div>
          ) : null
        }
      />
      <div className="@container min-w-0">
        <div className="grid min-w-0 grid-cols-[minmax(0,1fr)] gap-6 @3xl:grid-cols-[200px_minmax(0,1fr)]">
          <div className="@3xl:hidden">
            <label htmlFor="settings-section" className="sr-only">
              Settings section
            </label>
            <Select
              id="settings-section"
              value={active?.id ?? ''}
              placeholder="Choose a section"
              onChange={go}
              options={SETTINGS_SECTIONS.map((s) => ({ value: s.id, label: s.label, description: s.group }))}
            />
          </div>
          <nav aria-label="Settings sections" className="hidden flex-col gap-4 @3xl:flex">
            {groups.map((g) => (
              <div key={g} className="flex flex-col gap-0.5">
                <h2 className="m-0 px-3 pb-1 text-xs font-semibold text-muted">{g}</h2>
                {SETTINGS_SECTIONS.filter((s) => s.group === g).map((s) => (
                  <Link
                    key={s.id}
                    to="/p/$projectSlug/settings/$section"
                    params={{ projectSlug: ctx.project.slug, section: s.id }}
                    search={search.scope ? { scope: search.scope } : {}}
                    aria-current={s.id === active?.id ? 'page' : undefined}
                    className={clsx(
                      'flex h-9 items-center gap-2 rounded-md px-3 text-base font-medium no-underline',
                      s.id === active?.id ? 'bg-surface text-ink shadow-[0_0_0_1px_var(--bz-line)]' : 'text-ink-2 hover:bg-hover',
                    )}
                  >
                    {s.label}
                    {s.id === 'feedback' && openFeedback > 0 ? (
                      <span className="ml-auto min-w-5 rounded-full bg-accent px-1.5 text-center text-xs font-semibold text-on-accent">
                        {openFeedback}
                        <span className="sr-only"> open</span>
                      </span>
                    ) : null}
                  </Link>
                ))}
              </div>
            ))}
          </nav>
          <div className="flex min-w-0 flex-col gap-4">
            {!active ? (
              <Card>
                <Empty
                  title="This settings page doesn’t exist"
                  action={
                    <ButtonLink to="/p/$projectSlug/settings/$section" params={{ projectSlug: ctx.project.slug, section: 'people' }}>
                      Go to People
                    </ButtonLink>
                  }
                >
                  The link may be old, or the section moved. Choose a section from the list.
                </Empty>
              </Card>
            ) : active.perProject && scope.id !== ctx.project.id ? (
              <ProjectProvider me={ctx.me} project={scope}>
                {active.render()}
              </ProjectProvider>
            ) : (
              active.render()
            )}
          </div>
        </div>
      </div>
    </Page>
  );
}
