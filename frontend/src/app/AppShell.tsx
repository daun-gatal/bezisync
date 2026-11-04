import { useEffect, useState, type ReactNode } from 'react';
import { Link, useNavigate, useRouterState } from '@tanstack/react-router';
import * as Dropdown from '@radix-ui/react-dropdown-menu';
import {
  Bell,
  BookOpen,
  Cable,
  Check,
  ChevronsUpDown,
  CircleQuestionMark,
  Compass,
  FlaskConical,
  Info,
  LogOut,
  Menu as MenuIcon,
  MessageSquare,
  Monitor,
  Moon,
  PanelLeftClose,
  PanelLeftOpen,
  Plus,
  Rows3,
  Search,
  Settings,
  ShieldCheck,
  Sparkles,
  Sun,
  X,
} from 'lucide-react';
import clsx from 'clsx';
import { useLogout, useProject, useProviders, useUpdateMe } from '@/lib/session';
import { readTourSeen, writeTourSeen } from '@/lib/tour';
import { THEMES, useTheme, type Theme } from '@/lib/theme';
import { useAlertCount } from '@/lib/useAlertCount';
import { useLiveUpdates } from '@/lib/useLiveUpdates';
import { ButtonLink } from '@/components/ui/button';
import { readStored, writeStored } from '@/components/ui/hooks';
import { InboxBell } from './InboxBell';
import { CommandPalette, OPEN_PALETTE } from './CommandPalette';
import { NotificationSettings } from './NotificationSettings';
import { canOpenSettings } from '@/features/settings/SettingsPage';
import { FeedbackDialog } from '@/features/feedback/FeedbackDialog';
import { AboutDialog } from './AboutDialog';

const COLLAPSED_KEY = 'bz.sidebar.collapsed';

export const NAV = [
  { id: 'pipelines', to: '/p/$projectSlug/pipelines', label: 'Pipelines', icon: Rows3 },
  { id: 'connections', to: '/p/$projectSlug/connections', label: 'Connections', icon: Cable },
  { id: 'alerts', to: '/p/$projectSlug/alerts', label: 'Alerts', icon: Bell },
  { id: 'settings', to: '/p/$projectSlug/settings/$section', label: 'Settings', icon: Settings, params: { section: 'people' } },
] as const;

const THEME_ICONS = { system: Monitor, light: Sun, dark: Moon } as const;
const menuItem = 'flex min-w-0 cursor-pointer items-center gap-2 rounded-md px-2 py-1.5 text-base text-ink outline-none data-highlighted:bg-hover';
const menuBox = 'z-50 max-w-[calc(100vw-24px)] min-w-60 rounded-lg border border-line bg-surface p-1 shadow-lg';
/** A row in the dark rail: nav links, footer buttons and menus share it. */
const railRow = (active = false, collapsed = false) =>
  clsx(
    'relative flex h-9 w-full min-w-0 items-center gap-2.5 rounded-md px-2.5 text-left text-base font-medium no-underline transition-colors',
    active ? 'bg-nav-active text-white shadow-[inset_3px_0_0_var(--bz-nav-mark)]' : 'text-nav-muted hover:bg-nav-hover hover:text-nav-ink',
    collapsed && 'md:justify-center md:px-0',
  );

/**
 * The frame around every project page: a dark navigation rail on desktop (collapsible to icons,
 * remembered per browser) and a top bar with a menu on phones. Layout is a grid, so the page
 * column always shrinks (minmax(0, 1fr)).
 */
export function AppShell({ children }: { children: ReactNode }) {
  const ctx = useProject();
  const { me, project, can, expert } = ctx;
  const logout = useLogout();
  const updateMe = useUpdateMe();
  const navigate = useNavigate();
  const [notifying, setNotifying] = useState(false);
  const [theme, setTheme] = useTheme();
  const [sendingFeedback, setSendingFeedback] = useState(false);
  const [aboutOpen, setAboutOpen] = useState(false);
  const [collapsed, setCollapsed] = useState(() => readStored(COLLAPSED_KEY) === '1');
  const [menuOpen, setMenuOpen] = useState(false);
  const pathname = useRouterState({ select: (s) => s.location.pathname });
  useEffect(() => setMenuOpen(false), [pathname]);
  const lab = useProviders().data?.lab ?? false;
  // The "New here?" card shows until the Tour has been opened or the card dismissed (this browser only).
  const [tourSeen, setTourSeen] = useState(readTourSeen);
  const markTourSeen = () => {
    writeTourSeen();
    setTourSeen(true);
  };
  useEffect(() => {
    if (pathname.endsWith('/tour') && !tourSeen) markTourSeen();
  }, [pathname]); // eslint-disable-line react-hooks/exhaustive-deps
  const firing = useAlertCount(project.id);
  useLiveUpdates(project.id);
  const slug = project.slug;
  const section = pathname.split('/')[3] ?? '';
  const toggleCollapsed = () => {
    writeStored(COLLAPSED_KEY, collapsed ? '0' : '1');
    setCollapsed(!collapsed);
  };
  // Labels hide on the desktop rail; they stay readable to screen readers.
  const label = collapsed ? 'md:sr-only' : 'min-w-0 truncate';
  const go = (to: string, params: Record<string, string> = {}) => void navigate({ to, params: { projectSlug: slug, ...params } } as never);

  return (
    <div className="grid min-h-screen grid-cols-[minmax(0,1fr)] grid-rows-[auto_1fr] bg-bg md:grid-cols-[auto_minmax(0,1fr)] md:grid-rows-[minmax(0,1fr)]">
      <a href="#main" className="sr-only focus:not-sr-only focus:absolute focus:top-2 focus:left-2 focus:z-50 focus:rounded focus:bg-surface focus:p-2">
        Skip to content
      </a>
      <aside
        data-collapsed={collapsed}
        className={clsx(
          'flex min-w-0 flex-col gap-4 border-nav-line bg-nav-bg px-3 py-2.5 text-nav-ink md:sticky md:top-0 md:h-screen md:overflow-y-auto md:border-r md:py-4',
          collapsed ? 'md:w-16 md:px-2' : 'md:w-60',
        )}
      >
        <div className="flex min-w-0 items-center gap-1">
          <Dropdown.Root>
            <Dropdown.Trigger
              title={collapsed ? `Project: ${project.name}` : undefined}
              className={clsx(
                'flex min-w-0 flex-1 items-center gap-2.5 rounded-md px-1.5 py-1.5 text-left hover:bg-nav-hover',
                collapsed && 'md:justify-center md:px-0',
              )}
            >
              <img src="/favicon.svg" alt="" className="size-7 shrink-0" />
              <span className={clsx('flex min-w-0 flex-1 flex-col', collapsed && 'md:sr-only')}>
                <strong className="text-base font-semibold text-nav-ink">BeziSync</strong>
                <span className="truncate text-xs text-nav-muted" title={project.name}>
                  Project: {project.name}
                </span>
              </span>
              <ChevronsUpDown className={clsx('size-4 shrink-0 text-nav-muted', collapsed && 'md:hidden')} aria-hidden />
              <span className="sr-only">Switch project</span>
            </Dropdown.Trigger>
            <Dropdown.Portal>
              <Dropdown.Content align="start" sideOffset={6} collisionPadding={12} className={clsx(menuBox, 'w-72')}>
                <Dropdown.Label className="px-2 py-1.5 text-xs font-semibold text-muted">Projects</Dropdown.Label>
                <div className="max-h-80 overflow-y-auto">
                  {me.projects.map((p) => (
                    <Dropdown.Item key={p.id} onSelect={() => go('/p/$projectSlug/pipelines', { projectSlug: p.slug })} className={menuItem}>
                      <span className="flex size-4 shrink-0 text-accent">{p.id === project.id ? <Check className="size-4" aria-hidden /> : null}</span>
                      <span className="min-w-0 wrap-anywhere">{p.name}</span>
                    </Dropdown.Item>
                  ))}
                </div>
                {me.user.is_superuser ? (
                  <>
                    <Dropdown.Separator className="my-1 h-px bg-line" />
                    <Dropdown.Item onSelect={() => go('/p/$projectSlug/settings/$section', { section: 'projects' })} className={menuItem}>
                      <span className="size-4 shrink-0" />
                      Manage projects…
                    </Dropdown.Item>
                  </>
                ) : null}
              </Dropdown.Content>
            </Dropdown.Portal>
          </Dropdown.Root>
          <button
            type="button"
            aria-expanded={menuOpen}
            aria-controls="app-sidebar-menu"
            onClick={() => setMenuOpen(!menuOpen)}
            className="grid size-9 shrink-0 place-items-center rounded-md text-nav-ink hover:bg-nav-hover md:hidden"
          >
            {menuOpen ? <X className="size-5" aria-hidden /> : <MenuIcon className="size-5" aria-hidden />}
            <span className="sr-only">{menuOpen ? 'Close menu' : 'Open menu'}</span>
          </button>
        </div>

        <div id="app-sidebar-menu" className={clsx('min-h-0 flex-1 flex-col gap-4 pb-2 md:flex md:pb-0', menuOpen ? 'flex' : 'hidden')}>
          {can('pipeline.edit') ? (
            <ButtonLink
              to="/p/$projectSlug/pipelines/new"
              params={{ projectSlug: slug }}
              variant="primary"
              icon={<Plus />}
              title={collapsed ? 'New pipeline' : undefined}
              className={clsx('w-full', collapsed && 'md:px-0')}
            >
              <span className={collapsed ? 'md:sr-only' : undefined}>New pipeline</span>
            </ButtonLink>
          ) : null}

          <CommandPalette collapsed={collapsed} />
          <nav aria-label="Primary" className="flex flex-col gap-0.5">
            {NAV.filter((item) => (item.id === 'settings' ? canOpenSettings(ctx) : true)).map((item) => {
              const active = section === item.id;
              return (
                <Link
                  key={item.id}
                  to={item.to}
                  params={{ projectSlug: slug, ...('params' in item ? item.params : {}) }}
                  aria-current={active ? 'page' : undefined}
                  title={collapsed ? item.label : undefined}
                  className={railRow(active, collapsed)}
                >
                  <item.icon className="size-4 shrink-0" aria-hidden />
                  <span className={label}>{item.label}</span>
                  {item.id === 'alerts' && firing > 0 ? (
                    <span
                      className={clsx(
                        'ml-auto rounded-full bg-bad px-1.5 text-xs font-semibold text-on-bad',
                        collapsed && 'md:absolute md:top-0 md:right-0 md:ml-0',
                      )}
                    >
                      {firing}
                      <span className="sr-only"> firing</span>
                    </span>
                  ) : null}
                </Link>
              );
            })}
          </nav>

          <div className="mt-auto flex flex-col gap-0.5">
            {!tourSeen && !collapsed ? (
              <div className="mb-2 flex flex-col gap-1.5 rounded-lg bg-nav-active p-3 text-sm text-nav-ink">
                <div className="flex items-start justify-between gap-2">
                  <strong className="font-semibold">New here?</strong>
                  <button
                    type="button"
                    aria-label="Dismiss the tour suggestion"
                    onClick={markTourSeen}
                    className="grid size-6 shrink-0 place-items-center rounded hover:bg-nav-hover"
                  >
                    <X className="size-3.5" aria-hidden />
                  </button>
                </div>
                <span>See what BeziSync does in five minutes.</span>
                <Link to="/p/$projectSlug/tour" params={{ projectSlug: slug }} className="self-start font-semibold text-nav-mark no-underline hover:underline">
                  Take the tour →
                </Link>
              </div>
            ) : null}
            <Dropdown.Root>
              <Dropdown.Trigger title={collapsed ? 'Help & tour' : undefined} className={railRow(false, collapsed)}>
                <CircleQuestionMark className="size-4 shrink-0" aria-hidden /> <span className={label}>Help &amp; tour</span>
                {!tourSeen ? (
                  <>
                    <span className="size-1.5 shrink-0 rounded-full bg-nav-mark" aria-hidden />
                    <span className="sr-only"> (new)</span>
                  </>
                ) : null}
              </Dropdown.Trigger>
              <Dropdown.Portal>
                <Dropdown.Content side="top" align="start" sideOffset={6} collisionPadding={12} className={menuBox}>
                  <Dropdown.Item onSelect={() => go('/p/$projectSlug/tour')} className={menuItem}>
                    <Compass className="size-4 shrink-0 text-muted" aria-hidden /> Tour
                    {!tourSeen ? <span className="ml-auto rounded-full bg-accent-soft px-1.5 text-xs font-semibold text-accent-ink">New</span> : null}
                  </Dropdown.Item>
                  <Dropdown.Item onSelect={() => go('/p/$projectSlug/welcome')} className={menuItem}>
                    <Sparkles className="size-4 shrink-0 text-muted" aria-hidden /> Getting started
                  </Dropdown.Item>
                  <Dropdown.Item onSelect={() => go('/p/$projectSlug/learn')} className={menuItem}>
                    <BookOpen className="size-4 shrink-0 text-muted" aria-hidden /> Learn
                  </Dropdown.Item>
                  <Dropdown.Item onSelect={() => window.dispatchEvent(new Event(OPEN_PALETTE))} className={menuItem}>
                    <Search className="size-4 shrink-0 text-muted" aria-hidden /> Find anything
                    <kbd className="ml-auto font-sans text-xs text-muted">Ctrl K</kbd>
                  </Dropdown.Item>
                  {lab ? (
                    <Dropdown.Item onSelect={() => window.open('/testing-policy', '_blank', 'noopener')} className={menuItem}>
                      <ShieldCheck className="size-4 shrink-0 text-muted" aria-hidden /> Testing policy
                    </Dropdown.Item>
                  ) : null}
                  <Dropdown.Item onSelect={() => setAboutOpen(true)} className={menuItem}>
                    <Info className="size-4 shrink-0 text-muted" aria-hidden /> About BeziSync
                  </Dropdown.Item>
                </Dropdown.Content>
              </Dropdown.Portal>
            </Dropdown.Root>
            <button type="button" onClick={() => setSendingFeedback(true)} title={collapsed ? 'Feedback' : undefined} className={railRow(false, collapsed)}>
              <MessageSquare className="size-4 shrink-0" aria-hidden /> <span className={label}>Feedback</span>
            </button>
            <InboxBell compact={collapsed} className={railRow(false, collapsed)} labelClass={label} />
            <Dropdown.Root>
              <Dropdown.Trigger title={collapsed ? me.user.email : undefined} className={clsx(railRow(false, collapsed), 'h-10')}>
                <span className="grid size-7 shrink-0 place-items-center rounded-full bg-nav-active text-sm font-semibold text-nav-ink" aria-hidden>
                  {(me.user.name || me.user.email).slice(0, 1).toUpperCase()}
                </span>
                <span className={label}>{me.user.email}</span>
              </Dropdown.Trigger>
              <Dropdown.Portal>
                <Dropdown.Content side="top" align="start" sideOffset={6} collisionPadding={12} className={menuBox}>
                  <Dropdown.Label className="truncate px-2 py-1.5 text-xs text-muted" title={me.user.email}>
                    Signed in as {me.user.email}
                  </Dropdown.Label>
                  <Dropdown.Label className="px-2 pt-1.5 pb-1 text-xs font-semibold text-muted">Theme</Dropdown.Label>
                  <Dropdown.RadioGroup value={theme} onValueChange={(v) => setTheme(v as Theme)}>
                    {THEMES.map((t) => {
                      const Icon = THEME_ICONS[t.value];
                      return (
                        <Dropdown.RadioItem key={t.value} value={t.value} onSelect={(e) => e.preventDefault()} className={menuItem}>
                          <Icon className="size-4 shrink-0 text-muted" aria-hidden />
                          {t.label}
                          {t.value === 'system' ? <span className="text-xs text-muted">follows your computer</span> : null}
                          <Dropdown.ItemIndicator className="ml-auto">
                            <Check className="size-4 text-accent" aria-hidden />
                          </Dropdown.ItemIndicator>
                        </Dropdown.RadioItem>
                      );
                    })}
                  </Dropdown.RadioGroup>
                  <Dropdown.Separator className="my-1 h-px bg-line" />
                  <Dropdown.Item onSelect={() => go('/p/$projectSlug/account')} className={menuItem}>
                    Account &amp; API tokens
                  </Dropdown.Item>
                  <Dropdown.Item onSelect={() => updateMe.mutate({ ui_mode: expert ? 'guided' : 'expert' })} className={clsx(menuItem, 'items-start')}>
                    <span className="flex min-w-0 flex-col gap-0.5">
                      <span>Switch to {expert ? 'Guided' : 'Expert'} mode</span>
                      <span className="text-xs text-muted">{expert ? 'Plain language, fewer options' : 'Shows Debezium properties and expert options'}</span>
                    </span>
                  </Dropdown.Item>
                  <Dropdown.Item onSelect={() => setNotifying(true)} className={menuItem}>
                    Notification settings
                  </Dropdown.Item>
                  <Dropdown.Separator className="my-1 h-px bg-line" />
                  <Dropdown.Item onSelect={() => logout.mutate()} className={menuItem}>
                    <LogOut className="size-4 shrink-0 text-muted" aria-hidden /> Sign out
                  </Dropdown.Item>
                </Dropdown.Content>
              </Dropdown.Portal>
            </Dropdown.Root>
            <button
              type="button"
              aria-pressed={collapsed}
              onClick={toggleCollapsed}
              title={collapsed ? 'Expand sidebar' : undefined}
              className={clsx(railRow(false, collapsed), 'hidden md:flex')}
            >
              {collapsed ? <PanelLeftOpen className="size-4 shrink-0" aria-hidden /> : <PanelLeftClose className="size-4 shrink-0" aria-hidden />}
              <span className={label}>{collapsed ? 'Expand sidebar' : 'Collapse sidebar'}</span>
            </button>
          </div>
        </div>
      </aside>
      <NotificationSettings open={notifying} onOpenChange={setNotifying} />
      <FeedbackDialog open={sendingFeedback} onOpenChange={setSendingFeedback} page={pathname} />
      <AboutDialog open={aboutOpen} onOpenChange={setAboutOpen} />
      <main id="main" tabIndex={-1} className="min-w-0 px-4 py-6 outline-none md:px-8">
        <div className="mx-auto flex max-w-[120rem] min-w-0 flex-col gap-5">
          {project.is_sandbox ? (
            <div role="note" className="flex items-center gap-2 rounded-md border border-dashed border-line-strong bg-subtle px-3 py-2 text-sm text-ink-2">
              <FlaskConical className="size-4 shrink-0" aria-hidden /> Sandbox · sample data, safe to experiment. Nothing here touches production.
            </div>
          ) : null}
          {children}
        </div>
      </main>
    </div>
  );
}
