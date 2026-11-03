import { useEffect, useMemo, useRef, useState } from 'react';
import { useNavigate } from '@tanstack/react-router';
import * as Dialog from '@radix-ui/react-dialog';
import { Search } from 'lucide-react';
import clsx from 'clsx';
import { useConnections } from '@/lib/connections';
import { usePipelines } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { canOpenSettings, SETTINGS_SECTIONS } from '@/features/settings/SettingsPage';

interface Item {
  id: string;
  label: string;
  hint: string;
  go: () => void;
}

/** ⌘K / Ctrl+K: jump to any pipeline, connection, page or action. */
/** Dispatch on `window` to open the palette from elsewhere (the Help menu). */
export const OPEN_PALETTE = 'bz:open-palette';

export function CommandPalette({ collapsed = false }: { collapsed?: boolean }) {
  const ctx = useProject();
  const { project, can } = ctx;
  const navigate = useNavigate();
  const pipelines = usePipelines(project.id);
  const connections = useConnections(project.id);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const listRef = useRef<HTMLUListElement>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        setOpen((o) => !o);
      }
    };
    const onOpen = () => setOpen(true);
    window.addEventListener('keydown', onKey);
    window.addEventListener(OPEN_PALETTE, onOpen);
    return () => {
      window.removeEventListener('keydown', onKey);
      window.removeEventListener(OPEN_PALETTE, onOpen);
    };
  }, []);

  const slug = project.slug;
  const items = useMemo<Item[]>(() => {
    const to = (path: string, params: Record<string, string> = {}, search: Record<string, string> = {}) => () =>
      void navigate({ to: path, params: { projectSlug: slug, ...params }, search } as never);
    const pages: Item[] = [
      { id: 'p:pipelines', label: 'Pipelines', hint: 'Page', go: to('/p/$projectSlug/pipelines') },
      ...(can('pipeline.edit') ? [{ id: 'a:new', label: 'New pipeline', hint: 'Action', go: to('/p/$projectSlug/pipelines/new') }] : []),
      { id: 'p:sources', label: 'Connections · sources', hint: 'Page', go: to('/p/$projectSlug/connections') },
      { id: 'p:sinks', label: 'Connections · destinations', hint: 'Page', go: to('/p/$projectSlug/connections', {}, { kind: 'sink' }) },
      { id: 'p:alerts', label: 'Alerts', hint: 'Page', go: to('/p/$projectSlug/alerts') },
      { id: 'p:tour', label: 'Tour · what BeziSync does', hint: 'Page', go: to('/p/$projectSlug/tour') },
      { id: 'p:welcome', label: 'Getting started', hint: 'Page', go: to('/p/$projectSlug/welcome') },
      { id: 'p:learn', label: 'Learn (glossary)', hint: 'Page', go: to('/p/$projectSlug/learn') },
      { id: 'p:account', label: 'Account · API tokens, notifications', hint: 'Page', go: to('/p/$projectSlug/account') },
      ...SETTINGS_SECTIONS.filter(() => canOpenSettings(ctx)).map((s) => ({
        id: `s:${s.id}`,
        label: `Settings · ${s.label}`,
        hint: 'Settings',
        go: to('/p/$projectSlug/settings/$section', { section: s.id }),
      })),
    ];
    const pipes: Item[] = (pipelines.data ?? [])
      .filter((p) => p.lifecycle !== 'deleted')
      .map((p) => ({
        id: `pl:${p.id}`,
        label: p.name,
        hint: `Pipeline · ${p.lifecycle}`,
        go: to('/p/$projectSlug/pipelines/$pipelineId', { pipelineId: p.id }),
      }));
    const conns: Item[] = (connections.data ?? []).map((c) => ({
      id: `c:${c.id}`,
      label: c.name,
      hint: c.kind === 'source' ? 'Source' : 'Destination',
      go: to('/p/$projectSlug/connections', {}, c.kind === 'sink' ? { kind: 'sink' } : {}),
    }));
    return [...pipes, ...conns, ...pages];
  }, [pipelines.data, connections.data, navigate, slug, can, ctx]);

  const q = query.trim().toLowerCase();
  const shown = (q ? items.filter((i) => `${i.label} ${i.hint}`.toLowerCase().includes(q)) : items).slice(0, 50);
  const choose = (item: Item | undefined) => {
    if (!item) return;
    setOpen(false);
    setQuery('');
    item.go();
  };
  useEffect(() => setActive(0), [query, open]);
  useEffect(() => {
    listRef.current?.querySelector(`[data-index="${active}"]`)?.scrollIntoView?.({ block: 'nearest' });
  }, [active]);

  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        title={collapsed ? 'Find anything' : undefined}
        className={clsx(
          'flex h-9 w-full min-w-0 items-center gap-2 rounded-md border border-nav-line bg-nav-hover px-3 text-left text-sm text-nav-muted hover:text-nav-ink',
          collapsed && 'md:justify-center md:px-0',
        )}
      >
        <Search className="size-4 shrink-0" aria-hidden /> <span className={collapsed ? 'md:sr-only' : 'min-w-0 truncate'}>Find anything</span>
        <kbd className={clsx('ml-auto rounded border border-nav-line px-1 font-sans text-xs', collapsed && 'md:hidden')}>Ctrl K</kbd>
      </button>
      <Dialog.Root open={open} onOpenChange={setOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-50 bg-scrim" />
          <Dialog.Content
            aria-describedby={undefined}
            className="fixed top-[12vh] left-1/2 z-50 w-[min(640px,calc(100vw-24px))] -translate-x-1/2 overflow-hidden rounded-xl border border-line bg-surface shadow-xl"
          >
            <Dialog.Title className="sr-only">Go to</Dialog.Title>
            <div className="flex items-center gap-2 border-b border-line px-3">
              <Search className="size-4 text-muted" aria-hidden />
              <input
                autoFocus
                role="combobox"
                aria-expanded="true"
                aria-controls="command-results"
                aria-activedescendant={shown[active] ? `cmd-${shown[active].id}` : undefined}
                aria-label="Search pipelines, connections and pages"
                placeholder="Search pipelines, connections and pages…"
                className="h-12 w-full border-0 bg-transparent text-base text-ink outline-none"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'ArrowDown') {
                    e.preventDefault();
                    setActive((a) => Math.min(a + 1, shown.length - 1));
                  } else if (e.key === 'ArrowUp') {
                    e.preventDefault();
                    setActive((a) => Math.max(a - 1, 0));
                  } else if (e.key === 'Enter') {
                    e.preventDefault();
                    choose(shown[active]);
                  }
                }}
              />
            </div>
            <ul id="command-results" role="listbox" ref={listRef} aria-label="Results" className="m-0 max-h-[min(20rem,60vh)] list-none overflow-auto p-1">
              {shown.length === 0 ? <li className="px-3 py-6 text-center text-sm text-muted">Nothing matches “{query}”.</li> : null}
              {shown.map((item, i) => (
                <li
                  key={item.id}
                  id={`cmd-${item.id}`}
                  role="option"
                  aria-selected={i === active}
                  data-index={i}
                  onMouseEnter={() => setActive(i)}
                  onClick={() => choose(item)}
                  className={clsx('flex min-w-0 cursor-pointer items-center justify-between gap-3 rounded-md px-3 py-2 text-base', i === active && 'bg-hover')}
                >
                  <span className="min-w-0 truncate" title={item.label}>
                    {item.label}
                  </span>
                  <span className="shrink-0 text-xs text-muted">{item.hint}</span>
                </li>
              ))}
            </ul>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </>
  );
}
