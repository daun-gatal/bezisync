import { useEffect, useState } from 'react';
import * as Popover from '@radix-ui/react-popover';
import { Link } from '@tanstack/react-router';
import { Inbox } from 'lucide-react';
import clsx from 'clsx';
import { InlineError } from '@/components/ui/feedback';
import { useInbox, useReadAll } from '@/lib/alerts';
import { ago } from '@/lib/format';
import { useProject } from '@/lib/session';

/** True from the md breakpoint up (the rail is a sidebar); false on phones (a top bar). */
function useWide(): boolean {
  const query = '(min-width: 768px)';
  const [wide, setWide] = useState(() => typeof window !== 'undefined' && window.matchMedia?.(query).matches);
  useEffect(() => {
    const m = window.matchMedia?.(query);
    if (!m) return;
    const on = () => setWide(m.matches);
    m.addEventListener('change', on);
    return () => m.removeEventListener('change', on);
  }, []);
  return Boolean(wide);
}

/**
 * The inbox: alerts and approval requests for this person. It opens beside the rail on desktop
 * and below the menu on phones, and never leaves the screen. Items are marked read on close.
 */
export function InboxBell({ compact = false, className, labelClass }: { compact?: boolean; className?: string; labelClass?: string }) {
  const { project } = useProject();
  const inbox = useInbox(project.id);
  const readAll = useReadAll(project.id);
  const unread = inbox.data?.unread ?? 0;
  const wide = useWide();
  return (
    <Popover.Root onOpenChange={(open) => !open && unread > 0 && readAll.mutate()}>
      <Popover.Trigger className={className} aria-label={`Inbox, ${unread} unread`} title={compact ? 'Inbox' : undefined}>
        <Inbox className="size-4 shrink-0" aria-hidden /> <span className={labelClass}>Inbox</span>
        {unread ? (
          <span className={clsx('rounded-full bg-accent px-1.5 text-xs font-semibold text-on-accent', compact ? 'md:absolute md:top-0 md:right-0' : 'ml-auto')}>{unread}</span>
        ) : null}
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Content
          side={wide ? 'right' : 'bottom'}
          align={wide ? 'end' : 'start'}
          sideOffset={8}
          collisionPadding={12}
          aria-label="Inbox"
          className="z-50 flex w-[min(360px,calc(100vw-24px))] flex-col rounded-xl border border-line bg-surface p-1 shadow-lg"
        >
          <div className="flex items-center justify-between gap-2 px-2 py-1.5">
            <strong className="text-sm font-semibold">Inbox</strong>
            <span className="text-xs text-muted">Marked read when you close it</span>
          </div>
          {inbox.isError ? (
            <InlineError error={inbox.error} doing="Couldn’t load your inbox" onRetry={() => void inbox.refetch()} />
          ) : (inbox.data?.items ?? []).length === 0 ? (
            <p className="m-0 px-2 py-5 text-center text-sm text-muted">Nothing new. Alerts and approval requests show up here.</p>
          ) : (
            <ul className="m-0 flex max-h-[min(24rem,60vh)] list-none flex-col overflow-y-auto p-0">
              {inbox.data?.items.map((n) => (
                <li key={n.id}>
                  <Link to={n.link as '/'} className={clsx('flex min-w-0 flex-col gap-0.5 rounded-md px-2 py-2 no-underline hover:bg-hover', !n.read && 'bg-accent-soft')}>
                    <span className="text-sm font-semibold wrap-anywhere text-ink">{n.title}</span>
                    {n.body ? <span className="line-clamp-2 text-xs wrap-anywhere text-ink-2">{n.body}</span> : null}
                    <span className="text-xs text-muted">{ago(n.created_at)}</span>
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </Popover.Content>
      </Popover.Portal>
    </Popover.Root>
  );
}
