import { useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import clsx from 'clsx';
import { toast } from 'sonner';
import { Button, ButtonAnchor } from '@/components/ui/button';
import { Empty, InlineError, Pill } from '@/components/ui/feedback';
import { Segmented } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { CursorPager, scrollBox, useScrollTopOn } from '@/components/ui/pager';
import { client, unwrap } from '@/lib/api';
import { FEEDBACK_KINDS, describeBrowser, feedbackKind, type Feedback, type FeedbackKind } from '@/lib/feedback';
import { ago, formatDateTime } from '@/lib/format';
import { useCursorPages } from '@/lib/paging';

type Status = 'open' | 'resolved' | 'all';

/** Open reports, for the Settings nav. */
export function useOpenFeedbackCount(enabled: boolean): number {
  const summary = useQuery({
    queryKey: ['feedback', 'summary'],
    queryFn: () => unwrap(client.GET('/api/v1/feedback', { params: { query: { status: 'open', limit: 1 } } })),
    enabled,
    staleTime: 30_000,
  });
  return summary.data?.open ?? 0;
}

export function FeedbackSection() {
  const qc = useQueryClient();
  const [status, setStatus] = useState<Status>('open');
  const [kind, setKind] = useState<FeedbackKind | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const list = useCursorPages(['feedback', status, kind], (cursor, limit) =>
    unwrap(client.GET('/api/v1/feedback', { params: { query: { status, kind, limit, cursor } } })),
  );
  const box = useRef<HTMLOListElement>(null);
  useScrollTopOn(box, list.page);
  const resolve = useMutation({
    mutationFn: (f: Feedback) =>
      unwrap(client.PATCH('/api/v1/feedback/{feedback_id}', { params: { path: { feedback_id: f.id } }, body: { resolved: !f.resolved_at } })),
    onSuccess: (f) => {
      toast.success(f.resolved_at ? 'Marked resolved' : 'Reopened');
      void qc.invalidateQueries({ queryKey: ['feedback'] });
    },
  });
  const first = list.query.data;
  const items = list.items;
  const statuses: { id: Status; label: string }[] = [
    { id: 'open', label: `Open${first ? ` (${first.open})` : ''}` },
    { id: 'resolved', label: `Resolved${first ? ` (${first.resolved})` : ''}` },
    { id: 'all', label: 'All' },
  ];

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-col gap-1">
        <h2 className="m-0 text-xl font-semibold">Feedback</h2>
        <p className="m-0 text-base text-muted">What people sent from the app. Only Admins see this.</p>
      </div>
      <Card pad={false}>
        <div className="flex flex-wrap items-center gap-3 border-b border-line px-card py-3">
          <Segmented label="Status" value={status} onChange={setStatus} options={statuses.map((s) => ({ value: s.id, label: s.label }))} />
          <div role="group" aria-label="Type" className="flex flex-wrap gap-1.5">
            {[{ kind: null, short: 'All types' } as const, ...FEEDBACK_KINDS].map((k) => (
              <button
                key={k.kind ?? 'all'}
                type="button"
                aria-pressed={kind === k.kind}
                onClick={() => setKind(k.kind)}
                className={clsx(
                  'h-7 rounded-full border px-2.5 text-sm font-medium',
                  kind === k.kind ? 'border-accent bg-accent-soft text-accent-ink' : 'border-line-strong bg-surface text-ink-2 hover:bg-hover',
                )}
              >
                {k.short}
              </button>
            ))}
          </div>
        </div>

        <InlineError error={resolve.error} doing="Couldn’t update the report" className="m-card" />
        <QueryState query={list.query} doing="Couldn’t load feedback">
          {() =>
            items.length === 0 ? (
              <Empty title="Nothing here">When people send feedback, it shows up in this list.</Empty>
            ) : (
              <ol ref={box} aria-label="Feedback, scrolls" tabIndex={0} className={`m-0 list-none p-0 ${scrollBox('page')}`}>
                {items.map((f) => {
                  const meta = feedbackKind(f.kind);
                  const open = expanded === f.id;
                  const done = Boolean(f.resolved_at);
                  return (
                    <li key={f.id} className="border-b border-line last:border-b-0">
                      <button
                        type="button"
                        aria-expanded={open}
                        onClick={() => setExpanded(open ? null : f.id)}
                        className={clsx(
                          'grid w-full grid-cols-[88px_minmax(0,1fr)] items-start gap-3.5 px-card py-3.5 text-left md:grid-cols-[96px_minmax(0,1fr)_auto]',
                          open ? 'bg-subtle' : 'bg-surface hover:bg-hover',
                          done && 'opacity-75',
                        )}
                      >
                        <Pill tone={meta.tone} className="justify-self-start">
                          {meta.short}
                        </Pill>
                        <span className="flex min-w-0 flex-col gap-1">
                          {/* Untrusted text: plain text only, never HTML or Markdown. */}
                          <span
                            className={clsx(
                              'text-base text-ink',
                              open ? 'whitespace-pre-wrap wrap-anywhere' : 'line-clamp-1 wrap-anywhere',
                              !done && 'font-medium',
                            )}
                          >
                            {f.message}
                          </span>
                          <span className="line-clamp-1 text-xs wrap-anywhere text-muted">
                            {f.email}
                            {f.page ? (
                              <>
                                {' '}
                                · <span className="font-mono">{f.page}</span>
                              </>
                            ) : null}
                          </span>
                        </span>
                        <span className="col-start-2 flex gap-2 text-xs whitespace-nowrap text-muted md:col-start-3 md:flex-col md:items-end md:gap-1">
                          <time dateTime={f.created_at} title={formatDateTime(f.created_at)}>
                            {ago(f.created_at)}
                          </time>
                          {done ? <span className="font-medium text-ok">● Resolved</span> : null}
                        </span>
                      </button>
                      {open ? (
                        <div className="flex flex-col gap-3 bg-subtle px-card pb-4">
                          <dl className="m-0 grid grid-cols-[repeat(auto-fill,minmax(min(100%,160px),1fr))] gap-3 rounded-lg border border-line bg-surface p-card text-xs">
                            <div>
                              <dt className="text-muted">From</dt>
                              <dd className="m-0 mt-0.5 break-all text-ink-2">{f.email}</dd>
                            </div>
                            <div>
                              <dt className="text-muted">Project</dt>
                              <dd className="m-0 mt-0.5 text-ink-2">{f.project_name || '—'}</dd>
                            </div>
                            <div>
                              <dt className="text-muted">Browser</dt>
                              <dd className="m-0 mt-0.5 text-ink-2" title={f.user_agent}>
                                {describeBrowser(f.user_agent)}
                              </dd>
                            </div>
                            <div>
                              <dt className="text-muted">Sent</dt>
                              <dd className="m-0 mt-0.5 font-mono text-ink-2">{formatDateTime(f.created_at)}</dd>
                            </div>
                            {done ? (
                              <div className="col-span-full">
                                <dt className="text-muted">Resolved</dt>
                                <dd className="m-0 mt-0.5 text-ink-2">
                                  {formatDateTime(f.resolved_at)} by {f.resolved_by}
                                </dd>
                              </div>
                            ) : null}
                          </dl>
                          <div className="flex flex-wrap justify-end gap-2">
                            <ButtonAnchor size="sm" href={`mailto:${encodeURIComponent(f.email)}?subject=${encodeURIComponent('Your BeziSync feedback')}`}>
                              Reply by email
                            </ButtonAnchor>
                            <Button size="sm" variant="primary" loading={resolve.isPending && resolve.variables?.id === f.id} onClick={() => resolve.mutate(f)}>
                              {done ? 'Reopen' : 'Mark resolved'}
                            </Button>
                          </div>
                        </div>
                      ) : null}
                    </li>
                  );
                })}
              </ol>
            )
          }
        </QueryState>
        <CursorPager
          page={list.page}
          count={items.length}
          hasOlder={list.hasOlder}
          onNewer={list.newer}
          onOlder={list.older}
          loading={list.query.isPlaceholderData}
          noun="reports"
        />
      </Card>
    </div>
  );
}
