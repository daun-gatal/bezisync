import { useRef } from 'react';
import { Download } from 'lucide-react';
import { ButtonAnchor } from '@/components/ui/button';
import { Empty } from '@/components/ui/feedback';
import { Card, QueryState } from '@/components/ui/layout';
import { CursorPager, scrollBox, useScrollTopOn } from '@/components/ui/pager';
import { List, ListRow } from '@/components/ui/table';
import { Short } from '@/components/ui/text';
import { client, unwrap } from '@/lib/api';
import { useProject } from '@/lib/session';
import { formatDateTime } from '@/lib/format';
import { useCursorPages } from '@/lib/paging';

export function AuditSection() {
  const { project } = useProject();
  const events = useCursorPages(['audit', project.id], (cursor, limit) =>
    unwrap(
      client.GET('/api/v1/projects/{project_id}/events', {
        params: { path: { project_id: project.id }, query: { audit_only: true, limit, cursor } },
      }),
    ),
  );
  const items = events.items;
  const box = useRef<HTMLDivElement>(null);
  useScrollTopOn(box, events.page);
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="flex min-w-0 flex-[1_1_280px] flex-col gap-1">
          <h2 className="m-0 text-xl font-semibold">Audit log</h2>
          <p className="m-0 text-base text-muted">Every change made in this project, by whom and when.</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <ButtonAnchor size="sm" icon={<Download />} href={`/api/v1/projects/${project.id}/audit:export?format=csv`} download>
            Export CSV
          </ButtonAnchor>
          <ButtonAnchor size="sm" icon={<Download />} href={`/api/v1/projects/${project.id}/audit:export?format=jsonl`} download>
            Export JSON Lines
          </ButtonAnchor>
        </div>
      </div>
      <Card pad={false}>
        <QueryState query={events.query} doing="Couldn’t load the audit log">
          {() =>
            items.length === 0 ? (
              <Empty title="No changes recorded yet" />
            ) : (
              <div ref={box} role="region" aria-label="Audit log, scrolls" tabIndex={0} className={scrollBox('page')}>
                <List label="Audit log">
                  {items.map((e) => (
                    <ListRow key={e.id}>
                      <time className="w-40 shrink-0 font-mono text-xs text-muted" dateTime={e.occurred_at}>
                        {formatDateTime(e.occurred_at)}
                      </time>
                      <span className="min-w-0 flex-[1_1_260px] text-base wrap-break-word">{e.summary}</span>
                      <Short max={40} className="text-xs text-muted">
                        {e.actor_label}
                      </Short>
                    </ListRow>
                  ))}
                </List>
              </div>
            )
          }
        </QueryState>
        <CursorPager
          page={events.page}
          count={items.length}
          hasOlder={events.hasOlder}
          onNewer={events.newer}
          onOlder={events.older}
          loading={events.query.isPlaceholderData}
          noun="changes"
        />
      </Card>
    </div>
  );
}
