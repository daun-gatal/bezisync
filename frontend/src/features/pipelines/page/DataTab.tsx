import { useState } from 'react';
import { toast } from 'sonner';
import { Button, ButtonLink } from '@/components/ui/button';
import { Empty, InlineError, Notice, Pill } from '@/components/ui/feedback';
import { Search } from '@/components/ui/form';
import { Card, QueryState, Split } from '@/components/ui/layout';
import { DataTable, List, ListRow } from '@/components/ui/table';
import { Ident, Meta, plural, Short } from '@/components/ui/text';
import { ago } from '@/lib/format';
import { useReviewSchema, useSchema, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { fullSpec } from '../spec';

const PAGE = 10;

/** What the pipeline captures and where it goes, then the tables' current schema and its changes. */
export function DataTab({ pipeline: p }: { pipeline: Pipeline }) {
  const { project, can } = useProject();
  const spec = fullSpec(p.spec);
  const destinations = (p.artifacts.destinations ?? {}) as Record<string, string>;
  const [q, setQ] = useState('');
  const tables = spec.source.tables.include.filter(
    (t) => !q || t.toLowerCase().includes(q.toLowerCase()) || (destinations[t] ?? '').toLowerCase().includes(q.toLowerCase()),
  );
  const rules = (t: string) => [
    ...(spec.source.columns[t]?.mask ?? []).map((c) => `${c} masked`),
    ...(spec.source.columns[t]?.exclude ?? []).map((c) => `${c} removed`),
    ...(spec.source.columns[t]?.keep ?? []).map((c) => `${c} kept as is`),
  ];
  return (
    <>
      <Card
        title={`Captured tables · ${spec.source.tables.include.length}`}
        pad={false}
        actions={
          can('pipeline.edit') && !['deleting', 'deleted'].includes(p.lifecycle) ? (
            <ButtonLink
              size="sm"
              variant="ghost"
              to="/p/$projectSlug/pipelines/new"
              params={{ projectSlug: project.slug }}
              search={{ draft: p.id, step: 'tables' }}
            >
              Tables and personal data
            </ButtonLink>
          ) : null
        }
      >
        {spec.source.tables.include.length > PAGE ? (
          <div className="border-b border-line px-card py-3">
            <Search label="Search tables" placeholder="Search tables or destinations…" value={q} onChange={setQ} />
          </div>
        ) : null}
        <DataTable
          label="Captured tables"
          rows={tables}
          rowKey={(t) => t}
          paging="card"
          empty={<Empty title={q ? `No table matches “${q}”` : 'No tables captured'} />}
          columns={[
            {
              id: 'table',
              header: 'Table',
              min: 180,
              cell: (t) => (
                <Short max={40} mono>
                  {t}
                </Short>
              ),
            },
            {
              id: 'dest',
              header: 'Destination',
              min: 180,
              wide: true,
              cell: (t) =>
                destinations[t] ? (
                  <Short max={40} mono>
                    {destinations[t]}
                  </Short>
                ) : (
                  <Meta>after deploy</Meta>
                ),
            },
            {
              id: 'rules',
              header: 'Personal data',
              priority: 2,
              min: 140,
              cell: (t) => (rules(t).length ? <span className="text-sm wrap-anywhere">{rules(t).join(', ')}</span> : <Meta>none</Meta>),
            },
          ]}
        />
      </Card>
      <SchemaSection pipeline={p} />
    </>
  );
}

function SchemaSection({ pipeline: p }: { pipeline: Pipeline }) {
  const { project, can } = useProject();
  const schema = useSchema(project.id, p.id);
  const review = useReviewSchema(project.id);
  const [q, setQ] = useState('');
  const [all, setAll] = useState(false);
  const [open, setOpen] = useState<string | null>(null);
  return (
    <QueryState query={schema} doing="Couldn’t load the schema">
      {(s) => {
        if (!s.tracked) {
          return (
            <Card>
              <Empty title="No fixed schema">MongoDB documents have no table schema to track; each event carries its own fields.</Empty>
            </Card>
          );
        }
        const tables = Object.entries(s.tables).filter(([t]) => !q || t.toLowerCase().includes(q.toLowerCase()));
        const shown = all ? tables : tables.slice(0, 8);
        return (
          <Split>
            <Card title="Current schema" pad={false} actions={<Meta>{s.checked_at ? `checked ${ago(s.checked_at)}` : 'not checked yet'}</Meta>}>
              {s.error ? (
                <div className="px-card pt-card">
                  <Notice tone="warn">Last check failed: {s.error}</Notice>
                </div>
              ) : null}
              {Object.keys(s.tables).length > 8 ? (
                <div className="border-b border-line px-card py-3">
                  <Search label="Search the schema" placeholder="Search tables…" value={q} onChange={setQ} />
                </div>
              ) : null}
              {tables.length === 0 ? (
                <Meta as="p" className="p-card">
                  {q ? `No table matches “${q}”.` : 'BeziSync reads the columns once the pipeline streams, then every few minutes.'}
                </Meta>
              ) : (
                <List label="Tables">
                  {shown.map(([table, columns]) => (
                    <li key={table} className="border-b border-line last:border-b-0">
                      <button
                        type="button"
                        aria-expanded={open === table || tables.length === 1}
                        onClick={() => setOpen(open === table ? null : table)}
                        className="flex min-h-row w-full min-w-0 items-center gap-3 px-card py-2 text-left hover:bg-hover"
                      >
                        <Short max={40} mono className="font-semibold">
                          {table}
                        </Short>
                        <Meta className="ml-auto shrink-0">{plural(columns.length, 'column')}</Meta>
                      </button>
                      {open === table || tables.length === 1 ? (
                        <table aria-label={`Columns of ${table}`} className="mb-2 w-full border-collapse text-sm">
                          <tbody>
                            {columns.map((c) => (
                              <tr key={c.name} className="border-t border-line">
                                <td className="py-1.5 pr-3 pl-card font-mono wrap-anywhere">{c.name}</td>
                                <td className="py-1.5 pr-3 font-mono wrap-anywhere text-ink-2">{c.type}</td>
                                <td className="py-1.5 pr-card text-xs whitespace-nowrap text-muted">{c.nullable ? 'nullable' : 'not null'}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      ) : null}
                    </li>
                  ))}
                </List>
              )}
              {tables.length > shown.length ? (
                <div className="border-t border-line px-card py-3">
                  <Button size="sm" onClick={() => setAll(true)}>
                    Show all {tables.length} tables
                  </Button>
                </div>
              ) : null}
            </Card>
            <Card
              title="Changes"
              pad={false}
              actions={
                s.pending_breaking > 0 && can('pipeline.operate') ? (
                  <Button
                    size="sm"
                    loading={review.isPending}
                    onClick={() => review.mutate(p.id, { onSuccess: () => toast.success('Schema changes marked as reviewed') })}
                  >
                    Mark as reviewed
                  </Button>
                ) : null
              }
            >
              {s.pending_breaking > 0 || review.error ? (
                <div className="flex flex-col gap-3 px-card pt-card">
                  {s.pending_breaking > 0 ? (
                    <Notice tone="warn" title={`${s.pending_breaking} breaking change${s.pending_breaking === 1 ? '' : 's'} to review`}>
                      Consumers reading these tables may fail. Check them, then mark the changes as reviewed to clear the alert.
                    </Notice>
                  ) : null}
                  <InlineError error={review.error} doing="Couldn’t mark the changes as reviewed" />
                </div>
              ) : null}
              {s.changes.length === 0 ? (
                <Meta as="p" className="p-card">
                  No changes since BeziSync started watching these tables.
                </Meta>
              ) : (
                <List label="Schema changes">
                  {s.changes.map((c) => (
                    <ListRow key={c.id} tone={c.breaking && !c.reviewed_at ? 'warn' : null}>
                      <Pill tone={c.breaking ? (c.reviewed_at ? 'grey' : 'warn') : 'ok'}>{c.breaking ? 'breaking' : 'compatible'}</Pill>
                      <Ident className="min-w-0 flex-[1_1_220px]">{c.description}</Ident>
                      {c.personal_data ? (
                        <span className="flex flex-wrap items-center gap-2">
                          <Pill tone="warn">looks like personal data · sent as is</Pill>
                          {can('pipeline.edit') ? (
                            <ButtonLink
                              size="sm"
                              variant="ghost"
                              to="/p/$projectSlug/pipelines/new"
                              params={{ projectSlug: project.slug }}
                              search={{ draft: p.id, step: 'tables' }}
                            >
                              Choose what to send
                            </ButtonLink>
                          ) : null}
                        </span>
                      ) : null}
                      <Meta>
                        {ago(c.detected_at)}
                        {c.reviewed_at ? ' · reviewed' : ''}
                      </Meta>
                    </ListRow>
                  ))}
                </List>
              )}
            </Card>
          </Split>
        );
      }}
    </QueryState>
  );
}
