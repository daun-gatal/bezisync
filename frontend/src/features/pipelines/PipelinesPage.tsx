import { useMemo, useState } from 'react';
import { Link, useNavigate, useSearch } from '@tanstack/react-router';
import { Rows3 } from 'lucide-react';
import { Button, ButtonLink } from '@/components/ui/button';
import { Empty, Pill, Skeleton } from '@/components/ui/feedback';
import { Search } from '@/components/ui/form';
import { Card, Page, PageHeader, QueryState, Toolbar } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import { Cell, DataTable, type Column } from '@/components/ui/table';
import { Segmented } from '@/components/ui/form';
import { Meta, plural, Short } from '@/components/ui/text';
import { AdoptDialog, ApplyYamlDialog } from './ImportDialogs';
import { ChangeDebeziumDialog } from './ChangeDebeziumDialog';
import { FleetHealth } from './FleetHealth';
import { TYPE_LABEL } from '@/lib/connections';
import { ENGINE_TITLE } from '@/lib/engines';
import { imageByPipeline, shortImage, useDebeziumImages, type PipelineImage } from '@/lib/debezium';
import { ago, formatBytes, formatDuration } from '@/lib/format';
import { usePipelines, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { FLEET, fleetGroup, HealthBadge, LifecycleBadge, needsAttention, pipelineTone, statusOf } from '@/lib/status';

export function PipelinesPage() {
  const { project, can } = useProject();
  const search = useSearch({ from: '/p/$projectSlug/pipelines' });
  const navigate = useNavigate({ from: '/p/$projectSlug/pipelines' });
  const pipelines = usePipelines(project.id);
  const images = useDebeziumImages(project.id);
  const imageOf = useMemo(() => imageByPipeline(images.data), [images.data]);
  const [importing, setImporting] = useState<'yaml' | 'adopt' | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [changing, setChanging] = useState(false);
  const canEdit = can('pipeline.edit');
  const all = (pipelines.data ?? []).filter((p) => p.lifecycle !== 'deleted');
  const attention = all.filter(needsAttention);
  const q = (search.q ?? '').trim().toLowerCase();
  const rows = all.filter(
    (p) =>
      (search.filter === 'attention' ? needsAttention(p) : search.filter ? fleetGroup(p) === search.filter : true) &&
      (!q || p.name.includes(q) || p.owner.toLowerCase().includes(q) || Object.entries(p.labels).some(([k, v]) => `${k}=${v}`.toLowerCase().includes(q))) &&
      (!search.debezium || imageOf.get(p.id)?.image === search.debezium),
  );
  const versions = images.data?.images ?? [];
  const picked = all.filter((p) => selected.has(p.id));
  // A new filter or search starts again from page 1.
  const setSearch = (patch: Partial<typeof search>) => void navigate({ search: (s) => ({ ...s, page: undefined, ...patch }), replace: true });
  const filterLabel = search.filter === 'attention' ? 'needing attention' : FLEET.find((g) => g.id === search.filter)?.label.toLowerCase();

  const columns: Column<Pipeline>[] = [
    {
      id: 'name',
      header: 'Pipeline',
      min: 200,
      cell: (p) => (
        <Cell
          main={
            <Link
              to="/p/$projectSlug/pipelines/$pipelineId"
              params={{ projectSlug: project.slug, pipelineId: p.id }}
              className="font-semibold text-ink no-underline hover:text-accent hover:underline"
            >
              {p.name}
            </Link>
          }
          sub={[p.owner || 'no owner', ...Object.entries(p.labels).map(([k, v]) => `${k}=${v}`)].join(' · ')}
        />
      ),
    },
    {
      id: 'flow',
      header: 'Source → Destination',
      min: 200,
      wide: true,
      cell: (p) => (
        <div className="flex min-w-0 flex-col gap-0.5 text-sm">
          <span title={`${p.source.name} (${TYPE_LABEL[p.source.type] ?? p.source.type})`} className="min-w-0">
            <Short max={34} mono>
              {p.source.name}
            </Short>
          </span>
          <span className="flex min-w-0 gap-1" title={p.sink ? `${p.sink.name} (${TYPE_LABEL[p.sink.type] ?? p.sink.type})` : undefined}>
            <span aria-hidden className="shrink-0 text-muted">
              →
            </span>
            {p.sink ? (
              <Short max={32} mono>
                {p.sink.name}
              </Short>
            ) : (
              <span className="text-muted">no destination yet</span>
            )}
          </span>
        </div>
      ),
    },
    { id: 'status', header: 'Status', min: 130, cell: (p) => <LifecycleBadge pipeline={p} /> },
    { id: 'health', header: 'Health', min: 110, cell: (p) => <HealthBadge pipeline={p} /> },
    {
      id: 'latency',
      header: 'Latency',
      priority: 2,
      fit: true,
      align: 'right',
      cell: (p) => <span className="font-mono text-sm">{formatDuration(statusOf(p).metrics?.ms_behind_source)}</span>,
    },
    {
      id: 'log',
      header: 'Source log',
      priority: 3,
      fit: true,
      align: 'right',
      cell: (p) => {
        const s = statusOf(p).source_impact;
        return <span className={s?.risk ? 'font-mono text-sm font-semibold text-risk' : 'font-mono text-sm'}>{formatBytes(s?.retained_bytes)}</span>;
      },
    },
    { id: 'engine', header: 'Engine', priority: 4, fit: true, cell: (p) => <span className="text-sm">{ENGINE_TITLE[p.engine]}</span> },
    {
      id: 'debezium',
      header: 'Debezium',
      priority: 3,
      min: 120,
      cell: (p) => (p.engine === 'debezium' ? <ImageCell image={imageOf.get(p.id)} /> : <span className="text-sm text-muted">—</span>),
    },
    {
      id: 'rate',
      header: 'Changes/s',
      priority: 4,
      fit: true,
      align: 'right',
      cell: (p) => {
        const r = statusOf(p).events_per_second;
        return <span className="font-mono text-sm">{r === null || r === undefined ? '—' : r.toFixed(r < 10 ? 1 : 0)}</span>;
      },
    },
    { id: 'updated', header: 'Updated', priority: 4, fit: true, cell: (p) => <Meta>{ago(statusOf(p).as_of ?? p.updated_at)}</Meta> },
  ];

  return (
    <Page>
      <PageHeader
        title="Pipelines"
        status={
          pipelines.data ? <span>{`${plural(all.length, 'pipeline')} · ${attention.length} need${attention.length === 1 ? 's' : ''} attention`}</span> : null
        }
        actions={
          canEdit ? (
            <>
              <Button onClick={() => setImporting('yaml')}>Import YAML</Button>
              <Button variant="ghost" onClick={() => setImporting('adopt')}>
                Adopt existing
              </Button>
            </>
          ) : null
        }
      />
      <ApplyYamlDialog open={importing === 'yaml'} onOpenChange={(o) => setImporting(o ? 'yaml' : null)} />
      <AdoptDialog open={importing === 'adopt'} onOpenChange={(o) => setImporting(o ? 'adopt' : null)} />

      {all.length > 0 ? <FleetHealth pipelines={all} active={search.filter} onPick={(g) => setSearch({ filter: g })} /> : null}

      <Card pad={false}>
        <div className="border-b border-line px-card py-3">
          <Toolbar>
            <Segmented
              label="Show"
              value={search.filter === 'attention' ? 'attention' : search.filter ? 'group' : 'all'}
              onChange={(v) => setSearch({ filter: v === 'attention' ? 'attention' : v === 'all' ? undefined : search.filter })}
              options={[
                { value: 'all', label: `All ${all.length}` },
                { value: 'attention', label: `Needs attention ${attention.length}` },
                ...(search.filter && search.filter !== 'attention'
                  ? [{ value: 'group' as const, label: FLEET.find((g) => g.id === search.filter)?.label ?? search.filter }]
                  : []),
              ]}
            />
            <Search
              label="Search pipelines"
              placeholder="Search name, owner, label…"
              value={search.q ?? ''}
              onChange={(v) => setSearch({ q: v || undefined })}
            />
            {versions.length > 0 || search.debezium ? (
              <div className="min-w-0 flex-[0_1_260px]">
                <Select
                  value={search.debezium ?? ''}
                  onChange={(v) => setSearch({ debezium: v || undefined })}
                  options={[
                    { value: '', label: 'Any Debezium version' },
                    ...(search.debezium && !versions.some((v) => v.image === search.debezium)
                      ? [{ value: search.debezium, label: `Debezium ${shortImage(search.debezium)}`, description: 'No pipelines run it now' }]
                      : []),
                    ...versions.map((v) => ({
                      value: v.image,
                      label: `Debezium ${shortImage(v.image)}`,
                      text: `Debezium ${shortImage(v.image)}`,
                      description: `${plural(v.pipelines.length, 'pipeline')}${v.standing === 'deprecated' ? ' · deprecated' : v.standing === 'unlisted' ? ' · not offered' : ''}`,
                    })),
                  ]}
                />
              </div>
            ) : null}
          </Toolbar>
        </div>
        {pipelines.isPending ? (
          <div className="p-card">
            <Skeleton lines={5} />
          </div>
        ) : (
          <QueryState query={pipelines} doing="Couldn’t load pipelines">
            {() => (
              <DataTable
                label="Pipelines"
                columns={columns}
                rows={rows}
                rowKey={(p) => p.id}
                rowLabel={(p) => p.name}
                rowTone={pipelineTone}
                page={search.page}
                onPage={(p) => void navigate({ search: (s) => ({ ...s, page: p > 1 ? p : undefined }) })}
                selection={
                  canEdit
                    ? { selected, onChange: setSelected, why: (p) => (imageOf.get(p.id) ? null : 'Not deployed yet, so it has no Debezium version to change.') }
                    : undefined
                }
                bulk={() => (
                  <Button size="sm" variant="primary" onClick={() => setChanging(true)}>
                    Change Debezium version…
                  </Button>
                )}
                empty={
                  all.length === 0 ? (
                    <Empty
                      icon={<Rows3 />}
                      title="No pipelines yet"
                      action={
                        canEdit ? (
                          <div className="flex flex-wrap justify-center gap-2">
                            <ButtonLink to="/p/$projectSlug/pipelines/new" params={{ projectSlug: project.slug }} variant="primary">
                              Create a pipeline
                            </ButtonLink>
                            <ButtonLink to="/p/$projectSlug/welcome" params={{ projectSlug: project.slug }} variant="ghost">
                              Getting started
                            </ButtonLink>
                          </div>
                        ) : null
                      }
                    >
                      A pipeline streams every change from database tables to a destination.
                    </Empty>
                  ) : (
                    <Empty
                      title={filterLabel ? `No pipelines ${filterLabel}${q ? ` match “${search.q}”` : ''}` : `Nothing matches “${search.q ?? ''}”`}
                      action={
                        <Button variant="ghost" onClick={() => setSearch({ q: undefined, filter: undefined, debezium: undefined })}>
                          Clear the search and filters
                        </Button>
                      }
                    />
                  )
                }
              />
            )}
          </QueryState>
        )}
      </Card>
      <ChangeDebeziumDialog pipelines={picked} open={changing} onOpenChange={setChanging} onDone={() => setSelected(new Set())} />
      <Meta as="p">Health is never green without fresh evidence. Grey “Unknown” or “Stale” means BeziSync has no recent signal, not that it is fine.</Meta>
    </Page>
  );
}

function ImageCell({ image }: { image: PipelineImage | undefined }) {
  if (!image) return <span className="text-muted">—</span>;
  return (
    <span className="flex min-w-0 flex-col items-start gap-1">
      <Short max={22} mono>
        {shortImage(image.image)}
      </Short>
      {image.standing === 'deprecated' ? <Pill tone="warn">deprecated</Pill> : image.standing === 'unlisted' ? <Pill tone="warn">not offered</Pill> : null}
    </span>
  );
}
