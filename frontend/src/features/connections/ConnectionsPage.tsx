import { useState } from 'react';
import { useNavigate, useSearch } from '@tanstack/react-router';
import { Cable, Copy, RefreshCw } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Pill } from '@/components/ui/feedback';
import { Card, Page, PageHeader, QueryState } from '@/components/ui/layout';
import { Confirm, Menu, Modal } from '@/components/ui/overlays';
import { Cell, DataTable, type Column } from '@/components/ui/table';
import { Tabs, TabPanel } from '@/components/ui/tabs';
import { Meta, plural } from '@/components/ui/text';
import { CheckList, reportBadge } from '@/components/CheckList';
import { ago } from '@/lib/format';
import { useProject } from '@/lib/session';
import { TYPE_LABEL, useCheckConnection, useConnections, useDeleteConnection, type Connection, type Kind } from '@/lib/connections';
import { missingLabel, useCopyTargets } from '@/lib/copy';
import { ConnectionDialog } from './ConnectionDialog';
import { CopyDialog } from './CopyDialog';

export function summaryLine(c: Connection): string {
  const cfg = c.config as Record<string, unknown>;
  if (c.type === 'kafka') return String(cfg.bootstrap_servers ?? '');
  if (c.type === 'http' || c.type === 'nats') return String(cfg.url ?? '');
  if (c.type === 'mongodb') return String(cfg.hosts ?? '');
  if (c.type === 'kinesis') return `${String(cfg.region ?? '')}${cfg.endpoint ? ` · ${String(cfg.endpoint)}` : ''}`;
  if (c.type === 'pubsub') return `project ${String(cfg.project_id ?? '')}`;
  if (c.type === 'pulsar') return `${String(cfg.service_url ?? '')} · ${String(cfg.tenant ?? 'public')}/${String(cfg.namespace ?? 'default')}`;
  if (c.type === 'eventhubs') return `hub ${String(cfg.hub_name ?? '')}`;
  if (c.type === 'sqs') return String(cfg.queue_url ?? '');
  if (c.type === 'sns') return `${String(cfg.region ?? '')} · account ${String(cfg.account_id ?? '')}`;
  if (c.type === 'jdbc') return `${String(cfg.host ?? '')}:${String(cfg.port ?? '')}/${String(cfg.database ?? '')}${cfg.schema && cfg.database_type !== 'mysql' && cfg.database_type !== 'mariadb' ? `.${String(cfg.schema)}` : ''}`;
  if (c.type === 'iceberg') return `${cfg.catalog_type === 'glue' ? 'AWS Glue' : String(cfg.uri ?? '')} · ${String(cfg.namespace ?? 'cdc')}`;
  if (c.type === 'oracle') return `${String(cfg.host ?? '')}:${String(cfg.port ?? '')}/${String(cfg.pdb || cfg.database || '')}`;
  if (cfg.host) return `${String(cfg.host)}:${String(cfg.port ?? '')}${cfg.database ? `/${String(cfg.database)}` : ''}`;
  return '';
}

export function ConnectionsPage() {
  const { project, can, expert } = useProject();
  const { kind: kindParam, page } = useSearch({ from: '/p/$projectSlug/connections' });
  const navigate = useNavigate({ from: '/p/$projectSlug/connections' });
  const kind: Kind = kindParam === 'sink' ? 'sink' : 'source';
  const connections = useConnections(project.id);
  const check = useCheckConnection(project.id);
  const remove = useDeleteConnection(project.id);
  const [dialog, setDialog] = useState<{ existing?: Connection } | null>(null);
  const [detailsId, setDetailsId] = useState<string | null>(null);
  const [deleting, setDeleting] = useState<Connection | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [copying, setCopying] = useState<Connection[] | null>(null);
  const copyTargets = useCopyTargets();
  /** The last re-check that failed to run, per connection, shown in its row. */
  const [checkErrors, setCheckErrors] = useState<Record<string, unknown>>({});
  const all = connections.data ?? [];
  const rows = all.filter((c) => c.kind === kind);
  const details = all.find((c) => c.id === detailsId) ?? null;
  const counts = { source: all.filter((c) => c.kind === 'source').length, sink: all.filter((c) => c.kind === 'sink').length };
  const canManage = can('connection.manage');
  const canCopy = canManage && copyTargets.length > 0;
  const noun = kind === 'source' ? 'source' : 'destination';

  const recheck = (c: Connection) =>
    check.mutate(
      { id: c.id },
      {
        onSuccess: () => setCheckErrors(({ [c.id]: _, ...rest }) => rest),
        onError: (e) => setCheckErrors((m) => ({ ...m, [c.id]: e })),
      },
    );
  const editable = (c: Connection) => canManage && (!c.managed || can('project.manage'));

  const columns: Column<Connection>[] = [
    {
      id: 'name',
      header: 'Name',
      min: 200,
      cell: (c) => (
        <Cell
          main={
            <button
              type="button"
              className="text-left font-semibold wrap-anywhere text-ink hover:text-accent hover:underline"
              onClick={() => setDetailsId(c.id)}
            >
              {c.name}
            </button>
          }
          sub={<span className="font-mono">{summaryLine(c)}</span>}
        />
      ),
    },
    {
      id: 'type',
      header: 'Type',
      min: 110,
      cell: (c) => (
        <span className="flex flex-wrap items-center gap-1.5">
          {TYPE_LABEL[c.type] ?? c.type}
          {c.managed ? <Pill tone="info">managed</Pill> : null}
        </span>
      ),
    },
    {
      id: 'ready',
      header: 'Readiness',
      min: 160,
      cell: (c) => (
        <div className="flex min-w-0 flex-col items-start gap-1">
          {c.missing_secrets.length ? <Pill tone="warn">{missingLabel(c.missing_secrets)}</Pill> : reportBadge(c.last_check)}
          {checkErrors[c.id] ? <span className="text-xs text-bad">Re-check didn’t run. Open it for details.</span> : null}
        </div>
      ),
    },
    { id: 'used', header: 'Used by', priority: 2, fit: true, cell: (c) => plural(c.used_by, 'pipeline') },
    { id: 'checked', header: 'Last checked', priority: 3, fit: true, cell: (c) => <Meta>{c.last_checked_at ? ago(c.last_checked_at) : 'never'}</Meta> },
    {
      id: 'actions',
      header: <span className="sr-only">Actions</span>,
      fit: true,
      cell: (c) => (
        <div className="flex justify-end gap-1.5">
          <Button size="sm" icon={<RefreshCw />} loading={check.isPending && check.variables?.id === c.id} onClick={() => recheck(c)}>
            Re-check
          </Button>
          {editable(c) ? (
            <>
              <Button size="sm" variant="ghost" onClick={() => setDialog({ existing: c })}>
                Edit
              </Button>
              <Button
                size="sm"
                variant="ghost"
                disabled={c.used_by > 0}
                why={c.used_by > 0 ? `Used by ${plural(c.used_by, 'pipeline')}. Point them at another connection or delete them first.` : null}
                onClick={() => {
                  remove.reset();
                  setDeleting(c);
                }}
              >
                Delete
              </Button>
            </>
          ) : null}
          {canCopy ? (
            <Menu
              label={`More actions for ${c.name}`}
              icon="more"
              variant="ghost"
              size="sm"
              items={[{ label: 'Copy to project…', icon: <Copy />, description: 'The original stays here.', onSelect: () => setCopying([c]) }]}
            />
          ) : null}
        </div>
      ),
    },
  ];

  return (
    <Page>
      <PageHeader
        title="Connections"
        description="Reusable databases and destinations. Secrets are write-only and never shown again."
        actions={
          canManage ? (
            <Button variant="primary" onClick={() => setDialog({})}>
              Add {noun}
            </Button>
          ) : null
        }
      />
      <Tabs
        idBase="connections"
        label="Connection type"
        value={kind}
        onChange={(k) => void navigate({ search: k === 'sink' ? { kind: 'sink' } : {} })}
        tabs={[
          { id: 'source', label: 'Sources', count: connections.data ? counts.source : null },
          { id: 'sink', label: 'Destinations', count: connections.data ? counts.sink : null },
        ]}
      />
      <TabPanel idBase="connections" value={kind}>
        <Card pad={false}>
          <QueryState query={connections} doing="Couldn’t load connections">
            {() => (
              <DataTable
                label={kind === 'source' ? 'Sources' : 'Destinations'}
                columns={columns}
                rows={rows}
                rowKey={(c) => c.id}
                rowTone={(c) => (c.missing_secrets.length ? 'warn' : c.last_check && !c.last_check.ok ? 'bad' : null)}
                rowLabel={(c) => c.name}
                selection={canCopy ? { selected, onChange: setSelected } : undefined}
                bulk={(picked) => (
                  <Button size="sm" variant="primary" icon={<Copy />} onClick={() => setCopying(picked)}>
                    Copy to project…
                  </Button>
                )}
                page={page}
                onPage={(p) => void navigate({ search: (s) => ({ ...s, page: p > 1 ? p : undefined }) })}
                noun={kind === 'source' ? 'sources' : 'destinations'}
                empty={
                  <Empty
                    icon={<Cable />}
                    title={`No ${noun}s yet`}
                    action={
                      canManage ? (
                        <Button variant="primary" onClick={() => setDialog({})}>
                          Add {noun}
                        </Button>
                      ) : null
                    }
                  >
                    {kind === 'source'
                      ? 'Add PostgreSQL, MySQL, SQL Server, MongoDB, Oracle, CockroachDB, Vitess or Spanner. BeziSync checks it is ready for change data capture.'
                      : 'Add Kafka, Apache Iceberg, a database (JDBC), Pub/Sub, Kinesis, SQS, SNS, Event Hubs, Pulsar, Redis, NATS or HTTP.'}
                  </Empty>
                }
              />
            )}
          </QueryState>
        </Card>
      </TabPanel>

      <CopyDialog
        connections={copying ?? []}
        open={copying !== null}
        onOpenChange={(o) => {
          if (!o) {
            setCopying(null);
            setSelected(new Set());
          }
        }}
      />
      <ConnectionDialog open={dialog !== null} onOpenChange={(o) => !o && setDialog(null)} kind={kind} existing={dialog?.existing} />

      <Modal
        open={details !== null}
        onOpenChange={(o) => !o && setDetailsId(null)}
        title={details?.name ?? ''}
        description={details ? `${TYPE_LABEL[details.type] ?? details.type} · ${summaryLine(details)}` : ''}
        size="lg"
      >
        {details ? (
          <>
            <InlineError error={checkErrors[details.id]} doing="Couldn’t run the checks" onRetry={() => recheck(details)} />
            {details.last_check ? (
              <CheckList report={details.last_check} expert={expert} subject={details.name} onRecheck={() => recheck(details)} rechecking={check.isPending} />
            ) : (
              <Empty
                title="Not checked yet"
                action={
                  <Button onClick={() => recheck(details)} loading={check.isPending}>
                    Run checks
                  </Button>
                }
              />
            )}
          </>
        ) : null}
      </Modal>

      <Confirm
        open={deleting !== null}
        onOpenChange={(o) => !o && setDeleting(null)}
        title={`Delete ${deleting?.name ?? ''}?`}
        effects={['The stored secret is destroyed.', 'No pipeline can use this connection afterwards.']}
        confirmLabel="Delete"
        typeToConfirm={deleting?.name}
        pending={remove.isPending}
        error={remove.error}
        doing="Couldn’t delete the connection"
        onConfirm={() =>
          deleting &&
          remove.mutate(deleting.id, {
            onSuccess: () => {
              toast.success(`Deleted ${deleting.name}`);
              setDeleting(null);
            },
          })
        }
      />
    </Page>
  );
}
