import { useState } from 'react';
import { Link, useNavigate, useParams, useSearch } from '@tanstack/react-router';
import { Pause, Play, RotateCw, Trash2, Wrench } from 'lucide-react';
import { toast } from 'sonner';
import { SettingsChangeList } from '@/components/SettingsChange';
import { Button, ButtonLink } from '@/components/ui/button';
import { InlineError, Notice, Pill, Skeleton } from '@/components/ui/feedback';
import { Field, RadioCards, Textarea } from '@/components/ui/form';
import { Page, PageHeader } from '@/components/ui/layout';
import { Confirm, Menu, Modal, type MenuEntry } from '@/components/ui/overlays';
import { Tabs, TabPanel } from '@/components/ui/tabs';
import { Ident, Short } from '@/components/ui/text';
import { ErrorPanel } from '@/app/ErrorPanel';
import { PIPELINE_TABS, type PipelineTab } from '@/app/router';
import { ApiError } from '@/lib/api';
import { ago } from '@/lib/format';
import { useDecideChange, useDeletePipeline, useDiff, useLifecycle, usePipeline, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { HealthBadge, LifecycleBadge, statusOf } from '@/lib/status';
import { ChangeDebeziumDialog } from './ChangeDebeziumDialog';
import { OutputImpact } from './transforms/Impact';
import { DataTab } from './page/DataTab';
import { HistoryTab } from './page/HistoryTab';
import { Overview } from './page/Overview';
import { SnapshotsTab } from './page/SnapshotsTab';
import { EventsTab, LogsTab } from './page/StreamTabs';

const TAB_LABEL: Record<PipelineTab, string> = {
  overview: 'Overview',
  data: 'Tables & schema',
  events: 'Events',
  logs: 'Logs',
  snapshots: 'Snapshots',
  history: 'History',
};

type Action = 'pause' | 'resume' | 'restart';

export function PipelinePage() {
  const { project, can } = useProject();
  const { pipelineId } = useParams({ from: '/p/$projectSlug/pipelines/$pipelineId' });
  const { tab: requested = 'overview' } = useSearch({ from: '/p/$projectSlug/pipelines/$pipelineId' });
  const navigate = useNavigate({ from: '/p/$projectSlug/pipelines/$pipelineId' });
  const pipeline = usePipeline(project.id, pipelineId);
  const lifecycle = useLifecycle(project.id);
  const [deleting, setDeleting] = useState(false);
  const [changingImage, setChangingImage] = useState(false);

  if (pipeline.isPending) return <Skeleton lines={8} />;
  if (pipeline.error) {
    if (pipeline.error instanceof ApiError && pipeline.error.status === 404) {
      return <ErrorPanel title="Pipeline not found" detail="It may have been deleted, or it belongs to another project." />;
    }
    return <InlineError error={pipeline.error} doing="Couldn’t load the pipeline" onRetry={() => void pipeline.refetch()} />;
  }
  const p = pipeline.data;
  const s = statusOf(p);
  const gone = ['deleting', 'deleted', 'cleanup_failed'].includes(p.lifecycle);
  const deployed = Boolean(p.deployed_version);
  const running = p.desired_state === 'running';
  const canOperate = can('pipeline.operate') && deployed && !gone;
  const canEdit = can('pipeline.edit') && !gone;
  // A pipeline being deleted keeps only what explains the deletion.
  const tabs = PIPELINE_TABS.filter((t) => !gone || t === 'overview' || t === 'history');
  const tab = tabs.includes(requested) ? requested : 'overview';
  const wizard = (step: 'source' | 'tables' | 'sink' | 'transform' | 'review') => ({
    to: '/p/$projectSlug/pipelines/new' as const,
    params: { projectSlug: project.slug },
    search: { draft: p.id, step },
  });

  const act = (action: Action) =>
    lifecycle.mutate(
      { id: p.id, action },
      { onSuccess: () => toast.success(`${action === 'pause' ? 'Pausing' : action === 'resume' ? 'Resuming' : 'Restarting'} ${p.name}`) },
    );
  const busy = (action: Action) => lifecycle.isPending && lifecycle.variables?.action === action;

  // One primary action, chosen by the pipeline's state.
  let primary: 'setup' | 'review' | 'resume' | 'restart' | null = null;
  if (!gone) {
    if (p.lifecycle === 'draft') primary = canEdit ? 'setup' : null;
    else if (p.pending_change) primary = null;
    else if (p.has_undeployed_changes && can('pipeline.deploy') && canEdit) primary = 'review';
    else if (!running && canOperate) primary = 'resume';
    else if (p.lifecycle === 'failed' && canOperate) primary = 'restart';
  }

  const operate: MenuEntry[] = canOperate
    ? [
        running
          ? {
              label: 'Pause',
              icon: <Pause />,
              description: p.engine === 'flink-cdc' ? 'Saves the position and stops the cluster' : 'Stops the connector; the position is kept',
              onSelect: () => act('pause'),
            }
          : null,
        !running && primary !== 'resume' ? { label: 'Resume', icon: <Play />, onSelect: () => act('resume') } : null,
        running && primary !== 'restart'
          ? { label: 'Restart', icon: <RotateCw />, description: 'Restarts the connector; it resumes where it stopped', onSelect: () => act('restart') }
          : null,
      ]
    : [];
  const change: MenuEntry[] =
    canEdit && p.lifecycle !== 'draft'
      ? [
          { label: 'Tables and columns', onSelect: () => void navigate(wizard('tables')) },
          { label: 'Destination and naming', onSelect: () => void navigate(wizard('sink')) },
          { label: 'Transforms', onSelect: () => void navigate(wizard('transform')) },
          { label: 'Name, runtime and settings', onSelect: () => void navigate(wizard('review')) },
        ]
      : [];
  const more: MenuEntry[] = [
    canEdit && deployed && p.engine === 'debezium' ? { label: 'Change Debezium version…', icon: <Wrench />, onSelect: () => setChangingImage(true) } : null,
    can('pipeline.deploy') && !gone ? { label: 'Delete…', icon: <Trash2 />, danger: true, onSelect: () => setDeleting(true) } : null,
  ];

  return (
    <Page>
      <PageHeader
        crumbs={[
          {
            label: 'Pipelines',
            link: (children, className) => (
              <Link to="/p/$projectSlug/pipelines" params={{ projectSlug: project.slug }} className={className}>
                {children}
              </Link>
            ),
          },
          { label: p.name },
        ]}
        title={p.name}
        status={
          <>
            <LifecycleBadge pipeline={p} />
            {p.lifecycle !== 'draft' && !gone ? <HealthBadge pipeline={p} /> : null}
            {deployed ? <span>v{p.deployed_version} deployed</span> : null}
            {s.forced ? <Pill tone="warn">Deployed despite failed checks</Pill> : null}
            <span className="flex min-w-0 items-center gap-1">
              Owner{' '}
              {p.owner ? (
                <Short max={40} className="font-medium text-ink-2">
                  {p.owner}
                </Short>
              ) : (
                'not set'
              )}
            </span>
          </>
        }
        actions={
          <>
            {operate.some(Boolean) ? <Menu label="Operate" items={operate} /> : null}
            {change.length ? <Menu label="Change" items={change} /> : null}
            {more.some(Boolean) ? <Menu label="More actions" icon="more" items={more} /> : null}
            {primary === 'setup' ? (
              <ButtonLink variant="primary" {...wizard('review')}>
                Continue setup
              </ButtonLink>
            ) : primary === 'review' ? (
              <ButtonLink variant="primary" {...wizard('review')}>
                Review and deploy
              </ButtonLink>
            ) : primary === 'resume' ? (
              <Button variant="primary" icon={<Play />} loading={busy('resume')} onClick={() => act('resume')}>
                Resume
              </Button>
            ) : primary === 'restart' ? (
              <Button variant="primary" icon={<RotateCw />} loading={busy('restart')} onClick={() => act('restart')}>
                Restart
              </Button>
            ) : null}
          </>
        }
      />
      <InlineError error={lifecycle.error} doing={`Couldn’t ${lifecycle.variables?.action ?? 'change'} the pipeline`} />

      {p.pending_change && !gone ? <ApprovalBanner pipeline={p} /> : null}
      {p.has_undeployed_changes && deployed && !gone && !p.pending_change ? (
        <Notice tone="info" title={`v${p.current_version} is saved but not deployed`}>
          The running connector uses v{p.deployed_version}.{primary === 'review' ? ' Review and deploy it when you’re ready.' : ''}
        </Notice>
      ) : null}
      {gone ? (
        <Notice
          tone={p.lifecycle === 'cleanup_failed' ? 'bad' : 'info'}
          title={
            p.lifecycle === 'deleting' ? 'This pipeline is being deleted' : p.lifecycle === 'deleted' ? 'This pipeline is deleted' : 'Cleanup didn’t finish'
          }
        >
          {p.lifecycle === 'deleting'
            ? 'BeziSync is stopping the connector and removing what it created. What was removed is listed below when it finishes.'
            : p.lifecycle === 'deleted'
              ? 'Only the cleanup report and the history remain.'
              : 'Some of what BeziSync created couldn’t be removed. The cleanup report says what is left and how to remove it.'}
        </Notice>
      ) : null}

      <Tabs
        idBase="pipeline"
        label="Pipeline sections"
        value={tab}
        onChange={(t) => void navigate({ search: t === 'overview' ? {} : { tab: t }, replace: true })}
        tabs={tabs.map((t) => ({ id: t, label: t === 'snapshots' && p.engine === 'flink-cdc' ? 'Positions' : TAB_LABEL[t] }))}
      />
      <TabPanel idBase="pipeline" value={tab}>
        {tab === 'overview' ? <Overview pipeline={p} /> : null}
        {tab === 'data' ? <DataTab pipeline={p} /> : null}
        {tab === 'events' ? <EventsTab pipeline={p} /> : null}
        {tab === 'logs' ? <LogsTab pipeline={p} /> : null}
        {tab === 'snapshots' ? <SnapshotsTab pipeline={p} /> : null}
        {tab === 'history' ? <HistoryTab pipeline={p} /> : null}
      </TabPanel>

      <DeleteDialog pipeline={p} open={deleting} onOpenChange={setDeleting} />
      <ChangeDebeziumDialog pipelines={[{ id: p.id, name: p.name }]} open={changingImage} onOpenChange={setChangingImage} />
    </Page>
  );
}

/** Delete: lists what is removed on the source, and makes the destination data an explicit choice. */
function DeleteDialog({ pipeline: p, open, onOpenChange }: { pipeline: Pipeline; open: boolean; onOpenChange: (o: boolean) => void }) {
  const { project } = useProject();
  const remove = useDeletePipeline(project.id);
  const [topics, setTopics] = useState<'keep' | 'delete'>('keep');
  const artifacts = ((p.artifacts.source ?? []) as { kind: string; name: string }[]).filter((a) => a.kind !== 'table' || !a.name.endsWith('heartbeat'));
  return (
    <Confirm
      open={open}
      onOpenChange={(o) => {
        if (o) {
          setTopics('keep');
          remove.reset();
        }
        onOpenChange(o);
      }}
      title={`Delete ${p.name}?`}
      effects={[
        'The connector stops.',
        artifacts.length ? (
          <>
            BeziSync removes what it created on the source, so the database stops keeping changes for it:{' '}
            {artifacts.map((a, i) => (
              <span key={`${a.kind}${a.name}`}>
                {i ? ', ' : ''}
                {a.kind.replace('_', ' ')} <Ident>{a.name}</Ident>
              </span>
            ))}
            .
          </>
        ) : (
          'Nothing was created on the source yet.'
        ),
        'Its stored position is removed.',
        topics === 'delete' ? 'The destination topics or streams and the data in them are deleted.' : 'The destination data stays where it is.',
      ]}
      confirmLabel={topics === 'delete' ? 'Delete pipeline and data' : 'Delete pipeline'}
      typeToConfirm={p.name}
      pending={remove.isPending}
      error={remove.error}
      doing="Couldn’t delete the pipeline"
      onConfirm={() =>
        remove.mutate(
          { id: p.id, dropDestinations: topics === 'delete' },
          {
            onSuccess: () => {
              toast.success(`Deleting ${p.name}`);
              onOpenChange(false);
            },
          },
        )
      }
    >
      <Field label="Destination data" group>
        {() => (
          <RadioCards
            label="Destination data"
            value={topics}
            onChange={setTopics}
            columns={1}
            options={[
              { value: 'keep', label: 'Keep it (recommended)', description: 'Consumers can still read what was delivered.' },
              { value: 'delete', label: 'Delete the destination topics or streams', description: 'The data in them is lost for every consumer.' },
            ]}
          />
        )}
      </Field>
    </Confirm>
  );
}

function ApprovalBanner({ pipeline: p }: { pipeline: Pipeline }) {
  const { project, can, me } = useProject();
  const decide = useDecideChange(project.id);
  const [rejecting, setRejecting] = useState(false);
  const [comment, setComment] = useState('');
  const cr = p.pending_change!;
  const mine = cr.requested_by === me.user.email;
  const run = (action: 'approve' | 'reject' | 'cancel', text = '') =>
    decide.mutate(
      { id: cr.id, action, comment: text },
      {
        onSuccess: () => {
          toast.success(action === 'approve' ? `Approved: deploying v${cr.version}` : action === 'reject' ? 'Rejected' : 'Request cancelled');
          setRejecting(false);
          setComment('');
        },
      },
    );
  return (
    <>
      <Notice
        tone="warn"
        title={`v${cr.version} is waiting for approval`}
        action={
          <>
            {can('pipeline.approve') && !mine ? (
              <>
                <Button size="sm" variant="primary" loading={decide.isPending && decide.variables?.action === 'approve'} onClick={() => run('approve')}>
                  Approve and deploy
                </Button>
                <Button
                  size="sm"
                  onClick={() => {
                    decide.reset();
                    setRejecting(true);
                  }}
                >
                  Reject…
                </Button>
              </>
            ) : null}
            {mine || can('pipeline.approve') ? (
              <Button size="sm" variant="ghost" loading={decide.isPending && decide.variables?.action === 'cancel'} onClick={() => run('cancel')}>
                Cancel request
              </Button>
            ) : null}
          </>
        }
      >
        Requested by {cr.requested_by ?? 'a deleted user'} {ago(cr.created_at)}. Checks at request time: {cr.preflight_summary || 'not recorded'}.
        {cr.force_reason ? <> Skips failed checks: “{cr.force_reason}”.</> : null}
        {mine ? ' Someone else with approval rights must approve it.' : ''}
        <div className="mt-3">
          <OutputImpact pipeline={p} version={cr.version} />
        </div>
        <RequestSettingsChange pipeline={p} version={cr.version} />
      </Notice>
      {!rejecting ? <InlineError error={decide.error} doing="Couldn’t record the decision" /> : null}
      <Modal
        open={rejecting}
        onOpenChange={setRejecting}
        dirty={Boolean(comment.trim())}
        title={`Reject v${cr.version}?`}
        description="The requester sees your reason."
        why={comment.trim() ? null : 'Say why, so the requester knows what to change.'}
        footer={
          <>
            <Button onClick={() => setRejecting(false)}>Cancel</Button>
            <Button variant="danger" disabled={!comment.trim()} loading={decide.isPending} onClick={() => run('reject', comment.trim())}>
              Reject
            </Button>
          </>
        }
      >
        <Field label="Reason">
          {(id) => <Textarea id={id} value={comment} onChange={(e) => setComment(e.target.value)} placeholder="What should change before this can ship?" />}
        </Field>
        <InlineError error={decide.error} doing="Couldn’t reject the request" />
      </Modal>
    </>
  );
}

/** What an approval request changes in the settings, against the deployed version. */
function RequestSettingsChange({ pipeline: p, version }: { pipeline: Pipeline; version: number }) {
  const { project } = useProject();
  const diff = useDiff(project.id, p.id, p.deployed_version ?? undefined, p.deployed_version ? version : undefined);
  const change = diff.data?.settings_change;
  if (!change) return null;
  return (
    <div className="mt-3 flex min-w-0 flex-col gap-2">
      <strong className="text-sm font-semibold">What changes in the settings</strong>
      <SettingsChangeList change={change} />
    </div>
  );
}
