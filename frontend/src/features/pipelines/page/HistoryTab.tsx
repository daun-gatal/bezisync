import { useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import clsx from 'clsx';
import { toast } from 'sonner';
import { SettingsChangeList, settingsChangeLines } from '@/components/SettingsChange';
import { Button } from '@/components/ui/button';
import { InlineError, Notice, Pill, Skeleton } from '@/components/ui/feedback';
import { Check, Field, Input } from '@/components/ui/form';
import { Card, QueryState, Split, Toolbar } from '@/components/ui/layout';
import { CodeBlock, Confirm, Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { CompactPager, PagedBox, scrollBox } from '@/components/ui/pager';
import { List, ListRow } from '@/components/ui/table';
import { Meta, Short } from '@/components/ui/text';
import { client, unwrap, type Schemas } from '@/lib/api';
import { TYPE_LABEL, useConnections, type Connection } from '@/lib/connections';
import { ago, formatDateTime, slugify } from '@/lib/format';
import { useActivity, useConfig, useDeploy, useDiff, useVersions, type Pipeline } from '@/lib/pipelines';
import { usePolicies } from '@/lib/policies';
import { useProject } from '@/lib/session';
import { useCreateTemplate, useTemplates } from '@/lib/templates';
import { useRuntimes } from '@/features/settings/RuntimesSection';
import { ApplyResults } from '../ImportDialogs';
import { CopyDialog } from '@/features/connections/CopyDialog';
import { missingLabel, useCopyTargets } from '@/lib/copy';

/** Versions, the generated configuration of the one you pick, deploy or roll back, then everything that happened. */
export function HistoryTab({ pipeline: p }: { pipeline: Pipeline }) {
  const { project, can } = useProject();
  const versions = useVersions(project.id, p.id);
  const [picked, setPicked] = useState<number | undefined>(undefined);
  const shown = picked ?? p.current_version ?? undefined;
  // The configuration is generated for a runtime; a draft without one has none yet.
  const config = useConfig(project.id, p.id, shown, Boolean(p.runtime));
  const [deploying, setDeploying] = useState<number | null>(null);
  const [dialog, setDialog] = useState<'yaml' | 'promote' | 'template' | null>(null);
  const gone = ['deleting', 'deleted'].includes(p.lifecycle);
  return (
    <>
      {!gone ? (
        <Toolbar className="justify-end">
          <Button size="sm" onClick={() => setDialog('yaml')}>
            Export YAML
          </Button>
          {p.current_version ? (
            <Button size="sm" onClick={() => setDialog('promote')}>
              Promote…
            </Button>
          ) : null}
          {can('project.manage') ? (
            <Button size="sm" onClick={() => setDialog('template')}>
              Save as template…
            </Button>
          ) : null}
        </Toolbar>
      ) : null}
      <Split ratio="side">
        <Card title="Versions" pad={false}>
          <QueryState query={versions} doing="Couldn’t load versions">
            {(list) => (
              <PagedBox rows={list} label="Versions" noun="versions">
                {(pageRows) => (
                  <List label="Versions">
                    {pageRows.map((v) => (
                      <li
                        key={v.number}
                        className={clsx(
                          'flex min-h-row min-w-0 items-center gap-2 border-b border-line px-card py-2 last:border-b-0',
                          shown === v.number && 'bg-accent-soft/50',
                        )}
                      >
                        <button
                          type="button"
                          aria-pressed={shown === v.number}
                          className="flex min-w-0 flex-1 flex-col gap-0.5 text-left"
                          onClick={() => setPicked(v.number)}
                        >
                          <span className="flex flex-wrap items-center gap-2">
                            <strong className="font-mono font-medium">v{v.number}</strong>
                            {v.deployed ? <Pill tone="info">deployed</Pill> : null}
                            {v.draft ? (
                              <Pill title="Saves to this change update this version until it is deployed or sent for approval.">draft</Pill>
                            ) : null}
                          </span>
                          <span className="line-clamp-2 text-xs wrap-anywhere text-muted">
                            {v.note || 'Edited'} · {v.created_by} · {ago(v.created_at)}
                          </span>
                        </button>
                        {can('pipeline.deploy') && !v.deployed && p.deployed_version && !gone ? (
                          <Button size="sm" onClick={() => setDeploying(v.number)}>
                            {v.number < (p.deployed_version ?? 0) ? 'Roll back' : 'Deploy'}
                          </Button>
                        ) : null}
                      </li>
                    ))}
                  </List>
                )}
              </PagedBox>
            )}
          </QueryState>
        </Card>
        <Card
          title={shown ? `Configuration of v${shown}` : 'Configuration'}
          actions={
            config.data ? (
              <Short max={36} mono className="text-xs text-muted">
                {config.data.image}
              </Short>
            ) : null
          }
        >
          {config.data && p.deployed_version && config.data.version !== p.deployed_version ? (
            <DiffView pipeline={p} from={p.deployed_version} to={config.data.version} />
          ) : null}
          {!p.runtime ? <Meta as="p">The configuration is generated once the pipeline has a runtime. Choose one in its settings.</Meta> : null}
          {p.runtime ? (
            <QueryState query={config} doing="Couldn’t load the configuration">
              {(c) => (
                <>
                  <CodeBlock code={c.properties} label="Compiled configuration" dark={false} />
                  <Meta as="p" className="wrap-anywhere">
                    Injected at runtime from encrypted storage: {c.secret_env.join(', ') || 'nothing'}
                  </Meta>
                </>
              )}
            </QueryState>
          ) : null}
        </Card>
      </Split>
      <ActivityCard pipeline={p} />
      <DeployConfirm pipeline={p} version={deploying} onClose={() => setDeploying(null)} />
      <SaveTemplateDialog pipeline={p} open={dialog === 'template'} onOpenChange={(o) => setDialog(o ? 'template' : null)} />
      <ExportDialog pipeline={p} open={dialog === 'yaml'} onOpenChange={(o) => setDialog(o ? 'yaml' : null)} />
      <PromoteDialog pipeline={p} open={dialog === 'promote'} onOpenChange={(o) => setDialog(o ? 'promote' : null)} />
    </>
  );
}

/** Deploy or roll back: says that the connector restarts, what changes, and whether it needs approval. */
function DeployConfirm({ pipeline: p, version, onClose }: { pipeline: Pipeline; version: number | null; onClose: () => void }) {
  const { project } = useProject();
  const deploy = useDeploy(project.id);
  const policies = usePolicies(project.id);
  const diff = useDiff(project.id, p.id, p.deployed_version ?? undefined, version ?? undefined);
  const rollback = version !== null && version < (p.deployed_version ?? 0);
  const notes = ((diff.data?.change as { notes?: string[] } | undefined)?.notes ?? []).slice(0, 6);
  return (
    <Confirm
      open={version !== null}
      onOpenChange={(o) => {
        if (!o) {
          deploy.reset();
          onClose();
        }
      }}
      tone="primary"
      title={rollback ? `Roll back to v${version}?` : `Deploy v${version}?`}
      effects={[
        `The connector restarts with v${version} and resumes from its stored position; consumers see a short pause.`,
        ...(notes.length
          ? notes.map((n) => `Changes from v${p.deployed_version}: ${n}`)
          : [diff.isPending ? 'Comparing the versions…' : `No configuration changes from v${p.deployed_version}.`]),
        ...settingsChangeLines(diff.data?.settings_change),
        ...(policies.data?.require_approval ? ['This project needs approval: the deploy waits until someone else with approval rights approves it.'] : []),
      ]}
      confirmLabel={rollback ? `Roll back to v${version}` : `Deploy v${version}`}
      pending={deploy.isPending}
      error={deploy.error}
      doing={rollback ? 'Couldn’t roll back' : 'Couldn’t deploy'}
      onConfirm={() =>
        version !== null &&
        deploy.mutate(
          { id: p.id, version },
          {
            onSuccess: (r) => {
              toast.success(r.pending_change ? `Sent v${version} for approval` : rollback ? `Rolling back to v${version}` : `Deploying v${version}`);
              onClose();
            },
          },
        )
      }
    />
  );
}

export function DiffView({ pipeline: p, from, to }: { pipeline: Pipeline; from: number; to: number }) {
  const { project } = useProject();
  const diff = useDiff(project.id, p.id, from, to);
  if (from === to) return null;
  return (
    <QueryState query={diff} doing="Couldn’t compare the versions" lines={3}>
      {(d) => {
        const notes = (d.change as { notes?: string[] }).notes ?? [];
        return (
          <div className="flex min-w-0 flex-col gap-2">
            <strong className="text-sm font-semibold">
              Changes from v{from} to v{to}
            </strong>
            {notes.length ? (
              <ul className="m-0 list-disc pl-5 text-sm text-ink-2 wrap-break-word">
                {notes.map((n) => (
                  <li key={n}>{n}</li>
                ))}
              </ul>
            ) : null}
            {d.settings_change ? <SettingsChangeList change={d.settings_change} /> : null}
            <pre
              aria-label="Version diff"
              tabIndex={0}
              data-scroll-x
              className="relative m-0 max-h-80 overflow-auto rounded-md border border-line bg-subtle p-3 font-mono text-xs leading-relaxed"
            >
              {d.lines.slice(2).map((l, i) => (
                <div
                  key={i}
                  className={clsx(
                    'w-max min-w-full',
                    l.startsWith('+') ? 'bg-ok-soft text-ok' : l.startsWith('-') ? 'bg-bad-soft text-bad' : l.startsWith('@@') ? 'text-muted' : '',
                  )}
                >
                  {l || ' '}
                </div>
              ))}
            </pre>
          </div>
        );
      }}
    </QueryState>
  );
}

function ActivityCard({ pipeline: p }: { pipeline: Pipeline }) {
  const { project } = useProject();
  const activity = useActivity(project.id, p.id);
  return (
    <Card title="Activity" pad={false}>
      <QueryState query={activity.query} doing="Couldn’t load the activity">
        {() =>
          activity.items.length === 0 ? (
            <Meta as="p" className="p-card">
              Nothing has happened yet.
            </Meta>
          ) : (
            <>
              <div role="region" aria-label="Activity, scrolls" tabIndex={0} className={scrollBox('card')}>
                <List label="Activity">
                  {activity.items.map((e) => (
                    <ListRow key={e.id}>
                      <time className="w-40 shrink-0 font-mono text-xs text-muted" dateTime={e.occurred_at}>
                        {formatDateTime(e.occurred_at)}
                      </time>
                      <span className="min-w-0 flex-[1_1_240px] text-base wrap-break-word">{e.summary}</span>
                      <Short max={36} className="text-xs text-muted">
                        {e.actor_label}
                      </Short>
                    </ListRow>
                  ))}
                </List>
              </div>
              <CompactPager total={activity.items.length} page={activity.page} hasOlder={activity.hasOlder} onPage={activity.goTo} noun="events" />
            </>
          )
        }
      </QueryState>
    </Card>
  );
}

function SaveTemplateDialog({ pipeline: p, open, onOpenChange }: { pipeline: Pipeline; open: boolean; onOpenChange: (open: boolean) => void }) {
  const { project } = useProject();
  const create = useCreateTemplate(project.id);
  const templates = useTemplates(project.id);
  const [title, setTitle] = useState('');
  const [description, setDescription] = useState('');
  const taken = (templates.data ?? []).map((t) => t.name);
  const missing = !title.trim()
    ? 'Give the template a title.'
    : taken.includes(slugify(title))
      ? 'A template with this name exists. Choose another title.'
      : null;
  return (
    <Modal
      open={open}
      onOpenChange={(o) => {
        onOpenChange(o);
        if (!o) {
          setTitle('');
          setDescription('');
          create.reset();
        }
      }}
      dirty={Boolean(title || description)}
      title="Save as template"
      description="Copies the destination, runtime, format, transforms, size and snapshot settings of the latest version. It leaves out the tables, column rules, message keys and steps limited to some tables."
      why={missing}
      footer={
        <>
          <Button onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button
            variant="primary"
            disabled={Boolean(missing)}
            loading={create.isPending}
            onClick={() =>
              create.mutate(
                { name: slugify(title).slice(0, 63), title: title.trim(), description, from_pipeline_id: p.id },
                {
                  onSuccess: (t) => {
                    toast.success(`Published template “${t.title}”`);
                    onOpenChange(false);
                  },
                },
              )
            }
          >
            Publish template
          </Button>
        </>
      }
    >
      <Field label="Title" hint={title ? `Saved as ${slugify(title).slice(0, 63) || '…'}` : undefined}>
        {(id) => <Input id={id} value={title} placeholder="Orders → Kafka, masked" onChange={(e) => setTitle(e.target.value)} />}
      </Field>
      <Field label="Description" optional>
        {(id) => <Input id={id} value={description} placeholder="When to use it" onChange={(e) => setDescription(e.target.value)} />}
      </Field>
      <InlineError error={create.error} doing="Couldn’t publish the template" />
    </Modal>
  );
}

function ExportDialog({ pipeline: p, open, onOpenChange }: { pipeline: Pipeline; open: boolean; onOpenChange: (open: boolean) => void }) {
  const { project } = useProject();
  const manifest = useQuery({
    queryKey: ['manifest', project.id, p.id, p.current_version],
    queryFn: () =>
      unwrap(client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/manifest', { params: { path: { project_id: project.id, pipeline_id: p.id } } })),
    enabled: open,
  });
  const download = () => {
    const url = URL.createObjectURL(new Blob([manifest.data?.yaml ?? ''], { type: 'application/yaml' }));
    const a = Object.assign(document.createElement('a'), { href: url, download: `${p.name}.yaml` });
    a.click();
    URL.revokeObjectURL(url);
  };
  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      size="lg"
      title={`${p.name}.yaml`}
      description="The latest version as config-as-code. Keep it in Git and apply it with `bezisync remote apply -f` or Import YAML. Secrets are never included."
      footer={
        <Button variant="primary" disabled={!manifest.data} onClick={download}>
          Download
        </Button>
      }
    >
      <QueryState query={manifest} doing="Couldn’t export the pipeline">
        {(m) => <CodeBlock code={m.yaml} label="Pipeline manifest" />}
      </QueryState>
    </Modal>
  );
}

function PromoteDialog({ pipeline: p, open, onOpenChange }: { pipeline: Pipeline; open: boolean; onOpenChange: (open: boolean) => void }) {
  const { project, me, can } = useProject();
  const targets = me.projects.filter((x) => x.id !== project.id && x.permissions.includes('pipeline.edit'));
  const [targetId, setTargetId] = useState('');
  const target = targets.find((t) => t.id === targetId);
  const connections = useConnections(targetId);
  const runtimes = useRuntimes(targetId);
  const [sourceId, setSourceId] = useState('');
  const [sinkId, setSinkId] = useState('');
  const [runtimeId, setRuntimeId] = useState('');
  const [deploy, setDeploy] = useState(false);
  // When the target has no matching source or destination, offer to copy this pipeline's own.
  const own = useConnections(project.id);
  // Copying needs connection.manage here and there; Promote itself only needs to see the pipeline.
  const copyTargets = useCopyTargets();
  const copyable = can('connection.manage') && copyTargets.some((t) => t.id === targetId);
  const [copying, setCopying] = useState<Connection[] | null>(null);
  const ownSource = own.data?.find((c) => c.id === p.source.id);
  const ownSink = p.sink ? own.data?.find((c) => c.id === p.sink?.id) : undefined;
  const promote = useMutation({
    mutationFn: (dryRun: boolean) =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/promote', {
          params: { path: { project_id: project.id, pipeline_id: p.id } },
          body: {
            target_project_id: targetId,
            source_id: sourceId,
            sink_id: sinkId || null,
            runtime_id: runtimeId || null,
            dry_run: dryRun,
            deploy: !dryRun && deploy,
          },
        }),
      ),
  });
  const [preview, setPreview] = useState<Schemas['ApplyResultRead'] | null>(null);
  const sources = (connections.data ?? []).filter((c) => c.kind === 'source' && c.type === p.source.type);
  const sinks = (connections.data ?? []).filter((c) => c.kind === 'sink' && (!p.sink || c.type === p.sink.type));
  const done = promote.data && promote.variables === false ? promote.data : null;
  const reset = () => {
    setPreview(null);
    promote.reset();
  };
  const missing = !targetId ? 'Choose the project to promote to.' : !sourceId ? 'Choose the source there.' : !preview ? 'Preview the result first.' : null;
  return (
    <Modal
      open={open}
      onOpenChange={(o) => {
        onOpenChange(o);
        if (!o) {
          reset();
          setTargetId('');
          setSourceId('');
          setSinkId('');
          setRuntimeId('');
        }
      }}
      size="lg"
      title={`Promote ${p.name}`}
      description="Copies the latest version into another project as a normal new version there: its policies and approvals apply. Map the connections to that project’s own."
      why={done ? null : missing}
      footer={
        done ? (
          <Button variant="primary" onClick={() => onOpenChange(false)}>
            Done
          </Button>
        ) : (
          <>
            <Button
              disabled={!targetId || !sourceId}
              loading={promote.isPending && promote.variables === true}
              onClick={() => promote.mutate(true, { onSuccess: setPreview })}
            >
              Preview
            </Button>
            <Button
              variant="primary"
              disabled={!preview}
              loading={promote.isPending && promote.variables === false}
              onClick={() => promote.mutate(false, { onSuccess: () => toast.success(`Promoted to ${target?.name ?? 'project'}`) })}
            >
              Promote
            </Button>
          </>
        )
      }
    >
      {targets.length === 0 ? <Notice tone="info">You can edit pipelines in no other project.</Notice> : null}
      {!done ? (
        <div className="grid grid-cols-[repeat(auto-fill,minmax(min(100%,260px),1fr))] gap-4">
          <Field label="To project">
            {(id) => (
              <Select
                id={id}
                value={targetId}
                onChange={(v) => {
                  setTargetId(v);
                  setSourceId('');
                  setSinkId('');
                  setRuntimeId('');
                  reset();
                }}
                options={targets.map((t) => ({ value: t.id, label: t.name }))}
              />
            )}
          </Field>
          <Field
            label={`Source there (${TYPE_LABEL[p.source.type] ?? p.source.type})`}
            hint={targetId && connections.data && !sources.length ? 'That project has no source of this type.' : undefined}
          >
            {(id) => (
              <Select
                id={id}
                value={sourceId}
                disabled={!targetId}
                onChange={(v) => {
                  setSourceId(v);
                  reset();
                }}
                options={sources.map((c) => ({ value: c.id, label: c.name }))}
              />
            )}
          </Field>
          <Field label="Destination there" optional>
            {(id) => (
              <Select
                id={id}
                value={sinkId}
                disabled={!targetId}
                placeholder="Same name, or choose later"
                onChange={(v) => {
                  setSinkId(v);
                  reset();
                }}
                options={[{ value: '', label: 'Same name, or choose later' }, ...sinks.map((c) => ({ value: c.id, label: c.name }))]}
              />
            )}
          </Field>
          <Field label="Runtime there" optional>
            {(id) => (
              <Select
                id={id}
                value={runtimeId}
                disabled={!targetId}
                placeholder="Same name, or choose later"
                onChange={(v) => {
                  setRuntimeId(v);
                  reset();
                }}
                options={[{ value: '', label: 'Same name, or choose later' }, ...(runtimes.data ?? []).map((r) => ({ value: r.id, label: r.name }))]}
              />
            )}
          </Field>
          {targetId && connections.data && copyable && ((!sources.length && ownSource) || (p.sink && !sinks.length && ownSink)) ? (
            <div className="col-span-full flex flex-wrap items-center gap-3 rounded-md border border-dashed border-line-strong bg-subtle px-3 py-2.5">
              <Meta className="min-w-0 flex-[1_1_240px]">
                {target?.name} has no {!sources.length ? `${TYPE_LABEL[p.source.type] ?? p.source.type} source` : `${TYPE_LABEL[p.sink?.type ?? ''] ?? p.sink?.type} destination`} yet.
              </Meta>
              <Button
                size="sm"
                variant="primary"
                onClick={() =>
                  setCopying([...(!sources.length && ownSource ? [ownSource] : []), ...(p.sink && !sinks.length && ownSink ? [ownSink] : [])])
                }
              >
                Copy {[!sources.length && ownSource ? ownSource.name : null, p.sink && !sinks.length && ownSink ? ownSink.name : null].filter(Boolean).join(' and ')} to {target?.name}…
              </Button>
            </div>
          ) : null}
          {(() => {
            const picked = connections.data?.find((c) => c.id === sourceId);
            return picked?.missing_secrets.length ? (
              <div className="col-span-full">
                <Notice tone="warn">
                  {picked.name}: {missingLabel(picked.missing_secrets).toLowerCase()} in {target?.name}. Promote saves the pipeline there, but it can’t deploy until someone enters it.
                </Notice>
              </div>
            ) : null;
          })()}
          {target?.permissions.includes('pipeline.deploy') ? (
            <div className="col-span-full">
              <Check checked={deploy} onChange={setDeploy} label="Deploy there (or request approval)" />
            </div>
          ) : null}
        </div>
      ) : null}
      <InlineError error={promote.error} doing="Couldn’t promote the pipeline" />
      {promote.isPending && promote.variables === true ? <Skeleton lines={3} /> : null}
      {done ? <ApplyResults results={[done]} /> : preview ? <ApplyResults results={[preview]} /> : null}
      <CopyDialog
        connections={copying ?? []}
        open={copying !== null}
        initialTarget={targetId}
        onOpenChange={(o) => !o && setCopying(null)}
        onCopied={(plan) => {
          for (const it of plan.items) {
            if (it.id && it.source_id === p.source.id) setSourceId(it.id);
            if (it.id && p.sink && it.source_id === p.sink.id) setSinkId(it.id);
          }
          reset();
        }}
      />
    </Modal>
  );
}
