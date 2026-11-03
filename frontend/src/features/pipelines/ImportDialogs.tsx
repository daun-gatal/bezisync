import { useState } from 'react';
import { useNavigate } from '@tanstack/react-router';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { InlineError, Notice, Pill } from '@/components/ui/feedback';
import { Check, Field, SlugInput, slugError, Textarea } from '@/components/ui/form';
import { Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import { client, unwrap, type Schemas } from '@/lib/api';
import { TYPE_LABEL, useConnections } from '@/lib/connections';
import { slugify } from '@/lib/format';
import { usePipelines } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { useRuntimes } from '@/features/settings/RuntimesSection';

type ApplyResult = Schemas['ApplyResultRead'];

export function DiffLines({ lines }: { lines: string[] }) {
  if (!lines.length) return null;
  return (
    <pre
      aria-label="Changes"
      data-scroll-x
      tabIndex={0}
      className="relative m-0 max-h-72 overflow-auto rounded-md border border-line bg-subtle p-3 font-mono text-xs leading-relaxed"
    >
      {lines.map((l, i) => (
        <div
          key={i}
          className={`w-max min-w-full ${l.startsWith('+') && !l.startsWith('+++') ? 'text-ok' : l.startsWith('-') && !l.startsWith('---') ? 'text-bad' : 'text-muted'}`}
        >
          {l}
        </div>
      ))}
    </pre>
  );
}

const ACTION: Record<string, { label: string; tone: 'ok' | 'info' | 'grey' }> = {
  created: { label: 'created', tone: 'ok' },
  updated: { label: 'updated', tone: 'ok' },
  unchanged: { label: 'unchanged', tone: 'grey' },
  would_create: { label: 'will be created', tone: 'info' },
  would_update: { label: 'will change', tone: 'info' },
};

export function ApplyResults({ results }: { results: ApplyResult[] }) {
  return (
    <div className="flex flex-col gap-3">
      {results.map((r) => (
        <div key={r.name} className="flex min-w-0 flex-col gap-2">
          <div className="flex min-w-0 flex-wrap items-center gap-2 text-base">
            <strong className="min-w-0 wrap-anywhere">{r.name}</strong>
            <Pill tone={ACTION[r.action]?.tone ?? 'grey'}>{ACTION[r.action]?.label ?? r.action}</Pill>
            {r.version ? <Meta>v{r.version}</Meta> : null}
            {r.deploy === 'waiting_for_approval' ? <Pill tone="warn">waiting for approval</Pill> : r.deploy ? <Pill tone="info">deploying</Pill> : null}
          </div>
          {r.notes.length ? (
            <ul className="m-0 list-disc pl-5 text-sm text-ink-2 wrap-break-word">
              {r.notes.map((n) => (
                <li key={n}>{n}</li>
              ))}
            </ul>
          ) : null}
          <DiffLines lines={r.diff} />
        </div>
      ))}
    </div>
  );
}

/** Paste manifests (YAML), preview the changes, then apply. */
export function ApplyYamlDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  const { project, can } = useProject();
  const qc = useQueryClient();
  const [text, setText] = useState('');
  const [deploy, setDeploy] = useState(false);
  const [preview, setPreview] = useState<ApplyResult[] | null>(null);
  const run = useMutation({
    mutationFn: (dryRun: boolean) =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/pipelines:apply', {
          params: { path: { project_id: project.id } },
          body: { manifest: text, dry_run: dryRun, deploy: !dryRun && deploy },
        }),
      ),
  });
  const close = (o: boolean) => {
    onOpenChange(o);
    if (!o) {
      setPreview(null);
      run.reset();
    }
  };
  const applied = run.data && !run.variables ? run.data : null;
  return (
    <Modal
      open={open}
      onOpenChange={close}
      size="lg"
      dirty={Boolean(text.trim()) && !applied}
      why={!applied && !preview ? (text.trim() ? 'Preview the changes first.' : 'Paste a manifest to start.') : null}
      title="Apply pipeline YAML"
      description="Paste one or more `kind: Pipeline` documents (from Export, the CLI or Git). Pipelines are matched by name; connections and the runtime by name in this project."
      footer={
        applied ? (
          <Button variant="primary" onClick={() => close(false)}>
            Done
          </Button>
        ) : (
          <>
            <Button loading={run.isPending && run.variables === true} disabled={!text.trim()} onClick={() => run.mutate(true, { onSuccess: setPreview })}>
              Preview changes
            </Button>
            <Button
              variant="primary"
              disabled={!preview}
              loading={run.isPending && run.variables === false}
              onClick={() =>
                run.mutate(false, {
                  onSuccess: (r) => {
                    toast.success(`Applied ${r.length} pipeline${r.length === 1 ? '' : 's'}`);
                    void qc.invalidateQueries({ queryKey: ['pipelines', project.id] });
                  },
                })
              }
            >
              Apply
            </Button>
          </>
        )
      }
    >
      {!applied ? (
        <>
          <Field label="Manifest (YAML)" hint={preview ? undefined : 'Editing the manifest clears the preview.'}>
            {(id) => (
              <Textarea
                id={id}
                mono
                className="min-h-56"
                spellCheck={false}
                value={text}
                placeholder={
                  'apiVersion: bezisync.io/v1\nkind: Pipeline\nmetadata: {name: orders-cdc}\nspec:\n  source: {connection: shop-db, tables: {include: [shop.orders]}}\n  sink: {connection: events}'
                }
                onChange={(e) => {
                  setText(e.target.value);
                  setPreview(null);
                }}
              />
            )}
          </Field>
          {can('pipeline.deploy') ? <Check checked={deploy} onChange={setDeploy} label="Deploy changed pipelines (or request approval)" /> : null}
        </>
      ) : null}
      <InlineError error={run.error} doing={run.variables ? 'Couldn’t preview the manifest' : 'Couldn’t apply the manifest'} />
      {applied ? <ApplyResults results={applied} /> : preview ? <ApplyResults results={preview} /> : null}
    </Modal>
  );
}

/** Map an existing Debezium Server application.properties to a new draft pipeline. */
export function AdoptDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  const { project } = useProject();
  const navigate = useNavigate();
  const connections = useConnections(project.id);
  const taken = (usePipelines(project.id).data ?? []).map((p) => p.name);
  const runtimes = useRuntimes(project.id);
  const [text, setText] = useState('');
  const [name, setName] = useState('');
  const [sourceId, setSourceId] = useState('');
  const [sinkId, setSinkId] = useState('');
  const [runtimeId, setRuntimeId] = useState('');
  const report = useMutation({
    mutationFn: () =>
      unwrap(client.POST('/api/v1/projects/{project_id}/pipelines:adopt', { params: { path: { project_id: project.id } }, body: { properties: text } })),
    onSuccess: (r) => {
      const hint = r.source_hint as Record<string, unknown>;
      const match = (connections.data ?? []).find(
        (c) => c.kind === 'source' && c.type === r.source_type && (c.config as Record<string, unknown>).host === hint.host,
      );
      if (match) setSourceId(match.id);
      if (!name && hint.database) setName(`${String(hint.database)}-cdc`);
    },
  });
  const create = useMutation({
    mutationFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/pipelines', {
          params: { path: { project_id: project.id } },
          body: {
            name: slugify(name),
            description: 'Adopted from an existing Debezium Server.',
            owner: '',
            source_id: sourceId,
            sink_id: sinkId || null,
            runtime_id: runtimeId || null,
            spec: report.data!.spec as Schemas['PipelineSpec'],
            labels: { adopted: 'true' },
          },
        }),
      ),
    onSuccess: (p) => {
      toast.success(`Created draft ${p.name} — review it before deploying`);
      onOpenChange(false);
      void navigate({ to: '/p/$projectSlug/pipelines/new', params: { projectSlug: project.slug }, search: { draft: p.id, step: 'tables' } });
    },
  });
  const r = report.data;
  const missing = !r
    ? text.trim()
      ? null
      : 'Paste the properties to analyse them.'
    : !name
      ? 'Name the pipeline.'
      : (slugError(name, taken) ?? (!sourceId ? 'Choose the source connection.' : null));
  const sources = (connections.data ?? []).filter((c) => c.kind === 'source' && (!r?.source_type || c.type === r.source_type));
  const sinks = (connections.data ?? []).filter((c) => c.kind === 'sink' && (!r?.sink_type || c.type === r.sink_type));
  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      size="lg"
      dirty={Boolean(text.trim())}
      title="Adopt an existing Debezium Server"
      description="Paste its application.properties. BeziSync maps what it understands, tells you what it manages itself, and creates a draft for you to review."
      why={missing}
      footer={
        r ? (
          <Button variant="primary" disabled={Boolean(missing)} loading={create.isPending} onClick={() => create.mutate()}>
            Create draft pipeline
          </Button>
        ) : (
          <Button variant="primary" disabled={!text.trim()} loading={report.isPending} onClick={() => report.mutate()}>
            Analyse
          </Button>
        )
      }
    >
      {!r ? (
        <Field label="application.properties">
          {(id) => (
            <Textarea
              id={id}
              mono
              className="min-h-56"
              spellCheck={false}
              value={text}
              onChange={(e) => setText(e.target.value)}
              placeholder={
                'debezium.source.connector.class=io.debezium.connector.postgresql.PostgresConnector\ndebezium.source.table.include.list=public.orders\n…'
              }
            />
          )}
        </Field>
      ) : (
        <>
          <p className="m-0 text-base wrap-anywhere">
            Source: <strong>{r.source_type ? (TYPE_LABEL[r.source_type] ?? r.source_type) : 'unknown'}</strong>
            {Object.keys(r.source_hint).length ? (
              <span className="text-muted">
                {' '}
                (
                {Object.entries(r.source_hint)
                  .map(([k, v]) => `${k} ${String(v)}`)
                  .join(', ')}
                )
              </span>
            ) : null}
            {' · '}Destination: <strong>{r.sink_type ? (TYPE_LABEL[r.sink_type] ?? r.sink_type) : 'unknown'}</strong>
          </p>
          {r.warnings.length ? (
            <Notice tone="warn" title="Before you switch over">
              <ul className="m-0 list-disc pl-5">
                {r.warnings.map((w) => (
                  <li key={w}>{w}</li>
                ))}
              </ul>
            </Notice>
          ) : null}
          <div className="grid grid-cols-[repeat(auto-fill,minmax(min(100%,260px),1fr))] gap-2 text-sm text-muted">
            <div>
              <strong className="text-ink">Mapped ({r.mapped.length})</strong>: tables, snapshot, naming, format, transforms.
            </div>
            <div className="wrap-anywhere">
              <strong className="text-ink">Kept as advanced ({r.kept.length})</strong>
              {r.kept.length ? `: ${r.kept.join(', ')}` : ''}
            </div>
            <div>
              <strong className="text-ink">Managed by BeziSync ({r.managed.length})</strong>: connection, offsets, slot, history, secrets.
            </div>
            <div>
              <strong className="text-ink">Replaced by BeziSync settings ({r.replaced.length})</strong>
            </div>
          </div>
          <div className="grid grid-cols-[repeat(auto-fill,minmax(min(100%,260px),1fr))] gap-4">
            <Field label="Pipeline name">{(id) => <SlugInput id={id} value={name} onChange={setName} taken={taken} />}</Field>
            <Field label="Source connection" hint={sources.length ? undefined : 'Add a matching source under Connections first.'}>
              {(id) => <Select id={id} value={sourceId} onChange={setSourceId} options={sources.map((c) => ({ value: c.id, label: c.name }))} />}
            </Field>
            <Field label="Destination" optional>
              {(id) => (
                <Select
                  id={id}
                  value={sinkId}
                  onChange={setSinkId}
                  placeholder="Choose later"
                  options={[{ value: '', label: 'Choose later' }, ...sinks.map((c) => ({ value: c.id, label: c.name }))]}
                />
              )}
            </Field>
            <Field label="Runtime" optional>
              {(id) => (
                <Select
                  id={id}
                  value={runtimeId}
                  onChange={setRuntimeId}
                  placeholder="Choose later"
                  options={[{ value: '', label: 'Choose later' }, ...(runtimes.data ?? []).map((rt) => ({ value: rt.id, label: rt.name }))]}
                />
              )}
            </Field>
          </div>
        </>
      )}
      <InlineError error={report.error ?? create.error} doing={report.error ? 'Couldn’t analyse the properties' : 'Couldn’t create the draft'} />
    </Modal>
  );
}
