import { captureOf, sinkCapsOf, TYPE_LABEL, useConnectorTypes, useDiscovery } from '@/lib/connections';
import { cleanValues, type FormValues, type JsonSchema } from '@/components/SchemaForm';
import { useEffect, useImperativeHandle, useMemo, useRef, useState, type Ref } from 'react';
import { Link, useNavigate } from '@tanstack/react-router';
import { Check as CheckIcon, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button, ButtonLink } from '@/components/ui/button';
import { InlineError, Notice, Pill, Skeleton } from '@/components/ui/feedback';
import { Field, RadioCards, SlugInput, slugError, Stepper, Switch, Textarea } from '@/components/ui/form';
import { Card, KeyValue } from '@/components/ui/layout';
import { CodeBlock, Modal } from '@/components/ui/overlays';
import { Select, TimeRange } from '@/components/ui/select';
import { Meta, plural } from '@/components/ui/text';
import { CheckList } from '@/components/CheckList';
import { useRuntimes } from '@/features/settings/RuntimesSection';
import { allows, sizeAllowed, usePolicies } from '@/lib/policies';
import { slugify } from '@/lib/format';
import { useDeploy, usePipelines, usePreflight, type Pipeline, type Preflight } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { flinkChoices, flinkSpecBlock, useEngines, type FlinkChoices } from '@/lib/engines';
import { OutputImpact, StepsSummary } from '../transforms/Impact';
import { deriveSinkOptions, fullSpec, optionsSchemaFor, parseAdvanced, RESOURCES, SNAPSHOT_MODES, type FullSpec } from '../spec';
import { useSaveSpec, type SaveBody, type StepHandle } from '../useSaveSpec';
import { FlinkRunSettings } from './FlinkSettings';
import { OwnerPicker } from './OwnerPicker';
import { personalSummary } from './personal';
import { StepHeader } from './shared';

/** Changes save on their own this long after the last edit; the final checks then re-run. */
const AUTOSAVE_MS = 800;

/**
 * Step 5: name, owner, runtime and safety settings, then the final checks and Deploy. Settings
 * save as you change them (into this change's draft version, so autosave never piles up versions)
 * and the checks re-run on their own, so there is no separate Save.
 */
function personalLine({ masked, removed, kept }: { masked: number; removed: number; kept: number }): string {
  const parts = [masked && `${masked} masked`, removed && `${removed} removed`, kept && `${kept} kept as is`].filter(Boolean);
  return parts.length ? parts.join(' · ') : 'No column masked or removed';
}

/** A Flink CDC size in a line: its TaskManagers, readers and memory in all. */
export function flinkSizeHelp(size: { taskmanagers: number; parallelism: number; total_memory_mib: number } | undefined): string {
  if (!size) return '';
  const gib = Math.round((size.total_memory_mib / 1024) * 10) / 10;
  return `A JobManager and ${size.taskmanagers} TaskManager${size.taskmanagers === 1 ? '' : 's'} · ${size.parallelism} readers · ${gib} GiB in all`;
}

export function ReviewStep({ pipeline, handle, onBack }: { pipeline: Pipeline; handle: Ref<StepHandle>; onBack: () => void }) {
  const { project, can, expert } = useProject();
  const navigate = useNavigate();
  const runtimes = useRuntimes(project.id);
  const preflight = usePreflight(project.id);
  const deploy = useDeploy(project.id);
  const policies = usePolicies(project.id).data;
  const connectorTypes = useConnectorTypes(project.id).data;
  const sinkCaps = sinkCapsOf(connectorTypes, pipeline.sink?.type);
  const capture = captureOf(connectorTypes, pipeline.source.type);
  const sourceLabel = TYPE_LABEL[pipeline.source.type] ?? 'This source';
  const sizeRank = (v: string) => RESOURCES.findIndex((r) => r.value === v);
  const tooSmall = (v: string) => Boolean(sinkCaps.min_resources) && sizeRank(v) < sizeRank(sinkCaps.min_resources ?? '');
  const others = (usePipelines(project.id).data ?? []).filter((p) => p.id !== pipeline.id).map((p) => p.name);
  const spec = useMemo(() => fullSpec(pipeline.spec), [pipeline.spec]);
  const deployed = Boolean(pipeline.deployed_version);
  const flink = pipeline.engine === 'flink-cdc';
  const flinkEngine = useEngines().data?.find((e) => e.id === 'flink-cdc');
  const [fc, setFc] = useState<FlinkChoices>(flinkChoices(spec.flink_cdc));
  const [name, setName] = useState(pipeline.name);
  const [owner, setOwner] = useState(pipeline.owner);
  const [runtimeId, setRuntimeId] = useState<string | undefined>(pipeline.runtime?.id);
  const [mode, setMode] = useState(spec.snapshot.mode);
  const [resources, setResources] = useState(spec.policies.resources);
  const [latency, setLatency] = useState(spec.policies.latency_target_seconds);
  const [snapshotWindow, setSnapshotWindow] = useState<string | null>(spec.snapshot.window ?? null);
  const [canary, setCanary] = useState(spec.policies.delivery_canary);
  const [advanced, setAdvanced] = useState(
    Object.entries(spec.advanced)
      .map(([k, v]) => `${k}=${v}`)
      .join('\n'),
  );
  const [force, setForce] = useState(false);
  const [reason, setReason] = useState('');
  const { save, pending: saving, error: saveError } = useSaveSpec(pipeline);
  const adv = parseAdvanced(advanced);
  const nameErr = deployed ? null : !name.trim() ? 'Give the pipeline a name.' : slugError(name, others);

  const discovery = useDiscovery(project.id, pipeline.source.id);
  // Options the destination derives from the tables (Iceberg's partition keys) follow the
  // tables and keys chosen in earlier steps.
  const sinkSchema = optionsSchemaFor(sinkCaps.options_schema as JsonSchema | null, pipeline.engine);
  const sinkOptions = deriveSinkOptions(
    // A Flink CDC pipeline keeps only its engine's options (no recorded partitions).
    pipeline.engine === 'flink-cdc' && sinkSchema ? cleanValues(sinkSchema, spec.sink.options as FormValues) : spec.sink.options,
    sinkSchema,
    spec.source.tables.include,
    spec.keys ?? {},
    discovery.data?.tables,
  );
  const nextSpec: FullSpec = {
    ...spec,
    sink: { ...spec.sink, options: sinkOptions },
    snapshot: { ...spec.snapshot, mode, window: flink ? null : snapshotWindow || null },
    policies: { ...spec.policies, resources, latency_target_seconds: latency, delivery_canary: flink ? false : canary },
    advanced: flink ? {} : adv.values,
    ...(flink ? { flink_cdc: flinkSpecBlock(fc) } : {}),
  };
  const body: SaveBody = {
    ...(slugify(name) !== pipeline.name && !deployed ? { name: slugify(name) } : {}),
    owner,
    runtime_id: runtimeId,
    spec: nextSpec,
  };
  const dirty =
    (!deployed && slugify(name) !== pipeline.name) ||
    owner !== pipeline.owner ||
    runtimeId !== pipeline.runtime?.id ||
    JSON.stringify(nextSpec) !== JSON.stringify(spec);
  const invalid = nameErr ?? adv.error;

  // Save shortly after the last change; one save at a time, so versions never conflict.
  const latest = useRef({ body, dirty, invalid, saving });
  latest.current = { body, dirty, invalid, saving };
  const signature = JSON.stringify(body);
  useEffect(() => {
    if (!dirty || invalid || saving) return;
    const t = window.setTimeout(() => {
      const now = latest.current;
      if (now.dirty && !now.invalid && !now.saving) void save(now.body);
    }, AUTOSAVE_MS);
    return () => window.clearTimeout(t);
  }, [signature, saving]); // eslint-disable-line react-hooks/exhaustive-deps

  useImperativeHandle(handle, () => ({
    dirty: () => latest.current.dirty,
    commit: async () => (!latest.current.dirty ? true : latest.current.invalid ? false : save(latest.current.body)),
  }));

  // The final checks run against exactly what is saved: after each save.
  useEffect(() => {
    if (pipeline.runtime && pipeline.sink) preflight.mutate({ id: pipeline.id });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pipeline.version]);

  // With exactly one usable runtime there is nothing to choose.
  useEffect(() => {
    if (runtimeId) return;
    const usable = (runtimes.data ?? []).filter((r) => r.enabled && allows(policies?.allowed_runtime_ids, r.id) && (r.last_test?.ok ?? true));
    if (usable.length === 1) setRuntimeId(usable[0]!.id);
  }, [runtimes.data, runtimeId, policies]);

  function doDeploy(forceReason?: string) {
    deploy.mutate(
      { id: pipeline.id, forceReason },
      {
        onSuccess: (p) => {
          if (p.pending_change) toast.success(`Sent ${p.name} v${p.pending_change.version} for approval`);
          else toast.success(`Deploying ${p.name} v${p.deployed_version}`);
          void navigate({ to: '/p/$projectSlug/pipelines/$pipelineId', params: { projectSlug: project.slug, pipelineId: p.id } });
        },
      },
    );
  }

  const report: Preflight | undefined = preflight.data;
  const blocking = report?.sections.flatMap((s) => s.report.checks.filter((c) => c.status === 'fail').map((c) => ({ ...c, section: s.key }))) ?? [];
  const forceable = policies?.allow_force_deploy !== false && blocking.length > 0 && blocking.every((c) => c.section === 'source' || c.section === 'sink');
  const deployWhy = !can('pipeline.deploy')
    ? 'You can save this draft; someone with deploy permission deploys it.'
    : invalid
      ? 'Fix the highlighted settings first.'
      : dirty || saving
        ? 'Saving your changes…'
        : !pipeline.runtime
          ? 'Choose a runtime first.'
          : preflight.isPending
            ? 'Waiting for the final checks.'
            : !report?.ok
              ? 'The final checks must pass.'
              : null;
  const runtimeOptions = (runtimes.data ?? []).map((r) => {
    const allowed = allows(policies?.allowed_runtime_ids, r.id);
    const why = !allowed ? 'Not allowed by this project’s policy.' : !r.enabled ? 'Turned off by an Admin.' : null;
    return {
      value: r.id,
      label: r.name,
      description: `${r.kind}${r.last_test && !r.last_test.ok ? ' · not ready' : ''}`,
      disabled: Boolean(why) && r.id !== runtimeId,
      why,
    };
  });

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <StepHeader
        title={deployed ? 'Review and deploy the change' : 'Review and deploy'}
        description="Settings save as you change them. The final checks run against exactly this configuration."
      />
      <Card
        title="Settings"
        actions={
          <span className="flex items-center gap-1.5 text-xs text-muted" aria-live="polite">
            {saving ? (
              <>
                <Loader2 className="size-3.5 animate-spin" aria-hidden /> Saving…
              </>
            ) : saveError ? (
              <span className="text-bad">Not saved</span>
            ) : dirty ? (
              invalid ? (
                <span className="text-warn">Not saved: fix the highlighted field</span>
              ) : (
                'Saving soon…'
              )
            ) : (
              <>
                <CheckIcon className="size-3.5 text-ok" aria-hidden /> All changes saved
              </>
            )}
          </span>
        }
      >
        <InlineError error={saveError} doing="Couldn’t save your changes" />
        <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,280px),1fr))] gap-4">
          <Field label="Name" error={nameErr} hint={deployed ? 'Deployed pipelines keep their name.' : 'Used in URLs, YAML and the {pipeline} placeholder.'}>
            {(id) => <SlugInput id={id} value={name} onChange={setName} disabled={deployed} taken={others} />}
          </Field>
          <Field label="Owner" optional hint="The team or on-call alias shown on alerts.">
            {(id) => <OwnerPicker id={id} value={owner} onChange={setOwner} />}
          </Field>
          <Field label="Runtime" hint={runtimes.data?.length ? 'Where the connector runs.' : undefined}>
            {(id) =>
              runtimes.data?.length === 0 ? (
                <Notice tone="warn" title="No runtime yet">
                  {can('runtime.manage') ? (
                    <Link
                      to="/p/$projectSlug/settings/$section"
                      params={{ projectSlug: project.slug, section: 'runtimes' }}
                      className="font-semibold underline"
                    >
                      Add one in Settings
                    </Link>
                  ) : (
                    'Ask an Admin to add one.'
                  )}
                </Notice>
              ) : (
                <Select id={id} value={runtimeId ?? ''} onChange={(v) => setRuntimeId(v || undefined)} options={runtimeOptions} />
              )
            }
          </Field>
          {expert ? (
            <Field label="Latency target" hint="Above this, the pipeline is shown as Lagging.">
              {(id) => <Stepper id={id} value={latency} onChange={setLatency} min={1} max={3600} unit="seconds" />}
            </Field>
          ) : null}
        </div>
        <Field label="Size" group>
          {() => (
            <RadioCards
              label="Size"
              value={resources}
              onChange={(v) => setResources(v as FullSpec['policies']['resources'])}
              options={RESOURCES.map((r) => ({
                value: r.value,
                label: r.title,
                description: flink ? flinkSizeHelp(flinkEngine?.sizes?.[r.value]) : r.help,
                disabled: (!sizeAllowed(policies, r.value) || (!flink && tooSmall(r.value)) || (flink && deployed)) && r.value !== resources,
                why: flink && deployed
                  ? 'A Flink CDC pipeline keeps the readers it was deployed with: Start over to change its size.'
                  : tooSmall(r.value)
                  ? `${TYPE_LABEL[pipeline.sink?.type ?? ''] ?? 'This destination'} needs at least ${sinkCaps.min_resources}.`
                  : `This project allows up to ${policies?.max_resources ?? 'a smaller size'}.`,
              }))}
            />
          )}
        </Field>
        {capture.snapshot_modes.length === 0 ? (
          <Notice tone="info" title="Existing rows">
            {sourceLabel} can’t copy the rows a table already has: the pipeline streams the changes made from the moment it first starts.
          </Notice>
        ) : (
          <Field label="Existing rows" group>
            {() => (
              <RadioCards
                label="Existing rows"
                columns={2}
                value={mode}
                onChange={setMode}
                options={SNAPSHOT_MODES.filter(
                  (m) =>
                    m.value === mode ||
                    (capture.snapshot_modes.includes(m.value) &&
                      (expert || ['initial', 'no_data'].includes(m.value)) &&
                      // A Flink CDC pipeline resumes from its saved position: no copy on every start.
                      !(flink && m.value === 'always')),
                ).map((m) => {
                  const offered = capture.snapshot_modes.includes(m.value);
                  return {
                    value: m.value,
                    label: m.title,
                    // Flink CDC pipelines don't need Debezium's terms.
                    description: flink ? m.help.replace(/\s*Debezium:.*$/, '') : m.help,
                    disabled: (!offered || !allows(policies?.allowed_snapshot_modes, m.value)) && m.value !== mode,
                    why: offered ? 'Not allowed in this project.' : `${sourceLabel} doesn’t offer this.`,
                  };
                })}
              />
            )}
          </Field>
        )}
        {flink ? (
          <Field label="Flink CDC" group>
            {() => (
              <FlinkRunSettings
                value={fc}
                onChange={setFc}
                sinkType={pipeline.sink?.type}
                expert={expert}
                catalogue={flinkEngine?.settings ?? []}
                deployed={deployed}
              />
            )}
          </Field>
        ) : null}
        {flink ? null : (
          <Switch
            checked={canary}
            onChange={setCanary}
            label="Verify delivery end to end"
            disabled={!capture.canary && !canary}
            why={`${sourceLabel} sources don’t support the delivery canary.`}
            description="Every few minutes BeziSync writes a tiny canary row to the source and checks it arrives at the destination, and alerts if it doesn’t. Needs a destination BeziSync can read back (not HTTP or SQS); adds one small canary topic, stream or table."
          />
        )}
        {flink ? null : (
          <TimeRange
            value={snapshotWindow}
            onChange={setSnapshotWindow}
            label="Only re-copy rows inside a maintenance window"
            description="Backfills and re-syncs wait for this daily window (UTC) to spare the source. The first copy is not delayed."
          />
        )}
        {expert && !flink ? (
          <Field label="Advanced Debezium properties" optional error={adv.error} hint="One key=value per line. BeziSync-managed keys are rejected.">
            {(id) => (
              <Textarea
                id={id}
                mono
                spellCheck={false}
                value={advanced}
                onChange={(e) => setAdvanced(e.target.value)}
                placeholder="debezium.source.poll.interval.ms=200"
              />
            )}
          </Field>
        ) : null}
      </Card>

      <Card
        title={flink ? "What reaches the destination" : "Events"}
        actions={
          <ButtonLink
            size="sm"
            variant="ghost"
            to="/p/$projectSlug/pipelines/new"
            params={{ projectSlug: project.slug }}
            search={{ draft: pipeline.id, step: 'transform' }}
          >
            Edit transforms
          </ButtonLink>
        }
      >
        <StepsSummary spec={spec} flink={flink} />
        {deployed ? <OutputImpact pipeline={pipeline} spec={spec} /> : null}
      </Card>

      <Card title="Final checks">
        <KeyValue
          items={[
            ['From', `${pipeline.source.name} · ${plural(spec.source.tables.include.length, 'table')}`],
            ['To', `${pipeline.sink?.name ?? '—'} · ${flink ? 'tables' : spec.sink.format.type.toUpperCase()}`],
            ['Personal data', personalLine(personalSummary(spec.source.columns, spec.source.tables.include))],
          ]}
        />
        {!pipeline.runtime || !pipeline.sink ? (
          <Notice tone="warn">{!pipeline.sink ? 'Choose a destination to run the final checks.' : 'Choose a runtime to run the final checks.'}</Notice>
        ) : preflight.isPending ? (
          <div className="flex flex-col gap-2">
            <Meta>Running the final checks…</Meta>
            <Skeleton lines={4} />
          </div>
        ) : preflight.error ? (
          <InlineError error={preflight.error} doing="Couldn’t run the final checks" onRetry={() => preflight.mutate({ id: pipeline.id })} />
        ) : report ? (
          <>
            <div className="flex flex-wrap items-center gap-2">
              <Pill tone={report.ok ? 'ok' : 'bad'}>{report.summary}</Pill>
              <Button size="sm" variant="ghost" onClick={() => preflight.mutate({ id: pipeline.id })}>
                Re-run
              </Button>
            </div>
            {report.change.notes?.length ? (
              <Notice tone="info" title="What deploying will do">
                <ul className="m-0 list-disc pl-5">
                  {(report.change.notes ?? []).map((n) => (
                    <li key={n}>{n}</li>
                  ))}
                </ul>
              </Notice>
            ) : null}
            {report.sections.map((s) => (
              <div key={s.key} className="rounded-lg border border-line p-card">
                <CheckList report={s.report} expert={expert} subject={s.title} title={s.title} />
              </div>
            ))}
            {report.properties ? (
              <details className="min-w-0">
                <summary className="cursor-pointer text-sm font-semibold">Generated configuration ({flink ? 'pipeline.yaml' : 'application.properties'})</summary>
                <Meta as="p" className="my-2">
                  Secrets appear only as ${'{'}…{'}'} references and are injected at runtime.
                </Meta>
                <CodeBlock code={report.properties} label="Generated configuration" dark={false} />
              </details>
            ) : null}
          </>
        ) : null}
      </Card>

      <InlineError error={deploy.error} doing="Couldn’t deploy" />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <Button onClick={onBack}>← Back</Button>
        <div className="flex flex-wrap items-center justify-end gap-3">
          {can('pipeline.deploy') && pipeline.approval_required ? <Meta>Deploys here need a second person’s approval.</Meta> : null}
          {deployWhy && !(report && !report.ok && forceable) ? <Meta>{deployWhy}</Meta> : null}
          {can('pipeline.deploy') && report && !report.ok && forceable && !dirty ? (
            <Button variant="danger" onClick={() => setForce(true)}>
              Deploy anyway…
            </Button>
          ) : null}
          <Button variant="primary" disabled={Boolean(deployWhy)} why={deployWhy} loading={deploy.isPending} onClick={() => doDeploy()}>
            {pipeline.approval_required ? 'Request approval' : deployed ? `Deploy v${pipeline.current_version}` : 'Deploy'}
          </Button>
        </div>
      </div>

      <Modal
        open={force}
        onOpenChange={setForce}
        dirty={Boolean(reason.trim())}
        title="Deploy despite failed checks?"
        description="The pipeline may fail to start or lose data. The skipped checks and your reason are recorded in the audit log."
        why={reason.trim().length < 10 ? 'Give a reason of at least 10 characters.' : null}
        footer={
          <>
            <Button onClick={() => setForce(false)}>Cancel</Button>
            <Button variant="danger" disabled={reason.trim().length < 10} loading={deploy.isPending} onClick={() => doDeploy(reason.trim())}>
              Deploy anyway
            </Button>
          </>
        }
      >
        <ul className="m-0 list-disc pl-5 text-base wrap-break-word">
          {blocking.map((c) => (
            <li key={c.code}>
              <strong>{c.title}:</strong> {c.message}
            </li>
          ))}
        </ul>
        <Field label="Reason">
          {(id) => <Textarea id={id} value={reason} onChange={(e) => setReason(e.target.value)} placeholder="Why it is safe to skip these checks" />}
        </Field>
        <InlineError error={deploy.error} doing="Couldn’t deploy" />
      </Modal>
    </div>
  );
}
