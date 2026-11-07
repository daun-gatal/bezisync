import { captureOf, sinkCapsOf, TYPE_LABEL, useConnectorTypes, useDiscovery } from '@/lib/connections';
import type { JsonSchema } from '@/components/SchemaForm';
import { useEffect, useImperativeHandle, useMemo, useRef, useState, type Ref } from 'react';
import { Link, useNavigate } from '@tanstack/react-router';
import { Check as CheckIcon, Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button, ButtonLink } from '@/components/ui/button';
import { InlineError, Notice, Pill, Skeleton } from '@/components/ui/feedback';
import { Field, RadioCards, SlugInput, slugError, Stepper, Switch, Textarea } from '@/components/ui/form';
import { RawProperties, type RawRow } from '@/components/RawProperties';
import { SettingsChangeList } from '@/components/SettingsChange';
import { freshRecommendations, Recommendations } from '@/components/Recommendations';
import { SettingsEditor } from '@/components/SettingsEditor';
import { Card, KeyValue } from '@/components/ui/layout';
import { CodeBlock, Modal } from '@/components/ui/overlays';
import { Select, TimeRange } from '@/components/ui/select';
import { Meta, plural } from '@/components/ui/text';
import { CheckList } from '@/components/CheckList';
import { useRuntimes } from '@/features/settings/RuntimesSection';
import { ApiError } from '@/lib/api';
import { allows, sizeAllowed, usePolicies } from '@/lib/policies';
import { canonicalSetting, jsonbSorted, rawErrors, tuningSettings, useClassify } from '@/lib/settings';
import { slugify } from '@/lib/format';
import { useDeploy, usePipelines, usePreflight, useSavedPositions, type Pipeline, type Preflight } from '@/lib/pipelines';
import { clusterFromPreset, manualError, manualErrors, streamingError, streamingErrors, usePackCluster } from '@/lib/cluster';
import { useProject } from '@/lib/session';
import { flinkChoices, flinkSpecBlock, useEngines, type FlinkChoices } from '@/lib/engines';
import { OutputImpact, StepsSummary, useShapeCtx } from '../transforms/Impact';
import { routeTargets } from '../transforms/model';
import { partitionByFor, partitionError, writtenByTable, type PartitionBy } from '../transforms/PartitionBy';
import { CUSTOM_CPUS, CUSTOM_MEMORIES, deriveSinkOptions, fullSpec, heapMiB, memoryMiB, optionsSchemaFor, PRESET_AMOUNTS, RESOURCES, SNAPSHOT_MODES, type FullSpec, flinkSinkOptions } from '../spec';
import { useSaveSpec, type SaveBody, type StepHandle } from '../useSaveSpec';
import { FlinkClusterEditor } from './FlinkClusterEditor';
import { FlinkRunSettings } from './FlinkSettings';
import { OwnerPicker } from './OwnerPicker';
import { personalSummary } from './personal';
import { BlockingSettings } from './BlockingSettings';
import { customErrors, focusReviewField, problemsSummary, rawLabel, reviewProblems, useAutosave, type ReviewProblem } from './review';
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
  const shapeCtx = useShapeCtx(pipeline);
  const [fc, setFc] = useState<FlinkChoices>(flinkChoices(spec.flink_cdc));
  const [name, setName] = useState(pipeline.name);
  const [owner, setOwner] = useState(pipeline.owner);
  const [runtimeId, setRuntimeId] = useState<string | undefined>(pipeline.runtime?.id);
  const [mode, setMode] = useState(spec.snapshot.mode);
  const [resources, setResources] = useState(spec.policies.resources);
  const [custom, setCustom] = useState(spec.policies.custom_resources ?? { cpu: '1', memory: '1536Mi' });
  const customAmount = { cpu: Number(custom.cpu), mem: memoryMiB(custom.memory) };
  const max = policies?.max_resources ? PRESET_AMOUNTS[policies.max_resources] : null;
  const minSize = sinkCaps.min_resources ? PRESET_AMOUNTS[sinkCaps.min_resources as keyof typeof PRESET_AMOUNTS] : null;
  // Each amount is checked on its own, so each error sits under its own field.
  const customErr = resources === 'custom' && !flink ? customErrors(customAmount, max, minSize, TYPE_LABEL[pipeline.sink?.type ?? ''] ?? 'This destination') : {};
  const [latency, setLatency] = useState(spec.policies.latency_target_seconds);
  const [snapshotWindow, setSnapshotWindow] = useState<string | null>(spec.snapshot.window ?? null);
  const [canary, setCanary] = useState(spec.policies.delivery_canary);
  const [rawRows, setRawRows] = useState<RawRow[]>(Object.entries(spec.advanced).map(([key, value]) => ({ key, value })));
  const [tuning, setTuning] = useState<Record<string, string>>(spec.tuning ?? {});
  const [placement, setPlacement] = useState(spec.placement ?? '');
  const [force, setForce] = useState(false);
  const [reason, setReason] = useState('');
  const { save, pending: saving, error: saveError, reset: resetSave } = useSaveSpec(pipeline);
  // The project's setting limits: hidden ones aren't offered, pinned ones are shown as set by
  // the project, and a maximum bounds the control.
  const limits = policies?.setting_limits ?? {};
  const tunable = tuningSettings(
    (useEngines().data ?? []).flatMap((e) => e.settings ?? []),
    pipeline.engine,
    pipeline.source.type,
    pipeline.sink?.type,
  ).filter((s) => !limits[s.key]?.hide);
  const pinned = Object.fromEntries(tunable.filter((s) => limits[s.key]?.pin).map((s) => [s.key, limits[s.key]!.pin!]));
  const caps = Object.fromEntries(
    tunable.filter((s) => s.type === 'int' && limits[s.key]?.max).map((s) => [s.key, Number(limits[s.key]!.max)]),
  );
  // Classified in both modes: Guided hides the rows but they still block saving.
  const verdicts = useClassify(project.id, pipeline.id, rawRows, rawRows.some((r) => r.key.trim() !== ''));
  // Compared with the stored spec (pipeline.spec), as the server compares with the current version.
  const raw = rawErrors(rawRows, verdicts, spec.advanced);
  // Stored maps come back in JSONB order: build them the same way, or the draft never matches.
  const advancedValues = jsonbSorted(Object.fromEntries(rawRows.filter((r) => r.key.trim()).map((r) => [r.key.trim(), r.value])));
  const useTypedSetting = (row: RawRow) => {
    const s = tunable.find((t) => t.key === row.key.trim());
    if (!s) return;
    setTuning({ ...tuning, [s.key]: canonicalSetting(s, row.value) ?? s.default });
    setRawRows(rawRows.filter((r) => r !== row));
  };
  const nameErr = deployed ? null : !name.trim() ? 'Give the pipeline a name.' : slugError(name, others);

  const discovery = useDiscovery(project.id, pipeline.source.id);
  // Partition by is edited in Transform (ADR 0007), which the step list can skip: said here too.
  // Preflight blocks the same cases on deploy, so this doesn't block saving.
  const partitionProblem =
    pipeline.sink?.type === 'iceberg'
      ? partitionError(
          partitionByFor((spec.sink.options.partition_by as PartitionBy | undefined) ?? {}, pipeline.engine),
          writtenByTable(spec.source.tables.include, discovery.data?.tables, spec, pipeline.engine, pipeline.source.type, spec.sink.options.write_mode),
          routeTargets(spec.transforms, spec.source.tables.include),
        )
      : null;
  // A running Flink CDC pipeline keeps its readers while it has a saved position to restore
  // (flink_readers_changed); after Start over they can change.
  const positions = useSavedPositions(project.id, pipeline.id, flink && deployed);
  const readersLocked = flink && deployed && (positions.data === undefined || positions.data.positions.some((s) => s.current));
  const clusterOn = flink && resources === 'custom' && fc.cluster !== undefined;
  const pack = usePackCluster(project.id, pipeline.id, fc.cluster, clusterOn && !(fc.cluster && (manualError(fc.cluster) ?? streamingError(fc.cluster))));
  const tableRows = useMemo(() => {
    const byName = new Map((discovery.data?.tables ?? []).map((t) => [`${t.schema}.${t.name}`, t.rows_estimate ?? null]));
    const rows = spec.source.tables.include.map((t) => byName.get(t) ?? null);
    return rows.length && rows.every((r) => r !== null) ? rows.reduce<number>((n, r) => n + (r ?? 0), 0) : null;
  }, [discovery.data, spec.source.tables.include]);
  // Options the destination derives from the tables (Iceberg's partition keys) follow the
  // tables and keys chosen in earlier steps.
  const sinkSchema = optionsSchemaFor(sinkCaps.options_schema as JsonSchema | null, pipeline.engine);
  const sinkOptions = deriveSinkOptions(
    // A Flink CDC pipeline keeps only its engine's options (no recorded partitions).
    pipeline.engine === 'flink-cdc' && sinkSchema ? flinkSinkOptions(sinkSchema, spec.sink.options) : spec.sink.options,
    sinkSchema,
    spec.source.tables.include,
    spec.keys ?? {},
    discovery.data?.tables,
  );
  const nextSpec: FullSpec = {
    ...spec,
    sink: { ...spec.sink, options: sinkOptions },
    snapshot: { ...spec.snapshot, mode, window: flink ? null : snapshotWindow || null },
    policies: {
      ...withoutCustom(spec.policies),
      resources,
      latency_target_seconds: latency,
      delivery_canary: flink ? false : canary,
      ...(resources === 'custom' && !flink ? { custom_resources: custom } : {}),
    },
    advanced: advancedValues,
    // A custom cluster is kept only while the size is custom (the server refuses it otherwise).
    ...(flink ? { flink_cdc: flinkSpecBlock(resources === 'custom' ? fc : { ...fc, cluster: undefined }) } : {}),
  };
  // tuning is dumped last, and only when set: the same here, or the draft never matches.
  delete nextSpec.tuning;
  if (Object.keys(tuning).length) nextSpec.tuning = jsonbSorted(tuning);
  // placement is dumped after tuning, and only when set.
  delete nextSpec.placement;
  if (placement) nextSpec.placement = placement;
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
  // Every client-side reason the draft can't be saved, in page order: the badge, the Deploy
  // reason and each field's error read this one list.
  const problems = reviewProblems({
    nameErr,
    custom: customErr,
    streaming: clusterOn && fc.cluster ? streamingErrors(fc.cluster) : {},
    manual: clusterOn && fc.cluster ? manualErrors(fc.cluster) : {},
    tuning,
    tunable,
    raw,
    flink,
  });
  const invalid = problems.length > 0;
  const problemOf = (field: ReviewProblem['field']) => problems.find((p) => p.field === field)?.message ?? null;
  const tuningError = problems.some((p) => p.field === 'tuning') ? 'Fix or remove the highlighted engine settings.' : null;
  // Each row says why it blocks (RawProperties); the section only points at them.
  const rawError = problems.some((p) => p.field === 'advanced') ? 'Fix or remove the highlighted properties.' : null;
  const blockingSettings = problems.filter((p) => p.field === 'tuning' || p.field === 'advanced');
  const removeBlocking = (p: ReviewProblem) => {
    if (p.field === 'tuning' && p.key !== undefined) {
      const next = { ...tuning };
      delete next[p.key];
      setTuning(next);
    } else if (p.field === 'advanced' && p.key !== undefined) {
      // A key set twice loses its last row; a refused one goes.
      const at = rawRows.map((r) => r.key.trim()).lastIndexOf(p.key);
      if (at >= 0) setRawRows(rawRows.filter((_, i) => i !== at));
    }
  };

  // Save shortly after the last change, one save at a time, so versions never conflict. A save
  // the server refused is not repeated until the draft changes, Try again, or the step is left.
  const signature = JSON.stringify(body);
  // Saves made here: a new version from anywhere else is someone else's (said below).
  const ownSave = useRef(false);
  const autosave = useAutosave({
    signature,
    dirty,
    invalid: Boolean(invalid),
    saving,
    save: () => {
      ownSave.current = true;
      return save(body);
    },
    delay: AUTOSAVE_MS,
  });
  const paused = autosave.paused && Boolean(saveError);
  // Back to the stored values: nothing is unsaved, so no stale error stays on screen.
  useEffect(() => {
    if (!dirty && saveError && !saving) resetSave();
  }, [dirty, saveError, saving, resetSave]);

  useImperativeHandle(handle, () => ({
    dirty: () => dirty,
    commit: async () => (!dirty ? true : invalid ? false : autosave.attempt()),
  }));

  // The final checks run against exactly what is saved: after each save, on the version saved.
  // Deploy then sends that version, so it deploys what was checked here, never a version someone
  // else saved after (Known issue 17).
  const check = () => preflight.mutate({ id: pipeline.id, version: pipeline.current_version ?? undefined });
  const checkedVersion = preflight.variables?.version;
  // A version someone else saved while this step is open: the summary and checks follow it, and
  // the step says so instead of switching quietly.
  const [othersVersion, setOthersVersion] = useState<number | null>(null);
  useEffect(() => {
    if (!saving && saveError) ownSave.current = false; // a refused save made no version
  }, [saving, saveError]);
  const seenVersion = useRef(pipeline.current_version);
  useEffect(() => {
    const next = pipeline.current_version;
    if (next !== seenVersion.current && !ownSave.current && seenVersion.current !== null && next !== null) setOthersVersion(next);
    seenVersion.current = next;
    ownSave.current = false;
    if (pipeline.runtime && pipeline.sink) check();
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
      { id: pipeline.id, version: checkedVersion, forceReason },
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
      ? `Fix ${problems[0]!.label} first.`
      : paused
        ? `Not saved: ${saveError instanceof Error ? saveError.message : 'the server refused the change.'}`
        : dirty || saving
        ? 'Saving your changes…'
        : !pipeline.runtime
          ? 'Choose a runtime first.'
          : preflight.isPending
            ? 'Waiting for the final checks.'
            : !report?.ok
              ? 'The final checks must pass.'
              : null;
  // The chosen runtime's placement profiles (Kubernetes runtimes only have them).
  const profiles = (runtimes.data ?? []).find((r) => r.id === runtimeId)?.placement_profiles ?? [];
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
      {partitionProblem ? (
        <Notice tone="warn" title="Partition by needs a change">
          <span className="block">{partitionProblem}</span>
          <Button size="sm" className="mt-2" onClick={onBack}>
            Fix it in Transform
          </Button>
        </Notice>
      ) : null}
      <Card
        title="Settings"
        actions={
          <span className="flex items-center gap-1.5 text-xs text-muted" aria-live="polite">
            {saving ? (
              <>
                <Loader2 className="size-3.5 animate-spin" aria-hidden /> Saving…
              </>
            ) : invalid ? (
              // Saved values can block too (a setting the project hid since): then nothing is
              // unsaved, but the deploy waits for the fix.
              <button type="button" className="cursor-pointer text-warn underline decoration-dotted underline-offset-2" onClick={() => focusReviewField(problems[0]!.field)}>
                {dirty ? `Not saved: fix ${problemsSummary(problems)}` : `Fix ${problemsSummary(problems)} before deploying`}
              </button>
            ) : paused ? (
              <span className="text-bad">Not saved</span>
            ) : dirty ? (
              'Saving soon…'
            ) : (
              <>
                <CheckIcon className="size-3.5 text-ok" aria-hidden /> All changes saved
              </>
            )}
          </span>
        }
      >
        <InlineError
          error={saveError}
          doing="Couldn’t save your changes"
          onRetry={invalid ? undefined : () => void autosave.attempt()}
          fix={saveError instanceof ApiError && saveError.code === 'managed_property' ? `Remove those lines from ${rawLabel(flink)}, or set each one where its message says.` : undefined}
        />
        <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,280px),1fr))] gap-4">
          <div data-review-field="name" className="min-w-0">
            <Field label="Name" error={nameErr} hint={deployed ? 'Deployed pipelines keep their name.' : 'Used in URLs, YAML and the {pipeline} placeholder.'}>
              {(id) => <SlugInput id={id} value={name} onChange={setName} disabled={deployed} taken={others} />}
            </Field>
          </div>
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
          {profiles.length || placement ? (
            <Field
              label="Placement"
              hint="Where on the runtime its pods run: a profile its Admins set (nodes, tolerations, priority)."
              error={placement && !profiles.some((x) => x.name === placement) ? `This runtime has no profile ${placement}.` : null}
            >
              {(id) => (
                <Select
                  id={id}
                  value={placement}
                  onChange={setPlacement}
                  options={[
                    { value: '', label: 'Anywhere', description: 'No placement profile' },
                    ...profiles.map((x) => ({
                      value: x.name,
                      label: x.name,
                      description: Object.entries(x.node_selector ?? {}).map(([k, v]) => `${k}=${v}`).join(', ') || 'Any node',
                    })),
                  ]}
                />
              )}
            </Field>
          ) : null}
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
              onChange={(v) => {
                const next = v as FullSpec['policies']['resources'];
                // A Flink CDC custom cluster starts from the size it was, with the same readers.
                if (flink && next === 'custom' && !fc.cluster) setFc({ ...fc, cluster: clusterFromPreset(flinkEngine?.sizes?.[resources]) });
                setResources(next);
              }}
              options={[
                ...RESOURCES.map((r) => ({
                  value: r.value as FullSpec['policies']['resources'],
                  label: r.title,
                  description: flink ? flinkSizeHelp(flinkEngine?.sizes?.[r.value]) : r.help,
                  disabled: (!sizeAllowed(policies, r.value) || (!flink && tooSmall(r.value)) || readersLocked) && r.value !== resources,
                  why: readersLocked
                    ? 'A Flink CDC pipeline keeps the readers it was deployed with: Start over to change its size.'
                    : tooSmall(r.value)
                    ? `${TYPE_LABEL[pipeline.sink?.type ?? ''] ?? 'This destination'} needs at least ${sinkCaps.min_resources}.`
                    : `This project allows up to ${policies?.max_resources ?? 'a smaller size'}.`,
                })),
                ...(expert && !flink
                  ? [{ value: 'custom' as const, label: 'Custom', description: 'Your own CPU and memory; BeziSync works out the Java heap.' }]
                  : flink && (expert || fc.cluster)
                    ? [{ value: 'custom' as const, label: 'Custom', description: 'Your own readers, what each needs, and the TaskManagers BeziSync packs them into.' }]
                    : []),
              ]}
            />
          )}
        </Field>
        {resources === 'custom' && !flink ? (
          <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,220px),1fr))] gap-4">
            <div data-review-field="cpu" className="min-w-0">
              <Field label="CPU" error={customErr.cpu}>
                {(id) => (
                  <Select id={id} value={custom.cpu} onChange={(cpu) => setCustom({ ...custom, cpu })} options={CUSTOM_CPUS.map((c) => ({ value: c, label: `${c} CPU` }))} />
                )}
              </Field>
            </div>
            <div data-review-field="memory" className="min-w-0">
              <Field label="Memory" error={customErr.memory} hint="Requested and the limit, so the connector is never squeezed.">
                {(id) => (
                  <Select
                    id={id}
                    value={custom.memory}
                    onChange={(memory) => setCustom({ ...custom, memory })}
                    options={CUSTOM_MEMORIES.map((m) => ({ value: m, label: m.endsWith('Gi') ? `${m.slice(0, -2)} GiB` : `${m.slice(0, -2)} MiB` }))}
                  />
                )}
              </Field>
            </div>
            <Field label="Java heap" hint="Worked out from memory, so the JVM always keeps room.">
              {() => <Meta className="pt-2 text-sm text-ink">{heapMiB(memoryMiB(custom.memory))} MiB</Meta>}
            </Field>
          </div>
        ) : null}
        {clusterOn && fc.cluster ? (
          <FlinkClusterEditor
            value={fc.cluster}
            onChange={(cluster) => setFc({ ...fc, cluster })}
            packed={pack.data}
            packing={pack.isFetching}
            readersLocked={readersLocked}
            rows={tableRows}
            errors={{
              streamingCpu: problemOf('streaming-cpu') ?? undefined,
              streamingMemory: problemOf('streaming-memory') ?? undefined,
              taskManagers: problemOf('task-managers') ?? undefined,
              tmMemory: problemOf('tm-memory') ?? undefined,
            }}
          />
        ) : null}
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
                catalogue={(flinkEngine?.settings ?? []).filter((s) => s.scope === 'engine')}
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
      </Card>

      {/* Suggestions from this preflight's evidence, in both modes: one click sets them. */}
      <Recommendations
        recs={freshRecommendations(report?.recommendations, tunable, tuning)}
        settings={tunable}
        onUse={(key, value) => setTuning({ ...tuning, [key]: value })}
      />
      {expert ? (
        <Card title="Engine settings">
          <div className="flex min-w-0 flex-col gap-6">
            <div data-review-field="tuning" className="min-w-0">
              <Field label={flink ? 'Flink CDC source, destination and pipeline options' : 'Debezium options'} optional group error={tuningError}>
                {() =>
                  tunable.length ? (
                    <SettingsEditor idBase="tuning" value={tuning} onChange={setTuning} settings={tunable} caps={caps} pinned={pinned} />
                  ) : (
                    <div className="flex min-w-0 flex-col gap-3">
                      <Meta>BeziSync offers no typed settings for this source and destination yet.</Meta>
                      {/* A saved setting nothing here offers still blocks: it can be removed. */}
                      <BlockingSettings problems={blockingSettings.filter((p) => p.field === 'tuning')} onRemove={removeBlocking} expert card={false} />
                    </div>
                  )
                }
              </Field>
            </div>
            <div data-review-field="advanced" className="min-w-0">
              <Field
                label={rawLabel(flink)}
                optional
                group
                error={rawError}
                hint={
                  !policies?.allow_advanced_properties
                    ? 'This project doesn’t allow advanced properties: a pipeline with any can’t deploy. An Admin can allow them in Settings → Policies.'
                    : flink
                      ? 'Pass-through options BeziSync doesn’t offer: source.jdbc.properties.*, source.debezium.*, and the destination’s sink.properties.*, table.create.properties.* or table.properties.*. The rest show in preflight as not verified.'
                      : 'For options BeziSync doesn’t offer, passed to Debezium as typed. Keys BeziSync sets, credentials and unsafe options are refused; the rest show in preflight as not verified.'
                }
              >
                {() => (
                  <RawProperties
                    idBase="raw"
                    rows={rawRows}
                    onChange={setRawRows}
                    verdicts={verdicts}
                    onUseSetting={useTypedSetting}
                    keyPlaceholder={flink ? 'source.jdbc.properties.…' : 'debezium.source.…'}
                  />
                )}
              </Field>
            </div>
          </div>
        </Card>
      ) : (
        // Guided mode doesn't draw the editor, but what blocks saving is still shown, with Remove.
        <BlockingSettings problems={blockingSettings} onRemove={removeBlocking} expert={false} />
      )}

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
        <StepsSummary spec={spec} flink={flink} ctx={shapeCtx} />
        {deployed ? <OutputImpact pipeline={pipeline} spec={spec} /> : null}
      </Card>

      <Card title="Final checks">
        {othersVersion !== null ? (
          <Notice tone="warn" title={`Someone else saved v${othersVersion}`}>
            While you were on this step another change was saved. What you see here and the checks below now show v{othersVersion}, and
            Deploy deploys v{othersVersion}: read it through before you deploy.
          </Notice>
        ) : null}
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
          <InlineError error={preflight.error} doing="Couldn’t run the final checks" onRetry={check} />
        ) : report ? (
          <>
            <div className="flex flex-wrap items-center gap-2">
              <Pill tone={report.ok ? 'ok' : 'bad'}>{report.summary}</Pill>
              <Button size="sm" variant="ghost" onClick={check}>
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
            {report.settings_change ? (
              <Notice
                tone={report.settings_change.impact === 'breaks_positions' ? 'bad' : report.settings_change.impact === 'next_copy' ? 'warn' : 'info'}
                title="What changes in the settings"
              >
                <SettingsChangeList change={report.settings_change} />
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
            {pipeline.approval_required ? 'Request approval' : deployed ? `Deploy v${checkedVersion ?? pipeline.current_version}` : 'Deploy'}
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

/** Policies without a custom size (it is sent only with resources: custom, as the server dumps it). */
function withoutCustom(p: FullSpec['policies']): FullSpec['policies'] {
  const { custom_resources: _c, ...rest } = p;
  void _c;
  return rest;
}
