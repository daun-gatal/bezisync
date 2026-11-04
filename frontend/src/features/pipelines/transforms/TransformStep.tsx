import { useEffect, useImperativeHandle, useMemo, useRef, useState, type ReactNode, type Ref } from 'react';
import { ArrowDown, ArrowUp, Lock, Plus, Trash2 } from 'lucide-react';
import clsx from 'clsx';
import { Button } from '@/components/ui/button';
import { InlineError, Notice, Skeleton } from '@/components/ui/feedback';
import { Card } from '@/components/ui/layout';
import { Menu, Modal } from '@/components/ui/overlays';
import { sinkCapsOf, TYPE_LABEL, useConnectorTypes, useDiscovery } from '@/lib/connections';
import { usePolicies } from '@/lib/policies';
import type { Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { fullSpec, stageOf, withShape, type FullSpec, type TransformStep as Step, type TransformType, type ValueFormats } from '../spec';
import { useSaveSpec, type StepHandle } from '../useSaveSpec';
import { AddFieldEditor, CustomEditor, FilterEditor, FlattenEditor, KeysEditor, RenameEditor, ShapeEditor, ValuesEditor, type EditorContext } from './editors';
import { PreviewPanel } from './PreviewPanel';
import {
  addStep,
  canMove,
  columnKind,
  DEBEZIUM_CLASS,
  DEBEZIUM_ONLY_STEPS,
  defaultStep,
  moveStep,
  NO_CAPS,
  stepCaps,
  stepErrors,
  summarize,
  summarizeValues,
  TYPE_TITLE,
  type Caps,
  type TableInfo,
} from './model';

const CHOICES: { group: string; items: { type: TransformType; help: string }[] }[] = [
  { group: 'Filter', items: [{ type: 'filter', help: 'Keep events by operation or column values.' }] },
  {
    group: 'Reshape',
    items: [
      { type: 'rename', help: 'Match the names consumers expect.' },
      { type: 'add_field', help: 'A fixed value or the pipeline’s name.' },
      { type: 'flatten', help: 'address.city becomes address_city.' },
    ],
  },
  { group: 'Expert', items: [{ type: 'custom', help: 'Any transform class the Debezium image ships.' }] },
];

function cleanValues(v: ValueFormats, caps: Caps): ValueFormats | undefined {
  const out: ValueFormats = {};
  for (const k of ['decimals', 'timestamps', 'binary'] as const) {
    const allowed = caps.value_formats[k] ?? [];
    if (v[k] && v[k] !== allowed[0]) Object.assign(out, { [k]: v[k] });
  }
  return Object.keys(out).length ? out : undefined;
}

/** Wizard step 4: how events look. Steps are grouped by the stage they run in. */
export function TransformStep({ pipeline, handle, onBack, onNext }: { pipeline: Pipeline; handle: Ref<StepHandle>; onBack: () => void; onNext: () => void }) {
  const { project, expert } = useProject();
  const spec = useMemo(() => fullSpec(pipeline.spec), [pipeline.spec]);
  const [steps, setSteps] = useState<Step[]>(spec.transforms);
  const [values, setValues] = useState<ValueFormats>(spec.values ?? {});
  const [keys, setKeys] = useState<Record<string, string[]>>(spec.keys ?? {});
  const [open, setOpen] = useState<string | null>(null);
  const [adding, setAdding] = useState(false);
  const discovery = useDiscovery(project.id, pipeline.source.id);
  const types = useConnectorTypes(project.id);
  const policies = usePolicies(project.id).data;
  const { save, pending, error: saveError } = useSaveSpec(pipeline);
  const sourceType = pipeline.source.type;
  const sinkCaps = sinkCapsOf(types.data, pipeline.sink?.type);
  const sinkTitle = TYPE_LABEL[pipeline.sink?.type ?? ''] ?? 'This destination';
  const sourceCaps: Caps = (types.data?.find((t) => t.kind === 'source' && t.type === sourceType)?.transforms as Caps | undefined) ?? NO_CAPS;
  // Flink CDC writes each column with its own type: value formats (and the decimals warning) are
  // Debezium's, and so are the outbox router, flattening and custom steps.
  const flink = pipeline.engine === 'flink-cdc';
  // A destination that cannot read Debezium's default (epoch) timestamps gets ISO text, or Kafka
  // Connect's types where the source has no ISO mode: the epoch choice is not offered.
  const caps: Caps = useMemo(() => stepCaps(sourceCaps, Boolean(sinkCaps.time_precision), flink), [flink, sinkCaps.time_precision, sourceCaps]);
  const shapeRequired = Boolean(sinkCaps.unwrap?.required);
  const captured = spec.source.tables.include;
  const tables: TableInfo[] = useMemo(
    () =>
      (discovery.data?.tables ?? [])
        .filter((t) => captured.includes(`${t.schema}.${t.name}`))
        .map((t) => ({
          name: `${t.schema}.${t.name}`,
          columns: t.columns ?? [],
          primaryKey: t.primary_key ?? [],
          replicaIdentity: t.replica_identity ?? null,
        })),
    [discovery.data, captured],
  );
  const draft: FullSpec = useMemo(() => {
    const { values: _v, keys: _k, ...rest } = spec;
    // On Flink CDC never saved (saving the step also clears formats left from before).
    const v = flink ? undefined : cleanValues(values, caps);
    return { ...rest, transforms: steps, ...(v ? { values: v } : {}), ...(Object.keys(keys).length ? { keys } : {}) };
  }, [spec, steps, values, keys, caps, flink]);
  const errors = stepErrors(steps);
  const dirty = JSON.stringify(draft) !== JSON.stringify(spec);
  useImperativeHandle(handle, () => ({
    dirty: () => dirty,
    commit: async () => (errors.size ? false : dirty ? save({ spec: draft }) : true),
  }));
  const shapeAt = steps.findIndex((t) => t.type === 'unwrap');
  const keyOf = (t: Step, i: number) => t.id ?? `step-${i}`;
  const ctxFor = (index: number): EditorContext => ({ sourceType, sink: sinkCaps, tables, caps, spec: draft, steps, index, expert });
  const update = (i: number, next: Step) => setSteps(steps.map((t, j) => (j === i ? next : t)));
  const remove = (i: number) => {
    setSteps(steps.filter((_, j) => j !== i));
    setOpen(null);
  };

  const decimalColumns = tables.flatMap((t) =>
    t.columns.filter((c) => columnKind(sourceType, c.type) === 'decimal').map((c) => `${t.name.split('.')[1]}.${c.name}`),
  );
  const nudgeDecimals = spec.sink.format.type === 'json' && decimalColumns.length > 0 && !values.decimals && (caps.value_formats.decimals?.length ?? 0) > 1;
  const unkeyed = caps.key_columns ? tables.filter((t) => !t.primaryKey.length && !keys[t.name]) : [];
  const fieldSteps = steps.filter((t) => stageOf(t) === 2);
  // Open the key editor once when tables without a key are found; it stays open while the
  // user picks columns (deriving "open" from unkeyed would close it on the first pick).
  const prompted = useRef(false);
  useEffect(() => {
    if (!prompted.current && unkeyed.length > 0 && open === null) {
      prompted.current = true;
      setOpen('keys');
    }
  }, [unkeyed.length, open]);

  function locked(type: TransformType): string | null {
    if (flink && DEBEZIUM_ONLY_STEPS.includes(type)) return 'Runs only on Debezium: this pipeline runs on Flink CDC.';
    if (type === 'custom') {
      if (!policies?.allow_custom_transforms) return 'Turned off by this project’s policy. Ask an Admin under Settings → Policies.';
      if (!expert) return 'Switch to Expert mode (account menu) to add custom transforms.';
      return null;
    }
    if (policies?.allowed_transforms && !policies.allowed_transforms.includes(type)) return 'Not allowed in this project.';
    if (stageOf({ type }) === 2 && shapeAt < 0) return 'Needs the “Row after change” event shape.';
    return null;
  }

  function add(type: TransformType) {
    const step = defaultStep(type, steps);
    setSteps(addStep(steps, step));
    setAdding(false);
    setOpen(step.id!);
  }

  function setShape(on: boolean) {
    const shape = defaultStep('unwrap', steps);
    const deletes = sinkCaps.unwrap?.delete_handling ?? [];
    if (deletes.length && !deletes.includes(shape.delete_handling ?? 'rewrite')) shape.delete_handling = deletes[0] as Step['delete_handling'];
    setSteps(withShape(steps, on ? shape : null));
    setOpen(null);
  }

  const row = (t: Step, i: number) => {
    const k = keyOf(t, i);
    const isOpen = open === k;
    const err = errors.get(i);
    const ctx = ctxFor(i);
    const editor: Record<TransformType, ReactNode> = {
      unwrap: <ShapeEditor step={t} ctx={ctx} onChange={(s) => update(i, s)} />,
      filter: <FilterEditor step={t} ctx={ctx} onChange={(s) => update(i, s)} />,
      rename: <RenameEditor step={t} ctx={ctx} onChange={(s) => update(i, s)} />,
      add_field: <AddFieldEditor step={t} ctx={ctx} onChange={(s) => update(i, s)} />,
      flatten: <FlattenEditor step={t} ctx={ctx} onChange={(s) => update(i, s)} />,
      custom: <CustomEditor step={t} ctx={ctx} onChange={(s) => update(i, s)} />,
      outbox: <p className="m-0 text-xs text-muted">The outbox router has no settings here yet; edit it through YAML or the API.</p>,
    };
    return (
      <StepRow
        key={k}
        n={i + 1}
        title={t.type === 'unwrap' ? 'Row after change' : TYPE_TITLE[t.type]}
        summary={t.type === 'unwrap' ? summarize(t).split(' · ').slice(1).join(' · ') : summarize(t)}
        technical={expert ? (t.type === 'custom' ? t.class : DEBEZIUM_CLASS[t.type]) : undefined}
        open={isOpen}
        error={err}
        inlineErrors={t.type === 'rename' || t.type === 'add_field'}
        onToggle={() => setOpen(isOpen ? null : k)}
        onUp={canMove(steps, i, -1) ? () => setSteps(moveStep(steps, i, -1)) : undefined}
        onDown={canMove(steps, i, 1) ? () => setSteps(moveStep(steps, i, 1)) : undefined}
        onRemove={t.type === 'unwrap' ? undefined : () => remove(i)}
      >
        {editor[t.type]}
        {t.type === 'unwrap' && !shapeRequired ? (
          <div className="flex flex-wrap items-center gap-2 border-t border-line pt-2">
            <Button size="sm" variant="ghost" disabled={fieldSteps.length > 0} onClick={() => setShape(false)}>
              Use the full change envelope instead
            </Button>
            {fieldSteps.length ? <span className="text-xs text-muted">Remove the field changes first: they need the row.</span> : null}
          </div>
        ) : null}
      </StepRow>
    );
  };

  const stage = (label: string, filter: (t: Step) => boolean) => {
    const rows = steps.map((t, i) => [t, i] as const).filter(([t]) => filter(t));
    return rows.length ? (
      <>
        <div className="mt-1 text-xs font-semibold tracking-wide text-muted uppercase">{label}</div>
        {rows.map(([t, i]) => row(t, i))}
      </>
    ) : null;
  };

  const blocked = errors.size ? 'Fix the highlighted steps to see the preview.' : null;

  return (
    <div className="flex flex-col gap-4">
      <div>
        <h2 className="m-0 text-lg font-semibold">How should events look?</h2>
        <p className="m-0 mt-1 text-sm text-muted">Optional. Most pipelines only need the event shape. You can change all of this later.</p>
      </div>
      <div className="grid min-w-0 grid-cols-[minmax(0,1fr)] items-start gap-4 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)]">
        <Card
          title="Steps"
          actions={
            <Button size="sm" variant="primary" icon={<Plus />} onClick={() => setAdding(true)}>
              Add transform
            </Button>
          }
        >
          <span className="text-xs text-muted">Applied top to bottom.</span>
          {discovery.isPending ? <Skeleton lines={2} /> : null}
          <SystemLine>BeziSync drops its own heartbeat and signal records</SystemLine>
          {nudgeDecimals ? (
            <Notice tone="warn" title="Decimals arrive as base64 text">
              <span className="block">
                {decimalColumns.slice(0, 3).join(', ')}
                {decimalColumns.length > 3 ? ` and ${decimalColumns.length - 3} more` : ''} would look like <code className="font-mono">"Mr4="</code>. Most
                consumers expect a number.
              </span>
              <span className="mt-2 flex flex-wrap gap-2">
                <Button size="sm" variant="primary" onClick={() => setValues({ ...values, decimals: 'double' })}>
                  Send decimals as numbers
                </Button>
                <Button size="sm" onClick={() => setValues({ ...values, decimals: 'string' })}>
                  Send as exact text
                </Button>
                <Button size="sm" variant="ghost" onClick={() => setValues({ ...values, decimals: 'precise' })}>
                  Keep base64
                </Button>
              </span>
            </Notice>
          ) : null}
          {Object.values(caps.value_formats).some((m) => (m?.length ?? 0) > 1) ? (
            <>
              <div className="mt-1 text-xs font-semibold tracking-wide text-muted uppercase">Value formats</div>
              <StepRow
                n={null}
                title="Value formats"
                summary={summarizeValues(cleanValues(values, caps)) ?? 'Connector defaults'}
                open={open === 'values'}
                technical={expert ? 'decimal.handling.mode · time.precision.mode · binary.handling.mode' : undefined}
                onToggle={() => setOpen(open === 'values' ? null : 'values')}
              >
                <ValuesEditor values={values} ctx={ctxFor(0)} onChange={setValues} />
              </StepRow>
            </>
          ) : null}
          {stage('Keep events', (t) => stageOf(t) === 0)}
          <div className="mt-1 text-xs font-semibold tracking-wide text-muted uppercase">Event shape</div>
          {shapeAt >= 0 ? (
            row(steps[shapeAt]!, shapeAt)
          ) : shapeRequired ? (
            <div className="flex flex-wrap items-center gap-2 rounded-lg border border-line bg-subtle px-3 py-2.5 text-sm">
              <Lock className="size-4 text-muted" aria-hidden />
              <span>
                <strong className="font-semibold">Row after change</strong> <span className="text-muted">· {sinkTitle} needs it; BeziSync adds it</span>
              </span>
              <Button size="sm" className="ml-auto" onClick={() => setShape(true)}>
                Adjust
              </Button>
            </div>
          ) : (
            <div className="flex flex-wrap items-center gap-2 rounded-lg border border-line bg-subtle px-3 py-2.5 text-sm">
              <span>
                <strong className="font-semibold">Full change envelope</strong> <span className="text-muted">· before, after and source metadata</span>
              </span>
              <Button size="sm" className="ml-auto" onClick={() => setShape(true)}>
                Use the row after change
              </Button>
            </div>
          )}
          {stage('Field changes · order matters', (t) => stageOf(t) === 2)}
          {stage('Other steps', (t) => stageOf(t) === -1 && t.type !== 'unwrap')}
          {caps.key_columns ? (
            <>
              <div className="mt-1 text-xs font-semibold tracking-wide text-muted uppercase">Message key</div>
              {unkeyed.length ? (
                <Notice tone="warn" className="wrap-anywhere">
                  {unkeyed.map((t) => t.name).join(', ')} {unkeyed.length === 1 ? 'has' : 'have'} no primary key, so{' '}
                  {unkeyed.length === 1 ? 'its events have' : 'their events have'} no key: no ordering per row and no compaction. Choose key columns below.
                </Notice>
              ) : null}
              <StepRow
                n={null}
                title="Message key"
                summary={
                  Object.keys(keys).length
                    ? Object.entries(keys)
                        .map(([t, c]) => `${t} → ${c.join(', ')}`)
                        .join(' · ')
                    : 'Primary keys'
                }
                technical={expert ? 'message.key.columns' : undefined}
                open={open === 'keys'}
                onToggle={() => setOpen(open === 'keys' ? null : 'keys')}
              >
                <KeysEditor keys={keys} ctx={ctxFor(steps.length)} onChange={setKeys} />
              </StepRow>
            </>
          ) : null}
          <SystemLine>BeziSync names each destination ({spec.sink.naming})</SystemLine>
        </Card>
        <PreviewPanel projectId={project.id} pipelineId={pipeline.id} spec={draft} steps={steps} tables={tables} blocked={blocked} />
      </div>
      <InlineError error={saveError} doing="Couldn’t save the transforms" />
      <div className="flex flex-wrap items-center justify-between gap-3">
        <Button onClick={onBack}>← Back</Button>
        <div className="flex flex-wrap items-center gap-3">
          {errors.size ? (
            <span className="text-sm text-bad">
              {errors.size} step{errors.size === 1 ? ' needs' : 's need'} attention.
            </span>
          ) : null}
          <Button variant="primary" disabled={errors.size > 0} why="Fix the highlighted steps first." loading={pending} onClick={onNext}>
            Continue to review →
          </Button>
        </div>
      </div>
      <Modal
        open={adding}
        onOpenChange={setAdding}
        title="Add a transform"
        description="Choose what you want to do; BeziSync puts the step where it works."
        size="lg"
      >
        <div className="flex flex-col gap-4">
          {CHOICES.map((g) => (
            <div key={g.group} className="flex flex-col gap-2">
              <div className="text-xs font-bold text-ink-2">{g.group}</div>
              <div className="grid grid-cols-[repeat(auto-fill,minmax(min(100%,240px),1fr))] gap-2">
                {g.items.map((c) => {
                  const why = locked(c.type);
                  return (
                    <button
                      key={c.type}
                      type="button"
                      disabled={Boolean(why)}
                      onClick={() => add(c.type)}
                      className={clsx(
                        'flex flex-col gap-1 rounded-lg border border-line p-3 text-left',
                        why ? 'cursor-not-allowed bg-subtle' : 'bg-surface hover:border-accent',
                      )}
                    >
                      <span className="flex items-center gap-1.5 text-base font-semibold">
                        {TYPE_TITLE[c.type]}
                        {why ? <Lock className="size-3.5 text-muted" aria-hidden /> : null}
                      </span>
                      <span className="text-xs text-muted">{why ?? c.help}</span>
                    </button>
                  );
                })}
              </div>
            </div>
          ))}
        </div>
      </Modal>
    </div>
  );
}

function SystemLine({ children }: { children: ReactNode }) {
  return (
    <div className="flex items-center gap-2 text-xs text-muted">
      <span className="w-3.5 border-t border-dashed border-line-strong" aria-hidden />
      {children}
    </div>
  );
}

function StepRow({
  n,
  title,
  summary,
  technical,
  open,
  error,
  inlineErrors,
  onToggle,
  onUp,
  onDown,
  onRemove,
  children,
}: {
  n: number | null;
  title: string;
  summary: string;
  technical?: string;
  open: boolean;
  error?: string;
  inlineErrors?: boolean;
  onToggle: () => void;
  onUp?: () => void;
  onDown?: () => void;
  onRemove?: () => void;
  children: ReactNode;
}) {
  const label = n ? `Step ${n}: ${title}` : title;
  return (
    <section
      aria-label={label}
      className={clsx(
        'min-w-0 rounded-lg border px-3 py-2.5',
        open ? 'border-accent bg-surface shadow-[0_0_0_2px_var(--bz-accent-soft)]' : error ? 'border-bad bg-bad-soft' : 'border-line bg-subtle',
      )}
    >
      <div className="flex min-w-0 items-start gap-2.5">
        {n !== null ? (
          <span
            className={clsx(
              'mt-0.5 grid size-6 shrink-0 place-items-center rounded-full text-xs font-bold',
              open ? 'bg-accent text-on-accent' : 'bg-grey-soft text-ink-2',
            )}
          >
            {n}
          </span>
        ) : null}
        <button type="button" className="min-w-0 flex-1 text-left" aria-expanded={open} onClick={onToggle}>
          <span className="block text-sm font-semibold">{title}</span>
          <span className="block text-xs wrap-break-word text-muted">{summary}</span>
          {technical ? <span className="block font-mono text-xs wrap-anywhere text-muted">{technical}</span> : null}
        </button>
        <span className="flex shrink-0 items-center gap-1">
          {onUp || onDown || onRemove ? (
            <Menu
              label={`More for step ${n ?? title}`}
              icon="more"
              variant="ghost"
              size="sm"
              items={[
                { label: 'Move up', icon: <ArrowUp />, disabled: !onUp, why: 'It can’t run earlier than this.', onSelect: () => onUp?.() },
                { label: 'Move down', icon: <ArrowDown />, disabled: !onDown, why: 'It can’t run later than this.', onSelect: () => onDown?.() },
                onRemove ? '-' : null,
                onRemove ? { label: 'Remove', icon: <Trash2 />, danger: true, onSelect: onRemove } : null,
              ]}
            />
          ) : null}
          <Button size="sm" variant="ghost" onClick={onToggle}>
            {open ? 'Done' : 'Edit'}
          </Button>
        </span>
      </div>
      {error && !open ? (
        <p role="alert" className="m-0 mt-1.5 text-xs text-bad">
          {error}
        </p>
      ) : null}
      {open ? (
        <div className="mt-3 flex min-w-0 flex-col gap-3 border-t border-dashed border-line-strong pt-3">
          {children}
          {error && !inlineErrors ? (
            <p role="alert" className="m-0 text-xs text-bad">
              {error}
            </p>
          ) : null}
        </div>
      ) : null}
    </section>
  );
}
