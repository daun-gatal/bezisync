import { ClusterDiagram } from '@/components/ClusterDiagram';
import { Notice, Skeleton } from '@/components/ui/feedback';
import { Field, Segmented, Stepper, Switch } from '@/components/ui/form';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import {
  JM_CPUS,
  JM_MEMORIES,
  MAX_PARALLELISM,
  MAX_SLOTS,
  MAX_TASK_MANAGERS,
  SLOT_CPUS,
  SLOT_MEMORIES,
  TM_CPUS,
  TM_MEMORIES,
  manualError,
  memoryLabel,
  mib,
  streamingError,
  streamingFrom,
  type PackedCluster,
} from '@/lib/cluster';
import type { FlinkCluster } from '@/lib/engines';

/** Rows a reader copies per second, as a deliberately rough guide (SnapshotsTab's figure). */
const ROWS_PER_READER_SECOND = 10_000;

const cpuOptions = (l: string[]) => l.map((c) => ({ value: c, label: `${c} CPU` }));
const memoryOptions = (l: string[]) => l.map((m) => ({ value: m, label: memoryLabel(m) }));

/** About how long the first copy of rows takes with this many readers (null when unknown). */
export function copyMinutes(rows: number | null, readers: number): number | null {
  if (rows === null) return null;
  return Math.max(1, Math.round(rows / (ROWS_PER_READER_SECOND * readers) / 60));
}

/**
 * A custom Flink CDC cluster (Review → Size, Expert mode): how many readers, what each reader's slot
 * and the JobManager need, and the shape — automatic (BeziSync packs the slots for the runtime's
 * nodes when it deploys; the diagram shows how it would now, and why) or manual TaskManagers.
 */
export function FlinkClusterEditor({
  value,
  onChange,
  packed,
  packing,
  readersLocked,
  rows,
}: {
  value: FlinkCluster;
  onChange: (c: FlinkCluster) => void;
  /** The server's packing of value on the pipeline's runtime (undefined while it is asked). */
  packed: PackedCluster | undefined;
  packing: boolean;
  /** The pipeline runs and has a saved position: its readers can't change until Start over. */
  readersLocked: boolean;
  /** Rows of the selected tables, for the copy time (null when unknown). */
  rows: number | null;
}) {
  const set = (patch: Partial<FlinkCluster>) => onChange({ ...value, ...patch });
  const manual = value.shape === 'manual';
  const tm = value.task_managers;
  const minutes = copyMinutes(rows, value.parallelism);
  const readersHint = readersLocked
    ? 'It runs with these readers and has a saved position: Start over (Positions) to change them.'
    : `Each reader copies tables in parallel; streaming reads the log with one.${minutes !== null ? ` The first copy takes roughly ${minutes} min.` : ''}`;
  const toManual = () =>
    set({
      shape: 'manual',
      // A manual shape is never resized.
      streaming_slot: undefined,
      task_managers: {
        count: packed?.task_managers ?? 1,
        slots: packed?.slots ?? Math.min(value.parallelism, MAX_SLOTS),
        // The smallest choice that still holds the packed TaskManager.
        cpu: TM_CPUS.find((c) => Number(c) >= Number(packed?.task_manager_cpu ?? 1)) ?? TM_CPUS.at(-1)!,
        memory: TM_MEMORIES.find((m) => mib(m) >= mib(packed?.task_manager_memory ?? '2Gi')) ?? TM_MEMORIES.at(-1)!,
      },
    });
  const shapeError = manualError(value);
  const shrinkError = streamingError(value);
  const stream = value.streaming_slot;
  // A streaming size no bigger than the copy one: the lists stop at the copy size.
  const streamCpus = SLOT_CPUS.filter((c) => Number(c) <= Number(value.slot.cpu));
  const streamMemories = SLOT_MEMORIES.filter((m) => mib(m) <= mib(value.slot.memory));
  return (
    <section aria-label="Cluster" className="flex min-w-0 flex-col gap-4 rounded-md border border-line p-4">
      <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,200px),1fr))] gap-4">
        <Field label="Readers" hint={readersHint}>
          {(id) => <Stepper id={id} value={value.parallelism} min={1} max={MAX_PARALLELISM} onChange={(parallelism) => set({ parallelism })} disabled={readersLocked} />}
        </Field>
        <Field label="Reader CPU" hint="Each reader’s slot.">
          {(id) => <Select id={id} value={value.slot.cpu} onChange={(cpu) => set({ slot: { ...value.slot, cpu } })} options={cpuOptions(SLOT_CPUS)} />}
        </Field>
        <Field label="Reader memory" hint="Each reader’s slot.">
          {(id) => <Select id={id} value={value.slot.memory} onChange={(memory) => set({ slot: { ...value.slot, memory } })} options={memoryOptions(SLOT_MEMORIES)} />}
        </Field>
        <Field label="JobManager CPU">
          {(id) => <Select id={id} value={value.job_manager.cpu} onChange={(cpu) => set({ job_manager: { ...value.job_manager, cpu } })} options={cpuOptions(JM_CPUS)} />}
        </Field>
        <Field label="JobManager memory">
          {(id) => (
            <Select id={id} value={value.job_manager.memory} onChange={(memory) => set({ job_manager: { ...value.job_manager, memory } })} options={memoryOptions(JM_MEMORIES)} />
          )}
        </Field>
      </div>
      <Switch
        label="Shrink after the first copy"
        checked={stream !== undefined && stream !== null}
        disabled={manual}
        why="A manual shape is never resized."
        description="Streaming reads the log with one reader: once the copy is done and a position is saved, BeziSync repacks the readers smaller (a restart from the saved position), and grows them back before a later copy."
        onChange={(on) => set({ streaming_slot: on ? streamingFrom(value.slot) : undefined })}
      />
      {stream && !manual ? (
        <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,200px),1fr))] gap-4">
          <Field label="Streaming reader CPU" error={shrinkError}>
            {(id) => <Select id={id} value={stream.cpu} onChange={(cpu) => set({ streaming_slot: { ...stream, cpu } })} options={cpuOptions(streamCpus)} />}
          </Field>
          <Field label="Streaming reader memory">
            {(id) => <Select id={id} value={stream.memory} onChange={(memory) => set({ streaming_slot: { ...stream, memory } })} options={memoryOptions(streamMemories)} />}
          </Field>
        </div>
      ) : null}
      <Field label="Shape" group hint={manual ? 'TaskManagers as you set them.' : 'BeziSync packs the readers into TaskManagers that fit the runtime’s nodes when it deploys, and keeps that shape until the next deploy.'}>
        {() => (
          <Segmented
            label="Shape"
            value={manual ? 'manual' : 'auto'}
            onChange={(v) => (v === 'manual' ? toManual() : set({ shape: 'auto', task_managers: undefined }))}
            options={[
              { value: 'auto', label: 'Automatic' },
              { value: 'manual', label: 'Manual' },
            ]}
          />
        )}
      </Field>
      {manual && tm ? (
        <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,200px),1fr))] gap-4">
          <Field label="TaskManagers" error={shapeError}>
            {(id) => <Stepper id={id} value={tm.count} min={1} max={MAX_TASK_MANAGERS} onChange={(count) => set({ task_managers: { ...tm, count } })} />}
          </Field>
          <Field label="Slots per TaskManager">
            {(id) => <Stepper id={id} value={tm.slots} min={1} max={MAX_SLOTS} onChange={(slots) => set({ task_managers: { ...tm, slots } })} />}
          </Field>
          <Field label="TaskManager CPU">
            {(id) => <Select id={id} value={tm.cpu} onChange={(cpu) => set({ task_managers: { ...tm, cpu } })} options={cpuOptions(TM_CPUS)} />}
          </Field>
          <Field label="TaskManager memory">
            {(id) => <Select id={id} value={tm.memory} onChange={(memory) => set({ task_managers: { ...tm, memory } })} options={memoryOptions(TM_MEMORIES)} />}
          </Field>
        </div>
      ) : null}
      {packed && !shapeError && !shrinkError ? (
        <>
          <ClusterDiagram title={manual ? 'Your TaskManagers' : packed.streaming ? 'During the first copy' : 'How it would be packed now'} shape={packed} />
          {manual ? null : <Meta className="text-sm text-ink-2">{packed.why}</Meta>}
          {packed.streaming ? <ClusterDiagram title="While streaming" shape={packed.streaming} /> : null}
          <Meta className="text-sm text-ink-2">
            {packed.streaming ? 'At its peak (the first copy)' : 'In all'}: {Number(packed.need.cpu.toFixed(2))} CPU and {memoryLabel(`${packed.need.memory_mib}Mi`)}
            {packed.streaming
              ? `; while streaming ${Number(packed.streaming.need.cpu.toFixed(2))} CPU and ${memoryLabel(`${packed.streaming.need.memory_mib}Mi`)}`
              : ''}
            {packed.node ? `; the largest node has ${packed.node.cpu} CPU and ${memoryLabel(`${packed.node.memory_mib}Mi`)}.` : '.'}
          </Meta>
          {packed.check && packed.check.status !== 'pass' ? (
            <Notice tone={packed.check.status === 'fail' ? 'bad' : 'info'} title={packed.check.title}>
              {packed.check.message}
            </Notice>
          ) : null}
        </>
      ) : packing ? (
        <Skeleton lines={2} />
      ) : null}
    </section>
  );
}
