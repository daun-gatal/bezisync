import type { Tone } from '@/components/ui/feedback';
import { plural } from '@/components/ui/text';
import { formatBytes } from '@/lib/format';
import type { FlinkStatus, SourceImpact } from '@/lib/status';
import type { FullSpec, TransformType } from '../spec';
import { effectiveDeletes, METADATA, metadataField, summarize, summarizeValues, TYPE_TITLE, type ShapeCtx } from '../transforms/model';

/** One fact in Source protection. `note` is a short second line. */
export interface Fact {
  label: string;
  value: string;
  note?: string;
  mono?: boolean;
  tone?: Tone;
}

export interface ProtectionView {
  pill: { tone: Tone; label: string };
  facts: Fact[];
  /** The log kept for this pipeline against its retention limit. */
  meter?: { label: string; ratio: number; text: string; tone: Tone };
  /** Shown instead of facts when the probe measured nothing (e.g. the slot does not exist yet). */
  note?: string;
}

/** What a time-kept history is called, per source (Source protection facts). */
const KEPT_LABEL: Record<string, string> = {
  oracle: 'Archived logs reach back',
  cockroachdb: 'Changes kept (GC TTL)',
  spanner: 'Change stream keeps changes for',
};

/** "under a minute", or formatWindow for longer spans. */
function formatSpan(seconds: number): string {
  return seconds < 60 ? 'under a minute' : formatWindow(seconds);
}

const WAL_STATUS: Record<string, Fact['value']> = {
  reserved: 'Kept normally',
  extended: 'Kept beyond max_wal_size',
  unreserved: 'About to be removed',
  lost: 'Removed',
};

/** "3 d", "2 d 4 h", "26 h 5 min", "45 min". */
export function formatWindow(seconds: number): string {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  if (h >= 48) return `${Math.floor(h / 24)} d${h % 24 ? ` ${h % 24} h` : ''}`;
  return h ? `${h} h${m ? ` ${m} min` : ''}` : `${m} min`;
}

/**
 * Source protection in the source's own terms. Nothing unmeasured reads as fine: a missing value
 * is "Unknown", and a probe that measured nothing shows its summary under a grey pill.
 */
export function protectionView(sourceType: string, i: SourceImpact): ProtectionView {
  if (i.error) return { pill: { tone: 'grey', label: 'Unknown' }, facts: [], note: `Couldn’t measure: ${i.error}` };
  const known = (v: number | null | undefined): v is number => v !== null && v !== undefined;
  const measured = [i.retained_bytes, i.slot_active, i.log_status, i.window_seconds, i.position_kept, i.log_used_bytes, i.behind_seconds, i.retention_seconds].some(
    (v) => v !== null && v !== undefined,
  );
  const timeKept = sourceType in KEPT_LABEL;
  const pill: ProtectionView['pill'] = i.lost
    ? { tone: 'bad', label: 'Lost' }
    : i.risk
      ? { tone: 'risk', label: 'At risk' }
      : !measured
        ? { tone: 'grey', label: 'Not measured yet' }
        : sourceType === 'sqlserver' && i.position_kept !== true
          ? { tone: 'grey', label: 'Unknown' } // safe only once the position is known to be inside the window
          : timeKept && !(known(i.behind_seconds) && known(i.retention_seconds))
            ? { tone: 'grey', label: 'Unknown' } // safe only once both the position and the window are known
            : { tone: 'ok', label: 'OK' };
  const facts: Fact[] = [];
  let meterLabel: string | null = null;

  if (sourceType === 'postgresql') {
    if (i.slot) {
      const use = i.slot_active === true ? 'in use' : i.slot_active === false ? 'not in use' : measured ? undefined : 'not created yet';
      facts.push({ label: 'Replication slot', value: i.slot, note: use, mono: true });
    }
    if (measured) {
      const st = i.log_status ?? null;
      facts.push(
        st
          ? { label: 'WAL status', value: WAL_STATUS[st] ?? st, tone: st === 'lost' ? 'bad' : st === 'unreserved' ? 'risk' : undefined }
          : { label: 'WAL status', value: 'Unknown', note: 'not reported before PostgreSQL 13' },
      );
      facts.push(
        i.headroom_bytes !== null && i.headroom_bytes !== undefined
          ? { label: 'Room before the slot is lost', value: formatBytes(i.headroom_bytes), mono: true }
          : i.no_cap
            ? { label: 'Room before the slot is lost', value: 'No cap', note: 'max_slot_wal_keep_size is -1; WAL grows until the disk is full' }
            : { label: 'Room before the slot is lost', value: 'Unknown' },
      );
    }
    meterLabel = 'WAL kept for this pipeline';
  } else if (sourceType === 'mysql') {
    if (i.reading_file) {
      facts.push({ label: 'Reading binlog file', value: i.reading_file, mono: true, ...(i.lost ? { note: 'purged from the server', tone: 'bad' as const } : {}) });
    }
    if (i.log_status === 'oldest') facts.push({ label: 'Position', value: 'Oldest file kept', note: 'the next purge loses it', tone: 'risk' });
    else if (i.log_status === 'ok') facts.push({ label: 'Position', value: 'Safe from the next purge' });
    if (i.retained_bytes !== null && i.retained_bytes !== undefined) facts.push({ label: 'Not read yet', value: formatBytes(i.retained_bytes), mono: true });
    meterLabel = 'Binlog waiting for this pipeline';
  } else if (sourceType === 'mongodb') {
    if (i.window_seconds !== null && i.window_seconds !== undefined) {
      facts.push({ label: 'Oplog window', value: formatWindow(i.window_seconds), tone: i.risk ? 'risk' : undefined });
    } else if (i.log_status === 'filling') {
      facts.push({ label: 'Oplog window', value: 'Still filling', note: 'holds every change since it was created' });
    }
    if (i.retained_bytes !== null && i.retained_bytes !== undefined) {
      const room = i.headroom_bytes;
      facts.push(
        room !== null && room !== undefined
          ? { label: 'Oplog used', value: `${formatBytes(i.retained_bytes)} of ${formatBytes(i.retained_bytes + room)}`, note: 'shared by every reader' }
          : { label: 'Oplog used', value: formatBytes(i.retained_bytes), note: 'capacity unknown' },
      );
    }
  } else if (sourceType === 'sqlserver') {
    // Each fact once: the retention, where the pipeline is against it, and the log as one bar.
    if (measured) {
      facts.push(
        i.window_seconds !== null && i.window_seconds !== undefined
          ? { label: 'CDC keeps changes for', value: formatWindow(i.window_seconds) }
          : { label: 'CDC keeps changes for', value: 'Unknown', note: 'reading the cleanup job needs db_owner' },
      );
      facts.push(
        i.position_kept === true
          ? { label: 'Pipeline position', value: 'Inside that window' }
          : i.position_kept === false
            ? { label: 'Pipeline position', value: 'Cleaned up', note: 'changes it had not read were removed', tone: 'bad' }
            : { label: 'Pipeline position', value: 'Unknown', note: 'not stored yet, or the change tables are not readable' },
      );
    }
  } else if (timeKept) {
    if (measured) {
      facts.push({ label: KEPT_LABEL[sourceType] ?? 'Changes kept for', value: known(i.retention_seconds) ? formatWindow(i.retention_seconds) : 'Unknown' });
      facts.push(
        known(i.behind_seconds)
          ? { label: 'Pipeline position', value: `${formatSpan(i.behind_seconds)} behind`, tone: i.lost ? 'bad' : i.risk ? 'risk' : undefined }
          : { label: 'Pipeline position', value: 'Not stored yet' },
      );
    }
  } else if (i.retained_bytes !== null && i.retained_bytes !== undefined) {
    facts.push({ label: 'Log kept for this pipeline', value: formatBytes(i.retained_bytes), mono: true });
  }

  const used = i.retained_bytes;
  const limit = i.limit_bytes;
  const meter =
    meterLabel && used !== null && used !== undefined && limit
      ? (() => {
          const ratio = used / limit;
          return { label: meterLabel, ratio, text: `${formatBytes(used)} of ${formatBytes(limit)} limit`, tone: (ratio > 1 ? 'risk' : ratio >= 0.8 ? 'warn' : 'ok') as Tone };
        })()
      : undefined;
  const logUsed = i.log_used_bytes;
  const logSize = i.log_size_bytes;
  const logMeter =
    sourceType === 'sqlserver' && logUsed !== null && logUsed !== undefined && logSize
      ? (() => {
          const ratio = logUsed / logSize;
          const wait = i.log_status && i.log_status !== 'nothing' ? ` · waits on ${i.log_status.replaceAll('_', ' ')}` : '';
          return {
            label: 'Transaction log used',
            ratio,
            text: `${formatBytes(logUsed)} of ${formatBytes(logSize)}${wait}`,
            tone: (ratio >= 0.9 ? 'risk' : ratio >= 0.8 ? 'warn' : 'ok') as Tone,
          };
        })()
      : undefined;
  const behind = i.behind_seconds;
  const kept = i.retention_seconds;
  const windowMeter =
    timeKept && known(behind) && known(kept) && kept > 0
      ? {
          label: 'Window used',
          ratio: behind / kept,
          text: `${formatSpan(behind)} of ${formatWindow(kept)}`,
          tone: (behind >= kept ? 'risk' : behind * 5 >= kept * 4 ? 'warn' : 'ok') as Tone,
        }
      : undefined;
  return { pill, facts, meter: meter ?? logMeter ?? windowMeter, note: facts.length ? undefined : i.summary || 'Nothing measured yet.' };
}

export type RowIcon = 'shape' | 'delete' | 'fields' | 'format' | 'key' | TransformType;

/** One line of the Transforms card: what it does in plain words, with field names as code. */
export interface Row {
  icon: RowIcon;
  title: string;
  detail: string;
  code?: string[];
}

/** The deployed spec as what consumers receive (output) and the steps that change events on the way. */
export function transformView(spec: FullSpec, flink?: { sinkType?: string }, ctx: ShapeCtx = {}): { output: Row[]; steps: Row[] } {
  const output: Row[] = [];
  if (flink) return flinkTransformView(spec, flink.sinkType);
  const unwrap = spec.transforms.find((t) => t.type === 'unwrap');
  // As the compiler runs it: Keep deleted rows (or the destination) adds the row shape and decides deletes.
  const effective = effectiveDeletes(spec.transforms, spec.sink.options, { ...ctx, flink: false });
  if (spec.transforms.some((t) => t.type === 'outbox')) {
    output.push({ icon: 'outbox', title: 'Outbox events', detail: 'Each row added to the outbox table becomes one message' });
  } else if (effective.mode !== 'envelope') {
    output.push({ icon: 'shape', title: 'The row after the change', detail: 'Not the full change envelope' });
    const deletes = effective.mode;
    output.push(
      deletes === 'rewrite'
        ? { icon: 'delete', title: 'Deletes are kept', detail: 'As the last row, flagged', code: ['__deleted = true'] }
        : deletes === 'tombstone'
          ? { icon: 'delete', title: 'Deletes become tombstones', detail: 'An empty message with the row’s key' }
          : { icon: 'delete', title: 'Deletes are dropped', detail: 'Consumers never see them' },
    );
    const extra = unwrap ? (unwrap.add_fields ?? []) : spec.transforms.some((t) => t.type === 'soft_delete') ? ['op', 'source.ts_ms'] : (ctx.unwrap?.add_fields ?? []);
    if (extra.length) {
      output.push({
        icon: 'fields',
        title: extra.length === 1 ? 'One extra field' : `${extra.length} extra fields`,
        detail: extra.map((f) => METADATA.find((m) => m.value === f)?.label ?? f).join(' · '),
        code: extra.map(metadataField),
      });
    }
  } else {
    output.push({ icon: 'shape', title: 'The full change envelope', detail: 'The row before and after the change, and where it came from' });
  }
  const values = summarizeValues(spec.values);
  if (values) output.push({ icon: 'format', title: 'Value formats', detail: values.charAt(0).toUpperCase() + values.slice(1) });
  const keys = Object.entries(spec.keys ?? {});
  if (keys.length) {
    output.push({
      icon: 'key',
      title: keys.length === 1 ? 'Custom message key' : `Custom message keys on ${plural(keys.length, 'table')}`,
      detail: keys.map(([table, cols]) => `${table} by ${cols.join(', ')}`).join(' · '),
    });
  }
  const steps = spec.transforms
    .filter((t) => t.type !== 'unwrap' && t.type !== 'outbox')
    .map((t): Row => ({ icon: t.type, title: TYPE_TITLE[t.type], detail: summarize(t) }));
  return { output, steps };
}

/** A Flink CDC pipeline writes tables, not events: rows by key, columns left out, chosen keys. */
function flinkTransformView(spec: FullSpec, sinkType?: string): { output: Row[]; steps: Row[] } {
  // Keep deleted rows marks every delete (__op = -D), whatever the destination.
  const softDelete = spec.transforms.some((t) => t.type === 'soft_delete');
  const keepDeletes = softDelete || (sinkType === 'iceberg' && (spec.sink.options.write_mode ?? 'upsert_keep_deletes') === 'upsert_keep_deletes');
  const output: Row[] = [{ icon: 'shape', title: 'Tables kept in sync', detail: 'Each row is written by its primary key' }];
  output.push(
    keepDeletes
      ? { icon: 'delete', title: 'Deleted rows are kept', detail: 'Marked by their change type', code: ['__op = -D'] }
      : { icon: 'delete', title: 'Deletes remove the row', detail: 'The destination mirrors the source' },
  );
  const removed = Object.values(spec.source.columns).reduce((n, r) => n + (r.exclude?.length ?? 0), 0);
  if (removed) output.push({ icon: 'fields', title: removed === 1 ? 'One column left out' : `${removed} columns left out`, detail: 'Never written to the destination' });
  const keys = Object.entries(spec.keys ?? {});
  if (keys.length) {
    output.push({
      icon: 'key',
      title: keys.length === 1 ? 'Chosen primary key' : `Chosen primary keys on ${plural(keys.length, 'table')}`,
      detail: keys.map(([table, cols]) => `${table} by ${cols.join(', ')}`).join(' · '),
    });
  }
  const steps = spec.transforms.filter((t) => t.type !== 'unwrap').map((t): Row => ({ icon: t.type, title: TYPE_TITLE[t.type], detail: summarize(t) }));
  return { output, steps };
}

/** The latest value of a series (its last point, or null when that point is unknown). */
export function latest(values: (number | null)[]): number | null {
  return values.length ? (values[values.length - 1] ?? null) : null;
}

/** Latency for an axis or readout: tenths under 10 ms so ticks don't repeat ("0.5 ms", "1 ms"). */
export function formatLatency(ms: number): string {
  if (ms < 10) return `${Number(ms.toFixed(1))} ms`;
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const s = ms / 1000;
  if (s < 60) return `${Number(s.toFixed(s < 10 ? 1 : 0))} s`;
  if (s < 3600) return `${Math.round(s / 60)} min`;
  return `${Number((s / 3600).toFixed(1))} h`;
}

/** A Flink CDC cluster in a few facts: the job, its TaskManagers, its saves and the first copy. */
export interface FlinkView {
  job: string;
  taskmanagers: string | null;
  saved: string | null;
  restored: string | null;
  copy: { table: string; chunks: number }[] | null;
  problem: string | null;
}

const JOB_PHASE: Record<string, string> = {
  starting: 'Starting Flink',
  waiting: 'Waiting for TaskManagers',
  submitting: 'Submitting the pipeline',
  resubmitting: 'Submitting again after a start-up race',
  running: 'Running',
  restarting: 'Restarting after a failure',
  stopping: 'Saving the position and stopping',
  failed: 'Failed',
  config_error: 'Can’t start: the image or settings',
  finished: 'Finished (copy only)',
};

export function flinkView(f: FlinkStatus | undefined, agoOf: (iso: string) => string): FlinkView | null {
  if (!f) return null;
  const copying = f.phase === 'running' && !f.streaming;
  return {
    job: (JOB_PHASE[f.phase ?? ''] ?? f.phase ?? 'Unknown') + (f.phase === 'running' ? (f.streaming ? ' · streaming changes' : ' · copying existing rows') : ''),
    taskmanagers: f.taskmanagers_wanted ? `${f.taskmanagers ?? 0} of ${f.taskmanagers_wanted} connected` : null,
    saved: f.last_saved ? `${agoOf(f.last_saved.taken_at)}${f.checkpoints_failed ? ` · ${f.checkpoints_failed} failed saves` : ''}` : null,
    restored: f.restored_from ? `#${f.restored_from.id}, saved ${agoOf(f.restored_from.taken_at)}` : null,
    copy: copying && f.chunks ? Object.entries(f.chunks).map(([table, chunks]) => ({ table, chunks })).sort((a, b) => a.table.localeCompare(b.table)) : null,
    problem: f.store_error ? `Saving positions failed: ${f.store_error}` : null,
  };
}

/** What a custom Flink CDC cluster that shrinks after its copy is doing (artifacts.flink_shape). */
export interface ShapeView {
  /** "Copy shape" or "Streaming shape", with when it shrank. */
  text: string;
  /** Shrink now is offered (the "suggest" policy, the copy done). */
  ready: boolean;
  /** BeziSync stopped shrinking it after failed attempts. */
  problem: string | null;
}

type ShapeSize = { task_managers?: number; slots?: number; task_manager_cpu?: string; task_manager_memory?: string };

function shapeWords(s: ShapeSize | undefined): string {
  if (!s?.task_managers) return '';
  return ` · ${s.task_managers} TaskManager${s.task_managers === 1 ? '' : 's'} × ${s.slots ?? 0} slots, ${s.task_manager_cpu} CPU and ${s.task_manager_memory} each`;
}

export function shapeView(artifacts: Record<string, unknown> | null | undefined, agoOf: (iso: string) => string): ShapeView | null {
  const a = (artifacts ?? {}).flink_shape as
    | { phase?: string; size?: ShapeSize; streaming_size?: ShapeSize; shrunk_at?: string; shrink_ready?: boolean; shrink_off?: boolean; shrink_failures?: number }
    | undefined;
  if (!a?.streaming_size) return null;
  const streaming = a.phase === 'streaming';
  return {
    text: streaming
      ? `Streaming shape${a.shrunk_at ? `, shrank ${agoOf(a.shrunk_at)}` : ''}${shapeWords(a.streaming_size)}`
      : `Copy shape${a.shrink_off ? '' : a.shrink_ready ? ', ready to shrink' : ', shrinks after the first copy'}${shapeWords(a.size)}`,
    ready: !streaming && a.shrink_ready === true && !a.shrink_off,
    problem: a.shrink_off
      ? `The streaming shape failed ${a.shrink_failures ?? 0} times before it saved a position, so BeziSync keeps the copy shape. Give each reader more while streaming, then deploy.`
      : null,
  };
}
