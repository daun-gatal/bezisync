import { Pill, type Tone } from '@/components/ui/feedback';
import type { Pipeline } from './pipelines';

/** Status older than this is shown as stale (the controller samples every few seconds). */
export const STALE_AFTER_MS = 60_000;

export interface StatusReason {
  code: string;
  message: string;
  fix?: string;
  severity?: 'info' | 'warn' | 'error';
}

export interface FlinkSaved {
  id: number;
  kind: 'checkpoint' | 'savepoint';
  reason: string;
  taken_at: string;
  size: number;
}

export interface FlinkComponent {
  name: string;
  role: 'jobmanager' | 'taskmanager' | 'tunnel' | string;
  state: string;
  restarts: number;
  ready?: boolean;
  node?: string;
}

export interface FlinkStatus {
  phase?: string;
  job_state?: string;
  streaming?: boolean;
  taskmanagers?: number;
  taskmanagers_wanted?: number;
  checkpoints_completed?: number;
  checkpoints_failed?: number;
  restarts?: number;
  last_checkpoint_at?: string;
  last_saved?: FlinkSaved;
  restored_from?: FlinkSaved;
  store_error?: string;
  /** Copy chunks each table's readers reached (by spec table). */
  chunks?: Record<string, number>;
  components?: FlinkComponent[];
}

export interface PipelineStatus {
  lifecycle?: string;
  health?: string;
  summary?: string;
  reasons?: StatusReason[];
  as_of?: string;
  deployed_version?: number | null;
  workload?: { phase?: string | null; restarts?: number | null; started_at?: string | null; reason?: string | null };
  metrics?: {
    connected?: boolean;
    snapshot_running?: boolean;
    snapshot_completed?: boolean;
    snapshot_tables_total?: number;
    snapshot_tables_remaining?: number;
    snapshot_rows_scanned?: number;
    ms_behind_source?: number;
    ms_since_last_event?: number;
    events_seen_streaming?: number;
    events_seen_snapshot?: number;
    queue_remaining?: number;
    queue_total?: number;
  } | null;
  forced?: boolean;
  events_per_second?: number | null;
  /** Flink CDC pipelines: what the helper beside the JobManager reported. */
  flink?: FlinkStatus;
  /** Delivery canary (pipelines that opt in). */
  canary?: {
    ok?: boolean;
    verified_at?: string;
    latency_ms?: number;
    sent_at?: string;
    pending?: { token: string; sent_at: string };
    error?: string;
  } | null;
  source_impact?: SourceImpact | null;
}

/** What the pipeline costs its source (backend connectors.SourceImpact). Unmeasured fields are null. */
export interface SourceImpact {
  at: string;
  retained_bytes?: number | null;
  slot_active?: boolean | null;
  log_status?: string | null;
  headroom_bytes?: number | null;
  /** The pipeline's source_retained_limit_gb policy in bytes (PostgreSQL, MySQL). */
  limit_bytes?: number | null;
  /** PostgreSQL 13+ with max_slot_wal_keep_size = -1: the slot is never invalidated. */
  no_cap?: boolean;
  slot?: string | null;
  reading_file?: string | null;
  window_seconds?: number | null;
  /** SQL Server: whether the changes after the stored position are still kept (false: cleaned up). */
  position_kept?: boolean | null;
  /** SQL Server's transaction log, shared by the whole database. */
  log_used_bytes?: number | null;
  log_size_bytes?: number | null;
  /** Time-kept histories: how far the stored position trails the source, and how far back the source keeps changes. */
  behind_seconds?: number | null;
  retention_seconds?: number | null;
  summary?: string;
  risk?: string | null;
  risk_fix?: string;
  lost?: boolean;
  error?: string;
}

export function statusOf(p: Pipeline): PipelineStatus {
  return (p.status ?? {}) as PipelineStatus;
}

export function isStale(p: Pipeline, now = Date.now()): boolean {
  const asOf = statusOf(p).as_of;
  if (!asOf || !p.deployed_version || ['paused', 'deleted', 'draft'].includes(p.lifecycle)) return false;
  return now - Date.parse(asOf) > STALE_AFTER_MS;
}

export const LIFECYCLE: Record<string, { label: string; tone: Tone }> = {
  draft: { label: 'Draft', tone: 'grey' },
  deploying: { label: 'Deploying', tone: 'info' },
  snapshotting: { label: 'Snapshotting', tone: 'info' },
  streaming: { label: 'Streaming', tone: 'info' },
  paused: { label: 'Paused', tone: 'grey' },
  failed: { label: 'Failed', tone: 'bad' },
  deleting: { label: 'Deleting', tone: 'grey' },
  deleted: { label: 'Deleted', tone: 'grey' },
  cleanup_failed: { label: 'Cleanup failed', tone: 'bad' },
};

export const HEALTH: Record<string, { label: string; tone: Tone }> = {
  healthy: { label: 'Healthy', tone: 'ok' },
  lagging: { label: 'Lagging', tone: 'warn' },
  at_risk: { label: 'At risk', tone: 'risk' },
  down: { label: 'Down', tone: 'bad' },
  unknown: { label: 'Unknown', tone: 'grey' },
};

export function snapshotProgress(p: Pipeline): string | null {
  const m = statusOf(p).metrics;
  if (!m?.snapshot_running || !m.snapshot_tables_total) return null;
  const done = m.snapshot_tables_total - (m.snapshot_tables_remaining ?? 0);
  return `${done}/${m.snapshot_tables_total} tables`;
}

export function LifecycleBadge({ pipeline }: { pipeline: Pipeline }) {
  const l = LIFECYCLE[pipeline.lifecycle] ?? { label: pipeline.lifecycle, tone: 'grey' as Tone };
  const progress = pipeline.lifecycle === 'snapshotting' ? snapshotProgress(pipeline) : null;
  return <Pill tone={l.tone}>{progress ? `${l.label} ${progress}` : l.label}</Pill>;
}

export function HealthBadge({ pipeline }: { pipeline: Pipeline }) {
  if (isStale(pipeline)) return <Pill tone="grey">Stale</Pill>;
  const h = HEALTH[pipeline.health] ?? HEALTH.unknown!;
  return <Pill tone={h.tone}>{h.label}</Pill>;
}

export function needsAttention(p: Pipeline): boolean {
  return ['failed', 'cleanup_failed'].includes(p.lifecycle) || ['lagging', 'at_risk', 'down'].includes(p.health) || isStale(p);
}

/** The stripe on a pipeline's row: only rows that need attention get one. */
export function pipelineTone(p: Pipeline): Tone | null {
  if (['failed', 'cleanup_failed'].includes(p.lifecycle) || p.health === 'down') return 'bad';
  if (p.health === 'at_risk') return 'risk';
  if (p.health === 'lagging') return 'warn';
  if (isStale(p)) return 'grey';
  return null;
}

/** Where a pipeline sits in the fleet health bar. Every pipeline is in exactly one group. */
export type FleetGroup = 'down' | 'lagging' | 'nosignal' | 'starting' | 'healthy' | 'paused' | 'draft';

export const FLEET: { id: FleetGroup; label: string; color: string }[] = [
  { id: 'healthy', label: 'Healthy', color: 'bg-ok' },
  { id: 'lagging', label: 'Lagging or at risk', color: 'bg-warn' },
  { id: 'down', label: 'Down or failed', color: 'bg-bad' },
  { id: 'nosignal', label: 'No recent signal', color: 'bg-line-strong' },
  { id: 'starting', label: 'Starting or stopping', color: 'bg-accent' },
  { id: 'paused', label: 'Paused', color: 'bg-grey' },
  // Hatched: not running yet, distinct from every status colour.
  { id: 'draft', label: 'Draft', color: 'bg-[repeating-linear-gradient(135deg,var(--bz-line-strong)_0_3px,transparent_3px_6px)]' },
];

export function fleetGroup(p: Pipeline): FleetGroup {
  if (p.lifecycle === 'draft') return 'draft';
  if (p.lifecycle === 'paused') return 'paused';
  if (['failed', 'cleanup_failed'].includes(p.lifecycle) || p.health === 'down') return 'down';
  if (['deploying', 'deleting'].includes(p.lifecycle)) return 'starting';
  if (isStale(p)) return 'nosignal';
  if (p.health === 'lagging' || p.health === 'at_risk') return 'lagging';
  if (p.health === 'healthy') return 'healthy';
  return 'nosignal';
}
