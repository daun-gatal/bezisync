import { useQuery } from '@tanstack/react-query';
import { client, unwrap, type Schemas } from '@/lib/api';

export type EngineId = Schemas['EngineRead']['id'];
export type Engine = Schemas['EngineRead'];

export const ENGINE_TITLE: Record<EngineId, string> = { debezium: 'Debezium', 'flink-cdc': 'Flink CDC' };

export const enginesKey = ['engines'] as const;
export const engineCatalogKey = (engine: EngineId) => ['debezium-catalog', engine] as const;

/** The engines and whether each can run pipelines (Flink CDC once an Admin registers an image). */
export function useEngines() {
  return useQuery({ queryKey: enginesKey, queryFn: () => unwrap(client.GET('/api/v1/engines')), staleTime: 60_000 });
}

/** One engine's image catalog (Settings → Engine versions). */
export function useEngineCatalog(engine: EngineId) {
  return useQuery({
    queryKey: engineCatalogKey(engine),
    queryFn: () => unwrap(client.GET('/api/v1/debezium-catalog', { params: { query: { engine } } })),
    staleTime: 60_000,
  });
}

/** A connector type only some engines can write, and whether one of them is available now. */
export function engineGate(t: Schemas['ConnectorType']): { engines: EngineId[]; available: boolean; reason: string } | null {
  if (!t.engines?.length) return null;
  return { engines: t.engines as EngineId[], available: t.available !== false, reason: t.unavailable_reason ?? '' };
}

/** Does the engine run a pipeline from sourceType to sinkType (the backend's engine.Supports)? */
export function runs(e: Engine, sourceType: string, sinkType?: string): boolean {
  const sourceOk = !e.sources || e.sources.includes(sourceType);
  if (!sinkType) return sourceOk;
  const sinkOk = e.sinks ? e.sinks.includes(sinkType) : !(e.except_sinks ?? []).includes(sinkType);
  return sourceOk && sinkOk;
}

/** The engines that can run the pair, in the order they are offered (Debezium first). */
export function engineChoices(engines: Engine[] | undefined, sourceType: string, sinkType: string): Engine[] {
  return (engines ?? []).filter((e) => runs(e, sourceType, sinkType));
}

/** The engine a pipeline gets for the pair: the current one while it still fits and is on, else
 *  the first available choice, an engine the project allows before one it doesn't (the backend's
 *  ChooseEngine). */
export function pickEngine(choices: Engine[], current: EngineId, policy?: EnginePolicy): EngineId {
  const on = choices.filter((e) => e.available);
  const allowed = on.filter((e) => engineAllowed(policy, e.id));
  for (const list of [allowed, on]) {
    if (list.some((e) => e.id === current)) return current;
    if (list.length) return list[0]!.id;
  }
  return current;
}

/** What each engine is for, in a line. */
export const ENGINE_PITCH: Record<EngineId, string> = {
  debezium: 'Streams every change as an event, as it happens. The default.',
  'flink-cdc': 'Keeps tables in sync: copies big tables in parallel chunks, upserts by key, and keeps saved positions you can restore.',
};

/** Flink CDC choices of a pipeline, with the defaults filled in (backend FlinkCDCSpec.Effective). */
export interface FlinkChoices {
  schema_changes: 'apply' | 'ignore' | 'stop';
  new_columns: 'hold' | 'send';
  freshness_seconds: 2 | 5 | 30;
  commit_seconds: 30 | 60 | 300;
  flink_conf: Record<string, string>;
  /** A custom cluster (with policies.resources: custom). */
  cluster?: FlinkCluster;
}

export type FlinkCluster = Schemas['FlinkCluster'];

export function flinkChoices(raw: Partial<FlinkChoices> | null | undefined): FlinkChoices {
  return {
    schema_changes: raw?.schema_changes ?? 'apply',
    new_columns: raw?.new_columns ?? 'hold',
    freshness_seconds: raw?.freshness_seconds ?? 5,
    commit_seconds: raw?.commit_seconds ?? 60,
    flink_conf: raw?.flink_conf ?? {},
    ...(raw?.cluster ? { cluster: raw.cluster } : {}),
  };
}

/** A cluster in the order the server dumps it (domain dumpCluster). */
function clusterBlock(c: FlinkCluster): FlinkCluster {
  const tm = c.task_managers;
  return {
    parallelism: c.parallelism,
    slot: { cpu: c.slot.cpu, memory: c.slot.memory },
    ...(c.streaming_slot ? { streaming_slot: { cpu: c.streaming_slot.cpu, memory: c.streaming_slot.memory } } : {}),
    job_manager: { cpu: c.job_manager.cpu, memory: c.job_manager.memory },
    shape: c.shape ?? 'auto',
    ...(tm ? { task_managers: { count: tm.count, slots: tm.slots, cpu: tm.cpu, memory: tm.memory } } : {}),
  };
}

/**
 * The spec's flink_cdc block as the server stores and returns it (domain dumpFlinkCDC): the
 * choices in that order, flink_conf only when it holds settings. Saving any other
 * shape makes the saved spec never equal the draft, so autosave would never settle. Keys are in
 * JSONB order (om.JSONBKeys): shorter first, then bytewise.
 */
export function flinkSpecBlock(c: FlinkChoices): Partial<FlinkChoices> {
  const keys = Object.keys(c.flink_conf).sort((a, b) => a.length - b.length || (a < b ? -1 : a > b ? 1 : 0));
  return {
    schema_changes: c.schema_changes,
    new_columns: c.new_columns,
    freshness_seconds: c.freshness_seconds,
    commit_seconds: c.commit_seconds,
    ...(keys.length ? { flink_conf: Object.fromEntries(keys.map((k) => [k, c.flink_conf[k]!])) } : {}),
    ...(c.cluster ? { cluster: clusterBlock(c.cluster) } : {}),
  };
}

/** The engine part of a project's policies, as the wizard reads it. Unreadable policies (or none
 *  loaded yet) decide nothing here: the server refuses saving with its own message. */
export interface EnginePolicy {
  allowed_engines?: string[] | null;
  unreadable?: boolean;
}

/** Does the project allow pipelines on the engine (domain.EngineViolation)? */
export function engineAllowed(policy: EnginePolicy | undefined, id: EngineId): boolean {
  if (!policy || policy.unreadable) return true;
  return policy.allowed_engines === null || policy.allowed_engines === undefined || policy.allowed_engines.includes(id);
}

/** Whether a destination type can be used for a pipeline from sourceType (ADR 0011):
 *  - ok: an allowed, available engine writes it (`engines` are those, in offer order);
 *  - unavailable: allowed engines write it but none is on (an Admin can fix that: `why` says how);
 *  - hidden: no engine runs the pair, or the project's policy allows none of those that do. */
export type SinkFit = { state: 'ok'; engines: Engine[] } | { state: 'unavailable'; why: string } | { state: 'hidden'; why: string };

export function sinkFit(engines: Engine[] | undefined, sourceType: string, sinkType: string, policy?: EnginePolicy): SinkFit {
  // Before /engines answers nothing is judged (the step waits for it).
  if (!engines) return { state: 'ok', engines: [] };
  const choices = engineChoices(engines, sourceType, sinkType);
  if (!choices.length) return { state: 'hidden', why: 'no engine runs this source to them' };
  const allowed = choices.filter((e) => engineAllowed(policy, e.id));
  if (!allowed.length) {
    return { state: 'hidden', why: `this project doesn’t allow ${choices.map((e) => ENGINE_TITLE[e.id]).join(' or ')}, which ${choices.length === 1 ? 'writes' : 'write'} them` };
  }
  const on = allowed.filter((e) => e.available);
  if (!on.length) return { state: 'unavailable', why: allowed.find((e) => e.reason)?.reason ?? `${ENGINE_TITLE[allowed[0]!.id]} isn’t available.` };
  return { state: 'ok', engines: on };
}

/** Does any engine the project allows read the source? (Debezium reads every source; a project
 *  that allows only Flink CDC can't use the others.) */
export function sourceFit(engines: Engine[] | undefined, sourceType: string, policy?: EnginePolicy): string | null {
  if (!engines) return null;
  const readers = engines.filter((e) => runs(e, sourceType));
  if (readers.some((e) => engineAllowed(policy, e.id))) return null;
  const allowed = engines.filter((e) => engineAllowed(policy, e.id)).map((e) => ENGINE_TITLE[e.id]);
  return allowed.length ? `This project allows only ${allowed.join(' and ')}, which can’t read this source.` : 'This project allows no engine.';
}

/** Does the engine write the destination as change events rather than as tables (ADR 0011)?
 *  Debezium always does; Flink CDC only for its event sinks (Kafka). */
export function writesEvents(engines: Engine[] | undefined, id: EngineId, sinkType: string | undefined): boolean {
  if (id !== 'flink-cdc') return true;
  const e = engines?.find((x) => x.id === id);
  // Until /engines answers: Kafka, the one event sink the backend lists.
  return (e?.event_sinks ?? ['kafka']).includes(sinkType ?? '');
}

/** What an engine is for on this destination, in a line. */
export function enginePitch(id: EngineId, events: boolean): string {
  if (id === 'flink-cdc' && events) return 'Streams every change as a Debezium-style JSON event: copies big tables in parallel chunks and keeps saved positions you can restore.';
  return ENGINE_PITCH[id];
}

/** The destinations the wizard leaves out (sinkFit "hidden"), one line per reason with a few
 *  names, so "where is my connection?" has an answer. */
export function hiddenLines(hidden: { name: string; why: string }[]): string[] {
  const byWhy = new Map<string, string[]>();
  for (const h of hidden) byWhy.set(h.why, [...(byWhy.get(h.why) ?? []), h.name]);
  return [...byWhy].map(([why, names]) => {
    const shown = names.slice(0, 3).join(', ');
    return `${names.length > 3 ? `${shown} and ${names.length - 3} more` : shown}: ${why}.`;
  });
}
