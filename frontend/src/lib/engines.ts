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
 *  the first available choice (the backend's ChooseEngine). */
export function pickEngine(choices: Engine[], current: EngineId): EngineId {
  if (choices.some((e) => e.id === current && e.available)) return current;
  return choices.find((e) => e.available)?.id ?? current;
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
}

export function flinkChoices(raw: Partial<FlinkChoices> | null | undefined): FlinkChoices {
  return {
    schema_changes: raw?.schema_changes ?? 'apply',
    new_columns: raw?.new_columns ?? 'hold',
    freshness_seconds: raw?.freshness_seconds ?? 5,
    commit_seconds: raw?.commit_seconds ?? 60,
    flink_conf: raw?.flink_conf ?? {},
  };
}
