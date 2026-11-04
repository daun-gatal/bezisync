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
