import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { client, unwrap, type Schemas } from './api';

export type Connection = Schemas['ConnectionRead'];
export type ConnectorType = Schemas['ConnectorType'];
export type Kind = 'source' | 'sink';

export const connectionsKey = (projectId: string) => ['connections', projectId] as const;

export function useConnectorTypes(projectId: string) {
  return useQuery({
    queryKey: ['connector-types', projectId],
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/connector-types', { params: { path: { project_id: projectId } } })),
    staleTime: Infinity,
  });
}

export function useConnections(projectId: string) {
  return useQuery({
    queryKey: connectionsKey(projectId),
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/connections', { params: { path: { project_id: projectId } } })),
    enabled: Boolean(projectId),
  });
}

export function useCreateConnection(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: Schemas['ConnectionCreate']) =>
      unwrap(client.POST('/api/v1/projects/{project_id}/connections', { params: { path: { project_id: projectId } }, body })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: connectionsKey(projectId) }),
  });
}

export function useUpdateConnection(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, version, body }: { id: string; version: number; body: Schemas['ConnectionUpdate'] }) =>
      unwrap(
        client.PATCH('/api/v1/projects/{project_id}/connections/{connection_id}', {
          params: { path: { project_id: projectId, connection_id: id }, header: { 'if-match': `"${version}"` } },
          body,
        }),
      ),
    onSuccess: () => void qc.invalidateQueries({ queryKey: connectionsKey(projectId) }),
  });
}

export function useDeleteConnection(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) =>
      unwrap(client.DELETE('/api/v1/projects/{project_id}/connections/{connection_id}', { params: { path: { project_id: projectId, connection_id: id } } })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: connectionsKey(projectId) }),
  });
}

export function useCheckConnection(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, tables }: { id: string; tables?: string[] }) =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/connections/{connection_id}/check', {
          params: { path: { project_id: projectId, connection_id: id } },
          body: { tables: tables ?? null },
        }),
      ),
    onSuccess: () => void qc.invalidateQueries({ queryKey: connectionsKey(projectId) }),
  });
}

export function useDraftCheck(projectId: string) {
  return useMutation({
    mutationFn: (body: Schemas['DraftCheck']) =>
      unwrap(client.POST('/api/v1/projects/{project_id}/connections:check', { params: { path: { project_id: projectId } }, body })),
  });
}

export function useDiscovery(projectId: string, connectionId: string | undefined) {
  return useQuery({
    queryKey: ['discovery', projectId, connectionId],
    enabled: Boolean(connectionId),
    staleTime: 5 * 60_000,
    queryFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/connections/{connection_id}/discover', {
          params: { path: { project_id: projectId, connection_id: connectionId ?? '' } },
        }),
      ),
  });
}

export const TYPE_LABEL: Record<string, string> = {
  postgresql: 'PostgreSQL',
  mysql: 'MySQL / MariaDB',
  sqlserver: 'SQL Server',
  mongodb: 'MongoDB',
  oracle: 'Oracle',
  cockroachdb: 'CockroachDB',
  vitess: 'Vitess',
  spanner: 'Google Cloud Spanner',
  kafka: 'Kafka',
  http: 'HTTP',
  pubsub: 'Google Pub/Sub',
  kinesis: 'AWS Kinesis',
  redis: 'Redis Streams',
  nats: 'NATS JetStream',
  iceberg: 'Apache Iceberg',
  jdbc: 'Database (JDBC)',
  sqs: 'Amazon SQS',
  sns: 'Amazon SNS',
  eventhubs: 'Azure Event Hubs',
  pulsar: 'Apache Pulsar',
};

export type SinkCaps = Schemas['SinkCaps'];

/** What a sink needs from the pipeline; the first sinks' rules when connector-types has none. */
export const DEFAULT_SINK_CAPS: SinkCaps = {
  naming: {
    default: 'cdc.{schema}.{table}',
    pattern: '^[A-Za-z0-9._-]+$',
    max_length: 249,
    message: "Use letters, digits, '.', '_' and '-'.",
    flatten_dots: false,
    single: false,
  },
  formats: ['json', 'avro', 'protobuf', 'cloudevents'],
  unwrap: null,
  outbox: true,
  no_tombstones: false,
  time_precision: false,
  options_schema: null,
  min_resources: null,
  image: null,
};

/** How a source captures (snapshot modes, re-copies, canary); every option open when connector-types has none. */
export function captureOf(types: ConnectorType[] | undefined, type: string | undefined): CaptureCaps {
  return types?.find((t) => t.kind === 'source' && t.type === type)?.capture ?? DEFAULT_CAPTURE;
}

export type CaptureCaps = Schemas['CaptureCaps'];

export const DEFAULT_CAPTURE: CaptureCaps = {
  snapshot_modes: ['initial', 'no_data', 'when_needed', 'initial_only', 'always'],
  incremental: true,
  maturity: 'stable',
  canary: true,
};

export function sinkCapsOf(types: ConnectorType[] | undefined, type: string | undefined): SinkCaps {
  return types?.find((t) => t.kind === 'sink' && t.type === type)?.sink ?? DEFAULT_SINK_CAPS;
}
