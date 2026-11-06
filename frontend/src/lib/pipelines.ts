import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { client, unwrap, type Schemas } from './api';
import { useCursorPages } from './paging';

export type Pipeline = Schemas['PipelineRead'];
export type PipelineSpec = Schemas['PipelineSpec'];
export type Preflight = Schemas['PreflightReport'];
export type Version = Schemas['VersionRead'];

const key = (projectId: string) => ['pipelines', projectId] as const;
const one = (projectId: string, id: string) => ['pipeline', projectId, id] as const;
const path = (projectId: string, id: string) => ({ params: { path: { project_id: projectId, pipeline_id: id } } });

export function usePipelines(projectId: string) {
  return useQuery({
    queryKey: key(projectId),
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/pipelines', { params: { path: { project_id: projectId } } })),
    refetchInterval: 5_000,
  });
}

export function usePipeline(projectId: string, id: string | undefined) {
  return useQuery({
    queryKey: one(projectId, id ?? ''),
    enabled: Boolean(id),
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}', path(projectId, id ?? ''))),
    refetchInterval: (q) => (q.state.data && ['deploying', 'deleting', 'snapshotting'].includes(q.state.data.lifecycle) ? 2_000 : 5_000),
  });
}

function useInvalidate(projectId: string) {
  const qc = useQueryClient();
  return (p?: Pipeline) => {
    void qc.invalidateQueries({ queryKey: key(projectId) });
    // A save may update the draft version in place: what is cached per version number is stale too.
    if (p) for (const k of ['versions', 'config', 'diff']) void qc.invalidateQueries({ queryKey: [k, projectId, p.id] });
    if (p) qc.setQueryData(one(projectId, p.id), p);
  };
}

export function useCreatePipeline(projectId: string) {
  const done = useInvalidate(projectId);
  return useMutation({
    mutationFn: (body: Schemas['PipelineCreate']) =>
      unwrap(client.POST('/api/v1/projects/{project_id}/pipelines', { params: { path: { project_id: projectId } }, body })),
    onSuccess: done,
  });
}

export function useUpdatePipeline(projectId: string) {
  const done = useInvalidate(projectId);
  return useMutation({
    mutationFn: ({ pipeline, body }: { pipeline: Pipeline; body: Schemas['PipelineUpdate'] }) =>
      unwrap(
        client.PATCH('/api/v1/projects/{project_id}/pipelines/{pipeline_id}', {
          params: { path: { project_id: projectId, pipeline_id: pipeline.id }, header: { 'if-match': `"${pipeline.version}"` } },
          body,
        }),
      ),
    onSuccess: done,
  });
}

export function usePreflight(projectId: string) {
  return useMutation({
    mutationFn: ({ id, version }: { id: string; version?: number }) =>
      unwrap(client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/preflight', { ...path(projectId, id), body: { version: version ?? null } })),
  });
}

export function useDeploy(projectId: string) {
  const done = useInvalidate(projectId);
  return useMutation({
    mutationFn: ({ id, version, forceReason }: { id: string; version?: number; forceReason?: string }) =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/deploy', {
          ...path(projectId, id),
          body: { version: version ?? null, force_reason: forceReason ?? null },
        }),
      ),
    onSuccess: done,
  });
}

export function useLifecycle(projectId: string) {
  const done = useInvalidate(projectId);
  return useMutation({
    mutationFn: ({ id, action }: { id: string; action: 'pause' | 'resume' | 'restart' }) => {
      if (action === 'pause') return unwrap(client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/pause', path(projectId, id)));
      if (action === 'resume') return unwrap(client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/resume', path(projectId, id)));
      return unwrap(client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/restart', path(projectId, id)));
    },
    onSuccess: done,
  });
}

/** Shrink now: a custom Flink CDC cluster that finished its copy moves to its streaming shape. */
export function useShrinkCluster(projectId: string) {
  const done = useInvalidate(projectId);
  return useMutation({
    mutationFn: (id: string) => unwrap(client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/cluster:shrink', path(projectId, id))),
    onSuccess: done,
  });
}

export function useDeletePipeline(projectId: string) {
  const done = useInvalidate(projectId);
  return useMutation({
    mutationFn: ({ id, dropDestinations }: { id: string; dropDestinations: boolean }) =>
      unwrap(
        client.DELETE('/api/v1/projects/{project_id}/pipelines/{pipeline_id}', {
          params: { path: { project_id: projectId, pipeline_id: id }, query: { drop_destinations: dropDestinations } },
        }),
      ),
    onSuccess: done,
  });
}

export function useSnapshot(projectId: string) {
  const done = useInvalidate(projectId);
  return useMutation({
    mutationFn: ({ id, tables, ignoreWindow = false }: { id: string; tables: string[]; ignoreWindow?: boolean }) =>
      unwrap(client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/snapshots', { ...path(projectId, id), body: { tables, ignore_window: ignoreWindow } })),
    onSuccess: done,
  });
}

export function useVersions(projectId: string, id: string) {
  return useQuery({
    queryKey: ['versions', projectId, id],
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/versions', path(projectId, id))),
  });
}

export function useConfig(projectId: string, id: string, version?: number, enabled = true) {
  return useQuery({
    queryKey: ['config', projectId, id, version],
    enabled,
    queryFn: () =>
      unwrap(
        client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/config', {
          params: { path: { project_id: projectId, pipeline_id: id }, query: { version: version ?? null } },
        }),
      ),
  });
}

/** The connector's logs; a Flink CDC pipeline's of one component (jobmanager, taskmanager-<n>, tunnel). */
export function useLogs(projectId: string, id: string, enabled: boolean, component?: string) {
  return useQuery({
    queryKey: ['logs', projectId, id, component ?? ''],
    enabled,
    refetchInterval: 5_000,
    queryFn: () =>
      unwrap(
        client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/logs', {
          params: { path: { project_id: projectId, pipeline_id: id }, query: { tail: 1000, ...(component ? { component } : {}) } },
        }),
      ),
  });
}

export function useEvents(projectId: string, id: string, table: string | undefined, enabled: boolean) {
  return useQuery({
    queryKey: ['events', projectId, id, table],
    enabled: enabled && Boolean(table),
    refetchInterval: 5_000,
    queryFn: () =>
      unwrap(
        client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/events', {
          params: { path: { project_id: projectId, pipeline_id: id }, query: { table: table ?? null, limit: 100 } },
        }),
      ),
  });
}

/** A resource's activity, newest first, a page at a time (the newest page refreshes itself). */
export function useActivity(projectId: string, resourceId: string) {
  return useCursorPages(
    ['activity', projectId, resourceId],
    (cursor, limit) =>
      unwrap(
        client.GET('/api/v1/projects/{project_id}/events', {
          params: { path: { project_id: projectId }, query: { resource_id: resourceId, limit, cursor } },
        }),
      ),
    { refetchInterval: 10_000 },
  );
}

export function useMetrics(projectId: string, id: string, range: '1h' | '6h' | '24h' | '7d' | '30d', enabled: boolean) {
  return useQuery({
    queryKey: ['metrics', projectId, id, range],
    enabled,
    refetchInterval: 15_000,
    queryFn: () =>
      unwrap(
        client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/metrics', {
          params: { path: { project_id: projectId, pipeline_id: id }, query: { range } },
        }),
      ),
  });
}

export function useOffsets(projectId: string, id: string, enabled: boolean) {
  return useQuery({
    queryKey: ['offsets', projectId, id],
    enabled,
    refetchInterval: 10_000,
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/offsets', path(projectId, id))),
  });
}

export type ResetMode = 'resnapshot' | 'skip_to_now' | 'from_time';

export function useResetOffsets(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, mode, confirm, fromTime }: { id: string; mode: ResetMode; confirm: string; fromTime?: string }) =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/offsets/reset', {
          ...path(projectId, id),
          body: { mode, confirm, ...(mode === 'from_time' && fromTime ? { from_time: fromTime } : {}) },
        }),
      ),
    onSuccess: (_d, v) => {
      void qc.invalidateQueries({ queryKey: ['offsets', projectId, v.id] });
      void qc.invalidateQueries({ queryKey: ['positions', projectId, v.id] });
      void qc.invalidateQueries({ queryKey: ['pipeline', projectId, v.id] });
    },
  });
}

export type SavedPosition = Schemas['SavedPositionRead'];

/** A Flink CDC pipeline's saved positions, newest first. */
export function useSavedPositions(projectId: string, id: string, enabled: boolean) {
  return useQuery({
    queryKey: ['positions', projectId, id],
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/positions', path(projectId, id))),
    enabled,
    refetchInterval: 30_000,
  });
}

/** Copies a saved position so the paused pipeline resumes from it. */
export function useRestorePosition(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, positionId, confirm }: { id: string; positionId: number; confirm: string }) =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/positions/{position_id}/restore', {
          params: { path: { project_id: projectId, pipeline_id: id, position_id: positionId } },
          body: { confirm },
        }),
      ),
    onSuccess: (_d, v) => {
      void qc.invalidateQueries({ queryKey: ['positions', projectId, v.id] });
      void qc.invalidateQueries({ queryKey: ['pipeline', projectId, v.id] });
    },
  });
}

export function useDiff(projectId: string, id: string, from: number | undefined, to: number | undefined) {
  return useQuery({
    queryKey: ['diff', projectId, id, from, to],
    enabled: from !== undefined && to !== undefined && from !== to,
    queryFn: () =>
      unwrap(
        client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/diff', {
          params: { path: { project_id: projectId, pipeline_id: id }, query: { from: from ?? 0, to: to ?? 0 } },
        }),
      ),
  });
}

export type ChangeRequest = Schemas['ChangeRequestRead'];

/** Approve, reject or cancel a deploy waiting for approval. */
export function useDecideChange(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, action, comment = '' }: { id: string; action: 'approve' | 'reject' | 'cancel'; comment?: string }) => {
      const params = { params: { path: { project_id: projectId, request_id: id } } };
      if (action === 'cancel') return unwrap(client.POST('/api/v1/projects/{project_id}/change-requests/{request_id}/cancel', params));
      if (action === 'approve') return unwrap(client.POST('/api/v1/projects/{project_id}/change-requests/{request_id}/approve', { ...params, body: { comment } }));
      return unwrap(client.POST('/api/v1/projects/{project_id}/change-requests/{request_id}/reject', { ...params, body: { comment } }));
    },
    onSuccess: () => void qc.invalidateQueries({ queryKey: key(projectId) }),
  });
}

export type SchemaState = Schemas['SchemaRead'];

export function useSchema(projectId: string, id: string) {
  return useQuery({
    queryKey: [...key(projectId), id, 'schema'],
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/schema', path(projectId, id))),
    refetchInterval: 30_000,
  });
}

export function useReviewSchema(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => unwrap(client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/schema/review', path(projectId, id))),
    onSuccess: () => void qc.invalidateQueries({ queryKey: key(projectId) }),
  });
}
