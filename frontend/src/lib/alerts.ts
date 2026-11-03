import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { client, unwrap, type Schemas } from './api';

export type Alert = Schemas['AlertRead'];
export type Rule = Schemas['RuleRead'];
export type Channel = Schemas['ChannelRead'];

const p = (projectId: string) => ({ params: { path: { project_id: projectId } } });

export function useAlerts(projectId: string, state: 'open' | 'resolved' | 'all' = 'open', pipelineId?: string) {
  return useQuery({
    queryKey: ['alerts', projectId, state, pipelineId],
    refetchInterval: 10_000,
    queryFn: () =>
      unwrap(client.GET('/api/v1/projects/{project_id}/alerts', { params: { path: { project_id: projectId }, query: { state, pipeline_id: pipelineId ?? null } } })),
  });
}

export function useAcknowledge(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => unwrap(client.POST('/api/v1/projects/{project_id}/alerts/{alert_id}/acknowledge', { params: { path: { project_id: projectId, alert_id: id } } })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['alerts', projectId] }),
  });
}

export function useRules(projectId: string) {
  return useQuery({ queryKey: ['alert-rules', projectId], queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/alert-rules', p(projectId))) });
}

export function useUpdateRule(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, body }: { id: string; body: Schemas['RuleUpdate'] }) =>
      unwrap(client.PATCH('/api/v1/projects/{project_id}/alert-rules/{rule_id}', { params: { path: { project_id: projectId, rule_id: id } }, body })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['alert-rules', projectId] }),
  });
}

export function useChannels(projectId: string) {
  return useQuery({ queryKey: ['alert-channels', projectId], queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/alert-channels', p(projectId))) });
}

export function useChannelKinds(projectId: string) {
  return useQuery({
    queryKey: ['alert-channel-kinds', projectId],
    staleTime: Infinity,
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/alert-channel-kinds', p(projectId))),
  });
}

export function useCreateChannel(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: Schemas['ChannelWrite']) => unwrap(client.POST('/api/v1/projects/{project_id}/alert-channels', { ...p(projectId), body })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['alert-channels', projectId] }),
  });
}

export function useUpdateChannel(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, body }: { id: string; body: Schemas['ChannelUpdate'] }) =>
      unwrap(client.PATCH('/api/v1/projects/{project_id}/alert-channels/{channel_id}', { params: { path: { project_id: projectId, channel_id: id } }, body })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['alert-channels', projectId] }),
  });
}

export function useDeleteChannel(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => unwrap(client.DELETE('/api/v1/projects/{project_id}/alert-channels/{channel_id}', { params: { path: { project_id: projectId, channel_id: id } } })),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ['alert-channels', projectId] });
      void qc.invalidateQueries({ queryKey: ['alert-rules', projectId] });
    },
  });
}

export function useTestChannel(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => unwrap(client.POST('/api/v1/projects/{project_id}/alert-channels/{channel_id}/test', { params: { path: { project_id: projectId, channel_id: id } } })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['alert-channels', projectId] }),
  });
}

export function useInbox(projectId: string) {
  return useQuery({ queryKey: ['inbox', projectId], refetchInterval: 15_000, queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/notifications', p(projectId))) });
}

export function useReadAll(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => unwrap(client.POST('/api/v1/projects/{project_id}/notifications/read-all', p(projectId))),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['inbox', projectId] }),
  });
}
