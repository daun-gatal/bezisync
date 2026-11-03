import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { fullSpec } from '@/features/pipelines/spec';
import { client, unwrap, type Schemas } from './api';

export type Template = Schemas['TemplateRead'];

const key = (projectId: string) => ['templates', projectId];

export function useTemplates(projectId: string) {
  return useQuery({
    queryKey: key(projectId),
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/templates', { params: { path: { project_id: projectId } } })),
  });
}

export function useCreateTemplate(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: Schemas['TemplateCreate']) =>
      unwrap(client.POST('/api/v1/projects/{project_id}/templates', { params: { path: { project_id: projectId } }, body })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: key(projectId) }),
  });
}

export function useDeleteTemplate(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) =>
      unwrap(client.DELETE('/api/v1/projects/{project_id}/templates/{template_id}', { params: { path: { project_id: projectId, template_id: id } } })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: key(projectId) }),
  });
}

/** One line describing what a template sets, for pickers and lists. */
export function templateSummary(t: Template): string {
  const spec = fullSpec(t.spec);
  const parts = [
    t.sink ? `to ${t.sink.name}` : 'any destination',
    spec.sink.format.type.toUpperCase(),
    `size ${spec.policies.resources}`,
    spec.snapshot.mode === 'initial' ? 'copies existing rows' : `snapshot ${spec.snapshot.mode}`,
  ];
  if (t.runtime) parts.push(`on ${t.runtime.name}`);
  return parts.join(' · ');
}
