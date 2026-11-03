import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { client, unwrap, type Schemas } from './api';

export type TransformPreview = Schemas['TransformPreview'];
export type PreviewRequest = Schemas['TransformPreviewRequest'];

/** Runs the steps on a sample event (pure; no source or destination access). */
export function useTransformPreview(projectId: string, pipelineId: string, body: PreviewRequest | null) {
  return useQuery({
    queryKey: ['transform-preview', projectId, pipelineId, body],
    enabled: Boolean(body),
    placeholderData: keepPreviousData,
    staleTime: 60_000,
    queryFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/transforms:preview', {
          params: { path: { project_id: projectId, pipeline_id: pipelineId } },
          body: body!,
        }),
      ),
  });
}
