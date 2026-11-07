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

export type ExpressionCheck = Schemas['ExpressionCheck'];
export type ExpressionCheckRequest = Schemas['ExpressionCheckRequest'];

/** Checks a Flink CDC expression as typed (ADR 0008): its canonical text and kind, or why it is
 *  refused and where. Pure on the server; refetched as the text changes (the caller debounces). */
export function useExpressionCheck(projectId: string, pipelineId: string, body: ExpressionCheckRequest | null) {
  return useQuery({
    queryKey: ['expression-check', projectId, pipelineId, body],
    enabled: Boolean(body?.expression.trim()),
    staleTime: 5 * 60_000,
    queryFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/expressions:check', {
          params: { path: { project_id: projectId, pipeline_id: pipelineId } },
          body: body!,
        }),
      ),
  });
}
