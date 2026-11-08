import { keepPreviousData, useQueries, useQuery } from '@tanstack/react-query';
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
    queryFn: () => checkExpression(projectId, pipelineId, body!),
  });
}

function checkExpression(projectId: string, pipelineId: string, body: ExpressionCheckRequest) {
  return unwrap(
    client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/expressions:check', {
      params: { path: { project_id: projectId, pipeline_id: pipelineId } },
      body,
    }),
  );
}

/** Several expression checks at once (the key columns of a Flink CDC table, ADR 0010), in the
 *  order given; an entry is undefined until its answer arrives. */
export function useExpressionChecks(projectId: string, pipelineId: string, bodies: ExpressionCheckRequest[]): { data: ExpressionCheck | undefined; failed: boolean }[] {
  return useQueries({
    queries: bodies.map((body) => ({
      queryKey: ['expression-check', projectId, pipelineId, body],
      staleTime: 5 * 60_000,
      queryFn: () => checkExpression(projectId, pipelineId, body),
    })),
  }).map((q) => ({ data: q.data, failed: q.isError }));
}
