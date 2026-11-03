import { useQuery } from '@tanstack/react-query';
import { client, unwrap, type Schemas } from './api';

export type Policies = Schemas['ProjectPolicies'];

export const SIZES = ['small', 'medium', 'large'] as const;

/** Project guardrails; the wizard applies them before the server has to refuse anything. */
export function usePolicies(projectId: string) {
  return useQuery({
    queryKey: ['policies', projectId],
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/policies', { params: { path: { project_id: projectId } } })),
    staleTime: 30_000,
  });
}

export function allows(list: string[] | null | undefined, value: string): boolean {
  return list === null || list === undefined || list.includes(value);
}

export function sizeAllowed(policies: Policies | undefined, size: string): boolean {
  const max = policies?.max_resources;
  return !max || SIZES.indexOf(size as (typeof SIZES)[number]) <= SIZES.indexOf(max);
}
