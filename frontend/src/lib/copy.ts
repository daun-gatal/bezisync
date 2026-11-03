import { useMutation, useQueryClient } from '@tanstack/react-query';
import { client, unwrap, type Schemas } from './api';
import { connectionsKey, type Connection } from './connections';
import { useProject } from './session';

/** Copying connections and secret stores to another project (never a move). */
export type CopyPlan = Schemas['CopyPlanRead'];
export type CopyPlanItem = Schemas['CopyPlanItemRead'];
export type CopyRequest = Schemas['CopyConnectionsRequest'];
export type StoreChoice = Schemas['StoreChoice'];
export type SecretsChoice = 'leave' | 'copy';

/** Projects the viewer may copy into: every other project where they manage connections (stores: Admins). */
export function useCopyTargets(what: 'connections' | 'stores' = 'connections') {
  const { me, project } = useProject();
  const perm = what === 'stores' ? 'project.manage' : 'connection.manage';
  return me.projects.filter((p) => p.id !== project.id && p.permissions.includes(perm));
}

/** Saved (not referenced) secret fields: what "Copy them" would carry over. */
export function savedSecrets(c: Connection): string[] {
  const refs = c.secret_refs ?? {};
  return c.secret_fields_set.filter((f) => !(f in refs));
}

/** "Needs its password" / "Needs token, key": what a copied connection still lacks. */
export function missingLabel(fields: string[]): string {
  if (fields.length === 1 && fields[0] === 'password') return 'Needs its password';
  return `Needs ${fields.join(', ')}`;
}

export function useCopyConnections(projectId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: CopyRequest) =>
      unwrap(client.POST('/api/v1/projects/{project_id}/connections:copy', { params: { path: { project_id: projectId } }, body })),
    onSuccess: (plan, body) => {
      if (!body.dry_run) {
        void qc.invalidateQueries({ queryKey: connectionsKey(plan.target_project_id) });
        void qc.invalidateQueries({ queryKey: ['secret-stores', plan.target_project_id] });
      }
    },
  });
}
