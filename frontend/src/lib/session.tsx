import { createContext, useContext, type ReactNode } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { client, unwrap, type Schemas } from './api';

export type Me = Schemas['MeRead'];
export type ProjectRef = Schemas['ProjectRef'];
export type Permission = string;

export const meKey = ['me'] as const;

export function useMeQuery() {
  return useQuery({
    queryKey: meKey,
    queryFn: () => unwrap(client.GET('/api/v1/auth/me')),
    retry: false,
    staleTime: 60_000,
  });
}

/** The sign-in options and whether this install is an open lab; fixed for the life of the page. */
export function useProviders() {
  return useQuery({ queryKey: ['auth-providers'], queryFn: () => unwrap(client.GET('/api/v1/auth/providers')), staleTime: Infinity });
}

export function useLogin() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: { email: string; password: string }) => unwrap(client.POST('/api/v1/auth/login', { body })),
    onSuccess: (me) => qc.setQueryData(meKey, me),
  });
}

export function useLogout() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => unwrap(client.POST('/api/v1/auth/logout')),
    onSettled: () => {
      qc.clear();
      window.location.assign('/login');
    },
  });
}

export function useUpdateMe() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (body: Schemas['MeUpdate']) => unwrap(client.PATCH('/api/v1/auth/me', { body })),
    onSuccess: (me) => qc.setQueryData(meKey, me),
  });
}

interface ProjectContextValue {
  me: Me;
  project: ProjectRef;
  can: (permission: Permission) => boolean;
  expert: boolean;
}

const ProjectContext = createContext<ProjectContextValue | null>(null);

export function ProjectProvider({ me, project, children }: { me: Me; project: ProjectRef; children: ReactNode }) {
  const perms = new Set(project.permissions);
  return (
    <ProjectContext.Provider
      value={{ me, project, can: (p) => perms.has(p), expert: me.user.ui_mode === 'expert' }}
    >
      {children}
    </ProjectContext.Provider>
  );
}

export function useProject(): ProjectContextValue {
  const value = useContext(ProjectContext);
  if (!value) throw new Error('useProject must be used inside a project route');
  return value;
}
