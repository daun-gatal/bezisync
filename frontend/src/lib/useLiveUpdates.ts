import { useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';

/**
 * Subscribe to the project's server-sent event stream and refresh pipeline queries the
 * moment the controller writes a new status. Polling stays as a fallback.
 */
export function useLiveUpdates(projectId: string): void {
  const qc = useQueryClient();
  useEffect(() => {
    if (typeof EventSource === 'undefined') return;
    const source = new EventSource(`/api/v1/projects/${projectId}/stream`, { withCredentials: true });
    source.addEventListener('pipeline', (event) => {
      try {
        const data = JSON.parse((event as MessageEvent<string>).data) as { id: string };
        void qc.invalidateQueries({ queryKey: ['pipelines', projectId] });
        void qc.invalidateQueries({ queryKey: ['pipeline', projectId, data.id] });
      } catch {
        // ignore malformed events
      }
    });
    return () => source.close();
  }, [projectId, qc]);
}
