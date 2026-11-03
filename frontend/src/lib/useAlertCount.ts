import { useAlerts } from './alerts';

/** Number of firing (unacknowledged) alerts in a project, for the navigation badge. */
export function useAlertCount(projectId: string): number {
  const alerts = useAlerts(projectId, 'open');
  return (alerts.data ?? []).filter((a) => a.state === 'firing').length;
}
