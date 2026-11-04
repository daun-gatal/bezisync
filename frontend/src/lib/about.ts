import { useQuery } from '@tanstack/react-query';
import { client, unwrap } from './api';

/** The public source repository. */
export const REPO_URL = 'https://github.com/daun-gatal/bezisync';

/** Licences and notices: the web UI's (served by the web image) and the server binary's. */
export const WEB_LICENSES_URL = '/licenses.txt';
export const SERVER_LICENSES_URL = '/api/v1/system/licenses';

/** The server's version and settings; fixed for the life of the page. */
export function useSystemInfo() {
  return useQuery({ queryKey: ['system-info'], queryFn: () => unwrap(client.GET('/api/v1/system/info')), staleTime: Infinity });
}
