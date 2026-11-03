import { useQuery } from '@tanstack/react-query';
import { client, unwrap, type Schemas } from '@/lib/api';

export type SecretStore = Schemas['StoreRead'];

/** A secret-store reference: ref+<store>://<path>#<key>. */
export interface SecretRef {
  store: string;
  path: string;
  /** Null: the whole value. */
  key: string | null;
}

// The same grammar as the API's (internal/secrets ParseRef).
const REF = /^ref\+([a-z0-9][a-z0-9-]{0,62}):\/\/([^#\s]+)(?:#([^\s#]+))?$/;

export function parseRef(value: unknown): SecretRef | null {
  if (typeof value !== 'string') return null;
  const m = REF.exec(value);
  if (!m) return null;
  return { store: m[1]!, path: m[2]!, key: m[3] ?? null };
}

export function formatRef(ref: SecretRef): string {
  return `ref+${ref.store}://${ref.path}${ref.key ? `#${ref.key}` : ''}`;
}

const normal = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, '');

// Other spellings of a field's name that secrets commonly use for the same thing.
const ALIASES: Record<string, string[]> = {
  password: ['pass', 'pwd', 'passwd'],
  access_key_id: ['aws_access_key_id', 'access_key', 'key_id'],
  secret_access_key: ['aws_secret_access_key', 'secret_key'],
  service_account_json: ['service_account', 'credentials', 'key_json'],
  token: ['auth_token', 'api_token'],
  api_key: ['apikey', 'key'],
};

/**
 * The key of a secret that most likely holds `field`: a key named like the field (or a common
 * spelling of it, also of its last word: `sasl_password` → `password`), else the only key.
 * Null when there is no clear answer: the person picks.
 */
export function guessKey(field: string, keys: string[]): string | null {
  const last = field.split('_').pop() ?? field;
  const names = [field, ...(ALIASES[field] ?? []), last, ...(ALIASES[last] ?? [])].map(normal);
  for (const name of names) {
    const hit = keys.find((k) => normal(k) === name);
    if (hit) return hit;
  }
  return keys.length === 1 ? keys[0]! : null;
}

export function useSecretStores(projectId: string) {
  return useQuery({
    queryKey: ['secret-stores', projectId],
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/secret-stores', { params: { path: { project_id: projectId } } })),
    staleTime: 60_000,
  });
}

/** The store's secret paths (never values); ok=false says why it can't list. */
export function useStoreSecrets(projectId: string, storeId: string | undefined) {
  return useQuery({
    queryKey: ['store-secrets', projectId, storeId],
    queryFn: () =>
      unwrap(
        client.GET('/api/v1/projects/{project_id}/secret-stores/{store_id}/secrets', {
          params: { path: { project_id: projectId, store_id: storeId! } },
        }),
      ),
    enabled: Boolean(storeId),
    staleTime: 60_000,
    retry: false,
  });
}

/** One secret's key names (never values). */
export function useSecretKeys(projectId: string, storeId: string | undefined, path: string) {
  return useQuery({
    queryKey: ['store-keys', projectId, storeId, path],
    queryFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/secret-stores/{store_id}/keys', {
          params: { path: { project_id: projectId, store_id: storeId! } },
          body: { path },
        }),
      ),
    enabled: Boolean(storeId && path),
    staleTime: 60_000,
    retry: false,
  });
}

/** Whether a reference resolves; the value is never returned. */
export function useRefCheck(projectId: string, storeId: string | undefined, path: string, key: string | null, enabled: boolean) {
  return useQuery({
    queryKey: ['check-ref', projectId, storeId, path, key],
    queryFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/secret-stores/{store_id}/check-ref', {
          params: { path: { project_id: projectId, store_id: storeId! } },
          body: { path, key },
        }),
      ),
    enabled: enabled && Boolean(storeId && path),
    staleTime: 30_000,
    retry: false,
  });
}
