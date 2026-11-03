import createClient, { type Middleware } from 'openapi-fetch';
import type { components, paths } from './api-schema';

export type Schemas = components['schemas'];

/** RFC 9457 problem returned by every failing API call. */
export interface Problem {
  status: number;
  code: string;
  detail: string;
  request_id?: string | null;
  errors?: { loc: string[]; message: string }[];
  [key: string]: unknown;
}

export class ApiError extends Error {
  readonly problem: Problem;
  constructor(problem: Problem) {
    super(problem.detail);
    this.name = 'ApiError';
    this.problem = problem;
  }
  get status(): number {
    return this.problem.status;
  }
  get code(): string {
    return this.problem.code;
  }
}

const UNSAFE = new Set(['POST', 'PUT', 'PATCH', 'DELETE']);

export function readCookie(name: string): string | undefined {
  return document.cookie
    .split('; ')
    .find((part) => part.startsWith(`${name}=`))
    ?.slice(name.length + 1);
}

let onUnauthorized: () => void = () => undefined;
export function setUnauthorizedHandler(handler: () => void): void {
  onUnauthorized = handler;
}

const csrf: Middleware = {
  onRequest({ request }) {
    if (UNSAFE.has(request.method)) {
      const token = readCookie('bz_csrf');
      if (token) request.headers.set('x-csrf-token', decodeURIComponent(token));
    }
    return request;
  },
  onResponse({ response, request }) {
    if (response.status === 401 && !request.url.endsWith('/auth/login') && !request.url.endsWith('/auth/me')) {
      onUnauthorized();
    }
    return response;
  },
};

export const client = createClient<paths>({ baseUrl: '', credentials: 'same-origin' });
client.use(csrf);

function toProblem(error: unknown, response: Response): Problem {
  if (error && typeof error === 'object' && 'code' in error && 'detail' in error) {
    return { ...(error as Problem), status: response.status };
  }
  return {
    status: response.status,
    code: `http_${response.status}`,
    detail: response.status >= 500 ? 'The server had a problem. Try again shortly.' : response.statusText,
    request_id: response.headers.get('x-request-id'),
  };
}

/** Unwrap an openapi-fetch result, throwing ApiError for problems. */
export async function unwrap<T>(
  promise: Promise<{ data?: T; error?: unknown; response: Response }>,
): Promise<T> {
  let result: { data?: T; error?: unknown; response: Response };
  try {
    result = await promise;
  } catch {
    throw new ApiError({ status: 0, code: 'network_error', detail: 'Cannot reach the BeziSync server.' });
  }
  if (result.error !== undefined || !result.response.ok) {
    throw new ApiError(toProblem(result.error, result.response));
  }
  return result.data as T;
}

export function problemMessage(error: unknown): string {
  if (error instanceof ApiError) {
    const fields = error.problem.errors?.map((e) => `${e.loc.slice(1).join('.')}: ${e.message}`) ?? [];
    return [error.problem.detail, ...fields].join(' ');
  }
  return error instanceof Error ? error.message : 'Unexpected error';
}
