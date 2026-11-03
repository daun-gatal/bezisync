import { ApiError } from './api';

/** What went wrong, in the words an inline error shows: what happened, why, what to do, request ID. */
export interface ErrorText {
  title: string;
  detail: string;
  fix: string;
  requestId: string | null;
  fields: { field: string; message: string }[];
  status: number;
}

const FIX: Record<string, string> = {
  network: 'Check your connection. BeziSync retries on its own, and nothing you entered is lost.',
  server: 'Try again. If it keeps failing, send the request ID to your BeziSync Admin.',
  conflict: 'Someone else changed this since you opened it. Reload to see their change, then try again.',
  forbidden: 'Ask a project Admin for the permission, or for them to do it.',
  invalid: 'Correct the values below and try again.',
  missing: 'It may have been deleted. Go back and reload the list.',
  session: 'Sign in again to continue. What you entered stays on the page.',
};

/** `doing` names the action that failed, e.g. "Couldn't save the pipeline". */
export function describeError(error: unknown, doing?: string): ErrorText {
  if (!(error instanceof ApiError)) {
    const detail = error instanceof Error ? error.message : 'Something unexpected happened.';
    return { title: doing ?? 'Something went wrong', detail, fix: FIX.server!, requestId: null, fields: [], status: 0 };
  }
  const p = error.problem;
  const fields = (p.errors ?? []).map((e) => ({ field: e.loc.slice(1).join('.'), message: e.message }));
  const base = { requestId: p.request_id ?? null, fields, status: p.status, detail: p.detail };
  if (p.status === 0) return { ...base, title: doing ?? 'Can’t reach BeziSync', detail: 'Your browser couldn’t reach the BeziSync server.', fix: FIX.network! };
  if (p.status === 401) return { ...base, title: doing ?? 'Your session ended', fix: FIX.session! };
  if (p.status === 403) return { ...base, title: doing ?? 'Not allowed', fix: FIX.forbidden! };
  if (p.status === 404) return { ...base, title: doing ?? 'Not found', fix: FIX.missing! };
  // 412: an edit based on an old version (the detail says so); 409: the request conflicts with the current state.
  if (p.status === 412) return { ...base, title: doing ?? 'Changed by someone else', fix: p.detail ? '' : FIX.conflict! };
  if (p.status === 409) return { ...base, title: doing ?? 'Not possible right now', fix: '' };
  if (p.status === 422 || p.status === 400) return { ...base, title: doing ?? 'Some values weren’t accepted', fix: fields.length ? FIX.invalid! : '' };
  if (p.status >= 500) return { ...base, title: doing ?? 'The server had a problem', fix: FIX.server! };
  return { ...base, title: doing ?? 'That didn’t work', fix: '' };
}

/** Network failures and 5xx are worth retrying; 4xx need the person to change something. */
export function isRetryable(error: unknown): boolean {
  return !(error instanceof ApiError) || error.status === 0 || error.status >= 500;
}

/** A failed sign-in (as opposed to a server or network problem). */
export function wrongPassword(error: unknown): boolean {
  return error instanceof ApiError && (error.status === 401 || error.status === 400);
}
