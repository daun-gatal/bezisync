import { expect, request, type APIRequestContext, type Locator, type Page } from '@playwright/test';

export const ADMIN = { email: 'admin@bezisync.local', password: process.env.ADMIN_PASSWORD ?? 'e2e-admin-password' };
export const BASE_URL = process.env.BASE_URL ?? 'http://localhost:8080';

/**
 * Sign in through the UI. The Admin belongs to every project, and home opens the first one
 * in the list (projects other specs created sort before "Default"), so the Admin lands on
 * the default project explicitly; other people land on their own home.
 */
export async function signIn(page: Page, email = ADMIN.email, password = ADMIN.password): Promise<void> {
  await page.goto(email === ADMIN.email ? `/login?next=${encodeURIComponent('/p/default/pipelines')}` : '/login');
  await page.getByLabel('Email').fill(email);
  await page.getByLabel('Password').fill(password);
  await page.getByRole('button', { name: 'Sign in' }).click();
  await expect(page.getByRole('heading', { name: 'Pipelines', level: 1 })).toBeVisible();
}

/** An API client signed in with a session cookie that sends the CSRF header. */
export class Api {
  private constructor(private readonly ctx: APIRequestContext, private readonly csrf: string) {}

  static async signIn(email = ADMIN.email, password = ADMIN.password): Promise<Api> {
    const ctx = await request.newContext({ baseURL: BASE_URL });
    const res = await ctx.post('/api/v1/auth/login', { data: { email, password } });
    expect(res.status(), await res.text()).toBe(200);
    const state = await ctx.storageState();
    const csrf = state.cookies.find((c) => c.name === 'bz_csrf')?.value ?? '';
    return new Api(ctx, csrf);
  }

  async get<T = unknown>(path: string): Promise<T> {
    const res = await this.ctx.get(path);
    expect(res.ok(), `${path}: ${await res.text()}`).toBeTruthy();
    return (await res.json()) as T;
  }

  async send<T = unknown>(method: 'POST' | 'PUT' | 'PATCH' | 'DELETE', path: string, data?: unknown, expected?: number): Promise<T> {
    const res = await this.ctx.fetch(path, { method, data, headers: { 'x-csrf-token': this.csrf } });
    if (expected !== undefined) expect(res.status(), await res.text()).toBe(expected);
    else expect(res.ok(), `${method} ${path}: ${await res.text()}`).toBeTruthy();
    const text = await res.text();
    return (text ? JSON.parse(text) : undefined) as T;
  }

  /** PATCH with optimistic concurrency (If-Match = resource version). */
  async patch<T = unknown>(path: string, data: unknown, version: number): Promise<T> {
    const res = await this.ctx.fetch(path, { method: 'PATCH', data, headers: { 'x-csrf-token': this.csrf, 'if-match': `"${version}"` } });
    expect(res.ok(), `PATCH ${path}: ${await res.text()}`).toBeTruthy();
    return (await res.json()) as T;
  }

  async rawWithoutCsrf(method: string, path: string, data?: unknown) {
    return this.ctx.fetch(path, { method, data });
  }

  async raw(method: string, path: string, data?: unknown, headers: Record<string, string> = {}) {
    return this.ctx.fetch(path, { method, data, headers: { 'x-csrf-token': this.csrf, ...headers } });
  }
}

export interface Me {
  user: { id: string; email: string };
  projects: { id: string; slug: string; name: string; permissions: string[] }[];
}

export async function defaultProject(api: Api): Promise<Me['projects'][number]> {
  const me = await api.get<Me>('/api/v1/auth/me');
  const project = me.projects.find((p) => p.slug === 'default');
  if (!project) throw new Error('default project missing');
  return project;
}

export function unique(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}${Math.floor(Math.random() * 1000)}`;
}

/** Choose an option in a BeziSync Select: a button that opens a listbox of options. */
export async function choose(trigger: Locator, option: string | RegExp): Promise<void> {
  await trigger.click();
  const list = trigger.page().getByRole('listbox');
  await list.getByRole('option', { name: option, exact: typeof option === 'string' }).click();
  await expect(list).toHaveCount(0);
}
