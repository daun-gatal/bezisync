import pg from 'pg';
import { SOURCE_PG_FROM_RUNNER } from './fixtures';

export async function sql<T = Record<string, unknown>>(query: string, params: unknown[] = []): Promise<T[]> {
  const client = new pg.Client(SOURCE_PG_FROM_RUNNER);
  await client.connect();
  try {
    return (await client.query(query, params)).rows as T[];
  } finally {
    await client.end();
  }
}

export async function waitFor<T>(what: string, fn: () => Promise<T | undefined | null | false>, timeoutMs = 240_000, everyMs = 2_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) return value;
    } catch (e) {
      last = e;
    }
    await new Promise((r) => setTimeout(r, everyMs));
  }
  throw new Error(`timed out waiting for ${what}${last ? `: ${String(last)}` : ''}`);
}


/** DDL as the owner of the shop schema (tests that change table structure use their own tables). */
export async function ownerSql(query: string): Promise<void> {
  const client = new pg.Client({ ...SOURCE_PG_FROM_RUNNER, user: 'cdc_reader', password: 'cdc-reader-pw' });
  await client.connect();
  try {
    await client.query(query);
  } finally {
    await client.end();
  }
}
