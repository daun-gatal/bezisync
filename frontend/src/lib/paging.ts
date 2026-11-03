import { useEffect, useState } from 'react';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { usePageSize } from '@/components/ui/pager';

export interface CursorPage {
  items: unknown[];
  next_cursor?: string | null;
}

/**
 * Pages a cursor-paged endpoint (newest first, no total) one page at a time with the viewer's
 * rows per page: page n is fetched with the cursor the page before returned. A changed `key`
 * (filters) or page size starts again from the newest page. Only the newest page refreshes on
 * its own (`refetchInterval`), so older pages hold still while someone reads them.
 */
export function useCursorPages<P extends CursorPage>(
  key: readonly unknown[],
  fetchPage: (cursor: string | null, limit: number) => Promise<P>,
  opts: { refetchInterval?: number; enabled?: boolean } = {},
) {
  const [size] = usePageSize();
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const reset = JSON.stringify([key, size]);
  useEffect(() => setCursors([null]), [reset]);
  const cursor = cursors[cursors.length - 1] ?? null;
  const page = cursors.length;
  const query = useQuery({
    queryKey: [...key, 'page', size, cursor],
    queryFn: () => fetchPage(cursor, size),
    placeholderData: keepPreviousData,
    refetchInterval: page === 1 ? opts.refetchInterval : undefined,
    enabled: opts.enabled,
  });
  const next = query.data?.next_cursor ?? null;
  return {
    query,
    page,
    items: (query.data?.items ?? []) as P['items'],
    hasOlder: Boolean(next),
    older: () => {
      if (next) setCursors((c) => [...c, next]);
    },
    newer: () => setCursors((c) => (c.length > 1 ? c.slice(0, -1) : c)),
    /** For a CompactPager: one step at a time. */
    goTo: (p: number) => (p < page ? setCursors((c) => (c.length > 1 ? c.slice(0, -1) : c)) : next ? setCursors((c) => [...c, next]) : undefined),
  };
}
