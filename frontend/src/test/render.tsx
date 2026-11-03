import type { ReactNode } from 'react';
import { render } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider, createMemoryHistory, createRootRoute, createRoute, createRouter, useRouterState } from '@tanstack/react-router';

/**
 * Renders `ui` at `path` inside a real router (so Link and navigation work), with a fresh
 * query client. Other paths render their pathname, so a test can see where a redirect went.
 */
export function renderAt(ui: () => ReactNode, { path = '/', at = path, client }: { path?: string; at?: string; client?: QueryClient } = {}) {
  const queryClient = client ?? new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const root = createRootRoute();
  const page = createRoute({ getParentRoute: () => root, path, component: ui });
  const elsewhere = createRoute({
    getParentRoute: () => root,
    path: '$',
    component: Elsewhere,
  });
  const router = createRouter({ routeTree: root.addChildren([page, elsewhere]), history: createMemoryHistory({ initialEntries: [at] }) });
  const result = render(
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router as never} />
    </QueryClientProvider>,
  );
  return { ...result, router, queryClient };
}

function Elsewhere() {
  const href = useRouterState({ select: (s) => s.location.href });
  return <p data-testid="elsewhere">{href}</p>;
}
