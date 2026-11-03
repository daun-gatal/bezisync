import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider } from '@tanstack/react-router';
import '@fontsource/ibm-plex-sans/400.css';
import '@fontsource/ibm-plex-sans/500.css';
import '@fontsource/ibm-plex-sans/600.css';
import '@fontsource/ibm-plex-sans/700.css';
import '@fontsource/ibm-plex-mono/400.css';
import '@fontsource/ibm-plex-mono/500.css';
import './styles.css';
import { makeRouter } from './app/router';
import { ErrorBoundary } from './app/ErrorPanel';
import { sessionExpiry } from './app/guard';
import { ApiError, setUnauthorizedHandler } from './lib/api';
import { meKey } from './lib/session';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: (count, error) => !(error instanceof ApiError && error.status >= 400 && error.status < 500) && count < 2,
      refetchOnWindowFocus: true,
      staleTime: 5_000,
    },
  },
});

const router = makeRouter(queryClient);

// A 401 mid-session opens the sign-in dialog over the page (nothing typed is lost). Without a
// session to begin with, the route guard sends people to the sign-in page instead.
setUnauthorizedHandler(() => {
  if (queryClient.getQueryData(meKey)) sessionExpiry.set(true);
  else if (!window.location.pathname.startsWith('/login')) {
    void router.navigate({ to: '/login', search: { next: window.location.pathname + window.location.search } });
  }
});

const root = document.getElementById('root');
if (!root) throw new Error('missing #root');

createRoot(root).render(
  <StrictMode>
    <ErrorBoundary>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </ErrorBoundary>
  </StrictMode>,
);
