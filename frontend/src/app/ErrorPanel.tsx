import { Component, type ErrorInfo, type ReactNode } from 'react';
import { useRouter, type ErrorComponentProps } from '@tanstack/react-router';
import { Home, RefreshCw } from 'lucide-react';
import { Button, ButtonLink } from '@/components/ui/button';
import { InlineError, Skeleton } from '@/components/ui/feedback';
import { Title } from '@/components/ui/text';
import { describeError } from '@/lib/errors';

/** A problem that replaces a whole page: what happened, why, what to do, and a way out. */
export function ErrorPanel({
  title,
  detail,
  requestId,
  onRetry,
  home = true,
  children,
}: {
  title: string;
  detail?: ReactNode;
  requestId?: string | null;
  onRetry?: () => void;
  home?: boolean;
  children?: ReactNode;
}) {
  return (
    <div role="alert" className="mx-auto my-16 flex w-full max-w-lg flex-col items-center gap-3 rounded-xl border border-line bg-surface p-8 text-center">
      <Title as="h1" lines="none" className="text-xl font-semibold">
        {title}
      </Title>
      {detail ? <p className="m-0 text-base text-muted wrap-break-word">{detail}</p> : null}
      {requestId ? <p className="m-0 font-mono text-xs text-muted">Request ID {requestId}</p> : null}
      {children}
      <div className="flex flex-wrap justify-center gap-2">
        {onRetry ? (
          <Button icon={<RefreshCw />} onClick={onRetry}>
            Try again
          </Button>
        ) : null}
        {home ? (
          <ButtonLink to="/" variant="ghost" icon={<Home />}>
            Go home
          </ButtonLink>
        ) : null}
      </div>
    </div>
  );
}

export function FullPageLoading() {
  return (
    <div className="mx-auto w-full max-w-md p-10">
      <Skeleton lines={3} />
    </div>
  );
}

export function FullPageError({ title, detail, error, onRetry }: { title: string; detail: string; error: unknown; onRetry: () => void }) {
  const e = error ? describeError(error) : null;
  return (
    <div className="min-h-screen bg-bg px-4">
      <ErrorPanel title={title} detail={detail} requestId={e?.requestId} onRetry={onRetry} home={false} />
    </div>
  );
}

/** Inline error for a failed query, with the request ID support needs. */
export function QueryError({ error, onRetry }: { error: unknown; onRetry?: () => void }) {
  return <InlineError error={error} onRetry={onRetry} />;
}

/** A crash inside one page: the shell and navigation stay, and Try again re-renders only the page. */
export function RouteError({ error, reset }: ErrorComponentProps) {
  const router = useRouter();
  const e = describeError(error, 'This page hit a problem');
  return (
    <ErrorPanel
      title={e.title}
      detail={`${e.detail} ${e.fix}`.trim()}
      requestId={e.requestId}
      onRetry={() => {
        reset();
        void router.invalidate();
      }}
    />
  );
}

export function NotFound() {
  return <ErrorPanel title="Page not found" detail="This address doesn’t exist. It may be an old link, or the page moved." />;
}

/** Last resort outside the router: the app itself failed to render. */
export class ErrorBoundary extends Component<{ children: ReactNode }, { error: Error | null }> {
  state: { error: Error | null } = { error: null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error(error, info.componentStack);
  }
  render() {
    if (this.state.error) {
      return (
        <div role="alert" className="mx-auto my-16 flex max-w-lg flex-col items-center gap-3 p-8 text-center">
          <Title as="h1" className="text-xl font-semibold">
            BeziSync couldn’t start
          </Title>
          <p className="m-0 text-base text-muted wrap-break-word">{this.state.error.message}</p>
          <Button onClick={() => window.location.reload()}>Reload the page</Button>
        </div>
      );
    }
    return this.props.children;
  }
}
