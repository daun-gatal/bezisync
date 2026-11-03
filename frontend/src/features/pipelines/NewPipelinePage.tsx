import { useRef, useState } from 'react';
import { Link, useBlocker, useNavigate, useSearch } from '@tanstack/react-router';
import { X } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { InlineError, Notice, Skeleton } from '@/components/ui/feedback';
import { Page, PageHeader } from '@/components/ui/layout';
import { Confirm, Modal } from '@/components/ui/overlays';
import { useDeletePipeline, usePipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { TransformStep } from './transforms/TransformStep';
import type { StepHandle } from './useSaveSpec';
import { ReviewStep } from './wizard/ReviewStep';
import { SinkStep } from './wizard/SinkStep';
import { SourceStep } from './wizard/SourceStep';
import { TablesStep } from './wizard/TablesStep';
import { createdHere, StepNav, stepState, type Step } from './wizard/shared';

/**
 * The pipeline wizard: Source → Tables → Destination → Transform → Review. A step unlocks
 * when the ones before it are done. Leaving a step by any route (Back, Continue, the step list)
 * saves it first; leaving the page with unsaved changes asks. Closing a draft made here asks
 * whether to keep it. For a deployed pipeline it edits one part, then Review shows the change.
 */
export function NewPipelinePage() {
  const { project, expert } = useProject();
  const search = useSearch({ from: '/p/$projectSlug/pipelines/new' });
  const navigate = useNavigate({ from: '/p/$projectSlug/pipelines/new' });
  const pipeline = usePipeline(project.id, search.draft);
  const remove = useDeletePipeline(project.id);
  const handle = useRef<StepHandle | null>(null);
  const [leaving, setLeaving] = useState(false);
  const [closing, setClosing] = useState(false);
  // The step on screen reports when its unsaved choices are complete (see stepState).
  const [ready, setReady] = useState(false);
  const p = pipeline.data;
  const requested: Step = search.step ?? (search.draft ? 'tables' : 'source');
  // A locked step (an old link, a typed URL) falls back to the first one that isn't.
  const step: Step =
    !p && search.draft
      ? requested
      : stepState(requested, p).locked
        ? ((['source', 'tables', 'sink'] as Step[]).find((s) => !stepState(s, p).locked && !stepState(s, p).done) ?? 'source')
        : requested;
  const deployed = Boolean(p?.deployed_version);

  // Leaving the wizard with unsaved changes in the current step asks first.
  const blocker = useBlocker({
    shouldBlockFn: ({ current, next }) => current.pathname !== next.pathname && Boolean(handle.current?.dirty()),
    enableBeforeUnload: () => Boolean(handle.current?.dirty()),
    withResolver: true,
  });

  const go = async (next: Step, draft?: string) => {
    const h = handle.current;
    if (h?.dirty() && !(await h.commit())) return;
    setReady(false);
    void navigate({ search: { step: next, draft: draft ?? search.draft }, replace: false });
  };

  const close = () => {
    if (p && !deployed && createdHere.has(p.id)) return setClosing(true);
    void (p
      ? navigate({ to: '/p/$projectSlug/pipelines/$pipelineId', params: { projectSlug: project.slug, pipelineId: p.id } })
      : navigate({ to: '/p/$projectSlug/pipelines', params: { projectSlug: project.slug } }));
  };

  const body =
    search.draft && pipeline.isPending ? (
      <Skeleton lines={6} />
    ) : search.draft && pipeline.error ? (
      <InlineError error={pipeline.error} doing="Couldn’t load the draft" onRetry={() => void pipeline.refetch()} />
    ) : step === 'source' ? (
      <SourceStep pipeline={p} initialTemplate={search.template} onNext={(id) => void navigate({ search: { step: 'tables', draft: id } })} />
    ) : !p ? (
      <Notice tone="warn" title="Start with a source">
        Pick the database to capture first.
      </Notice>
    ) : step === 'tables' ? (
      <TablesStep key={`tables-${p.id}`} pipeline={p} handle={handle} onReady={setReady} onBack={() => void go('source')} onNext={() => void go('sink')} />
    ) : step === 'sink' ? (
      <SinkStep key={`sink-${p.id}`} pipeline={p} handle={handle} onReady={setReady} onBack={() => void go('tables')} onNext={() => void go('transform')} />
    ) : step === 'transform' ? (
      <TransformStep key={`transform-${p.id}`} pipeline={p} handle={handle} onBack={() => void go('sink')} onNext={() => void go('review')} />
    ) : (
      <ReviewStep key={`review-${p.id}`} pipeline={p} handle={handle} onBack={() => void go('transform')} />
    );

  return (
    <Page>
      <PageHeader
        crumbs={[
          {
            label: 'Pipelines',
            link: (children, className) => (
              <Link to="/p/$projectSlug/pipelines" params={{ projectSlug: project.slug }} className={className}>
                {children}
              </Link>
            ),
          },
          ...(p && deployed
            ? [
                {
                  label: p.name,
                  link: (children: React.ReactNode, className: string) => (
                    <Link to="/p/$projectSlug/pipelines/$pipelineId" params={{ projectSlug: project.slug, pipelineId: p.id }} className={className}>
                      {children}
                    </Link>
                  ),
                },
              ]
            : []),
          { label: deployed ? 'Change' : 'New pipeline' },
        ]}
        title={deployed && p ? `Change ${p.name}` : 'New pipeline'}
        status={
          <>
            {p ? <span>Draft saved · v{p.current_version}</span> : null}
            <span>{expert ? 'Expert mode' : 'Guided mode'} · switch it in your account menu</span>
          </>
        }
        actions={
          <Button variant="ghost" icon={<X />} onClick={close}>
            Close
          </Button>
        }
      />
      <div className="@container min-w-0">
        <div className="grid min-w-0 grid-cols-[minmax(0,1fr)] gap-6 @3xl:grid-cols-[200px_minmax(0,1fr)]">
          <StepNav current={step} pipeline={p} ready={ready && (step === 'tables' || step === 'sink')} onGo={(s) => void go(s)} />
          <section aria-label="Step" className="min-w-0">
            {body}
          </section>
        </div>
      </div>

      <Modal
        open={blocker.status === 'blocked'}
        onOpenChange={(o) => !o && blocker.reset?.()}
        title="Leave without saving?"
        description="This step has changes that aren’t saved yet."
        size="sm"
        footer={
          <>
            <Button onClick={() => blocker.reset?.()}>Stay</Button>
            <Button variant="danger" onClick={() => blocker.proceed?.()}>
              Discard and leave
            </Button>
            <Button
              variant="primary"
              loading={leaving}
              onClick={async () => {
                setLeaving(true);
                const ok = (await handle.current?.commit()) ?? true;
                setLeaving(false);
                if (ok) blocker.proceed?.();
                else blocker.reset?.();
              }}
            >
              Save and leave
            </Button>
          </>
        }
      />

      <Confirm
        open={closing}
        onOpenChange={setClosing}
        tone="primary"
        title="Keep this draft?"
        effects={[
          'Keep it: it stays in the Pipelines list as a draft, and you can continue any time.',
          'Delete it: nothing was deployed, so nothing else is affected.',
        ]}
        confirmLabel="Keep the draft"
        onConfirm={() => {
          setClosing(false);
          if (p) {
            createdHere.delete(p.id);
            void navigate({ to: '/p/$projectSlug/pipelines', params: { projectSlug: project.slug } });
          }
        }}
        error={remove.error}
        doing="Couldn’t delete the draft"
      >
        <div>
          <Button
            variant="danger"
            loading={remove.isPending}
            onClick={() =>
              p &&
              remove.mutate(
                { id: p.id, dropDestinations: false },
                {
                  onSuccess: () => {
                    createdHere.delete(p.id);
                    setClosing(false);
                    toast.success(`Deleted the draft ${p.name}`);
                    void navigate({ to: '/p/$projectSlug/pipelines', params: { projectSlug: project.slug } });
                  },
                },
              )
            }
          >
            Delete the draft
          </Button>
        </div>
      </Confirm>
    </Page>
  );
}
