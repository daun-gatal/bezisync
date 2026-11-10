import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ProjectProvider, type Me } from '@/lib/session';
import type { Pipeline } from '@/lib/pipelines';
import { canDiscard, DiscardDialog, discardEffects } from '../DiscardChanges';

const me = {
  user: { id: 'u', email: 'a@b.c', name: '', is_superuser: false, ui_mode: 'guided' },
  projects: [{ id: 'p', slug: 'default', name: 'Default', role: 'editor', permissions: ['pipeline.edit', 'pipeline.view'], is_sandbox: false }],
} as unknown as Me;

const mutate = vi.fn();
vi.mock('@/lib/pipelines', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/pipelines')>()),
  useDiscardChanges: () => ({ mutate, reset: vi.fn(), isPending: false, error: null }),
}));

function pipeline(over: Partial<Pipeline> = {}): Pipeline {
  return {
    id: 'x', name: 'orders-cdc', version: 7, current_version: 5, deployed_version: 3, has_undeployed_changes: true, pending_change: null,
    ...over,
  } as unknown as Pipeline;
}

describe('canDiscard', () => {
  it('needs a deployed pipeline whose current version differs, nothing pending, and edit rights', () => {
    expect(canDiscard(pipeline(), true)).toBe(true);
    expect(canDiscard(pipeline(), false)).toBe(false);
    expect(canDiscard(undefined, true)).toBe(false);
    expect(canDiscard(pipeline({ deployed_version: null }), true)).toBe(false); // a draft is deleted instead
    expect(canDiscard(pipeline({ has_undeployed_changes: false }), true)).toBe(false);
    expect(canDiscard(pipeline({ pending_change: { id: 'cr' } as Pipeline['pending_change'] }), true)).toBe(false);
  });

  it('says the connector keeps running, where the change goes, and what it doesn’t revert', () => {
    const e = discardEffects(pipeline()).join(' ');
    expect(e).toContain('Settings go back to v3');
    expect(e).toContain('nothing restarts');
    expect(e).toContain('v5 stays in History');
    expect(e).toContain('Description, owner, labels and the runtime');
  });
});

describe('DiscardDialog', () => {
  it('discards the version on screen', async () => {
    const p = pipeline();
    render(
      <QueryClientProvider client={new QueryClient()}>
        <ProjectProvider me={me} project={me.projects[0]!}>
          <DiscardDialog pipeline={p} open onOpenChange={vi.fn()} />
        </ProjectProvider>
      </QueryClientProvider>,
    );
    expect(screen.getByText('Discard v5?')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Discard v5' }));
    expect(mutate).toHaveBeenCalledWith(p, expect.anything());
  });
});
