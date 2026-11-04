import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ProjectProvider, type Me } from '@/lib/session';
import type { Pipeline } from '@/lib/pipelines';
import { FlinkPositions } from '../FlinkPositions';

const me = {
  user: { id: 'u', email: 'a@b.c', name: '', is_superuser: false, ui_mode: 'guided' },
  projects: [{ id: 'p', slug: 'default', name: 'Default', role: 'operator', permissions: ['offsets.reset', 'pipeline.view'], is_sandbox: false }],
} as unknown as Me;

const reset = vi.fn();
vi.mock('@/lib/pipelines', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/pipelines')>()),
  useSavedPositions: () => ({
    status: 'success',
    isPending: false,
    isError: false,
    data: {
      positions: [
        { id: 9, kind: 'savepoint', reason: 'stop', flink_id: 0, size: 3000, taken_at: '2026-11-04T10:00:00Z', current: true },
        { id: 8, kind: 'checkpoint', reason: 'periodic', flink_id: 8, size: 3000, taken_at: '2026-11-04T09:59:00Z', current: true },
      ],
    },
  }),
  useRestorePosition: () => ({ mutate: vi.fn(), reset: vi.fn(), isPending: false, error: null }),
  useResetOffsets: () => ({ mutate: reset, reset: vi.fn(), isPending: false, error: null }),
}));

function pipeline(desired: 'running' | 'paused', source: string, fromTime: boolean): Pipeline {
  return {
    id: 'x', name: 'orders-lake', desired_state: desired, deployed_version: 2, engine: 'flink-cdc', source: { id: 's', name: 'src', type: source },
    caps: { start_from_time: fromTime, why: fromTime ? {} : { start_from_time: 'A PostgreSQL replication slot can’t go back in time.' } },
  } as unknown as Pipeline;
}

function renderCard(p: Pipeline) {
  render(
    <QueryClientProvider client={new QueryClient()}>
      <ProjectProvider me={me} project={me.projects[0]!}>
        <FlinkPositions pipeline={p} />
      </ProjectProvider>
    </QueryClientProvider>,
  );
}

describe('FlinkPositions', () => {
  it('lists positions; restoring and starting over wait for a pause', () => {
    renderCard(pipeline('running', 'mysql', true));
    expect(screen.getByText('Saved when it stopped')).toBeInTheDocument();
    expect(screen.getByText('Resumes from here')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Restore position 9' })).toBeNull(); // already the one it resumes from
    expect(screen.getByRole('button', { name: 'Restore position 8' })).toHaveAttribute('aria-disabled', 'true');
    expect(screen.getByRole('button', { name: 'Copy every table again…' })).toHaveAttribute('aria-disabled', 'true');
  });

  it('a PostgreSQL pipeline can’t start from a time, and says why', () => {
    renderCard(pipeline('paused', 'postgresql', false));
    expect(screen.getByRole('button', { name: 'Changes since a time…' })).toHaveAttribute('aria-disabled', 'true');
    expect(screen.getByRole('button', { name: 'Copy every table again…' })).not.toHaveAttribute('aria-disabled', 'true');
  });

  it('starting from a time needs a past time and the typed name', async () => {
    reset.mockClear();
    renderCard(pipeline('paused', 'mysql', true));
    await userEvent.click(screen.getByRole('button', { name: 'Changes since a time…' }));
    const start = screen.getByRole('button', { name: 'Start over' });
    expect(start).toBeDisabled();
    await userEvent.type(screen.getByLabelText(/Since/), '2026-11-01T08:30');
    await userEvent.type(screen.getByRole('textbox', { name: /orders-lake/ }), 'orders-lake');
    expect(start).toBeEnabled();
    await userEvent.click(start);
    expect(reset).toHaveBeenCalledWith(expect.objectContaining({ id: 'x', mode: 'from_time', confirm: 'orders-lake', fromTime: new Date('2026-11-01T08:30').toISOString() }), expect.anything());
  });
});
