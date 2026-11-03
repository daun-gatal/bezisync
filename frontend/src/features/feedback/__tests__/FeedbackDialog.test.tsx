import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { client } from '@/lib/api';
import { ProjectProvider, type Me } from '@/lib/session';
import { FeedbackDialog } from '../FeedbackDialog';

const lab = vi.hoisted(() => ({ on: true }));
vi.mock('@/lib/session', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/session')>()),
  useProviders: () => ({ data: { password: true, providers: [{ id: 'p1', label: 'Google' }], lab: lab.on } }),
}));

const me = {
  user: { id: 'u', email: 'tester@example.com', name: '', is_superuser: false, ui_mode: 'guided' },
  projects: [{ id: 'p', slug: 'playground', name: 'Playground', role: 'editor', permissions: [], is_sandbox: false }],
} as unknown as Me;

function show() {
  render(
    <QueryClientProvider client={new QueryClient()}>
      <ProjectProvider me={me} project={me.projects[0]!}>
        <FeedbackDialog open onOpenChange={() => undefined} page="/p/playground/pipelines" />
      </ProjectProvider>
    </QueryClientProvider>,
  );
}

afterEach(() => {
  vi.restoreAllMocks();
  lab.on = true;
});

describe('FeedbackDialog', () => {
  it('sends the chosen kind, the message and the page', async () => {
    const post = vi.spyOn(client, 'POST').mockResolvedValue({ data: { id: 'f' }, error: undefined, response: new Response() } as never);
    show();
    expect(screen.getByText(/BeziSync team/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Send feedback/ })).toBeDisabled();

    await userEvent.click(screen.getByRole('button', { name: /Idea/ }));
    expect(screen.getByRole('button', { name: /Idea/ })).toHaveAttribute('aria-pressed', 'true');
    await userEvent.type(screen.getByLabelText(/Message/), 'Show the properties before deploy.');
    expect(screen.getByText('34 / 5,000')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /Send feedback/ }));

    await waitFor(() => expect(post).toHaveBeenCalled());
    expect(post.mock.calls[0]![1]).toMatchObject({
      params: { path: { project_id: 'p' } },
      body: { kind: 'idea', message: 'Show the properties before deploy.', page: '/p/playground/pipelines' },
    });
    expect(await screen.findByText('Thanks, we got it')).toBeInTheDocument();
    expect(screen.getByText(/tester@example.com/)).toBeInTheDocument();
  });

  it('points security reports at the testing policy in a lab', async () => {
    show();
    await userEvent.click(screen.getByRole('button', { name: /Security issue/ }));
    expect(screen.getByRole('status')).toHaveTextContent('Only Admins see security reports.');
    expect(screen.getByRole('link', { name: 'testing policy' })).toHaveAttribute('href', '/testing-policy');
    expect(screen.getByRole('button', { name: 'Send security report' })).toBeInTheDocument();
  });

  it('outside a lab, feedback goes to the Admins and there is no policy link', async () => {
    lab.on = false;
    show();
    expect(screen.getByText(/Admins of this BeziSync/)).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: /Security issue/ }));
    expect(screen.queryByRole('link', { name: 'testing policy' })).not.toBeInTheDocument();
  });
});
