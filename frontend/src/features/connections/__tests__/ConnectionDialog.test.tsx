import { render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ProjectProvider, type Me } from '@/lib/session';
import { ConnectionDialog } from '../ConnectionDialog';

const types = [
  {
    kind: 'source', type: 'postgresql', title: 'PostgreSQL', description: 'pg',
    presets: [{ id: 'self_hosted', title: 'Self-hosted', defaults: {} }, { id: 'aws_rds', title: 'AWS RDS / Aurora', defaults: { ssl_mode: 'require' } }],
    config_schema: { required: ['host'], properties: { host: { type: 'string', title: 'Host' }, ssl_mode: { enum: ['disable', 'require'], type: 'string', default: 'prefer', title: 'TLS' } } },
    secret_schema: { properties: { password: { type: 'string', title: 'Password' } } },
  },
  // Written by Flink CDC only: StarRocks while the engine is off, Doris once it is on.
  {
    kind: 'sink', type: 'starrocks', title: 'StarRocks', description: 'olap', presets: [], config_schema: { properties: {} }, secret_schema: { properties: {} },
    engines: ['flink-cdc'], available: false, unavailable_reason: "Flink CDC isn't turned on: an Admin registers a Flink CDC image under Settings → Engine versions.",
  },
  {
    kind: 'sink', type: 'doris', title: 'Apache Doris', description: 'doris tables', presets: [], config_schema: { properties: {} }, secret_schema: { properties: {} },
    engines: ['flink-cdc'], available: true,
  },
];

const me = {
  user: { id: 'u', email: 'a@b.c', name: '', is_superuser: true, ui_mode: 'guided' },
  projects: [{ id: 'p', slug: 'default', name: 'Default', role: 'editor', permissions: ['connection.manage', 'project.manage'], is_sandbox: false }],
} as unknown as Me;

vi.mock('@/lib/connections', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/connections')>()),
  useConnectorTypes: () => ({ data: types, isPending: false }),
}));

// One Vault store holding cdc/orders {user, password}; whether references resolve is set per test.
const vault = { id: 's1', name: 'vault', kind: 'vault', config: {}, secret_fields_set: ['token'], secrets_updated_at: null, last_test: null, used_by: [], version: 1 };
const resolves = { ok: true, message: 'Found a value (8 characters).' };
let refCheck: { ok: boolean; message: string } = resolves;
vi.mock('@/lib/secretRefs', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/secretRefs')>()),
  useSecretStores: () => ({ data: [vault], isSuccess: true }),
  useStoreSecrets: (_p: string, id?: string) => ({ data: id ? { ok: true, message: '', secrets: ['cdc/billing', 'cdc/orders'], truncated: false } : undefined, isPending: false }),
  useSecretKeys: (_p: string, id: string | undefined, path: string) => ({
    data: id && path ? { ok: true, message: '', keys: ['password', 'user'], whole_value: false } : undefined,
  }),
  useRefCheck: (_p: string, _id: string | undefined, _path: string, _key: string | null, enabled: boolean) => ({ data: enabled ? refCheck : undefined }),
}));

function renderDialog() {
  render(
    <QueryClientProvider client={new QueryClient()}>
      <ProjectProvider me={me} project={me.projects[0]!}>
        <ConnectionDialog open onOpenChange={() => undefined} kind="source" />
      </ProjectProvider>
    </QueryClientProvider>,
  );
}

describe('ConnectionDialog', () => {
  it('switching provider keeps typed values and applies preset defaults', async () => {
    render(
      <QueryClientProvider client={new QueryClient()}>
        <ProjectProvider me={me} project={me.projects[0]!}>
          <ConnectionDialog open onOpenChange={() => undefined} kind="source" />
        </ProjectProvider>
      </QueryClientProvider>,
    );
    await userEvent.click(await screen.findByRole('button', { name: /PostgreSQL/ }));
    await userEvent.type(screen.getByLabelText('Host'), 'db.internal');
    await userEvent.click(screen.getByRole('radio', { name: 'AWS RDS / Aurora' }));
    await waitFor(() => expect(screen.getByLabelText('TLS')).toHaveTextContent('require'));
    expect(screen.getByLabelText('Host')).toHaveValue('db.internal');
  });

  it('takes the password from a store: the key is pre-picked and only a resolving reference saves', async () => {
    refCheck = resolves;
    renderDialog();
    await userEvent.click(await screen.findByRole('button', { name: /PostgreSQL/ }));
    await userEvent.type(screen.getByLabelText('Name'), 'orders-db');
    await userEvent.type(screen.getByLabelText('Host'), 'db.internal');
    await userEvent.click(screen.getByRole('button', { name: 'From a secret store' }));
    expect(screen.getByLabelText('Store')).toHaveTextContent('vault');
    expect(screen.getByRole('button', { name: 'Save source' })).toBeDisabled();
    await userEvent.click(screen.getByLabelText('Secret'));
    await userEvent.click(screen.getByRole('option', { name: 'cdc/orders' }));
    await waitFor(() => expect(screen.getByLabelText('Key')).toHaveTextContent('password'));
    expect(await screen.findByText('Found in vault. Only the reference ref+vault://cdc/orders#password is saved.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Save source' })).toBeEnabled();
    expect(screen.queryByRole('button', { name: 'Save anyway' })).toBeNull();
  });

  it('a reference that does not resolve blocks saving unless you save anyway', async () => {
    refCheck = { ok: false, message: 'Vault has no secret at this path.' };
    renderDialog();
    await userEvent.click(await screen.findByRole('button', { name: /PostgreSQL/ }));
    await userEvent.type(screen.getByLabelText('Name'), 'orders-db');
    await userEvent.click(screen.getByRole('button', { name: 'From a secret store' }));
    await userEvent.click(screen.getByLabelText('Secret'));
    await userEvent.click(screen.getByRole('option', { name: 'cdc/billing' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Vault has no secret at this path.');
    expect(screen.getByRole('button', { name: 'Save source' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Save anyway' })).toBeEnabled();
    // Back to typing: the picker no longer holds the save.
    await userEvent.click(screen.getByRole('button', { name: 'Type it' }));
    expect(screen.queryByRole('button', { name: 'Save anyway' })).toBeNull();
  });
});

describe('Flink CDC destinations', () => {
  it('are marked, and offered only once the engine is on', async () => {
    render(
      <QueryClientProvider client={new QueryClient()}>
        <ProjectProvider me={me} project={me.projects[0]!}>
          <ConnectionDialog open onOpenChange={() => undefined} kind="sink" />
        </ProjectProvider>
      </QueryClientProvider>,
    );
    const starrocks = await screen.findByRole('button', { name: /StarRocks/ });
    expect(starrocks).toBeDisabled();
    expect(starrocks).toHaveTextContent('Settings → Engine versions');
    const doris = screen.getByRole('button', { name: /Apache Doris/ });
    expect(doris).toBeEnabled();
    expect(doris).toHaveTextContent('Flink CDC');
    expect(doris).toHaveTextContent('doris tables');
  });
});
