import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { CheckList, reportBadge, type CheckReport } from '../CheckList';

const report: CheckReport = {
  ok: false,
  summary: '1 issue blocks deploy',
  duration_ms: 20,
  checked_at: new Date().toISOString(),
  checks: [
    { code: 'pg_login', title: 'Sign in', status: 'pass', category: 'auth', message: 'Signed in.', technical: '', why: '', fix: null, blocking: false, doc: '' },
    {
      code: 'pg_wal_level', title: 'Logical replication', status: 'fail', category: 'cdc_setup', blocking: true, doc: '',
      message: "The database isn't set up to publish changes yet.", technical: 'wal_level = replica', why: 'Needs a restart.',
      fix: { kind: 'sql', code: 'ALTER SYSTEM SET wal_level = logical;', restart_required: true, who: 'DBA' },
    },
  ],
};

describe('CheckList', () => {
  it('leads with failures, shows the fix and hides passed checks until asked', async () => {
    const onRecheck = vi.fn();
    render(<CheckList report={report} expert={false} subject="orders-pg" onRecheck={onRecheck} />);
    expect(screen.getByText('1 issue blocks deploy')).toBeInTheDocument();
    expect(screen.getByLabelText('Fix for Logical replication')).toHaveTextContent('ALTER SYSTEM SET wal_level = logical;');
    expect(screen.getByText('needs a restart')).toBeInTheDocument();
    expect(screen.getByText('wal_level = replica')).toBeInTheDocument();
    expect(screen.queryByText('Signed in.')).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Show 1 passed check' }));
    expect(screen.getByText('Signed in.')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Re-check' }));
    expect(onRecheck).toHaveBeenCalled();
  });

  it('shows what BeziSync can’t verify in grey, never as Ready, and lists it without asking', () => {
    const unverified: CheckReport = {
      ok: true,
      summary: 'Ready · 1 not verified',
      duration_ms: 5,
      checked_at: new Date().toISOString(),
      checks: [
        { code: 'config_valid', title: 'Configuration', status: 'pass', category: 'destination', message: 'Generated 40 properties.', technical: '', why: '', fix: null, blocking: false, doc: '' },
        { code: 'advanced_unverified', title: 'Advanced properties', status: 'unknown', category: 'destination', message: 'Passed to Debezium as typed, not verified by BeziSync: debezium.source.poll.interval.ms.', technical: '', why: '', fix: null, blocking: false, doc: '' },
      ],
    };
    render(<CheckList report={unverified} expert={false} subject="orders" />);
    expect(screen.getByText(/not verified by BeziSync: debezium\.source\.poll\.interval\.ms/)).toBeInTheDocument();
    expect(screen.getByLabelText('Not verified')).toBeInTheDocument();
    expect(screen.queryByText('Generated 40 properties.')).toBeNull();
    const { container } = render(reportBadge(unverified));
    expect(container).toHaveTextContent('Ready · 1 not verified');
    expect(container.textContent).not.toBe('Ready');
  });
});
