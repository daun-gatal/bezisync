import { useState } from 'react';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { flinkChoices, type FlinkChoices } from '@/lib/engines';
import { FlinkRunSettings } from '../FlinkSettings';
import { flinkSizeHelp } from '../ReviewStep';

const catalogue = [
  { key: 'restart-strategy.type', title: 'Restart strategy', type: 'enum' as const, default: 'exponential-delay', choices: ['exponential-delay', 'fixed-delay'], why: 'How it restarts.' },
  { key: 'pekko.ask.timeout', title: 'Internal call timeout', type: 'duration' as const, default: '10 s', min: '10s', max: '10m', why: 'Waits.' },
];

function Harness({ sinkType, expert, onValue }: { sinkType: string; expert: boolean; onValue: (v: FlinkChoices) => void }) {
  const [v, setV] = useState(flinkChoices(undefined));
  return (
    <FlinkRunSettings
      value={v}
      onChange={(next) => {
        setV(next);
        onValue(next);
      }}
      sinkType={sinkType}
      expert={expert}
      catalogue={catalogue}
      deployed={false}
    />
  );
}

describe('FlinkRunSettings', () => {
  it('offers Freshness only for batch-loaded destinations, and calls the interval a commit on Iceberg', () => {
    const { unmount } = render(<Harness sinkType="starrocks" expert={false} onValue={() => undefined} />);
    expect(screen.getByLabelText('Freshness')).toBeInTheDocument();
    expect(screen.getByLabelText('Save the position every')).toBeInTheDocument();
    unmount();
    render(<Harness sinkType="iceberg" expert={false} onValue={() => undefined} />);
    expect(screen.queryByLabelText('Freshness')).toBeNull();
    expect(screen.getByLabelText('Commit every')).toBeInTheDocument();
    expect(screen.queryByLabelText(/Add a Flink setting/)).toBeNull();
  });

  it('adds catalogue settings with their default and removes them (Expert)', async () => {
    let last = flinkChoices(undefined);
    render(<Harness sinkType="doris" expert onValue={(v) => (last = v)} />);
    await userEvent.click(screen.getByLabelText(/Add a Flink setting/));
    await userEvent.click(screen.getByRole('option', { name: /Internal call timeout/ }));
    expect(last.flink_conf).toEqual({ 'pekko.ask.timeout': '10 s' });
    const field = screen.getByLabelText('Internal call timeout');
    await userEvent.clear(field);
    await userEvent.type(field, '30 s');
    expect(last.flink_conf['pekko.ask.timeout']).toBe('30 s');
    await userEvent.click(screen.getByRole('button', { name: 'Remove Internal call timeout' }));
    expect(last.flink_conf).toEqual({});
  });

  it('chooses how schema changes and new columns are handled', async () => {
    let last = flinkChoices(undefined);
    render(<Harness sinkType="fluss" expert={false} onValue={(v) => (last = v)} />);
    const group = screen.getByRole('radiogroup', { name: 'New columns' });
    await userEvent.click(within(group).getByRole('radio', { name: /Send/ }));
    expect(last.new_columns).toBe('send');
  });
});

describe('flinkSizeHelp', () => {
  it('describes the cluster', () => {
    expect(flinkSizeHelp({ taskmanagers: 1, parallelism: 2, total_memory_mib: 3072 })).toBe('A JobManager and 1 TaskManager · 2 readers · 3 GiB in all');
    expect(flinkSizeHelp({ taskmanagers: 4, parallelism: 8, total_memory_mib: 9728 })).toBe('A JobManager and 4 TaskManagers · 8 readers · 9.5 GiB in all');
    expect(flinkSizeHelp(undefined)).toBe('');
  });
});
