import { render, screen, within } from '@testing-library/react';
import { SettingsChangeList, settingsChangeLines, type SettingsChange } from '../SettingsChange';

const change: SettingsChange = {
  impact: 'first_copy',
  settings: [
    { key: 'debezium.source.poll.interval.ms', title: 'Poll interval', from: '500', to: null, impact: 'restart' },
    { key: 'debezium.source.snapshot.max.threads', title: 'Snapshot threads', from: null, to: '4', impact: 'first_copy' },
    { key: 'debezium.source.driver.x', title: 'debezium.source.driver.x', from: null, to: '1', impact: 'restart', unverified: true },
  ],
};

describe('SettingsChangeList', () => {
  it('groups changes by what they do, heaviest first, and says when BeziSync can’t verify one', () => {
    render(<SettingsChangeList change={change} />);
    const groups = screen.getAllByRole('region');
    expect(groups[0]).toHaveAccessibleName('Only matters while the pipeline first copies existing rows');
    expect(within(groups[0]!).getByText('Snapshot threads')).toBeInTheDocument();
    expect(within(groups[1]!).getByText(/500 → default/)).toBeInTheDocument();
    expect(within(groups[1]!).getByText(/not verified by BeziSync/)).toBeInTheDocument();
  });
  it('gives a confirmation one line per change, heaviest first', () => {
    const lines = settingsChangeLines(change);
    expect(lines[0]).toBe('Snapshot threads: default → 4 — only matters while the pipeline first copies existing rows.');
    expect(settingsChangeLines(undefined)).toEqual([]);
  });
});
