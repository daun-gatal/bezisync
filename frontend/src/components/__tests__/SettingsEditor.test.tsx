import { useState } from 'react';
import { render, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { SettingsEditor } from '../SettingsEditor';
import { RawProperties, type RawRow, type Verdict } from '../RawProperties';
import type { Setting } from '@/lib/settings';

const threads = {
  engine: 'debezium', scope: 'source', stage: 'snapshot', impact: 'first_copy', key: 'debezium.source.snapshot.max.threads',
  title: 'Snapshot threads', type: 'int', default: '1', min: '1', max: '16', why: 'Copies chunks at once.',
} as Setting;
const poll = {
  engine: 'debezium', scope: 'source', stage: 'streaming', impact: 'restart', key: 'debezium.source.poll.interval.ms',
  title: 'Poll interval', type: 'int', default: '500', min: '50', max: '10000', why: 'How long to wait.',
} as Setting;

function Harness({ initial, caps, onValue }: { initial: Record<string, string>; caps?: Record<string, number>; onValue: (v: Record<string, string>) => void }) {
  const [v, setV] = useState(initial);
  return (
    <SettingsEditor
      idBase="t"
      value={v}
      caps={caps}
      settings={[threads, poll]}
      onChange={(next) => {
        setV(next);
        onValue(next);
      }}
    />
  );
}

describe('SettingsEditor', () => {
  it('groups set settings by stage, says what they do, and respects a project cap', async () => {
    const onValue = vi.fn();
    render(<Harness initial={{ 'debezium.source.snapshot.max.threads': '7' }} caps={{ 'debezium.source.snapshot.max.threads': 8 }} onValue={onValue} />);
    const snapshot = screen.getByRole('region', { name: 'Snapshot' });
    expect(within(snapshot).getByText('First copy')).toBeInTheDocument();
    expect(screen.getByText(/This project allows at most 8\./)).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Increase' }));
    expect(onValue).toHaveBeenLastCalledWith({ 'debezium.source.snapshot.max.threads': '8' });
    expect(screen.getByRole('button', { name: 'Increase' })).toBeDisabled();
    // Removing restores the default (the key leaves the map).
    await userEvent.click(screen.getByRole('button', { name: 'Remove Snapshot threads' }));
    expect(onValue).toHaveBeenLastCalledWith({});
  });
  it('flags a key that is no longer a setting for this pipeline', () => {
    render(<Harness initial={{ 'debezium.source.gone': '1' }} onValue={() => undefined} />);
    expect(screen.getByText('Not a setting this pipeline can use: remove it.')).toBeInTheDocument();
  });
});

function RawHarness({ verdicts, onUse }: { verdicts: Record<string, Verdict>; onUse?: (r: RawRow) => void }) {
  const [rows, setRows] = useState<RawRow[]>([
    { key: 'debezium.source.driver.loginTimeout', value: '30' },
    { key: 'debezium.source.slot.drop.on.stop', value: 'true' },
    { key: 'debezium.source.snapshot.max.threads', value: '4' },
    { key: 'debezium.source.driver.loginTimeout', value: '10' },
  ]);
  return <RawProperties idBase="r" rows={rows} onChange={setRows} verdicts={verdicts} onUseSetting={onUse} keyPlaceholder="debezium.source.…" />;
}

describe('RawProperties', () => {
  it('shows what the server says of each key, flags duplicates, and offers typed settings', async () => {
    const onUse = vi.fn();
    const verdicts: Record<string, Verdict> = {
      'debezium.source.driver.loginTimeout': { key: 'debezium.source.driver.loginTimeout', class: 'allowed', message: '', fix: '', setting: null },
      'debezium.source.slot.drop.on.stop': { key: 'debezium.source.slot.drop.on.stop', class: 'forbidden', message: 'Dropping the slot loses WAL.', fix: '', setting: null },
      'debezium.source.snapshot.max.threads': { key: 'debezium.source.snapshot.max.threads', class: 'allowed', message: '', fix: '', setting: threads },
    };
    render(<RawHarness verdicts={verdicts} onUse={onUse} />);
    expect(screen.getAllByText('Not verified')).toHaveLength(2);
    expect(screen.getByText('Not allowed')).toBeInTheDocument();
    expect(screen.getByText('Dropping the slot loses WAL.')).toBeInTheDocument();
    expect(screen.getByText('This key is already set above.')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Use the typed setting' }));
    expect(onUse).toHaveBeenCalledWith({ key: 'debezium.source.snapshot.max.threads', value: '4' });
    await userEvent.click(screen.getByRole('button', { name: 'Add a property' }));
    expect(screen.getByLabelText('Property 5 name')).toHaveValue('');
  });
});

describe('SettingsEditor pins', () => {
  it('draws a boolean setting a project policy pins as disabled (Known issue 13)', async () => {
    const flag = {
      engine: 'debezium', scope: 'source', stage: 'streaming', impact: 'restart', key: 'debezium.source.provide.transaction.metadata',
      title: 'Transaction metadata', type: 'bool', default: 'false', why: 'Adds transaction boundaries.',
    } as Setting;
    const onChange = vi.fn();
    render(<SettingsEditor idBase="p" value={{}} settings={[flag]} pinned={{ [flag.key]: 'true' }} onChange={onChange} />);
    const on = screen.getByRole('button', { name: 'On' });
    const off = screen.getByRole('button', { name: 'Off' });
    expect(on).toHaveAttribute('aria-pressed', 'true');
    expect(off).toBeDisabled();
    await userEvent.click(off);
    expect(onChange).not.toHaveBeenCalled();
    expect(screen.getByText(/Set by project policy\./)).toBeInTheDocument();
  });
});
