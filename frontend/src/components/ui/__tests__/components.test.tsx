import { useState } from 'react';
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { ApiError } from '@/lib/api';
import { Button } from '../button';
import { InlineError, Meter, Pill } from '../feedback';
import { SlugInput, Stepper, Switch } from '../form';
import { Confirm, Modal, OneTimeSecret } from '../overlays';
import { Select } from '../select';
import { DataTable } from '../table';
import { Tabs } from '../tabs';
import { Short, shorten, Title } from '../text';

const LONG = 'orders-postgres-primary-to-kafka-analytics-pii-masking-v2-eu-west-1';

describe('text', () => {
  it('titles wrap anywhere, clamp, and carry the full text as a tooltip', () => {
    render(<Title>{LONG}</Title>);
    const h = screen.getByRole('heading', { level: 1 });
    expect(h).toHaveAttribute('title', LONG);
    expect(h.className).toContain('wrap-anywhere');
    expect(h.className).toContain('line-clamp-2');
  });

  it('shortens in the middle, keeping the start and the distinguishing end', () => {
    const s = shorten(LONG, 32);
    expect(s).toHaveLength(32);
    expect(s.startsWith('orders-postgres')).toBe(true);
    expect(s.endsWith('eu-west-1')).toBe(true);
    expect(shorten('short', 32)).toBe('short');
  });

  it('Short shows the shortened form but screen readers hear the full value', () => {
    render(<Short max={20}>{LONG}</Short>);
    expect(screen.getByText(LONG)).toHaveClass('sr-only');
    expect(screen.getByTitle(LONG)).toBeInTheDocument();
  });
});

describe('Pill', () => {
  it('is a glyph and text, and long text truncates with a tooltip', () => {
    render(<Pill tone="bad">{LONG}</Pill>);
    const text = screen.getByText(LONG);
    expect(text).toHaveClass('truncate');
    expect(text.parentElement).toHaveAttribute('title', LONG);
    expect(text.parentElement?.textContent).toBe(`■${LONG}`);
  });
});

describe('Meter', () => {
  it('is a meter with the amount in words, and stays full past the limit', () => {
    const { rerender } = render(<Meter label="WAL kept" ratio={0.25} text="2.5 GB of 10 GB limit" />);
    const m = screen.getByRole('meter', { name: 'WAL kept' });
    expect(m).toHaveAttribute('aria-valuenow', '25');
    expect(m).toHaveAttribute('aria-valuetext', '2.5 GB of 10 GB limit');
    rerender(<Meter label="WAL kept" ratio={3} text="30 GB of 10 GB limit" tone="risk" />);
    expect(screen.getByRole('meter')).toHaveAttribute('aria-valuenow', '100');
    expect(screen.getByText('30 GB of 10 GB limit').className).toContain('text-risk');
    rerender(<Meter label="WAL kept" ratio={Number.NaN} text="unknown" />);
    expect(screen.getByRole('meter')).toHaveAttribute('aria-valuenow', '0');
  });
});

describe('Button', () => {
  it('a disabled button with a reason stays focusable, explains itself and does nothing', async () => {
    const onClick = vi.fn();
    render(
      <Button disabled why="Only editors can deploy." onClick={onClick}>
        Deploy
      </Button>,
    );
    const b = screen.getByRole('button', { name: 'Deploy' });
    expect(b).toHaveAttribute('aria-disabled', 'true');
    expect(b).toHaveAccessibleDescription('Only editors can deploy.');
    await userEvent.click(b);
    expect(onClick).not.toHaveBeenCalled();
  });

  it('loading buttons are disabled and busy', () => {
    render(<Button loading>Save</Button>);
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Save' })).toHaveAttribute('aria-busy', 'true');
  });
});

describe('InlineError', () => {
  it('says what happened, what to do, and shows the request ID with Try again for server errors', async () => {
    const retry = vi.fn();
    render(
      <InlineError
        error={new ApiError({ status: 503, code: 'x', detail: 'Database is restarting.', request_id: 'req_123' })}
        doing="Couldn’t load pipelines"
        onRetry={retry}
      />,
    );
    const alert = screen.getByRole('alert');
    expect(alert).toHaveTextContent('Couldn’t load pipelines');
    expect(alert).toHaveTextContent('Database is restarting.');
    expect(alert).toHaveTextContent('Request ID req_123');
    await userEvent.click(within(alert).getByRole('button', { name: 'Try again' }));
    expect(retry).toHaveBeenCalled();
  });

  it('does not offer Try again for a validation error, and lists the fields', () => {
    render(
      <InlineError
        error={new ApiError({ status: 422, code: 'invalid', detail: 'Invalid input', errors: [{ loc: ['body', 'name'], message: 'is taken' }] })}
        onRetry={() => undefined}
      />,
    );
    expect(screen.queryByRole('button', { name: 'Try again' })).toBeNull();
    expect(screen.getByRole('alert')).toHaveTextContent('name is taken');
  });
});

describe('Confirm', () => {
  function Harness({ onConfirm = () => undefined }: { onConfirm?: () => void }) {
    const [open, setOpen] = useState(true);
    return (
      <>
        <button type="button" onClick={() => setOpen(true)}>
          Open
        </button>
        <Confirm
          open={open}
          onOpenChange={setOpen}
          title="Delete orders-cdc?"
          effects={['Stops the connector', 'Removes its replication slot']}
          confirmLabel="Delete pipeline"
          typeToConfirm="orders-cdc"
          onConfirm={onConfirm}
        />
      </>
    );
  }

  it('lists what happens and needs the name typed', async () => {
    const onConfirm = vi.fn();
    render(<Harness onConfirm={onConfirm} />);
    expect(screen.getByText('Removes its replication slot')).toBeInTheDocument();
    const confirm = screen.getByRole('button', { name: 'Delete pipeline' });
    expect(confirm).toBeDisabled();
    await userEvent.type(screen.getByRole('textbox'), 'orders-cdc');
    expect(confirm).toBeEnabled();
    await userEvent.click(confirm);
    expect(onConfirm).toHaveBeenCalled();
  });

  it('clears the typed name after Cancel, so the next one is not pre-confirmed', async () => {
    render(<Harness />);
    await userEvent.type(screen.getByRole('textbox'), 'orders-cdc');
    await userEvent.click(screen.getByRole('button', { name: 'Cancel' }));
    await waitFor(() => expect(screen.queryByRole('textbox')).toBeNull());
    await userEvent.click(screen.getByRole('button', { name: 'Open' }));
    expect(screen.getByRole('textbox')).toHaveValue('');
    expect(screen.getByRole('button', { name: 'Delete pipeline' })).toBeDisabled();
  });
});

describe('Modal', () => {
  it('asks before discarding unsaved input, and Keep editing keeps it', async () => {
    const onOpenChange = vi.fn();
    render(
      <Modal open onOpenChange={onOpenChange} title={LONG} dirty>
        <input aria-label="Name" defaultValue="typed" />
      </Modal>,
    );
    expect(screen.getByRole('heading', { name: LONG })).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Close' }));
    expect(screen.getByRole('alertdialog')).toHaveTextContent('Discard your changes?');
    await userEvent.click(screen.getByRole('button', { name: 'Keep editing' }));
    expect(onOpenChange).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: 'Close' }));
    await userEvent.click(screen.getByRole('button', { name: 'Discard' }));
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('closes at once when nothing is unsaved', async () => {
    const onOpenChange = vi.fn();
    render(<Modal open onOpenChange={onOpenChange} title="Hello" />);
    await userEvent.keyboard('{Escape}');
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });
});

describe('OneTimeSecret', () => {
  it('cannot be dismissed until the secret is marked as copied', async () => {
    const onDone = vi.fn();
    render(<OneTimeSecret open onDone={onDone} title="Your new token" secret="bzt_abc" label="API token" />);
    await userEvent.keyboard('{Escape}');
    expect(screen.getByRole('dialog')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Close' })).toBeNull();
    const done = screen.getByRole('button', { name: 'Done' });
    expect(done).toBeDisabled();
    await userEvent.click(screen.getByRole('checkbox', { name: /I’ve copied it/ }));
    await userEvent.click(done);
    expect(onDone).toHaveBeenCalled();
  });
});

describe('inputs', () => {
  it('Stepper: clearing the box puts the last value back instead of saving 0, and values clamp', async () => {
    const onChange = vi.fn();
    render(<Stepper id="s" value={30} onChange={onChange} min={1} max={60} />);
    const box = screen.getByRole('textbox');
    await userEvent.clear(box);
    fireEvent.blur(box);
    expect(box).toHaveValue('30');
    expect(onChange).not.toHaveBeenCalled();
    await userEvent.type(box, '0');
    fireEvent.blur(box);
    expect(onChange).toHaveBeenCalledWith(60);
  });

  it('SlugInput never rewrites what you type, shows the saved form, and flags taken names', async () => {
    function Harness() {
      const [v, setV] = useState('');
      return <SlugInput id="n" value={v} onChange={setV} taken={['orders-cdc']} />;
    }
    render(<Harness />);
    await userEvent.type(screen.getByRole('textbox'), 'Orders CDC');
    expect(screen.getByRole('textbox')).toHaveValue('Orders CDC');
    expect(screen.getByRole('alert')).toHaveTextContent('“orders-cdc” is already used here');
  });

  it('Switch is a labelled switch with its description', async () => {
    const onChange = vi.fn();
    render(<Switch checked={false} onChange={onChange} label="Mask PII" description="Hashes emails and phone numbers." />);
    const sw = screen.getByRole('switch', { name: 'Mask PII' });
    expect(sw).toHaveAccessibleDescription('Hashes emails and phone numbers.');
    await userEvent.click(sw);
    expect(onChange).toHaveBeenCalledWith(true);
  });

  it('Select is found by its label, picks with the mouse or keys, and skips disabled options', async () => {
    function Harness() {
      const [v, setV] = useState<'small' | 'medium' | 'large'>('small');
      return (
        <>
          <label htmlFor="size">Size</label>
          <Select
            id="size"
            value={v}
            onChange={setV}
            options={[
              { value: 'small', label: 'Small', description: '0.5 CPU' },
              { value: 'medium', label: 'Medium', description: '1 CPU' },
              { value: 'large', label: 'Large', disabled: true, why: 'Your project allows up to Medium.' },
            ]}
          />
        </>
      );
    }
    render(<Harness />);
    const trigger = screen.getByLabelText('Size');
    await userEvent.click(trigger);
    expect(screen.getByRole('option', { name: 'Large' })).toHaveAccessibleDescription('Your project allows up to Medium.');
    await userEvent.click(screen.getByRole('option', { name: 'Large' }));
    expect(trigger).toHaveTextContent('Small');
    await userEvent.click(screen.getByRole('option', { name: 'Medium' }));
    expect(trigger).toHaveTextContent('Medium');
    trigger.focus();
    await userEvent.keyboard('{ArrowDown}{ArrowUp}{Enter}');
    expect(trigger).toHaveTextContent('Small');
  });
});

describe('DataTable', () => {
  const rows = Array.from({ length: 25 }, (_, i) => ({ id: `p${i}`, name: `pipe-${i}`, draft: i === 0 }));
  function Harness() {
    const [selected, setSelected] = useState(new Set<string>());
    return (
      <DataTable
        label="Pipelines"
        rows={rows}
        rowKey={(r) => r.id}
        rowLabel={(r) => r.name}
        columns={[{ id: 'name', header: 'Name', cell: (r) => r.name }]}
        selection={{ selected, onChange: setSelected, why: (r) => (r.draft ? 'Drafts have no Debezium version yet.' : null) }}
        bulk={(sel) => <span>bulk {sel.length}</span>}
      />
    );
  }

  it('pages long lists and select-all picks every selectable row, not just the shown page', async () => {
    window.localStorage.setItem('bz.pageSize', '10');
    render(<Harness />);
    expect(screen.getAllByRole('row')).toHaveLength(11);
    expect(screen.getByText('1–10 of 25 pipelines')).toBeInTheDocument();
    expect(screen.getByRole('checkbox', { name: 'Select pipe-0' })).toBeDisabled();
    await userEvent.click(screen.getByRole('checkbox', { name: 'Select all 24' }));
    expect(screen.getByText('bulk 24')).toBeInTheDocument();
    await userEvent.click(screen.getByRole('button', { name: 'Page 3' }));
    expect(screen.getByText('21–25 of 25 pipelines')).toBeInTheDocument();
    expect(screen.getAllByRole('row')).toHaveLength(6);
    expect(screen.getByRole('button', { name: /Next/ })).toBeDisabled();
    // The rows-per-page choice is the viewer's, for every list.
    await userEvent.click(screen.getByRole('button', { name: /10 per page/ }));
    await userEvent.click(screen.getByRole('menuitem', { name: '50 per page' }));
    expect(screen.getAllByRole('row')).toHaveLength(26);
    expect(window.localStorage.getItem('bz.pageSize')).toBe('50');
    window.localStorage.removeItem('bz.pageSize');
  });
});

describe('Tabs', () => {
  it('is a tablist where arrow keys move the selection', async () => {
    function Harness() {
      const [v, setV] = useState<'a' | 'b'>('a');
      return (
        <Tabs
          idBase="t"
          label="Sections"
          value={v}
          onChange={setV}
          tabs={[
            { id: 'a', label: 'Overview' },
            { id: 'b', label: 'Tables', count: 3 },
          ]}
        />
      );
    }
    render(<Harness />);
    const first = screen.getByRole('tab', { name: 'Overview' });
    expect(first).toHaveAttribute('aria-selected', 'true');
    first.focus();
    await userEvent.keyboard('{ArrowRight}');
    expect(screen.getByRole('tab', { name: /Tables/ })).toHaveAttribute('aria-selected', 'true');
  });
});
