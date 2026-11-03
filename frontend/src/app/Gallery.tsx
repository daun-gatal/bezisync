import { useState } from 'react';
import { Plus, Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Meter, Notice, Pill, Skeleton } from '@/components/ui/feedback';
import { Check, ChipsInput, Field, Input, RadioCards, Search, Segmented, SlugInput, Stepper, Switch } from '@/components/ui/form';
import { Card, KeyValue, Metric, Page, PageHeader, Split } from '@/components/ui/layout';
import { CodeBlock, Confirm, Menu, Modal } from '@/components/ui/overlays';
import { Combobox, Select, TimeRange } from '@/components/ui/select';
import { Cell, DataTable } from '@/components/ui/table';
import { Tabs, TabPanel } from '@/components/ui/tabs';
import { Ident, Short } from '@/components/ui/text';
import { ApiError } from '@/lib/api';

/**
 * Development only (/__ui): every component with stress data (names at their longest, with
 * nothing to break at), for checking layout by eye at any width. Not part of production builds.
 */
const LONG = 'orders-postgres-primary-to-kafka-analytics-pii-masking-v2-eu1';
const ROWS = Array.from({ length: 23 }, (_, i) => ({
  id: `r${i}`,
  name: i % 3 ? `pipeline-${i}` : `${LONG}-${i}`,
  owner: i % 2 ? 'payments-platform-data-engineering-oncall' : '',
}));

export function Gallery() {
  const [tab, setTab] = useState<'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g'>('a');
  const [selected, setSelected] = useState(new Set<string>());
  const [v, setV] = useState('medium');
  const [on, setOn] = useState(true);
  const [n, setN] = useState(30);
  const [name, setName] = useState('Orders CDC');
  const [chips, setChips] = useState(['oncall@example.com']);
  const [win, setWin] = useState<string | null>('22:00-06:00');
  const [modal, setModal] = useState(false);
  const [confirm, setConfirm] = useState(false);
  const options = [
    { value: 'small', label: 'Small', description: '0.5 CPU · 768 MiB' },
    { value: 'medium', label: 'Medium', description: '1 CPU · 1.5 GiB' },
    { value: 'large', label: 'Large', disabled: true, why: 'Your project allows up to Medium.' },
  ];
  return (
    <div className="min-h-screen bg-bg px-4 py-6 md:px-8">
      <Page>
        <PageHeader
          crumbs={[{ label: 'Gallery' }, { label: LONG }]}
          title={LONG}
          status={
            <>
              <Pill tone="ok">Healthy</Pill>
              <Pill tone="warn">Lagging</Pill>
              <Pill tone="bad">{LONG}</Pill>
              <Pill tone="grey">Stale</Pill>
              <Pill tone="info">Streaming</Pill>
            </>
          }
          actions={
            <>
              <Menu
                label="More actions"
                icon="more"
                items={[
                  { label: 'Edit', onSelect: () => undefined },
                  '-',
                  { label: 'Delete', icon: <Trash2 />, danger: true, disabled: true, why: 'Used by 3 pipelines.', onSelect: () => undefined },
                ]}
              />
              <Button disabled why="Only editors can deploy.">
                Deploy
              </Button>
              <Button variant="primary" icon={<Plus />} onClick={() => setModal(true)}>
                Open a modal
              </Button>
            </>
          }
        />
        <Tabs
          idBase="g"
          label="Gallery"
          value={tab}
          onChange={setTab}
          tabs={['a', 'b', 'c', 'd', 'e', 'f', 'g'].map((id) => ({ id: id as 'a', label: `Section ${id.toUpperCase()} with a long name`, count: 12 }))}
        />
        <TabPanel idBase="g" value={tab}>
          <Split ratio="main">
            <Card title={LONG} actions={<Button size="sm">Action</Button>}>
              <KeyValue
                items={[
                  ['Source', <Short mono>{LONG}</Short>],
                  ['Owner', 'payments-platform-data-engineering-oncall-rotation'],
                  ['Ident', <Ident>{LONG}</Ident>],
                ]}
              />
              <div className="flex flex-wrap gap-6">
                <Metric label="Behind the source" value="1.2 s" foot={LONG} />
                <Metric label="Down" value="3" tone="bad" />
              </div>
              <Meter label="WAL kept for this pipeline" ratio={0.024} text="245 MB of 10 GB limit" />
              <Meter label={LONG} ratio={1.7} text="17 GB of 10 GB limit" tone="risk" />
              <Notice tone="warn" title="A notice with an action" action={<Button size="sm">Fix it</Button>}>
                {LONG} has no primary key.
              </Notice>
              <InlineError
                error={new ApiError({ status: 503, code: 'x', detail: 'The database is restarting.', request_id: 'req_0123456789abcdef' })}
                doing="Couldn’t load pipelines"
                onRetry={() => undefined}
              />
              <Skeleton />
            </Card>
            <Card title="Inputs">
              <Field label="Name" hint="Shown as a slug.">
                {(id) => <SlugInput id={id} value={name} onChange={setName} taken={['orders-cdc']} />}
              </Field>
              <Field label="Size">{(id) => <Select id={id} value={v} onChange={setV} options={options} />}</Field>
              <Field label="Owner">{(id) => <Combobox id={id} value={v} onChange={setV} options={options} />}</Field>
              <Field label="Latency">{(id) => <Stepper id={id} value={n} onChange={setN} unit="seconds" />}</Field>
              <Field label="Recipients">{(id) => <ChipsInput id={id} values={chips} onChange={setChips} />}</Field>
              <Field label="Free text">{(id) => <Input id={id} placeholder={LONG} />}</Field>
              <Search label="Search" value="" onChange={() => undefined} />
              <Switch checked={on} onChange={setOn} label="A switch" description={`With a description: ${LONG}`} />
              <Check checked={on} onChange={setOn} label={LONG} description="A long label wraps." />
              <Segmented label="Range" value="6h" onChange={() => undefined} options={['1h', '6h', '24h'].map((x) => ({ value: x, label: x }))} />
              <TimeRange value={win} onChange={setWin} label="Maintenance window" />
            </Card>
          </Split>
          <Card title="Radio cards">
            <RadioCards label="Size" value={v} onChange={setV} options={[...options, { value: LONG, label: LONG, description: LONG }]} />
          </Card>
          <Card title="Data table" pad={false}>
            <DataTable
              label="Rows"
              rows={ROWS}
              rowKey={(r) => r.id}
              rowLabel={(r) => r.name}
              rowTone={(r) => (r.id === 'r1' ? 'bad' : r.id === 'r2' ? 'warn' : null)}

              selection={{ selected, onChange: setSelected, why: (r) => (r.id === 'r3' ? 'Drafts can’t be picked.' : null) }}
              bulk={(s) => <Button size="sm">Act on {s.length}</Button>}
              columns={[
                { id: 'name', header: 'Name', min: 200, cell: (r) => <Cell main={r.name} sub={r.owner || 'no owner'} /> },
                { id: 'state', header: 'State', min: 120, cell: () => <Pill tone="info">Streaming</Pill> },
                { id: 'lag', header: 'Latency', priority: 2, fit: true, align: 'right', cell: () => '1.2 s' },
                { id: 'img', header: 'Image', priority: 3, min: 140, cell: () => <Short max={24} mono>{`quay.io/debezium/server:${LONG}`}</Short> },
              ]}
            />
          </Card>
          <Card title="Code and empty states">
            <CodeBlock code={`debezium.source.table.include.list=${LONG}\n`.repeat(3)} label="Code" />
            <Empty title={LONG}>An empty state with a long title.</Empty>
            <Button variant="danger" onClick={() => setConfirm(true)}>
              Open a confirmation
            </Button>
          </Card>
        </TabPanel>
      </Page>
      <Modal
        open={modal}
        onOpenChange={setModal}
        dirty
        title={LONG}
        description="Closing asks first, because this one is dirty."
        footer={<Button onClick={() => setModal(false)}>Close</Button>}
      >
        <p className="m-0">{LONG.repeat(3)}</p>
      </Modal>
      <Confirm
        open={confirm}
        onOpenChange={setConfirm}
        title={`Delete ${LONG}?`}
        effects={['The connector stops.', `Replication slot ${LONG} is dropped.`]}
        confirmLabel="Delete"
        typeToConfirm={LONG}
        onConfirm={() => setConfirm(false)}
      />
    </div>
  );
}
