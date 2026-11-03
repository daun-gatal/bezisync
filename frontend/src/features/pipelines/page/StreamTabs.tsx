import { useMemo, useState } from 'react';
import { Empty, Notice, Pill } from '@/components/ui/feedback';
import { Search, Segmented } from '@/components/ui/form';
import { Card, QueryState, Toolbar } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import { Meta, Short } from '@/components/ui/text';
import { useEvents, useLogs, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { fullSpec } from '../spec';
import { EventsMatch } from '../transforms/EventsMatch';

export function EventsTab({ pipeline: p }: { pipeline: Pipeline }) {
  const { project, can } = useProject();
  const tables = fullSpec(p.spec).source.tables.include;
  const [table, setTable] = useState(tables[0]);
  const allowed = can('pipeline.preview_data');
  const events = useEvents(project.id, p.id, table, allowed && Boolean(p.deployed_version));
  if (!allowed) {
    return (
      <Card>
        <Empty title="You can’t preview data">Previewing events needs the Operator role or higher.</Empty>
      </Card>
    );
  }
  if (!p.deployed_version) {
    return (
      <Card>
        <Empty title="No events yet">Deploy the pipeline to see events.</Empty>
      </Card>
    );
  }
  const records = [...(events.data?.records ?? [])].reverse();
  return (
    <Card title="Latest events" actions={<Meta className="hidden sm:inline">Read-only · refreshes every 5 s</Meta>}>
      <Toolbar>
        <div className="min-w-0 flex-[0_1_320px]">
          <label htmlFor="events-table" className="sr-only">
            Table
          </label>
          <Select
            id="events-table"
            value={table}
            onChange={setTable}
            options={tables.map((t) => ({ value: t, label: <span className="font-mono">{t}</span>, text: t }))}
          />
        </div>
        {events.data ? (
          <Short max={48} mono className="text-xs text-muted">
            {events.data.destination}
          </Short>
        ) : null}
      </Toolbar>
      <Toolbar>
        {events.data?.masked_fields.length ? (
          <Pill tone="warn">{`Masked: ${events.data.masked_fields.join(', ')}`}</Pill>
        ) : events.data ? (
          <Pill tone="grey">No fields masked</Pill>
        ) : null}
        {events.data?.readable && table ? <EventsMatch pipeline={p} table={table} values={(events.data.records ?? []).map((r) => r.value)} /> : null}
      </Toolbar>
      <Meta as="p">BeziSync reads without committing consumer offsets, so your consumers are unaffected.</Meta>
      <QueryState query={events} doing="Couldn’t read events">
        {(data) =>
          !data.readable ? (
            <Notice tone="info">BeziSync can’t read this destination type back.</Notice>
          ) : records.length === 0 ? (
            <Empty title="No events delivered yet" />
          ) : (
            <pre
              aria-label="Events"
              tabIndex={0}
              data-scroll-x
              className="relative m-0 max-h-[560px] overflow-auto rounded-md bg-code-bg p-4 font-mono text-xs leading-relaxed text-code-ink"
            >
              {records
                .map((r) => `${r.timestamp ? new Date(String(r.timestamp)).toLocaleTimeString() : ''}  #${String(r.offset ?? '')}  ${JSON.stringify(r.value)}`)
                .join('\n')}
            </pre>
          )
        }
      </QueryState>
    </Card>
  );
}

const LEVELS = ['ERROR', 'WARN', 'INFO', 'DEBUG'] as const;
type Level = (typeof LEVELS)[number];

/** Keep lines at or above `level`. A line without a level (a stack trace, a wrapped message) belongs to the line before it. */
export function filterLogs(lines: string[], level: Level, query: string): string[] {
  const allowed = new Set(LEVELS.slice(0, LEVELS.indexOf(level) + 1));
  const q = query.trim().toLowerCase();
  const out: string[] = [];
  let current: Level | null = null;
  for (const line of lines) {
    const own = LEVELS.find((l) => line.includes(` ${l} `)) ?? null;
    if (own) current = own;
    const lineLevel = own ?? current;
    if (lineLevel && !allowed.has(lineLevel)) continue;
    if (q && !line.toLowerCase().includes(q)) continue;
    out.push(line);
  }
  return out;
}

export function LogsTab({ pipeline: p }: { pipeline: Pipeline }) {
  const { project } = useProject();
  const logs = useLogs(project.id, p.id, Boolean(p.deployed_version));
  const [query, setQuery] = useState('');
  const [level, setLevel] = useState<Level>('INFO');
  const lines = useMemo(() => filterLogs(logs.data?.lines ?? [], level, query), [logs.data, level, query]);
  if (!p.deployed_version) {
    return (
      <Card>
        <Empty title="No logs yet">Deploy the pipeline to see its connector’s logs.</Empty>
      </Card>
    );
  }
  return (
    <Card title="Logs" actions={<Meta className="hidden sm:inline">Secrets redacted · live</Meta>}>
      <Toolbar>
        <Search label="Search logs" placeholder="slot, error…" value={query} onChange={setQuery} />
        <Segmented label="Level" value={level} onChange={setLevel} options={LEVELS.map((l) => ({ value: l, label: l === 'DEBUG' ? 'All' : `${l}+` }))} />
      </Toolbar>
      <QueryState query={logs} doing="Couldn’t load the logs">
        {() => (
          <pre
            aria-label="Connector logs"
            tabIndex={0}
            data-scroll-x
            className="relative m-0 max-h-[600px] overflow-auto rounded-md bg-code-bg p-4 font-mono text-xs leading-relaxed text-code-ink"
          >
            {lines.length ? lines.slice(-800).join('\n') : query ? `No line matches “${query}”.` : 'No log lines yet.'}
          </pre>
        )}
      </QueryState>
    </Card>
  );
}
