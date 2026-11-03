import { useState } from 'react';
import { Search } from '@/components/ui/form';
import { Card, Page, PageHeader } from '@/components/ui/layout';
import { Meta } from '@/components/ui/text';

interface Term {
  term: string;
  /** The Debezium / database name, when different. */
  aka?: string;
  plain: string;
  why: string;
}

/** Plain-language glossary: what each Debezium concept means and why BeziSync cares. */
export const GLOSSARY: Term[] = [
  {
    term: 'Change data capture (CDC)',
    plain: 'Reading every insert, update and delete from a database’s own change log, as it happens, without querying tables.',
    why: 'Downstream systems stay in sync with no extra load on the application.',
  },
  {
    term: 'Debezium Server',
    plain: 'The open-source program that reads a database’s change log and sends events to a destination. BeziSync runs one per pipeline.',
    why: 'BeziSync configures, starts and watches it so you never edit its properties by hand.',
  },
  {
    term: 'Pipeline',
    aka: 'connector',
    plain: 'One source database, the tables you picked, and one destination — plus how events look and how much CPU/memory they get.',
    why: 'Every saved change is a version you can deploy or roll back.',
  },
  {
    term: 'Snapshot',
    aka: 'snapshot.mode',
    plain: 'Copying the rows that already exist before streaming new changes.',
    why: '“Copy existing rows” gives downstream a full picture; “Only new changes” starts empty.',
  },
  {
    term: 'Incremental snapshot',
    aka: 'signal: execute-snapshot',
    plain: 'Copying a table in small chunks while changes keep streaming.',
    why: 'How BeziSync backfills an added table or re-syncs one without stopping the pipeline.',
  },
  {
    term: 'Write-ahead log (WAL)',
    aka: 'PostgreSQL',
    plain: 'PostgreSQL’s record of every change, which CDC reads.',
    why: 'The database keeps WAL until every reader has consumed it — a stopped pipeline can fill the disk.',
  },
  {
    term: 'Replication slot',
    aka: 'PostgreSQL',
    plain: 'A bookmark in the WAL that PostgreSQL keeps for one reader.',
    why: 'Each pipeline holds one. BeziSync shows how much WAL it retains and drops it when you delete the pipeline.',
  },
  {
    term: 'Publication',
    aka: 'PostgreSQL',
    plain: 'The list of tables PostgreSQL publishes changes for.',
    why: 'BeziSync creates and updates it to match your table selection.',
  },
  {
    term: 'Binary log (binlog)',
    aka: 'MySQL / MariaDB',
    plain: 'MySQL’s change log.',
    why: 'If it is purged before the pipeline reads it, the pipeline must re-snapshot — BeziSync warns when retention is short.',
  },
  {
    term: 'Change tables',
    aka: 'SQL Server CDC',
    plain: 'Tables SQL Server fills with changes once CDC is enabled for a table.',
    why: 'Each captured table must have CDC enabled; the checks give the exact commands.',
  },
  {
    term: 'Oplog / change streams',
    aka: 'MongoDB',
    plain: 'MongoDB’s replica-set change log and the API to follow it.',
    why: 'Needs a replica set; a small oplog limits how long a pipeline may be stopped.',
  },
  {
    term: 'Offset',
    aka: 'LSN, binlog position, resume token',
    plain: 'The exact point in the change log a pipeline has reached.',
    why: 'Stored in BeziSync’s state database, so restarts resume without loss. Reset only with care.',
  },
  {
    term: 'Heartbeat',
    aka: 'heartbeat.interval.ms',
    plain: 'A tiny periodic write so the pipeline keeps moving even when your tables are quiet.',
    why: 'Prevents WAL build-up on idle databases and lets BeziSync measure end-to-end latency.',
  },
  {
    term: 'Signal',
    aka: 'signal table',
    plain: 'A command sent to a running pipeline through a small table (for example “snapshot this table”).',
    why: 'BeziSync sends signals for you from buttons — you never insert them by hand.',
  },
  {
    term: 'Transform (SMT)',
    aka: 'single message transform',
    plain: 'A step that changes each event before it is sent: keep only some events, keep only the new row, rename or add fields.',
    why: 'Set them in the pipeline’s Transform step; the preview shows the event after every step. “Row” shape (unwrap) is easiest to consume; “Full envelope” keeps before/after and metadata.',
  },
  {
    term: 'Masking',
    aka: 'column.mask.hash',
    plain: 'Replacing a column’s value with a salted hash before it leaves the database host.',
    why: 'Personal data never reaches the destination, yet equal values still match for joins. Choose per column in a pipeline’s Tables step: Debezium can hash text columns only, so other columns are removed instead.',
  },
  {
    term: 'MCP',
    aka: 'Model Context Protocol',
    plain: 'The protocol AI agents (Claude, Cursor, …) use to call tools in other systems.',
    why: 'With MCP on, an agent can check, build and operate pipelines for you, with exactly your permissions. Connect one under Account → AI agents.',
  },
  {
    term: 'Schema history',
    aka: 'schema.history.internal',
    plain: 'Debezium’s record of table structures over time (MySQL, SQL Server, Oracle).',
    why: 'Needed to decode older changes after a column is added or dropped. BeziSync stores it for you.',
  },
  {
    term: 'Topic / stream',
    plain: 'Where events for one table land in the destination (Kafka topic, Kinesis stream, Iceberg or database table, SNS topic, NATS subject…).',
    why: 'The naming template decides these names; preview it before deploying.',
  },
  {
    term: 'Health',
    plain: 'Healthy, Lagging, At risk, Down or Unknown — based only on fresh evidence from the connector.',
    why: 'BeziSync never shows green without proof. Unknown (grey) means it cannot currently tell.',
  },
];

export function LearnPage() {
  const [q, setQ] = useState('');
  const query = q.trim().toLowerCase();
  const terms = GLOSSARY.filter((t) => !query || `${t.term} ${t.aka ?? ''} ${t.plain}`.toLowerCase().includes(query));
  return (
    <Page>
      <PageHeader title="Learn" description="Debezium and CDC in plain words: what each term means and why it matters to your pipelines." />
      <Search label="Search the glossary" placeholder="Search: slot, snapshot, offset…" value={q} onChange={setQ} />
      <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,340px),1fr))] gap-4">
        {terms.map((t) => (
          <Card key={t.term}>
            <h2 className="m-0 text-lg font-semibold wrap-break-word">
              {t.term} {t.aka ? <span className="text-xs font-normal text-muted">· {t.aka}</span> : null}
            </h2>
            <p className="m-0 text-base">{t.plain}</p>
            <p className="m-0 text-base text-muted">{t.why}</p>
          </Card>
        ))}
      </div>
      {terms.length === 0 ? <Meta as="p">No term matches “{q}”.</Meta> : null}
    </Page>
  );
}
