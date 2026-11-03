import { useState, type ReactNode } from 'react';
import { Link } from '@tanstack/react-router';
import { useMutation } from '@tanstack/react-query';
import clsx from 'clsx';
import { ArrowRight, ArrowUpRight, Box, Check, Container, X } from 'lucide-react';
import { Button, ButtonAnchor } from '@/components/ui/button';
import { InlineError, Pill as Badge } from '@/components/ui/feedback';
import { Title } from '@/components/ui/text';
import { FeedbackDialog } from '@/features/feedback/FeedbackDialog';
import { client, unwrap } from '@/lib/api';
import { useProject, useProviders } from '@/lib/session';

type ProjectPath = '/p/$projectSlug/pipelines' | '/p/$projectSlug/pipelines/new' | '/p/$projectSlug/connections' | '/p/$projectSlug/account';

const PAINS = [
  {
    before: 'A stuck replication slot quietly keeps WAL until the primary’s disk fills.',
    after: 'Retained WAL is on screen, the source turns At risk, and an alert fires long before the disk is full.',
  },
  {
    before: 'A connector deploys, then fails on wal_level, permissions or a missing publication.',
    after: 'Preflight catches it before the deploy and hands you the exact SQL or setting to fix.',
  },
  {
    before: 'Dashboards stay green while nothing is flowing.',
    after: 'No fresh evidence means Unknown, never healthy, and a canary can prove delivery end to end.',
  },
  {
    before: 'Only the one Debezium expert can safely change a pipeline.',
    after: 'Anyone ships changes inside guardrails, with versions, approvals and a full audit trail.',
  },
];

const STAGES = [
  {
    title: 'Create',
    line: 'Wizard, template or YAML',
    lead: 'Start however suits you: a guided wizard, a template an Admin prepared, a YAML file in git, or the Debezium config you already run.',
    who: 'Editors',
    points: [
      'The wizard discovers schemas, tables and columns for you',
      'Shape events without learning SMTs: filter, rename and add fields from pickers, with a live preview',
      'Templates come with the destination and runtime already chosen',
      'Import an existing application.properties: BeziSync maps it and never keeps plain passwords',
      'Or keep it as YAML in git and apply it with the CLI',
    ],
  },
  {
    title: 'Check',
    line: 'Preflight with exact fixes',
    lead: 'Before anything deploys, BeziSync checks the source, the destination, the runtime and your project’s policies.',
    who: 'Runs automatically on every deploy',
    points: [
      'Source readiness: log level, permissions, publications, primary keys',
      'Fixes tailored to your provider (self-hosted, RDS, Cloud SQL, Azure), ready to copy',
      'Policy checks can never be skipped, even with “Deploy anyway”',
    ],
  },
  {
    title: 'Deploy',
    line: 'Snapshot, approval, go',
    lead: 'Choose how the pipeline starts, and let a second person approve it when the project asks for that.',
    who: 'Editors; approvers when required',
    points: [
      'Start with a snapshot of existing rows, or only new changes',
      'Approvals deploy exactly the version that was reviewed, after preflight runs again',
      'Snapshots wait for the maintenance windows you set',
    ],
  },
  {
    title: 'Run',
    line: 'Watched, not just running',
    lead: 'While it streams, BeziSync watches the pipeline and the database it reads from, and only calls it healthy with fresh evidence.',
    who: 'Everyone on the project; alerts to your channels',
    points: [
      'Lag, throughput and errors, with At risk before retained WAL or binlog hurts the source',
      'Schema changes detected; breaking ones flagged for review',
      'Live event preview, and an optional canary that proves delivery end to end',
      'The last error with a diagnosis, plus logs and per-table snapshot progress',
    ],
  },
  {
    title: 'Change',
    line: 'Versions and one-click ops',
    lead: 'Every change is a version. Operations that used to need a config file and a restart are buttons that explain what they will do.',
    who: 'Operators and editors',
    points: [
      'Compare versions and roll back in one click',
      'Snapshot tables again, pause, resume, restart, manage offsets',
      'Move to a newer Debezium as an ordinary, reversible version',
    ],
  },
  {
    title: 'Retire',
    line: 'Clean removal',
    lead: 'Deleting a pipeline cleans up after it, so nothing is left behind holding log on your production database.',
    who: 'Editors; recorded in the audit log',
    points: [
      'Stops the connector and removes what it created on the source (for PostgreSQL, its replication slot and publication)',
      'Drops its stored offsets and schema history and, if you choose, the destination’s topics or streams, reporting every step',
      'Who did what stays in the audit log',
    ],
  },
];

const TRY: { title: string; body: string; to: ProjectPath; cta: string; demo: ReactNode }[] = [
  {
    title: 'Preflight checks with the exact fix',
    body: 'Before anything deploys, BeziSync checks the source, the destination and the runtime. Each problem comes with the fix for your provider and a message ready to send to your DBA.',
    to: '/p/$projectSlug/connections',
    cta: 'Check a connection',
    demo: (
      <>
        <span className="font-semibold text-bad">■ wal_level is “replica”; CDC needs “logical”</span>
        <code className="block rounded-md bg-code-bg px-2.5 py-2 text-xs whitespace-pre-wrap text-code-ink">
          {'ALTER SYSTEM SET wal_level = logical;\n-- then restart PostgreSQL'}
        </code>
        <span className="text-ok">● Replication user has REPLICATION</span>
      </>
    ),
  },
  {
    title: 'Protects your source database',
    body: 'A stuck pipeline makes PostgreSQL keep WAL until the disk fills. BeziSync watches it and warns you long before that.',
    to: '/p/$projectSlug/pipelines',
    cta: 'Open a pipeline’s source health',
    demo: (
      <>
        <span className="flex justify-between gap-2">
          <span className="text-ink-2">Retained WAL</span>
          <span className="font-semibold">3.4 GB of 4 GB safe size</span>
        </span>
        <span aria-hidden className="block h-2 overflow-hidden rounded-full bg-grey-soft">
          <span className="block h-full w-[85%] bg-risk" />
        </span>
        <Badge tone="risk" className="self-start">
          At risk
        </Badge>
      </>
    ),
  },
  {
    title: 'Health you can trust',
    body: 'Green needs fresh evidence. No recent metrics means Unknown, and a canary can prove events really reach the destination.',
    to: '/p/$projectSlug/pipelines',
    cta: 'See the Pipelines list',
    demo: (
      <span className="flex flex-wrap gap-2">
        <Badge tone="ok">Healthy</Badge>
        <Badge tone="grey">Unknown · no metrics for 2 min</Badge>
        <Badge tone="warn">Lagging 45 s</Badge>
      </span>
    ),
  },
  {
    title: 'One-click operations',
    body: 'Snapshot a table, pause, resume or restart, and manage offsets, without touching a config file. Each action says what it will do first.',
    to: '/p/$projectSlug/pipelines',
    cta: 'Operate a pipeline',
    demo: (
      <span className="flex flex-wrap gap-1.5">
        {['Snapshot tables', 'Pause', 'Restart', 'Offsets'].map((b) => (
          <span key={b} className="inline-flex h-7.5 items-center rounded-md border border-line-strong bg-surface px-2.5 font-medium">
            {b}
          </span>
        ))}
      </span>
    ),
  },
  {
    title: 'Every change is a version',
    body: 'Each change becomes one version you can compare. A bad change is one click from rolling back.',
    to: '/p/$projectSlug/pipelines',
    cta: 'Edit a pipeline and compare',
    demo: (
      <span className="flex flex-col divide-y divide-line">
        <span className="flex justify-between gap-2 py-1.5">
          <span>
            <strong>v4</strong> · added order_items
          </span>
          <span className="font-semibold text-ok">Deployed</span>
        </span>
        <span className="flex justify-between gap-2 py-1.5">
          <span>
            <strong>v3</strong> · masked customers.email
          </span>
          <span className="font-semibold text-accent">Roll back</span>
        </span>
        <span className="flex justify-between gap-2 py-1.5">
          <span>
            <strong>v2</strong> · first deploy
          </span>
          <span className="text-muted">Compare</span>
        </span>
      </span>
    ),
  },
  {
    title: 'Personal data stays out',
    body: 'Columns that look like personal data are masked as soon as you pick a table. Choose per column to mask, remove or keep it, then check the live event preview to see exactly what the destination gets.',
    to: '/p/$projectSlug/pipelines/new',
    cta: 'Start a pipeline',
    demo: (
      <span className="grid grid-cols-2 gap-x-3 gap-y-1">
        <span className="text-muted">email</span>
        <code className="font-mono text-xs">9f2c…e41a (masked)</code>
        <span className="text-muted">phone</span>
        <code className="font-mono text-xs text-muted">removed</code>
        <span className="text-muted">status</span>
        <code className="font-mono text-xs">PAID</code>
      </span>
    ),
  },
  {
    title: 'Guided or Expert, your choice',
    body: 'Guided mode explains every choice in plain words, and Learn translates Debezium terms. Expert mode shows the raw properties, technical check details and advanced settings.',
    to: '/p/$projectSlug/account',
    cta: 'Switch mode in your account',
    demo: (
      <>
        <span className="inline-flex gap-0.5 self-start rounded-lg bg-grey-soft p-0.5">
          <span className="rounded-md bg-surface px-2.5 py-0.5 font-semibold">Guided</span>
          <span className="px-2.5 py-0.5 text-ink-2">Expert</span>
        </span>
        <span className="text-ink-2">“Logical replication is off, so changes can’t be read yet.”</span>
        <code className="font-mono text-xs text-muted">wal_level=replica (required: logical)</code>
      </>
    ),
  },
  {
    title: 'Pipelines as code',
    body: 'Keep pipelines as YAML in git. Make a personal API token and let CI show the diff, then apply it. The same checks, policies and approvals still apply.',
    to: '/p/$projectSlug/account',
    cta: 'Create an API token',
    demo: (
      <code className="block rounded-md bg-code-bg px-3 py-2.5 text-xs leading-relaxed whitespace-pre-wrap text-code-ink">
        {'$ bezisync remote diff -f pipelines/\n~ shop-orders: + table order_items\n$ bezisync remote apply -f pipelines/ --deploy'}
      </code>
    ),
  },
  {
    title: 'Work with an AI agent',
    body: 'When an Admin turns on MCP, agents such as Claude can check, build and operate pipelines for you. They do only what your role allows, and never handle passwords or keys.',
    to: '/p/$projectSlug/account',
    cta: 'Connect an agent',
    demo: (
      <>
        <span className="text-muted">“Why is shop-orders lagging?”</span>
        <span>
          <strong>get_pipeline_metrics</strong> · <strong>check_pipeline</strong>
        </span>
        <span className="text-ok">● Retained WAL 3.1 GB: the slot is behind a long transaction.</span>
      </>
    ),
  },
];

const SOURCES = ['PostgreSQL', 'MySQL / MariaDB', 'SQL Server', 'MongoDB', 'Oracle', 'CockroachDB', 'Vitess', 'Google Cloud Spanner'];
const DESTINATIONS = ['Kafka', 'Apache Iceberg', 'PostgreSQL, MySQL, SQL Server (JDBC)', 'Google Pub/Sub', 'Amazon Kinesis', 'Amazon SQS', 'Amazon SNS', 'Azure Event Hubs', 'Apache Pulsar', 'Redis Streams', 'NATS JetStream', 'HTTP'];

const RUNTIMES = [
  {
    title: 'Docker',
    icon: Container,
    body: 'One container per pipeline on a host you already run. Health and metrics are read inside the container, so no network path to it is needed.',
    points: ['The local Docker socket', 'A remote daemon over TCP, with TLS client certificates', 'Your own Docker network for the connectors'],
  },
  {
    title: 'Kubernetes',
    icon: Box,
    body: 'A Deployment, ConfigMap and Secret per pipeline in the namespace you choose, with your service account and image pull secrets.',
    points: ['The cluster BeziSync runs in', 'Another cluster with a kubeconfig token', 'GKE and EKS: BeziSync mints short-lived tokens itself'],
  },
  {
    title: 'Remote agent',
    icon: ArrowUpRight,
    body: 'For networks you can’t open. A small agent next to Docker or Kubernetes dials out to BeziSync, so nothing needs inbound access.',
    points: [
      'Install with a Helm chart or one docker run',
      'Preflight checks run on the agent, inside your network',
      'Its token is shown once and can be rotated',
    ],
    highlight: true,
  },
];

const ADMIN: { id: string; title: string; line: string; body: string; demo: ReactNode }[] = [
  {
    id: 'guardrails',
    title: 'Policies and approvals',
    line: 'Rules every pipeline must follow; four-eyes deploys',
    body: 'Admins decide what is allowed per project: runtimes, destinations, snapshot modes and sizes. With approvals on, a deploy waits for a second person, and preflight runs again before it ships. Policy failures can never be forced.',
    demo: (
      <div className="flex flex-col gap-3">
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
          {[
            ['Largest connector size', 'medium', 'text-ink'],
            ['Approval to deploy', 'On', 'text-ok'],
            ['“Deploy anyway”', 'Off', 'text-bad'],
            ['Snapshot modes', 'initial, when_needed', 'text-ink'],
          ].map(([k, v, c]) => (
            <span key={k} className="flex justify-between gap-2 rounded-lg border border-line px-3 py-2.5">
              {k} <strong className={c}>{v}</strong>
            </span>
          ))}
        </div>
        <div className="flex flex-wrap items-center gap-3 rounded-[10px] bg-warn-soft px-3.5 py-3 text-warn">
          <span className="min-w-60 flex-1">
            <strong>dana.k</strong> wants to deploy <strong>shop-orders v5</strong>. Preflight passed.
          </span>
          <span className="rounded-md bg-accent px-2.5 py-1 font-semibold text-on-accent">Approve</span>
          <span className="rounded-md border border-line-strong bg-surface px-2.5 py-1 font-medium text-ink">Reject</span>
        </div>
      </div>
    ),
  },
  {
    id: 'audit',
    title: 'Audit log',
    line: 'Who changed what, when, and why',
    body: 'Every change is recorded with its author, including forced deploys with the checks that were skipped and the reason given. Export it for compliance as CSV or JSON Lines.',
    demo: (
      <div className="overflow-hidden rounded-[10px] border border-line">
        {[
          ['14:02:11', 'Deployed shop-orders v5 (approved by lee.m)', 'dana.k'],
          ['13:40:57', 'Changed connection shop-postgres (password)', 'lee.m'],
          ['13:12:03', 'Forced deploy: skipped 1 warning, reason “hotfix”', 'sam.o'],
        ].map(([t, s, a]) => (
          <div key={t} className="grid grid-cols-[80px_minmax(0,1fr)] gap-3 border-b border-line px-3.5 py-2 md:grid-cols-[90px_minmax(0,1fr)_90px]">
            <code className="font-mono text-xs text-muted">{t}</code>
            <span>{s}</span>
            <span className="hidden text-muted md:block">{a}</span>
          </div>
        ))}
        <div className="flex gap-4 bg-subtle px-3.5 py-2 font-semibold text-accent">
          <span>Export CSV</span>
          <span>Export JSON Lines</span>
        </div>
      </div>
    ),
  },
  {
    id: 'people',
    title: 'People, roles and SSO',
    line: 'Viewer, operator, editor, Admin',
    body: 'Give each person the least they need, per project. Admins add your identity providers in Settings → Sign-in, let their groups decide roles automatically, and can turn off email and password sign-in once single sign-on works.',
    demo: (
      <div className="flex flex-col gap-2.5">
        <div className="grid grid-cols-2 gap-2 md:grid-cols-4">
          {[
            ['Viewer', 'sees health and history'],
            ['Operator', '+ pause, snapshot, ack'],
            ['Editor', '+ create and deploy'],
            ['Admin', 'everything, everywhere'],
          ].map(([r, d]) => (
            <span key={r} className="rounded-lg border border-line p-2.5">
              <strong>{r}</strong>
              <br />
              <span className="text-muted">{d}</span>
            </span>
          ))}
        </div>
        <span className="rounded-lg border border-line bg-subtle px-3 py-2.5">
          Single sign-on with Google, Microsoft, Okta, Keycloak, GitLab, GitHub (organizations and teams) or any OpenID Connect, OAuth2 or SAML 2.0 provider: groups map to project roles and approvers, re-checked at every sign-in.
        </span>
      </div>
    ),
  },
  {
    id: 'agents',
    title: 'AI agents (MCP)',
    line: 'Agents that do only what their person may',
    body: 'Turn on MCP in Settings and agents connect with an API token or by signing in, for the projects, access and time their person chooses. Every tool runs with that person’s role, policies and approvals and lands in the audit log; tools that delete or reset stay off until you turn them on.',
    demo: (
      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
        {[
          ['Reads (pipelines, health, checks)', 'On', 'text-ok'],
          ['Changes (create, deploy, pause)', 'On', 'text-ok'],
          ['Deletes and offset resets', 'Off', 'text-bad'],
          ['Passwords, keys and tokens', 'Never', 'text-bad'],
        ].map(([k, v, c]) => (
          <span key={k} className="flex justify-between gap-2 rounded-lg border border-line px-3 py-2.5">
            {k} <strong className={c}>{v}</strong>
          </span>
        ))}
      </div>
    ),
  },
  {
    id: 'secrets',
    title: 'Encrypted, write-only secrets',
    line: 'Never shown again, or kept in your vault',
    body: 'Passwords are encrypted at rest and can be replaced but never read back, by anyone. Or keep them in your own secret store and give BeziSync only a reference.',
    demo: (
      <div className="flex flex-col gap-2.5">
        <span className="flex h-9 items-center justify-between rounded-lg border border-line-strong px-3 text-muted">
          Password: set · write-only <span className="text-accent">Replace</span>
        </span>
        <code className="rounded-md bg-code-bg px-2.5 py-2 font-mono text-xs text-code-ink">ref+vault://kv/shop#password</code>
        <span className="flex flex-wrap gap-1.5">
          {['HashiCorp Vault', 'AWS Secrets Manager', 'Google Secret Manager', 'Kubernetes Secrets'].map((s) => (
            <span key={s} className="rounded-full bg-grey-soft px-2.5 py-0.5">
              {s}
            </span>
          ))}
        </span>
      </div>
    ),
  },
  {
    id: 'versions',
    title: 'Debezium upgrades',
    line: 'See who runs what; move pipelines safely',
    body: 'A catalog of approved Debezium versions shows which pipelines run which. Move them in bulk as ordinary, reversible versions; changing a default never silently restarts anything.',
    demo: (
      <div className="flex flex-col gap-2.5">
        <span className="flex justify-between gap-2 rounded-lg border border-line px-3 py-2.5">
          <span>
            <strong>Debezium 3.7.0.Final</strong> · default
          </span>
          <span className="font-semibold text-ok">9 pipelines</span>
        </span>
        <span className="flex justify-between gap-2 rounded-lg bg-warn-soft px-3 py-2.5 text-warn">
          <span>
            <strong>3.5.2.Final</strong> · deprecated
          </span>
          <span className="font-semibold">3 pipelines → Move to 3.7</span>
        </span>
        <span className="text-muted">Each move is a normal new version: preflight runs, approvals apply, and rollback is the previous version.</span>
      </div>
    ),
  },
  {
    id: 'alerts',
    title: 'Alerts where you work',
    line: 'Slack, PagerDuty, webhooks, email',
    body: 'Rules for lag, retained WAL, failed deliveries and schema changes, each sent to the channels you choose, plus an in-app inbox for everyone on the project.',
    demo: (
      <div className="flex flex-col gap-2">
        {[
          ['Retained WAL above the safe size', 'Slack · PagerDuty'],
          ['Lag over 60 s for 5 min', 'Slack'],
          ['Breaking schema change not reviewed', 'Email · webhook'],
        ].map(([r, c]) => (
          <span key={r} className="flex justify-between gap-2 rounded-lg border border-line px-3 py-2.5">
            <span>{r}</span>
            <span className="text-muted">{c}</span>
          </span>
        ))}
      </div>
    ),
  },
  {
    id: 'projects',
    title: 'Projects and promotion',
    line: 'Staging and production, kept apart',
    body: 'Projects keep teams and environments apart, each with its own members, runtimes, connections and policies. Promote a pipeline from staging and it goes through production’s own policies and approvals.',
    demo: (
      <div className="flex flex-col gap-2.5">
        <div className="flex flex-wrap items-center gap-3">
          <span className="min-w-40 flex-1 rounded-lg border border-line p-3">
            <strong>staging</strong>
            <br />
            <span className="text-muted">shop-orders v5 · tested</span>
          </span>
          <span className="rounded-full bg-accent px-3 py-1 font-semibold text-on-accent">Promote →</span>
          <span className="min-w-40 flex-1 rounded-lg border border-line p-3">
            <strong>production</strong>
            <br />
            <span className="text-muted">waiting for approval</span>
          </span>
        </div>
        <span className="text-muted">Production’s connections and runtime are used; its policies are checked; its approvers decide.</span>
      </div>
    ),
  },
];

const ANSWERS = [
  {
    id: 'yes',
    label: 'Yes, we run Debezium today',
    thanks: 'Thank you! What would you need before using it at work? Your setup, your sources, a must-have feature?',
  },
  { id: 'maybe', label: 'Maybe, not yet', thanks: 'Thanks! What is missing, or what would make it a yes?' },
  { id: 'no', label: 'Not for us', thanks: 'Thanks for being honest. A sentence on why helps us most.' },
];

function Section({ id, title, intro, aside, children }: { id: string; title: string; intro?: ReactNode; aside?: ReactNode; children: ReactNode }) {
  return (
    <section aria-labelledby={`${id}-h`} id={id} className="flex flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="flex flex-col gap-1.5">
          <h2 id={`${id}-h`} className="m-0 text-xl font-bold">
            {title}
          </h2>
          {intro ? <p className="m-0 max-w-2xl text-lg text-muted">{intro}</p> : null}
        </div>
        {aside}
      </div>
      {children}
    </section>
  );
}

/** What BeziSync does, for people who only see part of it (editors in a lab never open Settings). */
export function TourPage() {
  const { project, me } = useProject();
  const lab = useProviders().data?.lab ?? false;
  const slug = project.slug;
  const [stage, setStage] = useState(3);
  const [admin, setAdmin] = useState(ADMIN[0]!.id);
  const [answer, setAnswer] = useState<string | null>(null);
  const [telling, setTelling] = useState(false);
  const page = `/p/${slug}/tour`;
  const vote = useMutation({
    mutationFn: (label: string) =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/feedback', {
          params: { path: { project_id: project.id } },
          body: { kind: 'other', message: `Would your team use BeziSync? ${label}`, page },
        }),
      ),
    onError: () => setAnswer(null),
  });
  const current = STAGES[stage]!;
  const shown = ADMIN.find((a) => a.id === admin) ?? ADMIN[0]!;
  const chosen = ANSWERS.find((a) => a.id === answer);
  const sample = lab || project.is_sandbox;

  return (
    <div className="mx-auto flex w-full max-w-[1040px] flex-col gap-14 pb-10">
      <section aria-labelledby="tour-h" className="flex flex-col gap-4">
        <span className="self-start rounded-full bg-accent-soft px-2.5 py-0.5 text-xs font-semibold text-accent-ink">Tour · 5 minutes</span>
        <Title id="tour-h" lines="none" className="max-w-3xl text-2xl font-bold tracking-tight">
          Debezium, without the surprises.
        </Title>
        <p className="m-0 max-w-2xl text-lg leading-relaxed text-ink-2">
          BeziSync runs Debezium Server for you and checks everything around it: before you deploy, while it streams, and whenever someone changes it. Here is
          what you can try in {project.name} right now, and what Admins add on top.
        </p>
        <div className="mt-1 flex flex-wrap gap-2.5">
          <ButtonAnchor href="#try" variant="primary">
            Try it in {project.name}
          </ButtonAnchor>
          <ButtonAnchor href="#admins">See what Admins get</ButtonAnchor>
        </div>
      </section>

      <Section id="pain" title="The problems it takes away">
        <div className="overflow-hidden rounded-xl border border-line bg-surface">
          <div className="grid grid-cols-2 border-b border-line bg-subtle px-5 py-2.5 text-xs font-semibold text-muted">
            <span>Running Debezium by hand</span>
            <span>With BeziSync</span>
          </div>
          {PAINS.map((p) => (
            <div key={p.before} className="grid grid-cols-2 gap-6 border-b border-line px-5 py-4 text-sm leading-normal last:border-b-0">
              <span className="flex gap-2.5 text-ink-2">
                <X className="mt-0.5 size-4 shrink-0 text-bad" aria-label="Before" />
                {p.before}
              </span>
              <span className="flex gap-2.5">
                <Check className="mt-0.5 size-4 shrink-0 text-ok" aria-label="After" />
                {p.after}
              </span>
            </div>
          ))}
        </div>
      </Section>

      <Section
        id="lifecycle"
        title="A pipeline’s whole life, covered"
        intro="Debezium starts the connector. BeziSync looks after everything before, during and after. Pick a stage."
      >
        <div role="tablist" aria-label="Pipeline stages" className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6">
          {STAGES.map((s, i) => (
            <button
              key={s.title}
              type="button"
              role="tab"
              id={`stage-${i}`}
              aria-selected={stage === i}
              aria-controls="stage-panel"
              onClick={() => setStage(i)}
              className={clsx(
                'flex min-h-16 items-start gap-2.5 rounded-[10px] px-3 py-2.5 text-left',
                stage === i ? 'border-[1.5px] border-accent bg-accent-soft' : 'border border-line-strong bg-surface hover:bg-hover',
              )}
            >
              <span
                className={clsx(
                  'grid size-6 shrink-0 place-items-center rounded-full text-xs font-bold',
                  stage === i ? 'bg-accent text-on-accent' : 'bg-grey-soft text-ink-2',
                )}
              >
                {i + 1}
              </span>
              <span className="flex flex-col gap-0.5">
                <span className="text-sm font-semibold">{s.title}</span>
                <span className="text-xs leading-snug text-muted">{s.line}</span>
              </span>
            </button>
          ))}
        </div>
        <div
          role="tabpanel"
          id="stage-panel"
          aria-labelledby={`stage-${stage}`}
          className="flex flex-wrap gap-5 rounded-xl border border-line bg-surface p-5.5"
        >
          <div className="flex min-w-60 flex-1 flex-col gap-1.5">
            <span className="text-xs font-semibold text-accent-ink">
              Stage {stage + 1} of {STAGES.length}
            </span>
            <h3 className="m-0 text-xl font-bold">{current.title}</h3>
            <p className="m-0 text-sm leading-relaxed text-ink-2">{current.lead}</p>
            <span className="mt-1.5 text-sm text-muted">{current.who}</span>
          </div>
          <ul className="m-0 flex min-w-80 flex-[2] list-none flex-col gap-2 p-0">
            {current.points.map((p) => (
              <li key={p} className="flex gap-2.5 rounded-lg border border-line bg-subtle px-3 py-2.5 text-base leading-snug">
                <Check className="mt-0.5 size-4 shrink-0 text-ok" aria-hidden />
                {p}
              </li>
            ))}
          </ul>
        </div>
      </Section>

      <Section id="try" title="Try it now" intro={`Everything below works in ${project.name}${sample ? ', on live sample data' : ''}.`}>
        <div className="grid grid-cols-[repeat(auto-fill,minmax(min(100%,300px),1fr))] gap-4">
          {TRY.map((t) => (
            <article key={t.title} className="flex flex-col gap-3 rounded-xl border border-line bg-surface p-4.5">
              <h3 className="m-0 text-base font-semibold">{t.title}</h3>
              <p className="m-0 text-base leading-normal text-ink-2">{t.body}</p>
              <div className="flex flex-col gap-2 rounded-[10px] border border-line bg-subtle p-3 text-sm">{t.demo}</div>
              <Link
                to={t.to}
                params={{ projectSlug: slug }}
                className="mt-auto inline-flex items-center gap-1 text-base font-semibold text-accent hover:text-accent-hover"
              >
                {t.cta} <ArrowRight className="size-3.5" aria-hidden />
              </Link>
            </article>
          ))}
        </div>
      </Section>

      <Section
        id="stack"
        title="Works with your stack"
        intro={`Every source can feed every destination.${lab ? ' All of them are in the Playground, with live traffic.' : ''}`}
      >
        <div className="flex flex-wrap items-stretch gap-4">
          {[
            ['Sources', SOURCES],
            ['Destinations', DESTINATIONS],
          ].map(([label, items], i) => (
            <div key={label as string} className="flex min-w-72 flex-1 items-center gap-4">
              {i === 1 ? <ArrowRight className="size-5 shrink-0 text-muted" aria-hidden /> : null}
              <div className="flex flex-1 flex-col gap-2.5 self-stretch rounded-xl border border-line bg-surface p-4.5">
                <span className="text-xs font-semibold text-muted">{label as string}</span>
                <ul className="m-0 flex list-none flex-wrap gap-2 p-0 text-base font-medium">
                  {(items as string[]).map((s) => (
                    <li key={s} className="rounded-lg border border-line-strong px-3 py-1.5">
                      {s}
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          ))}
        </div>
        <div className="grid grid-cols-1 gap-3 text-sm text-ink-2 md:grid-cols-2">
          <span className="rounded-[10px] border border-line bg-subtle px-3.5 py-3">
            <strong className="text-ink">Behind a bastion?</strong> PostgreSQL, MySQL and SQL Server can be reached through an SSH tunnel, with the bastion’s
            host key pinned.
          </span>
          <span className="rounded-[10px] border border-line bg-subtle px-3.5 py-3">
            <strong className="text-ink">Managed databases.</strong> Presets for RDS, Cloud SQL, Azure and self-hosted set sensible defaults and tailor every
            fix.
          </span>
        </div>
      </Section>

      <Section
        id="runtimes"
        title="Runs on your infrastructure"
        intro="Each pipeline is its own Debezium Server, started where your data lives. An Admin connects a runtime once; after that, picking it is one choice in the pipeline wizard."
      >
        <div className="grid grid-cols-[repeat(auto-fit,minmax(min(100%,280px),1fr))] gap-4">
          {RUNTIMES.map((r) => (
            <article
              key={r.title}
              className={clsx('flex flex-col gap-3 rounded-xl bg-surface p-4.5', r.highlight ? 'border-[1.5px] border-accent' : 'border border-line')}
            >
              <div className="flex items-center gap-2.5">
                <span
                  className={clsx('grid size-8 place-items-center rounded-lg', r.highlight ? 'bg-accent text-on-accent' : 'bg-accent-soft text-accent-ink')}
                  aria-hidden
                >
                  <r.icon className="size-4" />
                </span>
                <h3 className="m-0 text-base font-semibold">{r.title}</h3>
              </div>
              <p className="m-0 text-base leading-normal text-ink-2">{r.body}</p>
              <ul className="m-0 flex flex-col gap-1 pl-4.5 text-sm text-ink-2">
                {r.points.map((p) => (
                  <li key={p}>{p}</li>
                ))}
              </ul>
            </article>
          ))}
        </div>
        <div className="flex flex-wrap gap-x-5 gap-y-2 rounded-[10px] border border-line bg-subtle px-4 py-3 text-sm text-ink-2">
          <span>
            <strong className="text-ink">Direct</strong> · BeziSync talks to the Docker or Kubernetes API
          </span>
          <span>
            <strong className="text-ink">Agent</strong> · the agent fetches its work; works with either
          </span>
          <span>
            <strong className="text-ink">Cordon</strong> · stop new pipelines on a runtime while running ones keep going
          </span>
        </div>
      </Section>

      <Section
        id="admins"
        title="What Admins get"
        intro={
          me.user.is_superuser
            ? 'Admins set up runtimes and guardrails once; then anyone on the team can ship pipelines safely. As an Admin, you find all of this in Settings.'
            : 'Admins set up runtimes and guardrails once; then anyone on the team can ship pipelines safely. You can’t open these screens yourself, so here is how they look.'
        }
        aside={
          <Badge tone="grey">
            Admins only
          </Badge>
        }
      >
        <div className="flex flex-wrap items-stretch gap-4">
          <div role="tablist" aria-label="Admin capabilities" aria-orientation="vertical" className="flex min-w-64 flex-1 flex-col gap-1 md:max-w-80">
            {ADMIN.map((a) => (
              <button
                key={a.id}
                type="button"
                role="tab"
                id={`admin-${a.id}`}
                aria-selected={admin === a.id}
                aria-controls="admin-panel"
                onClick={() => setAdmin(a.id)}
                className={clsx(
                  'flex flex-col gap-0.5 rounded-[10px] px-3 py-2.5 text-left',
                  admin === a.id ? 'border-[1.5px] border-accent bg-surface' : 'border border-transparent hover:bg-hover',
                )}
              >
                <span className="text-sm font-semibold">{a.title}</span>
                <span className="text-sm leading-snug text-muted">{a.line}</span>
              </button>
            ))}
          </div>
          <div
            role="tabpanel"
            id="admin-panel"
            aria-labelledby={`admin-${shown.id}`}
            className="flex min-h-96 min-w-0 flex-[999_1_420px] flex-col gap-4 rounded-xl border border-line bg-surface p-5.5 text-sm"
          >
            <div className="flex flex-col gap-1">
              <h3 className="m-0 text-lg font-semibold">{shown.title}</h3>
              <p className="m-0 text-sm leading-relaxed text-ink-2">{shown.body}</p>
            </div>
            {shown.demo}
          </div>
        </div>
      </Section>

      <section aria-labelledby="vote-h" className="flex flex-col gap-3.5 rounded-2xl border border-line bg-surface p-7">
        <h2 id="vote-h" className="m-0 text-xl font-bold">
          Would your team use BeziSync?
        </h2>
        <p className="m-0 max-w-2xl text-lg leading-normal text-ink-2">
          BeziSync is pre-release, and your answer decides what we build next. One click is enough; a sentence is gold.
        </p>
        <div role="group" aria-label="Would your team use BeziSync?" className="flex flex-wrap gap-2">
          {ANSWERS.map((a) => (
            <button
              key={a.id}
              type="button"
              aria-pressed={answer === a.id}
              disabled={answer !== null}
              onClick={() => {
                setAnswer(a.id);
                vote.mutate(a.label);
              }}
              className={clsx(
                'h-11 rounded-[10px] px-4 text-sm font-semibold disabled:cursor-default',
                answer === a.id
                  ? 'border-[1.5px] border-accent bg-accent-soft text-accent-ink'
                  : 'border border-line-strong bg-surface text-ink enabled:hover:bg-hover disabled:opacity-60',
              )}
            >
              {a.label}
            </button>
          ))}
        </div>
        <InlineError error={vote.error} doing="Couldn’t send your answer" />
        {chosen && vote.isSuccess ? (
          <div role="status" className="flex flex-wrap items-center gap-3 rounded-[10px] bg-ok-soft px-4 py-3.5 text-sm text-ok">
            <span className="min-w-70 flex-1">{chosen.thanks}</span>
            <Button variant="primary" onClick={() => setTelling(true)}>
              Tell us in Feedback
            </Button>
          </div>
        ) : null}
      </section>

      <FeedbackDialog open={telling} onOpenChange={setTelling} page={page} initialKind="idea" />
    </div>
  );
}
