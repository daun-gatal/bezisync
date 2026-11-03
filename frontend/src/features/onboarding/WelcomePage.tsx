import { Check } from 'lucide-react';
import clsx from 'clsx';
import { ButtonLink } from '@/components/ui/button';
import { Card, Page, PageHeader } from '@/components/ui/layout';
import { List, ListRow } from '@/components/ui/table';
import { Meta } from '@/components/ui/text';
import { useRuntimes } from '@/features/settings/RuntimesSection';
import { useConnections } from '@/lib/connections';
import { usePipelines } from '@/lib/pipelines';
import { useProject } from '@/lib/session';

export function WelcomePage() {
  const { project, can } = useProject();
  const runtimes = useRuntimes(project.id);
  const connections = useConnections(project.id);
  const pipelines = usePipelines(project.id);
  const steps = [
    {
      done: (runtimes.data ?? []).some((r) => r.last_test?.ok),
      title: 'Connect a runtime',
      body: 'Where connectors run: a Kubernetes namespace or a Docker host. BeziSync checks it has the permissions it needs.',
      cta: { label: 'Connect', to: '/p/$projectSlug/settings/$section' as const, params: { section: 'runtimes' } },
      allowed: can('runtime.manage'),
    },
    {
      done: (connections.data ?? []).some((c) => c.kind === 'source'),
      title: 'Add a source database',
      body: 'PostgreSQL, MySQL/MariaDB, SQL Server, MongoDB, Oracle, CockroachDB, Vitess or Google Cloud Spanner. We check it is ready for change data capture and tell you exactly what to fix.',
      cta: { label: 'Add source', to: '/p/$projectSlug/connections' as const, params: {} },
      allowed: can('connection.manage'),
    },
    {
      done: (pipelines.data ?? []).some((p) => p.deployed_version),
      title: 'Create your first pipeline',
      body: 'Choose tables and a destination. Final checks run automatically before anything is deployed.',
      cta: { label: 'Create pipeline', to: '/p/$projectSlug/pipelines/new' as const, params: {} },
      allowed: can('pipeline.edit'),
    },
  ];
  const done = steps.filter((s) => s.done).length;
  return (
    <Page>
      <PageHeader
        title="Welcome to BeziSync"
        description="Stream every change from your databases to Kafka, Apache Iceberg tables, another database, Pub/Sub, Kinesis, SQS, SNS, Event Hubs, Pulsar, Redis, NATS or any HTTP endpoint, with checks that catch problems before they reach production."
      />
      <Card title={`Set up project “${project.name}”`} actions={<Meta>{`${done} of ${steps.length} done`}</Meta>} pad={false}>
        <List label="Setup steps">
          {steps.map((s, i) => (
            <ListRow key={s.title}>
              <span
                className={clsx(
                  'grid size-8 shrink-0 place-items-center rounded-full text-sm font-bold',
                  s.done ? 'bg-ok-soft text-ok' : 'bg-grey-soft text-ink-2',
                )}
              >
                {s.done ? (
                  <>
                    <Check className="size-4" aria-hidden />
                    <span className="sr-only">Done</span>
                  </>
                ) : (
                  <>
                    <span className="sr-only">Step </span>
                    {i + 1}
                  </>
                )}
              </span>
              <span className="flex min-w-0 flex-[1_1_240px] flex-col gap-0.5">
                <strong className="font-semibold">{s.title}</strong>
                <span className="text-base text-muted">{s.body}</span>
              </span>
              {s.allowed ? (
                <ButtonLink to={s.cta.to as never} params={{ projectSlug: project.slug, ...s.cta.params } as never} variant={s.done ? 'secondary' : 'primary'}>
                  {s.done ? 'View' : s.cta.label}
                </ButtonLink>
              ) : (
                <Meta>Ask an Admin</Meta>
              )}
            </ListRow>
          ))}
        </List>
      </Card>
      <Meta as="p">
        New to change data capture? A pipeline copies existing rows once (a <em>snapshot</em>) and then streams every insert, update and delete as an event.
      </Meta>
    </Page>
  );
}
