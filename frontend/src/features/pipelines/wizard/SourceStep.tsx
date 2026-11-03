import { useEffect, useState } from 'react';
import { Plus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Notice, Skeleton } from '@/components/ui/feedback';
import { Field } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import { CheckList } from '@/components/CheckList';
import { ConnectionDialog } from '@/features/connections/ConnectionDialog';
import { allows, usePolicies } from '@/lib/policies';
import { templateSummary, useTemplates } from '@/lib/templates';
import { TYPE_LABEL, useCheckConnection, useConnections } from '@/lib/connections';
import { slugify } from '@/lib/format';
import { useCreatePipeline, usePipelines, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { ConnectionCards, createdHere, StepHeader } from './shared';

/** Step 1: the database to capture. Continuing creates the draft (with a name you can change on Review). */
export function SourceStep({ pipeline, initialTemplate, onNext }: { pipeline?: Pipeline; initialTemplate?: string; onNext: (draftId: string) => void }) {
  const { project, can, expert } = useProject();
  const connections = useConnections(project.id);
  const pipelines = usePipelines(project.id);
  const check = useCheckConnection(project.id);
  const create = useCreatePipeline(project.id);
  const [selected, setSelected] = useState<string | undefined>(pipeline?.source.id);
  const [adding, setAdding] = useState(false);
  const templates = useTemplates(project.id);
  const [templateId, setTemplateId] = useState<string>(initialTemplate ?? '');
  const template = templates.data?.find((t) => t.id === templateId);
  const policies = usePolicies(project.id).data;
  const sources = (connections.data ?? []).filter(
    (c) => c.kind === 'source' && (allows(policies?.allowed_source_types, c.type) || c.id === pipeline?.source.id),
  );
  const current = sources.find((c) => c.id === selected);
  const report = check.data && check.variables?.id === selected ? check.data : current?.last_check;

  // A source picked for the first time (or checked long ago) is checked once; after that, "Re-check" is explicit.
  useEffect(() => {
    if (current && !pipeline && (!current.last_check || Date.now() - Date.parse(current.last_check.checked_at) > 5 * 60_000)) {
      check.mutate({ id: current.id });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [current?.id]);

  function next() {
    if (pipeline) return onNext(pipeline.id);
    if (!current) return;
    const taken = new Set((pipelines.data ?? []).map((p) => p.name));
    let name = slugify(`${current.name}-cdc`);
    for (let i = 2; taken.has(name); i++) name = slugify(`${current.name}-cdc-${i}`);
    create.mutate(
      { name, source_id: current.id, description: '', owner: '', labels: {}, spec: {}, template_id: template?.id ?? null },
      {
        onSuccess: (p) => {
          createdHere.add(p.id);
          onNext(p.id);
        },
      },
    );
  }

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <StepHeader title="Where should changes come from?" description="Pick a database. BeziSync checks it can publish changes before you go further." />
      {policies?.allowed_source_types ? (
        <Notice tone="info">This project allows these sources: {policies.allowed_source_types.map((t) => TYPE_LABEL[t] ?? t).join(', ')}.</Notice>
      ) : null}
      {pipeline ? <Notice tone="info">The source of an existing pipeline can’t change. To capture another database, create a new pipeline.</Notice> : null}
      {!pipeline && templates.data?.length ? (
        <Card>
          <Field
            label="Start from"
            hint={
              template
                ? `${template.description ? `${template.description} ` : ''}Sets: ${templateSummary(template)}.`
                : 'A template sets the destination, format, size and safety defaults your team agreed on.'
            }
          >
            {(id) => (
              <Select
                id={id}
                value={templateId}
                onChange={setTemplateId}
                placeholder="Blank pipeline"
                options={[
                  { value: '', label: 'Blank pipeline' },
                  ...templates.data.map((t) => ({ value: t.id, label: t.title, description: t.description || undefined })),
                ]}
              />
            )}
          </Field>
        </Card>
      ) : null}
      <QueryState query={connections} doing="Couldn’t load the sources">
        {() =>
          sources.length === 0 && !can('connection.manage') ? (
            <Card>
              <Empty title="No sources available">Ask an editor or an Admin to add a source connection.</Empty>
            </Card>
          ) : (
            <ConnectionCards label="Source" items={sources} selected={selected} onSelect={(c) => setSelected(c.id)} disabled={Boolean(pipeline)} />
          )
        }
      </QueryState>
      {can('connection.manage') && !pipeline ? (
        <div>
          <Button icon={<Plus />} onClick={() => setAdding(true)}>
            Add a source
          </Button>
        </div>
      ) : null}
      {current && report ? (
        <Card>
          <CheckList
            report={report}
            expert={expert}
            subject={current.name}
            title={`Readiness checks · ${current.name}`}
            onRecheck={() => check.mutate({ id: current.id })}
            rechecking={check.isPending}
          />
        </Card>
      ) : current && check.isPending ? (
        <Card>
          <Skeleton />
        </Card>
      ) : null}
      <InlineError error={check.error} doing="Couldn’t check the source" onRetry={current ? () => check.mutate({ id: current.id }) : undefined} />
      {current && report && !report.ok ? (
        <Notice tone="warn">You can continue and fix this later. Deploying stays blocked until the checks pass.</Notice>
      ) : null}
      <InlineError error={create.error} doing="Couldn’t create the draft" />
      <div className="flex justify-end">
        <Button variant="primary" disabled={!current && !pipeline} why="Choose a source first." loading={create.isPending} onClick={next}>
          Continue to tables →
        </Button>
      </div>
      <ConnectionDialog
        open={adding}
        onOpenChange={setAdding}
        kind="source"
        onSaved={(c) => {
          setSelected(c.id);
          check.mutate({ id: c.id });
        }}
      />
    </div>
  );
}
