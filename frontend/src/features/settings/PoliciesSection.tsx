import { useEffect, useState, type ReactNode } from 'react';
import clsx from 'clsx';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { useBlocker } from '@tanstack/react-router';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { InlineError, Notice } from '@/components/ui/feedback';
import { Check, Field, Stepper, Switch } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import { client, unwrap } from '@/lib/api';
import { TYPE_LABEL, useConnectorTypes } from '@/lib/connections';
import { usePipelines, type Pipeline } from '@/lib/pipelines';
import { allows, usePolicies, type Policies } from '@/lib/policies';
import { useProject } from '@/lib/session';
import { fullSpec, memoryMiB, PRESET_AMOUNTS, SNAPSHOT_MODES } from '@/features/pipelines/spec';
import { TYPE_TITLE } from '@/features/pipelines/transforms/model';
import { useRuntimes } from './RuntimesSection';

const TRANSFORM_OPTIONS = (['unwrap', 'filter', 'rename', 'add_field', 'flatten', 'outbox'] as const).map((t) => ({ value: t, label: TYPE_TITLE[t] }));
const SIZES = ['small', 'medium', 'large'] as const;

/** An allow-list: "Any" (null) allows everything, including what is added later. An empty list is refused. */
function AllowList({
  legend,
  options,
  value,
  onChange,
  disabled,
  anyHint,
}: {
  legend: string;
  options: { value: string; label: string }[];
  value: string[] | null | undefined;
  onChange: (next: string[] | null) => void;
  disabled: boolean;
  anyHint: string;
}) {
  const restricted = value !== null && value !== undefined;
  return (
    <fieldset className="m-0 flex min-w-0 flex-col gap-2 border-0 p-0">
      <legend className="mb-1 text-sm font-medium text-ink-2">{legend}</legend>
      <Check checked={!restricted} disabled={disabled} onChange={(on) => onChange(on ? null : options.map((o) => o.value))} label="Any" description={anyHint} />
      {restricted ? (
        <div className="grid grid-cols-[repeat(auto-fill,minmax(min(100%,180px),1fr))] gap-x-4 gap-y-1.5 pl-6">
          {options.map((o) => (
            <Check
              key={o.value}
              label={o.label}
              checked={value.includes(o.value)}
              disabled={disabled}
              onChange={(on) => onChange(on ? [...value, o.value] : value.filter((v) => v !== o.value))}
            />
          ))}
          {value.length === 0 ? (
            <p role="alert" className="col-span-full m-0 text-xs text-bad">
              Allow at least one, or choose Any.
            </p>
          ) : null}
        </div>
      ) : null}
    </fieldset>
  );
}

function Group({ title, children }: { title: string; children: ReactNode }) {
  return (
    <Card title={title}>
      <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,300px),1fr))] gap-6">{children}</div>
    </Card>
  );
}

/** Why a deployed pipeline's next deploy would be refused under these policies, if it would. */
export function blockedBy(p: Pipeline, policy: Policies, runtimeName: (id: string) => string): string[] {
  const spec = fullSpec(p.spec);
  const out: string[] = [];
  if (!allows(policy.allowed_source_types, p.source.type)) out.push(`source type ${TYPE_LABEL[p.source.type] ?? p.source.type}`);
  if (p.sink && !allows(policy.allowed_sink_types, p.sink.type)) out.push(`destination type ${TYPE_LABEL[p.sink.type] ?? p.sink.type}`);
  if (p.runtime && !allows(policy.allowed_runtime_ids, p.runtime.id)) out.push(`runtime ${runtimeName(p.runtime.id)}`);
  if (policy.max_resources) {
    const limit = PRESET_AMOUNTS[policy.max_resources];
    const custom = spec.policies.custom_resources;
    if (spec.policies.resources === 'custom' && custom) {
      if (Number(custom.cpu) > limit.cpu || memoryMiB(custom.memory) > limit.mem) out.push(`a custom size (${custom.cpu} CPU, ${custom.memory})`);
    } else if (spec.policies.resources !== 'custom' && SIZES.indexOf(spec.policies.resources) > SIZES.indexOf(policy.max_resources)) {
      out.push(`size ${spec.policies.resources}`);
    }
  }
  if (!allows(policy.allowed_snapshot_modes, spec.snapshot.mode)) out.push(`snapshot mode ${spec.snapshot.mode}`);
  for (const t of spec.transforms) {
    if (t.type === 'custom') {
      if (!policy.allow_custom_transforms) out.push('a custom transform');
    } else if (policy.allowed_transforms && !policy.allowed_transforms.includes(t.type as never)) out.push(`the ${TYPE_TITLE[t.type]} transform`);
    if (t.condition && !policy.allow_scripted_transforms) out.push('a filter script');
  }
  if (Object.keys(spec.advanced ?? {}).length > 0 && !policy.allow_advanced_properties) out.push('advanced Debezium properties');
  return [...new Set(out)];
}

export function PoliciesSection() {
  const { project, can } = useProject();
  const qc = useQueryClient();
  const policies = usePolicies(project.id);
  const types = useConnectorTypes(project.id);
  const runtimes = useRuntimes(project.id);
  const pipelines = usePipelines(project.id);
  const [draft, setDraft] = useState<Policies | null>(null);
  const manage = can('project.manage');
  useEffect(() => {
    if (policies.data) setDraft(policies.data);
  }, [policies.data]);
  const save = useMutation({
    mutationFn: (body: Policies) => unwrap(client.PUT('/api/v1/projects/{project_id}/policies', { params: { path: { project_id: project.id } }, body })),
    onSuccess: (p) => {
      qc.setQueryData(['policies', project.id], p);
      toast.success('Policies saved');
    },
  });
  const dirty = Boolean(draft && policies.data && JSON.stringify(draft) !== JSON.stringify(policies.data));
  const blocker = useBlocker({ shouldBlockFn: () => dirty, enableBeforeUnload: () => dirty, withResolver: true });

  return (
    <QueryState query={policies} doing="Couldn’t load the policies">
      {(saved) => {
        if (!draft) return null;
        const set = (patch: Partial<Policies>) => setDraft({ ...draft, ...patch });
        // Newer switches are returned only when on: drop them when off so "dirty" compares like with like.
        const toggle = (
          key: 'require_approval' | 'allow_force_deploy' | 'allow_scripted_transforms' | 'allow_custom_transforms' | 'allow_advanced_properties',
          on: boolean,
        ) => {
          if ((key === 'allow_scripted_transforms' || key === 'allow_custom_transforms' || key === 'allow_advanced_properties') && !on) {
            const rest = { ...draft };
            delete rest[key];
            setDraft(rest);
          } else set({ [key]: on });
        };
        const typeOptions = (kind: 'source' | 'sink') =>
          (types.data ?? []).filter((t) => t.kind === kind).map((t) => ({ value: t.type, label: TYPE_LABEL[t.type] ?? t.title }));
        const lists = [draft.allowed_source_types, draft.allowed_sink_types, draft.allowed_runtime_ids, draft.allowed_snapshot_modes, draft.allowed_transforms];
        const empty = lists.some((l) => l !== null && l !== undefined && l.length === 0);
        const runtimeName = (id: string) => runtimes.data?.find((r) => r.id === id)?.name ?? 'removed runtime';
        const blocked = dirty
          ? (pipelines.data ?? [])
              .filter((p) => p.deployed_version && !['deleted', 'deleting'].includes(p.lifecycle))
              .map((p) => ({ p, now: blockedBy(p, draft, runtimeName), before: blockedBy(p, saved, runtimeName) }))
              .filter((x) => x.now.length > x.before.length)
          : [];
        const limit = draft.max_pipelines_per_source;
        return (
          <div className="flex min-w-0 flex-col gap-4">
            <div className="flex flex-col gap-1">
              <h2 className="m-0 text-xl font-semibold">Policies</h2>
              <p className="m-0 text-base text-muted">
                Guardrails for everyone creating pipelines in {project.name}. The wizard applies them up front, and BeziSync enforces them when saving and
                deploying.
              </p>
            </div>
            {!manage ? <Notice tone="info">Only Admins can change policies.</Notice> : null}
            <Group title="What pipelines may connect to">
              <AllowList
                legend="Sources"
                anyHint="Including types added later."
                options={typeOptions('source')}
                value={draft.allowed_source_types}
                onChange={(v) => set({ allowed_source_types: v })}
                disabled={!manage}
              />
              <AllowList
                legend="Destinations"
                anyHint="Including types added later."
                options={typeOptions('sink')}
                value={draft.allowed_sink_types}
                onChange={(v) => set({ allowed_sink_types: v })}
                disabled={!manage}
              />
              <AllowList
                legend="Runtimes"
                anyHint="Including runtimes added later."
                options={(runtimes.data ?? []).map((r) => ({ value: r.id, label: r.name }))}
                value={draft.allowed_runtime_ids}
                onChange={(v) => set({ allowed_runtime_ids: v })}
                disabled={!manage}
              />
            </Group>
            <Group title="How pipelines may run">
              <Field label="Largest size" hint="Pipelines can’t use a bigger resource preset.">
                {(id) => (
                  <Select
                    id={id}
                    value={draft.max_resources ?? ''}
                    disabled={!manage}
                    onChange={(v) => set({ max_resources: (v || null) as Policies['max_resources'] })}
                    options={[
                      { value: '', label: 'No limit' },
                      { value: 'small', label: 'Small', description: '0.5 CPU · 768 MiB' },
                      { value: 'medium', label: 'Medium', description: '1 CPU · 1.5 GiB' },
                      { value: 'large', label: 'Large', description: '2 CPU · 3 GiB' },
                    ]}
                  />
                )}
              </Field>
              <div className="flex min-w-0 flex-col gap-2">
                <Switch
                  checked={limit !== null && limit !== undefined}
                  disabled={!manage}
                  onChange={(on) => set({ max_pipelines_per_source: on ? 2 : null })}
                  label="Limit pipelines per source"
                  description="Each pipeline holds a replication slot or log reader on the database."
                />
                {limit !== null && limit !== undefined ? (
                  <div className="pl-12">
                    <label htmlFor="pipelines-per-source" className="sr-only">
                      Pipelines per source
                    </label>
                    <Stepper
                      id="pipelines-per-source"
                      value={limit}
                      min={1}
                      max={100}
                      disabled={!manage}
                      unit="per source"
                      onChange={(v) => set({ max_pipelines_per_source: v })}
                    />
                  </div>
                ) : null}
              </div>
              <AllowList
                legend="Snapshot modes"
                anyHint="Every way of copying existing rows."
                options={SNAPSHOT_MODES.map((m) => ({ value: m.value, label: m.title }))}
                value={draft.allowed_snapshot_modes}
                onChange={(v) => set({ allowed_snapshot_modes: v })}
                disabled={!manage}
              />
            </Group>
            <Group title="How events may be shaped">
              <AllowList
                legend="Built-in transforms"
                anyHint="Including transforms added later."
                options={TRANSFORM_OPTIONS}
                value={draft.allowed_transforms}
                onChange={(v) => {
                  if (v === null) {
                    const { allowed_transforms: _all, ...rest } = draft;
                    setDraft(rest);
                  } else set({ allowed_transforms: v as Policies['allowed_transforms'] });
                }}
                disabled={!manage}
              />
              <div className="flex min-w-0 flex-col gap-4">
                <Switch
                  checked={Boolean(draft.allow_custom_transforms)}
                  disabled={!manage}
                  onChange={(on) => toggle('allow_custom_transforms', on)}
                  label="Allow custom transforms"
                  description="Lets Experts add steps with any class the Debezium image ships. BeziSync can’t preview or check custom classes."
                />
                <Switch
                  checked={Boolean(draft.allow_scripted_transforms)}
                  disabled={!manage}
                  onChange={(on) => toggle('allow_scripted_transforms', on)}
                  label="Allow scripted transforms"
                  description="Lets pipelines run filter scripts their authors wrote (Groovy or JavaScript). Scripts run inside the connector, next to its database credentials: allow them only for trusted Editors."
                />
                <Switch
                  checked={Boolean(draft.allow_advanced_properties)}
                  disabled={!manage}
                  onChange={(on) => toggle('allow_advanced_properties', on)}
                  label="Allow advanced Debezium properties"
                  description="Lets Experts pass raw Debezium properties for options BeziSync doesn’t offer. BeziSync refuses keys it sets, credentials and unsafe options, but can’t verify the rest: preflight shows them as not verified."
                />
              </div>
            </Group>
            <Group title="Safety">
              <Switch
                checked={Boolean(draft.require_approval)}
                disabled={!manage}
                onChange={(on) => toggle('require_approval', on)}
                label="Require approval to deploy"
                description="Deploys and rollbacks wait until a second person with approval rights approves them."
              />
              <Switch
                checked={Boolean(draft.allow_force_deploy)}
                disabled={!manage}
                onChange={(on) => toggle('allow_force_deploy', on)}
                label="Allow “Deploy anyway”"
                description="Lets editors deploy despite failed source or destination checks, with a recorded reason. Policy checks can never be skipped."
              />
            </Group>
            {blocked.length ? (
              <Notice tone="warn" title={`${blocked.length} running pipeline${blocked.length === 1 ? '' : 's'} would be blocked at the next deploy`}>
                <ul className="m-0 flex list-disc flex-col gap-0.5 pl-5">
                  {blocked.slice(0, 10).map(({ p, now }) => (
                    <li key={p.id} className="wrap-anywhere">
                      <strong>{p.name}</strong>: uses {now.join(', ')}
                    </li>
                  ))}
                  {blocked.length > 10 ? <li>and {blocked.length - 10} more</li> : null}
                </ul>
                They keep running; only their next deploy is refused until they change.
              </Notice>
            ) : null}
            <InlineError error={save.error} doing="Couldn’t save the policies" />
            {manage ? (
              <div
                className={clsx(
                  'flex flex-wrap items-center justify-end gap-2 rounded-lg border border-line bg-surface px-card py-3',
                  dirty && 'sticky bottom-0 z-10 shadow-lg',
                )}
              >
                <Meta className="mr-auto">{empty ? 'An allow-list can’t be empty.' : dirty ? 'You have unsaved changes.' : 'No changes to save.'}</Meta>
                {dirty ? (
                  <Button variant="ghost" onClick={() => setDraft(saved)}>
                    Discard
                  </Button>
                ) : null}
                <Button variant="primary" disabled={!dirty || empty} loading={save.isPending} onClick={() => save.mutate(draft)}>
                  Save policies
                </Button>
              </div>
            ) : null}
            <Modal
              open={blocker.status === 'blocked'}
              onOpenChange={(o) => !o && blocker.reset?.()}
              title="Leave without saving the policies?"
              size="sm"
              footer={
                <>
                  <Button onClick={() => blocker.reset?.()}>Stay</Button>
                  <Button variant="danger" onClick={() => blocker.proceed?.()}>
                    Discard and leave
                  </Button>
                </>
              }
            >
              <p className="m-0 text-base text-ink-2">Your policy changes for {project.name} haven’t been saved.</p>
            </Modal>
          </div>
        );
      }}
    </QueryState>
  );
}
