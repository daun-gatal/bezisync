import { useEffect, useMemo, useState } from 'react';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { Button, ButtonLink } from '@/components/ui/button';
import { InlineError, Notice, Pill, Skeleton } from '@/components/ui/feedback';
import { Field, Segmented } from '@/components/ui/form';
import { Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { List, ListRow } from '@/components/ui/table';
import { Meta, plural, Title } from '@/components/ui/text';
import { client, unwrap } from '@/lib/api';
import { TYPE_LABEL, type Connection } from '@/lib/connections';
import { missingLabel, savedSecrets, useCopyConnections, useCopyTargets, type CopyPlan, type CopyRequest, type SecretsChoice, type StoreChoice } from '@/lib/copy';
import { useProject } from '@/lib/session';

/** Each connection's request item: its own name, a free name on a clash, or an update of the target's. */
function itemsFor(connections: Connection[], choice: Record<string, 'rename' | 'update'>, suggested: Record<string, string>): CopyRequest['connections'] {
  return connections.map((c) => {
    const clash = suggested[c.id];
    if (!clash) return { id: c.id, update: false };
    return (choice[c.id] ?? 'rename') === 'update' ? { id: c.id, update: true } : { id: c.id, name: clash, update: false };
  });
}

const show = (v: unknown) => (v === undefined || v === null || v === '' ? '(empty)' : typeof v === 'string' ? v : JSON.stringify(v));

/**
 * Copy one or more connections to another project. Each change of a choice asks the server for
 * the plan (a dry run): name clashes, secret stores the target lacks, the target's policies and
 * what the copy will still need. The copy itself is one request; saved secrets stay behind
 * unless an Admin copies them.
 */
export function CopyDialog({
  connections,
  open,
  onOpenChange,
  initialTarget,
  onCopied,
}: {
  connections: Connection[];
  open: boolean;
  onOpenChange: (open: boolean) => void;
  initialTarget?: string;
  /** After a copy, with the result (Promote picks the new connection from it). */
  onCopied?: (plan: CopyPlan) => void;
}) {
  const { project, me } = useProject();
  const admin = me.user.is_superuser;
  const targets = useCopyTargets();
  const [targetId, setTargetId] = useState(initialTarget ?? '');
  const [choice, setChoice] = useState<Record<string, 'rename' | 'update'>>({});
  const [secrets, setSecrets] = useState<SecretsChoice>('leave');
  const [stores, setStores] = useState<Record<string, StoreChoice>>({});
  const copy = useCopyConnections(project.id);
  const target = targets.find((t) => t.id === targetId);
  const anySaved = connections.some((c) => savedSecrets(c).length > 0);

  useEffect(() => {
    if (open) {
      setTargetId(initialTarget ?? '');
      setChoice({});
      setSecrets('leave');
      setStores({});
      copy.reset();
    }
  }, [open]);

  // The first plan (no names chosen) tells which names clash and a free name for each.
  const first = usePlan(project.id, targetId, connections, {}, {}, secrets, open);
  const suggested = useMemo(() => Object.fromEntries((first.data?.items ?? []).map((i) => [i.source_id, i.exists ? i.suggested_name : ''])), [first.data]);
  const request = (dry: boolean): CopyRequest => ({
    target_project_id: targetId,
    dry_run: dry,
    secrets,
    stores,
    connections: itemsFor(connections, choice, suggested),
  });
  const plan = usePlan(project.id, targetId, connections, choice, suggested, secrets, open && Boolean(first.data), stores);
  const done = copy.data && copy.variables?.dry_run === false ? copy.data : null;
  const current = plan.data ?? null;
  const blocked = !targetId ? 'Choose the project to copy to.' : !current ? 'Checking…' : !current.ready ? 'Resolve what the plan says first.' : null;
  const title = connections.length === 1 ? `Copy ${connections[0]!.name} to another project` : `Copy ${connections.length} connections to another project`;

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      size="lg"
      title={title}
      description="The originals stay where they are. The copies are checked against the other project’s policies and recorded in both audit logs."
      why={done ? null : blocked}
      footer={
        done ? (
          <>
            <ButtonLink variant="ghost" to="/p/$projectSlug/connections" params={{ projectSlug: target?.slug ?? project.slug }} onClick={() => onOpenChange(false)}>
              Open {target?.name ?? 'the project'}’s connections
            </ButtonLink>
            <Button variant="primary" onClick={() => onOpenChange(false)}>
              {onCopied ? 'Continue' : 'Done'}
            </Button>
          </>
        ) : (
          <>
            <Button onClick={() => onOpenChange(false)}>Cancel</Button>
            <Button
              variant="primary"
              disabled={Boolean(blocked)}
              loading={copy.isPending}
              onClick={() => copy.mutate(request(false), { onSuccess: (p) => onCopied?.(p) })}
            >
              {connections.length === 1 ? `Copy to ${target?.name ?? 'project'}` : `Copy ${connections.length} to ${target?.name ?? 'project'}`}
            </Button>
          </>
        )
      }
    >
      {targets.length === 0 ? <Notice tone="info">You manage connections in no other project.</Notice> : null}
      {done ? (
        <CopyResult plan={done} />
      ) : (
        <>
          <Field label="Copy to">
            {(id) => (
              <Select
                id={id}
                value={targetId}
                onChange={(v) => {
                  setTargetId(v);
                  setChoice({});
                  setStores({});
                }}
                options={targets.map((t) => ({ value: t.id, label: t.name }))}
              />
            )}
          </Field>
          {targetId && !current ? <Skeleton lines={3} /> : null}
          <InlineError error={first.error ?? plan.error} doing="Couldn’t check the copy" />
          {current ? (
            <>
              <section aria-label="Connections to copy" className="flex flex-col gap-2">
                <List label="Connections to copy">
                  {current.items.map((it) => {
                    const own = it.problems.filter((p) => !p.includes('already has a connection named'));
                    return (
                      <ListRow key={it.source_id} className="flex-col items-stretch">
                        <div className="flex min-w-0 flex-wrap items-center gap-2">
                          <Title as="strong" lines={1} className="font-mono font-medium">
                            {it.source_name}
                          </Title>
                          <Meta>
                            {TYPE_LABEL[it.type] ?? it.type} {it.kind === 'sink' ? 'destination' : 'source'}
                          </Meta>
                          {it.action === 'create' && it.name !== it.source_name ? <Meta>→ {it.name}</Meta> : null}
                          {it.missing_secrets.length ? (
                            <Pill tone="warn">
                              {missingLabel(it.missing_secrets)} in {current.target_project}
                            </Pill>
                          ) : (
                            <Pill tone="ok">Secrets in place</Pill>
                          )}
                        </div>
                        {suggested[it.source_id] ? (
                          <div className="flex flex-col gap-1.5 border-l-2 border-line pl-3">
                            <Meta as="p">
                              {current.target_project} already has <span className="font-mono">{it.action === 'update' ? it.name : it.source_name}</span>.
                            </Meta>
                            <Segmented
                              label={`What to do with ${it.source_name}`}
                              value={choice[it.source_id] ?? 'rename'}
                              onChange={(v) => setChoice((c) => ({ ...c, [it.source_id]: v }))}
                              options={[
                                { value: 'rename', label: `Copy as ${suggested[it.source_id]}` },
                                { value: 'update', label: 'Update the existing one' },
                              ]}
                            />
                            {it.action === 'update' ? (
                              <>
                                {it.changes.length ? (
                                  <ul aria-label={`Changes to ${it.name}`} className="m-0 flex list-none flex-col gap-0.5 rounded-md border border-line bg-subtle p-2 font-mono text-xs">
                                    {it.changes.map((ch) => (
                                      <li key={ch.field} className="wrap-anywhere">
                                        {ch.field}: <span className="text-bad">{show(ch.from)}</span> → <span className="text-ok">{show(ch.to)}</span>
                                      </li>
                                    ))}
                                  </ul>
                                ) : (
                                  <Meta as="p">The settings are already the same.</Meta>
                                )}
                                {it.used_by.length ? (
                                  <Notice tone="warn">
                                    {plural(it.used_by.length, 'pipeline')} in {current.target_project} use it ({it.used_by.join(', ')}) and pick up the change at their next deploy.
                                  </Notice>
                                ) : null}
                              </>
                            ) : null}
                          </div>
                        ) : null}
                        {own.map((p) => (
                          <p key={p} className="m-0 text-sm text-bad">
                            {p}
                          </p>
                        ))}
                      </ListRow>
                    );
                  })}
                </List>
              </section>
              {current.stores.map((s) => {
                const value = stores[s.name] ? (stores[s.name]!.action === 'use' ? `use:${stores[s.name]!.use ?? ''}` : 'copy') : '';
                const options = [
                  ...(admin ? [{ value: 'copy', label: `Copy ${s.name} too`, description: 'Without its credentials unless saved secrets are copied.' }] : []),
                  ...current.target_stores.map((t) => ({ value: `use:${t}`, label: `Use ${t}`, description: `References point at ${current.target_project}’s ${t}.` })),
                ];
                return (
                  <Field
                    key={s.name}
                    label={`Secret store ${s.name}`}
                    hint={`${s.used_by.join(', ')} read${s.used_by.length === 1 ? 's' : ''} secrets from ${s.name}, which ${current.target_project} doesn’t have.`}
                  >
                    {(id) =>
                      options.length ? (
                        <Select
                          id={id}
                          value={value}
                          placeholder="Choose what to do"
                          onChange={(v) => setStores((m) => ({ ...m, [s.name]: v === 'copy' ? { action: 'copy' } : { action: 'use', use: v.slice(4) } }))}
                          options={options}
                        />
                      ) : (
                        <Notice tone="warn">{current.target_project} has no secret store, and only Admins can copy one. Ask an Admin, or copy the other connections.</Notice>
                      )
                    }
                  </Field>
                );
              })}
              {anySaved ? (
                <Field
                  label="Saved passwords and keys"
                  hint={
                    secrets === 'leave'
                      ? `Settings are copied; someone in ${current.target_project} enters the secrets. Separate credentials per environment are safer.`
                      : 'Copied on the server and kept encrypted; nobody sees them. Both audit logs record it.'
                  }
                >
                  {() =>
                    admin ? (
                      <Segmented
                        label="Saved passwords and keys"
                        value={secrets}
                        onChange={setSecrets}
                        options={[
                          { value: 'leave', label: 'Leave them out (recommended)' },
                          { value: 'copy', label: 'Copy them' },
                        ]}
                      />
                    ) : (
                      <Meta>Left out: only Admins copy saved secrets between projects.</Meta>
                    )
                  }
                </Field>
              ) : null}
              {current.ready ? (
                <Notice tone="ok">Ready: {current.target_project}’s policies allow these connections.</Notice>
              ) : null}
            </>
          ) : null}
          <InlineError error={copy.error} doing="Couldn’t copy" />
        </>
      )}
    </Modal>
  );
}

function usePlan(
  projectId: string,
  targetId: string,
  connections: Connection[],
  choice: Record<string, 'rename' | 'update'>,
  suggested: Record<string, string>,
  secrets: SecretsChoice,
  enabled: boolean,
  stores: Record<string, StoreChoice> = {},
) {
  const body: CopyRequest = {
    target_project_id: targetId,
    dry_run: true,
    secrets,
    stores,
    connections: itemsFor(connections, choice, suggested),
  };
  return useQuery({
    queryKey: ['copy-plan', projectId, body],
    enabled: enabled && Boolean(targetId) && connections.length > 0,
    placeholderData: keepPreviousData,
    queryFn: () => unwrap(client.POST('/api/v1/projects/{project_id}/connections:copy', { params: { path: { project_id: projectId } }, body })),
  });
}

function CopyResult({ plan }: { plan: CopyPlan }) {
  return (
    <div className="flex flex-col gap-3">
      <List label="Copied">
        {plan.items.map((it) => (
          <ListRow key={it.source_id}>
            <span className="flex min-w-0 flex-[1_1_220px] flex-col gap-0.5">
              <Title as="strong" lines={1} className="font-mono font-medium">
                {it.name}
              </Title>
              <Meta>{it.action === 'update' ? 'updated' : 'created'}</Meta>
            </span>
            {it.missing_secrets.length ? <Pill tone="warn">{missingLabel(it.missing_secrets)}</Pill> : <Pill tone="ok">Ready to check</Pill>}
          </ListRow>
        ))}
        {plan.stores
          .filter((s) => s.action === 'copy')
          .map((s) => (
            <ListRow key={s.name}>
              <span className="flex min-w-0 flex-[1_1_220px] flex-col gap-0.5">
                <Title as="strong" lines={1} className="font-mono font-medium">
                  {s.name}
                </Title>
                <Meta>secret store created</Meta>
              </span>
              <Pill tone={plan.secrets === 'copy' ? 'ok' : 'warn'}>{plan.secrets === 'copy' ? 'Credentials copied' : 'Add its credentials'}</Pill>
            </ListRow>
          ))}
      </List>
      <Meta as="p">
        Recorded in the audit logs of both projects. A connection that still needs a secret can be picked for pipelines in {plan.target_project}, but nothing deploys
        until someone enters it there.
      </Meta>
    </div>
  );
}
