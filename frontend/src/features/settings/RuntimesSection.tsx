import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { KeyRound, Pencil, Power, Server, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Notice, Pill } from '@/components/ui/feedback';
import { Check, Field, Input, RadioCards, SlugInput, slugError } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { CodeBlock, Confirm, Menu, Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { Meta, plural, Short } from '@/components/ui/text';
import { cleanValues, defaultsOf, SchemaForm, type FormValues, type JsonSchema } from '@/components/SchemaForm';
import { PlacementProfiles, profileError, type PlacementProfile } from './PlacementProfiles';
import { client, unwrap, type Schemas } from '@/lib/api';
import { ago, formatBytes, slugify } from '@/lib/format';
import { shortImage } from '@/lib/debezium';
import { useProject } from '@/lib/session';
import { useCatalog } from '@/lib/debezium';

type Runtime = Schemas['RuntimeRead'];
type Kind = 'kubernetes' | 'docker';
type Mode = 'direct' | 'agent';

/** In agent mode the agent reaches the runtime locally: only placement settings remain. */
const AGENT_FIELDS: Record<Kind, string[]> = { kubernetes: ['namespace', 'image_pull_secrets', 'service_account'], docker: ['network'] };

function forMode(schema: JsonSchema | undefined, kind: Kind, mode: Mode): JsonSchema | undefined {
  if (!schema || mode === 'direct') return schema;
  const keep = AGENT_FIELDS[kind];
  const props = Object.fromEntries(Object.entries(schema.properties ?? {}).filter(([k]) => keep.includes(k)));
  return { ...schema, properties: props, required: (schema.required ?? []).filter((k) => keep.includes(k)) };
}

function placement(rt: Runtime): string {
  const cfg = rt.config as Record<string, unknown>;
  if (rt.kind === 'kubernetes') return `Kubernetes · namespace ${String(cfg.namespace)}`;
  if (rt.mode === 'agent') return `Docker${cfg.network ? ` · network ${String(cfg.network)}` : ''}`;
  return `Docker · ${String(cfg.host || 'local socket')}`;
}

export function useRuntimes(projectId: string) {
  return useQuery({
    queryKey: ['runtimes', projectId],
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/runtimes', { params: { path: { project_id: projectId } } })),
    enabled: Boolean(projectId),
  });
}

function capacityLine(rt: Runtime): string {
  const c = rt.capacity as Record<string, number> | null;
  if (!c || c.cpu_allocatable === undefined) return '';
  const cpu = `CPU ${(c.cpu_requested ?? 0).toFixed(1)} / ${c.cpu_allocatable}`;
  const mem = c.memory_allocatable ? ` · memory ${formatBytes(c.memory_requested ?? 0)} / ${formatBytes(c.memory_allocatable)}` : '';
  return `${cpu}${mem} · ${c.workloads ?? 0} connectors`;
}

export function RuntimesSection() {
  const { project, can, expert } = useProject();
  const qc = useQueryClient();
  const runtimes = useRuntimes(project.id);
  const kinds = useQuery({
    queryKey: ['runtime-kinds', project.id],
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/runtime-kinds', { params: { path: { project_id: project.id } } })),
    staleTime: Infinity,
  });
  const [open, setOpen] = useState(false);
  const [kind, setKind] = useState<Kind>('kubernetes');
  const [mode, setMode] = useState<Mode>('direct');
  const [enrolled, setEnrolled] = useState<Runtime | null>(null);
  const [rotating, setRotating] = useState<Runtime | null>(null);
  const [editing, setEditing] = useState<Runtime | null>(null);
  const catalog = useCatalog();
  const [disabling, setDisabling] = useState<Runtime | null>(null);
  const [deleting, setDeleting] = useState<Runtime | null>(null);
  const [edit, setEdit] = useState<{ name: string; defaultImage: string; config: FormValues; secrets: FormValues; profiles: PlacementProfile[] }>({
    name: '',
    defaultImage: '',
    config: {},
    secrets: {},
    profiles: [],
  });
  const [name, setName] = useState('');
  const [config, setConfig] = useState<FormValues>({});
  const [secrets, setSecrets] = useState<FormValues>({});
  const kindDef = kinds.data?.find((k) => k.kind === kind);
  const configSchema = forMode(kindDef?.config_schema as JsonSchema | undefined, kind, mode);

  const test = useMutation({
    mutationFn: (id: string) =>
      unwrap(client.POST('/api/v1/projects/{project_id}/runtimes/{runtime_id}/test', { params: { path: { project_id: project.id, runtime_id: id } } })),
    onSuccess: () => void qc.invalidateQueries({ queryKey: ['runtimes', project.id] }),
  });
  const create = useMutation({
    mutationFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/runtimes', {
          params: { path: { project_id: project.id } },
          body: {
            name: slugify(name),
            kind,
            mode,
            config: cleanValues(configSchema as JsonSchema, config),
            secrets: mode === 'agent' ? {} : cleanValues(kindDef?.secret_schema as JsonSchema, secrets, config),
          },
        }),
      ),
    onSuccess: (rt) => {
      if (rt.agent_token) setEnrolled(rt);
      else toast.success(rt.last_test?.ok ? `Added ${rt.name}: ${rt.last_test.message}` : `Added ${rt.name}. It isn’t ready yet: its card says why.`);
      setOpen(false);
      void qc.invalidateQueries({ queryKey: ['runtimes', project.id] });
    },
  });

  const rotate = useMutation({
    mutationFn: (id: string) =>
      unwrap(client.POST('/api/v1/projects/{project_id}/runtimes/{runtime_id}/agent-token', { params: { path: { project_id: project.id, runtime_id: id } } })),
    onSuccess: (rt) => {
      setRotating(null);
      setEnrolled(rt);
      void qc.invalidateQueries({ queryKey: ['runtimes', project.id] });
    },
  });

  const update = useMutation({
    mutationFn: ({ rt, body }: { rt: Runtime; body: Schemas['RuntimeUpdate'] }) =>
      unwrap(
        client.PATCH('/api/v1/projects/{project_id}/runtimes/{runtime_id}', {
          params: { path: { project_id: project.id, runtime_id: rt.id }, header: { 'if-match': `"${rt.version}"` } },
          body,
        }),
      ),
    onSuccess: (rt, { body }) => {
      if (body.enabled === false) toast.success(`Disabled ${rt.name}`);
      else if (body.enabled === true) toast.success(`Enabled ${rt.name}`);
      else toast.success(`Saved ${rt.name}`);
      setEditing(null);
      setDisabling(null);
      void qc.invalidateQueries({ queryKey: ['runtimes', project.id] });
    },
  });
  const remove = useMutation({
    mutationFn: (rt: Runtime) =>
      unwrap(client.DELETE('/api/v1/projects/{project_id}/runtimes/{runtime_id}', { params: { path: { project_id: project.id, runtime_id: rt.id } } })),
    onSuccess: (_, rt) => {
      toast.success(`Deleted ${rt.name}`);
      setDeleting(null);
      void qc.invalidateQueries({ queryKey: ['runtimes', project.id] });
    },
  });

  function startEdit(rt: Runtime) {
    setEdit({ name: rt.name, defaultImage: rt.default_image ?? '', config: rt.config as FormValues, secrets: {}, profiles: rt.placement_profiles ?? [] });
    setEditing(rt);
  }

  const editDef = kinds.data?.find((k) => k.kind === editing?.kind);
  const editSchema = editing ? forMode(editDef?.config_schema as JsonSchema | undefined, editing.kind as Kind, editing.mode as Mode) : undefined;

  function saveEdit() {
    if (!editing || !editSchema || !editDef) return;
    update.mutate({
      rt: editing,
      body: {
        name: slugify(edit.name),
        default_image: edit.defaultImage.trim() || null,
        config: cleanValues(editSchema, edit.config),
        ...(editing.mode === 'direct' ? { secrets: cleanValues(editDef.secret_schema as JsonSchema, edit.secrets, edit.config) } : {}),
        ...(editing.kind === 'kubernetes' && JSON.stringify(edit.profiles) !== JSON.stringify(editing.placement_profiles ?? [])
          ? { placement_profiles: edit.profiles }
          : {}),
      },
    });
  }

  function start(k: Kind) {
    const def = kinds.data?.find((x) => x.kind === k);
    setKind(k);
    setMode('direct');
    setSecrets({});
    setConfig(def ? defaultsOf(def.config_schema as JsonSchema) : {});
  }
  const names = (runtimes.data ?? []).map((r) => r.name);
  const offered = (catalog.data?.entries ?? []).map((e) => ({
    value: e.image,
    label: <span className="font-mono">{e.image}</span>,
    text: e.image,
    description: [e.label, e.is_default ? 'the catalog default' : ''].filter(Boolean).join(' · ') || undefined,
    disabled: e.deprecated,
    why: 'Deprecated by your Admins: pick a supported version.',
  }));
  const customImage = edit.defaultImage && !offered.some((o) => o.value === edit.defaultImage);
  const addMissing = !name.trim() ? 'Name the runtime.' : slugError(name, names);
  const editMissing = !edit.name.trim()
    ? 'Name the runtime.'
    : (slugError(
        edit.name,
        names.filter((n) => n !== editing?.name),
      ) ?? edit.profiles.map((p) => profileError(p, edit.profiles)).find(Boolean) ?? null);
  const [copied, setCopied] = useState(false);

  const manage = can('runtime.manage');
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="flex min-w-0 flex-[1_1_320px] flex-col gap-1">
          <h2 className="m-0 text-xl font-semibold">Runtimes</h2>
          <p className="m-0 text-base text-muted">Where connectors run. Each pipeline runs as one Debezium Server on a runtime.</p>
        </div>
        {manage ? (
          <Button
            variant="primary"
            onClick={() => {
              start('kubernetes');
              setName('');
              create.reset();
              setOpen(true);
            }}
          >
            Add runtime
          </Button>
        ) : null}
      </div>
      <QueryState query={runtimes} doing="Couldn’t load runtimes">
        {(list) =>
          list.length === 0 ? (
            <Card>
              <Empty icon={<Server />} title="No runtime yet">
                Connect a Kubernetes namespace or a Docker host before creating pipelines.
              </Empty>
            </Card>
          ) : (
            <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,360px),1fr))] gap-4">
              {list.map((rt) => (
                <Card
                  key={rt.id}
                  title={rt.name}
                  actions={
                    <>
                      <Button size="sm" loading={test.isPending && test.variables === rt.id} onClick={() => test.mutate(rt.id)}>
                        Test
                      </Button>
                      {manage ? (
                        <Menu
                          label={`More actions for ${rt.name}`}
                          icon="more"
                          variant="ghost"
                          size="sm"
                          items={[
                            {
                              label: 'Edit',
                              icon: <Pencil />,
                              onSelect: () => {
                                update.reset();
                                startEdit(rt);
                              },
                            },
                            {
                              label: rt.enabled ? 'Disable' : 'Enable',
                              icon: <Power />,
                              onSelect: () => (rt.enabled ? setDisabling(rt) : update.mutate({ rt, body: { enabled: true } })),
                            },
                            rt.mode === 'agent' ? { label: 'New agent token', icon: <KeyRound />, onSelect: () => setRotating(rt) } : null,
                            '-',
                            {
                              label: 'Delete',
                              icon: <Trash2 />,
                              danger: true,
                              disabled: rt.pipelines > 0,
                              why: `Move or delete its ${plural(rt.pipelines, 'pipeline')} first.`,
                              onSelect: () => {
                                remove.reset();
                                setDeleting(rt);
                              },
                            },
                          ]}
                        />
                      ) : null}
                    </>
                  }
                >
                  <div className="flex flex-wrap items-center gap-2">
                    {!rt.enabled ? (
                      <Pill tone="warn">Disabled</Pill>
                    ) : rt.last_test ? (
                      <Pill tone={rt.last_test.ok ? 'ok' : 'bad'}>{rt.last_test.ok ? 'Ready' : 'Not ready'}</Pill>
                    ) : (
                      <Pill tone="grey">Not tested</Pill>
                    )}
                    {rt.mode === 'agent' ? <Pill tone="info">agent</Pill> : null}
                    <Meta>
                      {!rt.enabled
                        ? 'Running pipelines keep running; nothing new is deployed here.'
                        : rt.mode === 'agent'
                          ? rt.last_seen_at
                            ? `agent ${rt.agent_version ?? ''} seen ${ago(rt.last_seen_at)}`
                            : 'agent never connected'
                          : rt.last_test
                            ? `tested ${ago(rt.last_test.tested_at)}`
                            : ''}
                    </Meta>
                  </div>
                  {rt.last_test && !rt.last_test.ok ? <p className="m-0 text-base text-bad wrap-break-word">{rt.last_test.message}</p> : null}
                  <Meta as="p" className="wrap-anywhere">
                    {placement(rt)} · {plural(rt.pipelines, 'pipeline')}
                    {capacityLine(rt) ? ` · ${capacityLine(rt)}` : ''}
                  </Meta>
                  <span className="flex min-w-0 items-center gap-1.5 text-sm text-muted">
                    Debezium{' '}
                    <Short max={40} mono className="text-ink-2">
                      {shortImage(rt.effective_image)}
                    </Short>
                  </span>
                  {test.error && test.variables === rt.id ? <InlineError error={test.error} doing="Couldn’t test the runtime" /> : null}
                </Card>
              ))}
            </div>
          )
        }
      </QueryState>
      <InlineError error={update.error && !editing ? update.error : null} doing="Couldn’t change the runtime" />
      <Modal
        open={open}
        onOpenChange={setOpen}
        size="lg"
        dirty={Boolean(name)}
        title="Add a runtime"
        description={kindDef?.description}
        why={addMissing}
        footer={
          <>
            <Button onClick={() => setOpen(false)}>Cancel</Button>
            <Button variant="primary" loading={create.isPending} disabled={Boolean(addMissing)} onClick={() => create.mutate()}>
              Add runtime
            </Button>
          </>
        }
      >
        <Field label="Kind" group>
          {() => (
            <RadioCards
              label="Kind"
              value={kind}
              onChange={(k) => start(k)}
              columns={2}
              options={[
                { value: 'kubernetes', label: 'Kubernetes', description: 'A namespace in a cluster.' },
                { value: 'docker', label: 'Docker', description: 'A Docker host.' },
              ]}
            />
          )}
        </Field>
        <Field label="Name" hint="Pipelines and manifests refer to the runtime by this name.">
          {(id) => <SlugInput id={id} value={name} taken={names} placeholder={kind === 'kubernetes' ? 'prod-k8s' : 'edge-docker'} onChange={setName} />}
        </Field>
        <Field label="How BeziSync reaches it" group>
          {() => (
            <RadioCards
              label="How BeziSync reaches it"
              value={mode}
              onChange={setMode}
              columns={2}
              options={[
                {
                  value: 'direct',
                  label: kind === 'kubernetes' ? 'Directly (in-cluster or kubeconfig)' : 'Directly (Docker API)',
                  description: 'BeziSync talks to the runtime API itself.',
                },
                {
                  value: 'agent',
                  label: 'Through an agent',
                  description:
                    'A small agent next to the runtime dials out to BeziSync. No inbound access or runtime credentials: best for remote clusters and hosts.',
                },
              ]}
            />
          )}
        </Field>
        {kindDef && configSchema ? (
          <>
            <SchemaForm idPrefix="rt" schema={configSchema} values={config} onChange={setConfig} expert={expert} />
            {mode === 'direct' ? (
              <SchemaForm idPrefix="rts" schema={kindDef.secret_schema as JsonSchema} values={secrets} onChange={setSecrets} expert={expert} secret context={config} />
            ) : null}
          </>
        ) : null}
        {mode === 'agent' ? (
          <Notice tone="info">
            After adding it you get a one-time install command with the agent’s token. The runtime becomes ready once the agent connects.
          </Notice>
        ) : kind === 'kubernetes' ? (
          <Notice tone="info">
            BeziSync tests that it may create Deployments, ConfigMaps and Secrets, and read pods, logs and the pod proxy in the namespace.
          </Notice>
        ) : (
          <Notice tone="warn">Access to a Docker daemon is equivalent to root on that host. Prefer TLS with client certificates, or rootless Docker.</Notice>
        )}
        <InlineError error={create.error} doing="Couldn’t add the runtime" />
      </Modal>
      <Modal
        open={enrolled !== null}
        onOpenChange={() => undefined}
        closable={false}
        size="lg"
        title={`Install the agent for ${enrolled?.name ?? ''}`}
        description="Run this where the runtime lives. The agent only makes outbound HTTPS requests to BeziSync."
        why={copied ? null : 'Copy the command first: the token won’t be shown again.'}
        footer={
          <Button
            variant="primary"
            disabled={!copied}
            onClick={() => {
              setEnrolled(null);
              setCopied(false);
            }}
          >
            Done
          </Button>
        }
      >
        <Notice tone="warn" title="Copy it now">
          The token is shown only once. If you lose it, create a new token (the old one stops working).
        </Notice>
        {enrolled?.agent_install ? <CodeBlock label="Install command" code={enrolled.agent_install} wrap /> : null}
        {enrolled?.agent_token ? <CodeBlock label="Agent token" code={enrolled.agent_token} wrap /> : null}
        <Notice tone="info" title="Private registry?">
          If the chart and images are private, sign in first (<code className="font-mono">helm registry login ghcr.io</code> or{' '}
          <code className="font-mono">docker login ghcr.io</code>) and, on Kubernetes, create the image pull secret in the namespace and list it in the
          runtime’s image pull secrets.
        </Notice>
        <Check checked={copied} onChange={setCopied} label="I’ve copied it and stored it somewhere safe" />
      </Modal>
      <Modal
        open={editing !== null}
        onOpenChange={(o) => !o && setEditing(null)}
        size="lg"
        title={`Edit ${editing?.name ?? ''}`}
        description={editing?.mode === 'agent' ? 'The agent reaches this runtime locally; only placement settings apply.' : editDef?.description}
        why={editMissing}
        footer={
          <>
            <Button onClick={() => setEditing(null)}>Cancel</Button>
            <Button variant="primary" loading={update.isPending} disabled={Boolean(editMissing)} onClick={saveEdit}>
              Save changes
            </Button>
          </>
        }
      >
        <Field
          label="Name"
          hint={editing && slugify(edit.name) !== editing.name ? 'Pipeline manifests refer to runtimes by name: update them after renaming.' : undefined}
        >
          {(id) => <SlugInput id={id} value={edit.name} taken={names.filter((n) => n !== editing?.name)} onChange={(v) => setEdit({ ...edit, name: v })} />}
        </Field>
        <Field
          label="Default Debezium image"
          optional
          hint={`Pipelines that don’t pin an image use this from their next deploy; running pipelines are not restarted. The catalog default is ${catalog.data?.default_image ?? '…'}.`}
        >
          {(id) => (
            <Select
              id={id}
              value={customImage ? '__other' : edit.defaultImage}
              onChange={(v) => setEdit({ ...edit, defaultImage: v === '__other' ? edit.defaultImage || ' ' : v })}
              options={[
                { value: '', label: 'Follow the catalog default' },
                ...offered,
                { value: '__other', label: 'Another image…', description: 'Type any image reference' },
              ]}
            />
          )}
        </Field>
        {customImage ? (
          <Field label="Image reference" hint="registry/repository:tag or @sha256 digest. The runtime must be able to pull it.">
            {(id) => (
              <Input id={id} className="font-mono" value={edit.defaultImage.trim()} onChange={(e) => setEdit({ ...edit, defaultImage: e.target.value })} />
            )}
          </Field>
        ) : null}
        {editing && editing.pipelines > 0 ? <Notice tone="info">Changing where this runtime points needs its running pipelines paused first.</Notice> : null}
        {editDef && editSchema ? (
          <>
            <SchemaForm idPrefix="rte" schema={editSchema} values={edit.config} onChange={(config) => setEdit({ ...edit, config })} expert={expert} />
            {editing?.mode === 'direct' ? (
              <SchemaForm
                idPrefix="rtes"
                schema={editDef.secret_schema as JsonSchema}
                values={edit.secrets}
                context={edit.config}
                onChange={(secrets) => setEdit({ ...edit, secrets })}
                expert={expert}
                secret
                setSecrets={editing.secret_fields_set}
              />
            ) : null}
          </>
        ) : null}
        {editing?.kind === 'kubernetes' ? <PlacementProfiles value={edit.profiles} onChange={(profiles) => setEdit({ ...edit, profiles })} /> : null}
        <InlineError error={update.error} doing="Couldn’t save the runtime" />
      </Modal>
      <Confirm
        open={disabling !== null}
        onOpenChange={(o) => !o && setDisabling(null)}
        title={`Disable ${disabling?.name ?? ''}?`}
        effects={[
          disabling?.pipelines
            ? `Its ${plural(disabling.pipelines, 'pipeline')} keep running and can still be paused, resumed and restarted.`
            : 'No pipelines run on it.',
          'Nobody can create, move or deploy pipelines onto it until it is enabled again.',
        ]}
        confirmLabel="Disable runtime"
        pending={update.isPending}
        error={update.error}
        doing="Couldn’t disable the runtime"
        onConfirm={() => disabling && update.mutate({ rt: disabling, body: { enabled: false } })}
      />
      <Confirm
        open={deleting !== null}
        onOpenChange={(o) => !o && setDeleting(null)}
        title={`Delete ${deleting?.name ?? ''}?`}
        effects={[
          'BeziSync forgets this runtime and its settings. Deleted pipelines keep their history.',
          ...(deleting?.mode === 'agent' ? ['Its agent token stops working; uninstall the agent where it runs.'] : []),
        ]}
        confirmLabel="Delete runtime"
        typeToConfirm={deleting?.name}
        pending={remove.isPending}
        error={remove.error}
        doing="Couldn’t delete the runtime"
        onConfirm={() => deleting && remove.mutate(deleting)}
      />
      <Confirm
        open={rotating !== null}
        onOpenChange={(o) => !o && setRotating(null)}
        title={`Create a new agent token for ${rotating?.name ?? ''}?`}
        effects={[
          'The current token stops working at once: the running agent disconnects.',
          'Its pipelines show Unknown until you reinstall the agent with the new token.',
        ]}
        confirmLabel="Create new token"
        pending={rotate.isPending}
        error={rotate.error}
        doing="Couldn’t create a new token"
        onConfirm={() => rotating && rotate.mutate(rotating.id)}
      />
    </div>
  );
}
