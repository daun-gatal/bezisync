import { useState, type ReactNode } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Copy, KeyRound, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Notice, Pill } from '@/components/ui/feedback';
import { Field, Input, Segmented, SlugInput, slugError } from '@/components/ui/form';
import { Card, KeyValue, QueryState } from '@/components/ui/layout';
import { Confirm, Menu, Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import { cleanValues, defaultsOf, SchemaForm, type FormValues, type JsonSchema } from '@/components/SchemaForm';
import { ApiError, client, unwrap, type Schemas } from '@/lib/api';
import { useCopyTargets, type SecretsChoice } from '@/lib/copy';
import { ago, slugify } from '@/lib/format';
import { useProject } from '@/lib/session';

type Store = Schemas['StoreRead'];
type StoreKind = Schemas['StoreKindRead'];

function TryReference({ store }: { store: Store }) {
  const { project } = useProject();
  const [path, setPath] = useState('');
  const [key, setKey] = useState('');
  const check = useMutation({
    mutationFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/secret-stores/{store_id}/check-ref', {
          params: { path: { project_id: project.id, store_id: store.id } },
          body: { path, key: key || null },
        }),
      ),
  });
  return (
    <form
      className="flex min-w-0 flex-col gap-3 border-t border-line pt-4"
      onSubmit={(e) => {
        e.preventDefault();
        check.mutate();
      }}
    >
      <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,180px),1fr))] items-end gap-3">
        <Field label="Path">
          {(id) => (
            <Input id={id} value={path} placeholder={store.kind === 'kubernetes' ? 'orders-db' : 'cdc/orders-db'} onChange={(e) => setPath(e.target.value)} />
          )}
        </Field>
        <Field label="Key" optional>
          {(id) => <Input id={id} value={key} placeholder="password" onChange={(e) => setKey(e.target.value)} />}
        </Field>
      </div>
      <div>
        <Button type="submit" size="sm" loading={check.isPending} disabled={!path} why="Enter a path.">
          Try reference
        </Button>
      </div>
      {check.data ? <Notice tone={check.data.ok ? 'ok' : 'bad'}>{check.data.message}</Notice> : null}
      <InlineError error={check.error} doing="Couldn’t try the reference" />
    </form>
  );
}

export function SecretStoresSection() {
  const { project, can, expert } = useProject();
  const qc = useQueryClient();
  const key = ['secret-stores', project.id];
  const stores = useQuery({
    queryKey: key,
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/secret-stores', { params: { path: { project_id: project.id } } })),
  });
  const kinds = useQuery({
    queryKey: ['secret-store-kinds', project.id],
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/secret-store-kinds', { params: { path: { project_id: project.id } } })),
    staleTime: Infinity,
  });
  const [editing, setEditing] = useState<{ kind: StoreKind; existing?: Store } | null>(null);
  const [name, setName] = useState('');
  const [config, setConfig] = useState<FormValues>({});
  const [secrets, setSecrets] = useState<FormValues>({});
  const [deleting, setDeleting] = useState<Store | null>(null);
  const [trying, setTrying] = useState<string | null>(null);
  const [copying, setCopying] = useState<Store | null>(null);
  const copyTargets = useCopyTargets('stores');
  const manage = can('project.manage');
  const refresh = () => void qc.invalidateQueries({ queryKey: key });

  // Saving tests the settings first and saves nothing when the test fails; "Save anyway" (force) skips that.
  const save = useMutation({
    mutationFn: async (force: boolean) => {
      if (!editing) throw new Error('nothing to save');
      const cfg = cleanValues(editing.kind.config_schema as JsonSchema, config);
      const sec = cleanValues(editing.kind.secret_schema as JsonSchema, secrets, config);
      if (editing.existing) {
        return unwrap(
          client.PATCH('/api/v1/projects/{project_id}/secret-stores/{store_id}', {
            params: { path: { project_id: project.id, store_id: editing.existing.id }, header: { 'if-match': `"${editing.existing.version}"` } },
            body: { config: cfg, secrets: sec, test_first: !force },
          }),
        );
      }
      return unwrap(
        client.POST('/api/v1/projects/{project_id}/secret-stores', {
          params: { path: { project_id: project.id } },
          body: { name: slugify(name), kind: editing.kind.kind as Schemas['StoreCreate']['kind'], config: cfg, secrets: sec, test_first: !force },
        }),
      );
    },
    onSuccess: (s) => {
      toast.success(s.last_test?.ok ? `${s.name}: ${s.last_test.message}` : `Saved ${s.name}. It isn’t connected yet: its card says why.`);
      setEditing(null);
      refresh();
    },
  });
  const test = useMutation({
    mutationFn: (id: string) =>
      unwrap(client.POST('/api/v1/projects/{project_id}/secret-stores/{store_id}/test', { params: { path: { project_id: project.id, store_id: id } } })),
    onSuccess: refresh,
  });
  const remove = useMutation({
    mutationFn: (id: string) =>
      unwrap(client.DELETE('/api/v1/projects/{project_id}/secret-stores/{store_id}', { params: { path: { project_id: project.id, store_id: id } } })),
    onSuccess: () => {
      toast.success(`Deleted ${deleting?.name ?? 'secret store'}`);
      setDeleting(null);
      refresh();
    },
  });

  function open(kind: StoreKind, existing?: Store) {
    setName(existing?.name ?? '');
    setConfig(existing ? { ...(existing.config as FormValues) } : defaultsOf(kind.config_schema as JsonSchema));
    setSecrets({});
    save.reset();
    setEditing({ kind, existing });
  }
  const kindOf = (k: string) => kinds.data?.find((x) => x.kind === k);
  const names = (stores.data ?? []).map((x) => x.name);
  const missing = editing && !editing.existing ? (!name.trim() ? 'Name the store.' : slugError(name, names)) : null;

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="flex min-w-0 flex-[1_1_320px] flex-col gap-1">
          <h2 className="m-0 text-xl font-semibold">Secret stores</h2>
          <p className="m-0 text-base text-muted">
            Keep connection secrets in your own vault. A secret field set to <code className="font-mono">ref+&lt;store&gt;://&lt;path&gt;#&lt;key&gt;</code> is
            read when needed and never stored by BeziSync.
          </p>
        </div>
        {manage && kinds.data?.length ? (
          <Menu label="Add store" variant="primary" items={kinds.data.map((k) => ({ label: k.title, description: k.description, onSelect: () => open(k) }))} />
        ) : null}
      </div>
      <QueryState query={stores} doing="Couldn’t load the secret stores">
        {(list) =>
          list.length === 0 ? (
            <Card>
              <Empty icon={<KeyRound />} title="No secret stores">
                Connections keep their secrets encrypted in BeziSync until you add a store.
              </Empty>
            </Card>
          ) : (
            <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,360px),1fr))] gap-4">
              {list.map((s) => (
                <Card
                  key={s.id}
                  title={s.name}
                  actions={
                    <>
                      <Button size="sm" loading={test.isPending && test.variables === s.id} onClick={() => test.mutate(s.id)}>
                        Test
                      </Button>
                      {manage && kindOf(s.kind) ? (
                        <Button size="sm" onClick={() => open(kindOf(s.kind)!, s)}>
                          Edit
                        </Button>
                      ) : null}
                      {manage ? (
                        <Menu
                          label={`More actions for ${s.name}`}
                          icon="more"
                          variant="ghost"
                          size="sm"
                          items={[
                            copyTargets.length ? { label: 'Copy to project…', icon: <Copy />, description: 'Same name and settings there.', onSelect: () => setCopying(s) } : null,
                            {
                              label: 'Delete',
                              icon: <Trash2 />,
                              danger: true,
                              disabled: s.used_by.length > 0,
                              why: `Used by ${s.used_by.join(', ')}: change those connections first.`,
                              onSelect: () => {
                                remove.reset();
                                setDeleting(s);
                              },
                            },
                          ]}
                        />
                      ) : null}
                    </>
                  }
                >
                  <div className="flex flex-wrap items-center gap-2">
                    {s.last_test ? (
                      <Pill tone={s.last_test.ok ? 'ok' : 'bad'}>{s.last_test.ok ? 'Connected' : 'Failing'}</Pill>
                    ) : (
                      <Pill tone="grey">Not tested</Pill>
                    )}
                    <Meta>
                      {kindOf(s.kind)?.title ?? s.kind}
                      {s.last_test ? ` · tested ${ago(s.last_test.tested_at)}` : ''}
                    </Meta>
                  </div>
                  {s.last_test && !s.last_test.ok ? <p className="m-0 text-base text-bad wrap-break-word">{s.last_test.message}</p> : null}
                  <StoreDetails store={s} kind={kindOf(s.kind)} />
                  <Meta as="p" className="wrap-anywhere">
                    {s.used_by.length ? `Used by ${s.used_by.join(', ')}` : 'Not used yet'}
                  </Meta>
                  {test.error && test.variables === s.id ? <InlineError error={test.error} doing="Couldn’t test the store" /> : null}
                  <div>
                    <Button size="sm" variant="ghost" aria-expanded={trying === s.id} onClick={() => setTrying(trying === s.id ? null : s.id)}>
                      Try a reference
                    </Button>
                  </div>
                  {trying === s.id ? <TryReference store={s} /> : null}
                </Card>
              ))}
            </div>
          )
        }
      </QueryState>

      <Modal
        open={editing !== null}
        onOpenChange={(o) => !o && setEditing(null)}
        size="lg"
        dirty={Boolean(name && !editing?.existing)}
        title={editing?.existing ? `Edit ${editing.existing.name}` : `Add ${editing?.kind.title ?? ''}`}
        description={editing?.kind.description}
        why={missing}
        footer={
          <>
            <Button onClick={() => setEditing(null)}>Cancel</Button>
            {testFailed(save.error) ? (
              <Button variant="ghost" loading={save.isPending && save.variables === true} onClick={() => save.mutate(true)}>
                Save anyway
              </Button>
            ) : null}
            <Button variant="primary" loading={save.isPending && save.variables === false} disabled={Boolean(missing)} onClick={() => save.mutate(false)}>
              {editing?.existing ? 'Test and save' : 'Add store'}
            </Button>
          </>
        }
      >
        {editing ? (
          <>
            {editing.existing ? (
              <Field label="Name" hint={`Connections refer to it as ref+${editing.existing.name}://…, so the name can’t change.`}>
                {(id) => <Input id={id} value={editing.existing!.name} readOnly className="font-mono" />}
              </Field>
            ) : null}
            {!editing.existing ? (
              <Field label="Name" hint="Used in references (ref+<name>://…): it can’t change later.">
                {(id) => (
                  <SlugInput
                    id={id}
                    value={name}
                    taken={names}
                    placeholder={editing.kind.kind === 'vault' ? 'vault' : `${editing.kind.kind}-prod`}
                    onChange={setName}
                  />
                )}
              </Field>
            ) : null}
            <SchemaForm idPrefix="ss" schema={editing.kind.config_schema as JsonSchema} values={config} onChange={setConfig} expert={expert} />
            <SchemaForm
              idPrefix="sss"
              schema={editing.kind.secret_schema as JsonSchema}
              values={secrets}
              context={config}
              onChange={setSecrets}
              expert={expert}
              secret
              setSecrets={editing.existing?.secret_fields_set ?? []}
            />
            <Notice tone="info">
              Grant BeziSync read-only access to just the secrets your connections need.{' '}
              {editing.existing
                ? 'Saving tests the new settings first; if the test fails, nothing changes.'
                : 'BeziSync tests the sign-in first; if the test fails, nothing is saved.'}
            </Notice>
            {testFailed(save.error) ? (
              <Notice tone="bad">
                {save.error instanceof Error ? save.error.message : ''} {editing.existing ? 'The store keeps its working settings. Fix them' : 'Fix the settings'}, or
                choose Save anyway if the store isn’t reachable from here yet.
              </Notice>
            ) : (
              <InlineError error={save.error} doing="Couldn’t save the store" />
            )}
          </>
        ) : null}
      </Modal>

      <CopyStoreDialog store={copying} onClose={() => setCopying(null)} />
      <Confirm
        open={deleting !== null}
        onOpenChange={(o) => !o && setDeleting(null)}
        title={`Delete ${deleting?.name ?? ''}?`}
        effects={['Its credentials are destroyed.', 'References to it stop resolving.']}
        confirmLabel="Delete"
        typeToConfirm={deleting?.name}
        pending={remove.isPending}
        error={remove.error}
        doing="Couldn’t delete the store"
        onConfirm={() => deleting && remove.mutate(deleting.id)}
      />
    </div>
  );
}

const testFailed = (e: unknown) => e instanceof ApiError && e.code === 'store_test_failed';

/** What a store is set to (never its credentials): its settings, which credentials are saved and when. */
function StoreDetails({ store: s, kind }: { store: Store; kind?: StoreKind }) {
  const props = ((kind?.config_schema as JsonSchema | undefined)?.properties ?? {}) as Record<string, { title?: string }>;
  const secretProps = ((kind?.secret_schema as JsonSchema | undefined)?.properties ?? {}) as Record<string, { title?: string }>;
  const settings = Object.entries(s.config as Record<string, unknown>).filter(([, v]) => v !== null && v !== '' && v !== undefined && typeof v !== 'object');
  const creds = s.secret_fields_set.map((f) => secretProps[f]?.title ?? f);
  return (
    <KeyValue
      items={[
        ...settings.map(([k, v]): [string, ReactNode] => [props[k]?.title ?? k, <span className="font-mono text-sm">{String(v)}</span>]),
        [
          'Credentials',
          creds.length ? (
            <span>
              {creds.join(', ')}
              {s.secrets_updated_at ? <Meta> · replaced {ago(s.secrets_updated_at)}</Meta> : null}
            </span>
          ) : (
            <Meta>None saved</Meta>
          ),
        ],
      ]}
    />
  );
}

/** Copy a store to another project under the same name; Admins only. */
function CopyStoreDialog({ store, onClose }: { store: Store | null; onClose: () => void }) {
  const { project } = useProject();
  const targets = useCopyTargets('stores');
  const [targetId, setTargetId] = useState('');
  const [secrets, setSecrets] = useState<SecretsChoice>('leave');
  const run = (dry: boolean) =>
    unwrap(
      client.POST('/api/v1/projects/{project_id}/secret-stores/{store_id}/copy', {
        params: { path: { project_id: project.id, store_id: store?.id ?? '' } },
        body: { target_project_id: targetId, secrets, dry_run: dry },
      }),
    );
  const plan = useQuery({ queryKey: ['store-copy-plan', store?.id, targetId, secrets], enabled: Boolean(store && targetId), queryFn: () => run(true) });
  const qc = useQueryClient();
  const copy = useMutation({
    mutationFn: () => run(false),
    onSuccess: (r) => {
      toast.success(`${r.action === 'update' ? 'Updated' : 'Copied'} ${r.store.name} in ${r.target_project}`);
      void qc.invalidateQueries({ queryKey: ['secret-stores', r.target_project_id] });
      close();
    },
  });
  const close = () => {
    setTargetId('');
    setSecrets('leave');
    copy.reset();
    onClose();
  };
  const target = targets.find((t) => t.id === targetId);
  return (
    <Modal
      open={store !== null}
      onOpenChange={(o) => !o && close()}
      title={`Copy ${store?.name ?? ''} to another project`}
      description="Connections there can then read secrets with the same references. The original stays here."
      why={!targetId ? 'Choose the project to copy to.' : null}
      footer={
        <>
          <Button onClick={close}>Cancel</Button>
          <Button variant="primary" disabled={!targetId || !plan.data} loading={copy.isPending} onClick={() => copy.mutate()}>
            {plan.data?.action === 'update' ? `Update ${store?.name ?? ''} in ${target?.name ?? ''}` : `Copy to ${target?.name ?? 'project'}`}
          </Button>
        </>
      }
    >
      <Field label="Copy to">
        {(id) => <Select id={id} value={targetId} onChange={setTargetId} options={targets.map((t) => ({ value: t.id, label: t.name }))} />}
      </Field>
      <Field
        label="Credentials"
        hint={
          secrets === 'leave'
            ? 'The copy has no credentials until someone there adds them (or the runtime’s own identity is enough).'
            : 'Copied on the server and kept encrypted; nobody sees them. Both audit logs record it.'
        }
      >
        {() => (
          <Segmented
            label="Credentials"
            value={secrets}
            onChange={setSecrets}
            options={[
              { value: 'leave', label: 'Leave them out (recommended)' },
              { value: 'copy', label: 'Copy them' },
            ]}
          />
        )}
      </Field>
      {plan.data?.action === 'update' ? (
        <Notice tone="warn">
          {plan.data.target_project} already has a store named {store?.name}. Its settings are replaced
          {secrets === 'leave' ? '; its own credentials stay.' : ', and so are its credentials.'}
        </Notice>
      ) : null}
      <InlineError error={plan.error ?? copy.error} doing="Couldn’t copy the store" />
    </Modal>
  );
}
