import clsx from 'clsx';
import { useCallback, useEffect, useMemo, useState } from 'react';
import { Link } from '@tanstack/react-router';
import { toast } from 'sonner';
import { Modal } from '@/components/ui/overlays';
import { Button } from '@/components/ui/button';
import { InlineError, Pill, Skeleton } from '@/components/ui/feedback';
import { Check, Field, Input, RadioCards, SlugInput, slugError } from '@/components/ui/form';
import { cleanValues, defaultsOf, SchemaForm, type FormValues, type JsonSchema, type LinkOptions } from '@/components/SchemaForm';
import { CheckList } from '@/components/CheckList';
import { engineGate, sinkFit, useEngines, type EnginePolicy } from '@/lib/engines';
import { allows } from '@/lib/policies';
import { slugify } from '@/lib/format';
import { useSecretStores } from '@/lib/secretRefs';
import { useProject } from '@/lib/session';
import {
  TYPE_LABEL,
  useConnections,
  useConnectorTypes,
  useCreateConnection,
  useDraftCheck,
  useUpdateConnection,
  type Connection,
  type ConnectorType,
  type Kind,
} from '@/lib/connections';
import { SecretFromStore, type RefStatus } from './SecretFromStore';

export function ConnectionDialog({
  open,
  onOpenChange,
  kind,
  existing,
  onSaved,
  fitting,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  kind: Kind;
  existing?: Connection;
  onSaved?: (conn: Connection) => void;
  /** From the wizard: offer only destination types this source can go to under the project's
   *  policies (ADR 0011). The Connections page passes nothing and lists every type. */
  fitting?: { sourceType: string; policy?: (EnginePolicy & { allowed_sink_types?: string[] | null }) | undefined };
}) {
  const { project, expert, can } = useProject();
  const types = useConnectorTypes(project.id);
  const engines = useEngines();
  const readable = fitting?.policy && !fitting.policy.unreadable ? fitting.policy : undefined;
  // In the wizard: types no allowed engine runs from the source (or the policy leaves out) aren't
  // offered; those whose engines are off say why (sinkFit).
  const fitOf = (t: ConnectorType) => (fitting && kind === 'sink' ? sinkFit(engines.data, fitting.sourceType, t.type, readable) : null);
  const choices = (types.data ?? []).filter(
    (t) => t.kind === kind && (!fitting || kind !== 'sink' || (allows(readable?.allowed_sink_types, t.type) && fitOf(t)?.state !== 'hidden')),
  );
  const [type, setType] = useState<ConnectorType | undefined>();
  const [provider, setProvider] = useState('self_hosted');
  const [name, setName] = useState('');
  const [nameTouched, setNameTouched] = useState(false);
  const [description, setDescription] = useState('');
  const [config, setConfig] = useState<FormValues>({});
  const [secrets, setSecrets] = useState<FormValues>({});
  const [managed, setManaged] = useState(false);
  const [touched, setTouched] = useState(false);
  const connections = useConnections(project.id).data;
  const taken = (connections ?? []).filter((c) => c.id !== existing?.id).map((c) => c.name);
  const links = useMemo(() => {
    const out: LinkOptions = {};
    for (const c of connections ?? []) (out[`${c.kind}/${c.type}`] ??= []).push({ value: c.id, label: c.name });
    return out;
  }, [connections]);
  const storeList = useSecretStores(project.id);
  const stores = storeList.data ?? [];
  // Secret fields that take their value from a store, and whether each reference resolves.
  const [refs, setRefs] = useState<Record<string, RefStatus>>({});
  const onRefStatus = useCallback((field: string, status: RefStatus | null) => {
    setRefs((cur) => {
      if (JSON.stringify(cur[field] ?? null) === JSON.stringify(status)) return cur;
      const next = { ...cur };
      if (status) next[field] = status;
      else delete next[field];
      return next;
    });
  }, []);
  const draft = useDraftCheck(project.id);
  const create = useCreateConnection(project.id);
  const update = useUpdateConnection(project.id);

  useEffect(() => {
    if (!open) return;
    draft.reset();
    create.reset();
    update.reset();
    setTouched(false);
    setSecrets({});
    setRefs({});
    if (existing) {
      setType(choices.find((t) => t.type === existing.type));
      setProvider(existing.provider);
      setName(existing.name);
      setDescription(existing.description);
      setConfig(existing.config);
      setManaged(existing.managed);
    } else {
      setType(undefined);
      setProvider('self_hosted');
      setName('');
      setNameTouched(false);
      setDescription('');
      setManaged(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, existing?.id, choices.length]);

  // A new connector type starts from its schema defaults.
  useEffect(() => {
    if (!existing && type) setConfig(defaultsOf(type.config_schema as JsonSchema));
  }, [type, existing]);

  // Switching provider keeps what the user typed and applies only the preset's own defaults.
  useEffect(() => {
    if (existing || !type) return;
    const preset = type.presets.find((p) => p.id === provider) as { defaults?: FormValues } | undefined;
    setConfig((cur) => ({ ...cur, ...(preset?.defaults ?? {}) }));
    draft.reset();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [provider]);

  const configSchema = (type?.config_schema ?? {}) as JsonSchema;
  const secretSchema = (type?.secret_schema ?? {}) as JsonSchema;
  const noun = kind === 'source' ? 'source' : 'destination';

  function payload() {
    return { config: cleanValues(configSchema, config), secrets: cleanValues(secretSchema, secrets) };
  }

  function runCheck() {
    if (!type) return;
    const { config: c, secrets: s } = payload();
    draft.mutate({ kind, type: type.type, provider, config: c, secrets: s, connection_id: existing?.id ?? null });
  }

  function save() {
    if (!type || (refProblem && !unresolved)) return;
    const { config: c, secrets: s } = payload();
    const done = (conn: Connection) => {
      toast.success(existing ? `Saved ${conn.name}` : `Added ${conn.name}`);
      onSaved?.(conn);
      onOpenChange(false);
    };
    if (existing) {
      update.mutate({ id: existing.id, version: existing.version, body: { description, provider, config: c, secrets: s, managed } }, { onSuccess: done });
    } else {
      create.mutate({ kind, type: type.type, name: slugify(name), description, provider, config: c, secrets: s, managed }, { onSuccess: done });
    }
  }
  const nameErr = existing ? null : !name ? null : slugError(name, taken);
  // A secret picked from a store must resolve before saving; "Save anyway" is for a store that
  // isn't reachable from here yet (the deploy checks catch it if it stays broken).
  const refProblem = Object.entries(refs).find(([, r]) => r.state !== 'ok');
  const unresolved = refProblem?.[1].state === 'unresolved';
  const fieldTitle = (field: string) => (secretSchema.properties?.[field]?.title ?? field).toLowerCase();
  const refWhy = !refProblem
    ? null
    : refProblem[1].state === 'incomplete'
      ? `Finish picking the ${fieldTitle(refProblem[0])} from a secret store.`
      : refProblem[1].state === 'checking'
        ? 'Checking the secret reference…'
        : `The ${fieldTitle(refProblem[0])} doesn’t resolve: ${refProblem[1].state === 'unresolved' ? refProblem[1].message : ''}`;
  const missing = !type ? null : !existing && !name.trim() ? 'Name the connection.' : (nameErr ?? refWhy);
  const changed =
    <T,>(set: (v: T) => void) =>
    (v: T) => {
      set(v);
      setTouched(true);
    };

  const preset = type?.presets.find((p) => p.id === provider) as { note?: string } | undefined;

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      size="lg"
      dirty={touched}
      title={existing ? `Edit ${existing.name}` : `Add ${noun}`}
      description={kind === 'source' ? 'A database BeziSync captures changes from.' : 'Where change events are delivered.'}
      why={missing}
      footer={
        type ? (
          <>
            <Button onClick={runCheck} loading={draft.isPending}>
              Test connection
            </Button>
            {unresolved && !(nameErr || (!existing && !name.trim())) ? (
              <Button variant="ghost" onClick={save} loading={create.isPending || update.isPending}>
                Save anyway
              </Button>
            ) : null}
            <Button variant="primary" onClick={save} loading={create.isPending || update.isPending} disabled={Boolean(missing)}>
              {existing ? 'Save changes' : `Save ${noun}`}
            </Button>
          </>
        ) : null
      }
    >
      {types.isPending ? (
        <Skeleton />
      ) : !type ? (
        <div className="grid grid-cols-[repeat(auto-fill,minmax(min(100%,220px),1fr))] gap-2.5">
          {choices.map((t) => {
            const gate = engineGate(t);
            const fit = fitOf(t);
            const off = fit ? fit.state === 'unavailable' : gate !== null && !gate.available;
            const why = fit?.state === 'unavailable' ? fit.why : gate?.reason;
            return (
              <button
                key={t.type}
                type="button"
                disabled={off}
                onClick={() => {
                  setType(t);
                  setTouched(true);
                }}
                className={clsx(
                  'flex min-w-0 flex-col gap-1 rounded-lg border border-line-strong bg-surface px-3.5 py-3 text-left',
                  off ? 'cursor-not-allowed opacity-70' : 'hover:border-accent hover:bg-hover',
                )}
              >
                <span className="flex min-w-0 flex-wrap items-center gap-2">
                  <strong className="text-base font-semibold">{TYPE_LABEL[t.type] ?? t.title}</strong>
                  {t.capture?.maturity === 'incubating' ? (
                    <Pill tone="info" title="Debezium marks this connector as incubating: it works, but may change between versions.">
                      Preview
                    </Pill>
                  ) : null}
                  {gate ? (
                    <Pill tone="grey" title="Only Flink CDC writes this destination.">
                      Flink CDC
                    </Pill>
                  ) : null}
                </span>
                <span className="text-sm text-muted">{off ? why : t.description}</span>
              </button>
            );
          })}
        </div>
      ) : (
        <>
          {!existing ? (
            <Button size="sm" variant="ghost" className="self-start" onClick={() => setType(undefined)}>
              ← {TYPE_LABEL[type.type] ?? type.title} · change type
            </Button>
          ) : null}
          {type.presets.length > 1 ? (
            <Field label="Provider" group>
              {() => (
                <RadioCards
                  label="Provider"
                  value={provider}
                  onChange={changed(setProvider)}
                  options={type.presets.map((p) => ({ value: p.id as string, label: p.title as string }))}
                />
              )}
            </Field>
          ) : null}
          {preset?.note ? <p className="m-0 text-sm text-muted">{preset.note}</p> : null}
          <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,260px),1fr))] gap-4">
            <Field label="Name" hint={existing ? 'A connection keeps its name.' : 'Used in pipeline YAML. Type it as you like; the saved form is shown.'}>
              {(id) =>
                existing ? (
                  <Input id={id} value={name} disabled />
                ) : (
                  <div
                    onFocus={() => {
                      const host = String(config.host ?? config.bootstrap_servers ?? '').split(/[.:,]/)[0];
                      if (!nameTouched && !name && host) setName(`${host}-${type.type}`);
                    }}
                  >
                    <SlugInput
                      id={id}
                      value={name}
                      taken={taken}
                      placeholder={kind === 'source' ? 'orders-pg' : 'events-kafka'}
                      onChange={(v) => {
                        setNameTouched(true);
                        changed(setName)(v);
                      }}
                    />
                  </div>
                )
              }
            </Field>
            <Field label="Description" optional>
              {(id) => <Input id={id} value={description} onChange={(e) => changed(setDescription)(e.target.value)} />}
            </Field>
          </div>
          <SchemaForm idPrefix="cfg" schema={configSchema} values={config} onChange={changed(setConfig)} expert={expert} links={links} />
          <SchemaForm
            idPrefix="sec"
            schema={secretSchema}
            values={secrets}
            onChange={changed(setSecrets)}
            expert={expert}
            secret
            setSecrets={existing?.secret_fields_set ?? []}
            secretRefs={existing?.secret_refs ?? {}}
            renderSecret={stores.length ? (field) => <SecretFromStore field={field} stores={stores} onStatus={onRefStatus} /> : undefined}
          />
          {storeList.isSuccess && !stores.length && Object.keys(secretSchema.properties ?? {}).length ? <SecretStoreHint manage={can('project.manage')} slug={project.slug} /> : null}
          {can('project.manage') ? (
            <Check
              checked={managed}
              onChange={changed(setManaged)}
              label="Managed by admins"
              description="Editors can use it in pipelines but can’t change or delete it."
            />
          ) : null}
          <InlineError error={draft.error} doing="Couldn’t run the checks" />
          {draft.data ? (
            <div className="rounded-lg border border-line p-card">
              <CheckList report={draft.data} expert={expert} subject={name || 'the database'} onRecheck={runCheck} rechecking={draft.isPending} />
            </div>
          ) : null}
          <InlineError error={create.error ?? update.error} doing={existing ? 'Couldn’t save the connection' : 'Couldn’t add the connection'} />
        </>
      )}
    </Modal>
  );
}

/** With no secret store in the project, secrets are typed; say where a store is added. */
function SecretStoreHint({ manage, slug }: { manage: boolean; slug: string }) {
  return (
    <p className="m-0 text-sm text-muted wrap-break-word">
      Keep secrets in Vault, AWS, Google Cloud or Kubernetes instead? BeziSync then saves only a reference and reads the value when it’s needed.{' '}
      {manage ? (
        <Link to="/p/$projectSlug/settings/$section" params={{ projectSlug: slug, section: 'secret-stores' }} className="font-semibold text-accent underline">
          Add a secret store
        </Link>
      ) : (
        'An admin can add a secret store in Settings.'
      )}
    </p>
  );
}
