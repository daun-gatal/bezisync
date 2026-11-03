import { useEffect, useMemo, useState } from 'react';
import { Notice } from '@/components/ui/feedback';
import { Field, Input, Segmented } from '@/components/ui/form';
import { Combobox, Select } from '@/components/ui/select';
import type { SecretFieldProps } from '@/components/SchemaForm';
import { useProject } from '@/lib/session';
import { formatRef, guessKey, parseRef, useRefCheck, useSecretKeys, useStoreSecrets, type SecretStore } from '@/lib/secretRefs';

const KIND: Record<string, string> = { vault: 'Vault', aws: 'AWS Secrets Manager', gcp: 'Google Secret Manager', kubernetes: 'Kubernetes' };

/** Where a picked reference stands; the dialog keeps a connection with a broken one from saving. */
export type RefStatus = { state: 'incomplete' | 'checking' | 'ok' } | { state: 'unresolved'; message: string };

/**
 * A secret field that can take its value from one of the project's secret stores: pick the store,
 * the secret (listed from the store) and its key (pre-picked when the name makes it clear). Only
 * the reference ref+<store>://<path>#<key> is saved; values never reach the browser.
 */
export function SecretFromStore({
  field,
  stores,
  onStatus,
}: {
  field: SecretFieldProps;
  stores: SecretStore[];
  onStatus: (name: string, status: RefStatus | null) => void;
}) {
  const { project } = useProject();
  const { name, value, onChange, savedRef, isSet, input, hint } = field;
  const typed = typeof value === 'string' && value !== '' && !value.startsWith('ref+');
  const [mode, setMode] = useState<'type' | 'store'>(() => (parseRef(value) || (!typed && savedRef) ? 'store' : 'type'));
  // The picked parts; a saved reference fills them until the person changes one.
  const initial = parseRef(value) ?? (value === undefined || value === '' ? parseRef(savedRef) : null);
  const [storeName, setStoreName] = useState(initial?.store ?? (stores.length === 1 ? stores[0]!.name : ''));
  const [path, setPath] = useState(initial?.path ?? '');
  const [key, setKey] = useState<string | null>(initial?.key ?? null);
  const store = stores.find((s) => s.name === storeName);

  const listed = useStoreSecrets(project.id, mode === 'store' ? store?.id : undefined);
  const canPick = listed.data?.ok === true && !listed.data.truncated;
  const keys = useSecretKeys(project.id, mode === 'store' ? store?.id : undefined, path);
  const keyNames = useMemo(() => (keys.data?.ok ? keys.data.keys : []), [keys.data]);
  const whole = keys.data?.ok === true && keys.data.whole_value;
  const complete = Boolean(store && path && (whole || key));
  const check = useRefCheck(project.id, store?.id, path, whole ? null : key, mode === 'store' && complete);

  // Pre-pick the key whose name matches the field, or the only one.
  useEffect(() => {
    if (!keys.data?.ok) return;
    if (keys.data.whole_value) return setKey(null);
    if (key && keyNames.includes(key)) return;
    setKey(guessKey(name, keyNames));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [keys.data]);

  // The field's value: the reference once complete. Unchanged from the saved one, it stays blank
  // so the saved reference is kept as it is.
  const ref = complete && store ? formatRef({ store: store.name, path, key: whole ? null : key }) : '';
  useEffect(() => {
    if (mode !== 'store') return;
    const next = ref === savedRef ? '' : ref;
    if ((value ?? '') !== next) onChange(next);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, ref]);

  useEffect(() => {
    if (mode !== 'store') return onStatus(name, null);
    if (!complete) return onStatus(name, { state: 'incomplete' });
    if (!check.data) return onStatus(name, { state: 'checking' });
    onStatus(name, check.data.ok ? { state: 'ok' } : { state: 'unresolved', message: check.data.message });
  }, [mode, complete, check.data, name, onStatus]);
  useEffect(() => () => onStatus(name, null), [name, onStatus]);

  const switchTo = (next: 'type' | 'store') => {
    setMode(next);
    if (next === 'type') onChange('');
  };

  return (
    <div className="flex min-w-0 flex-col gap-2.5">
      <Segmented
        label="Type it or pick it from a secret store"
        value={mode}
        onChange={switchTo}
        options={[
          { value: 'type', label: 'Type it' },
          { value: 'store', label: 'From a secret store' },
        ]}
      />
      {mode === 'type' ? (
        <>
          {input}
          {hint ? <p className="m-0 text-sm text-muted">{savedRef ? 'Type a value to replace the saved reference.' : hint}</p> : null}
        </>
      ) : (
        <>
          <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,180px),1fr))] items-start gap-3">
            <Field label="Store">
              {(sid) => (
                <Select
                  id={sid}
                  value={storeName}
                  placeholder="Choose a store"
                  onChange={(v) => {
                    setStoreName(v);
                    setPath('');
                    setKey(null);
                  }}
                  options={stores.map((s) => ({
                    value: s.name,
                    label: s.name,
                    description: `${KIND[s.kind] ?? s.kind}${s.last_test ? (s.last_test.ok ? ' · connected' : ' · not connected') : ''}`,
                  }))}
                />
              )}
            </Field>
            <Field label="Secret">
              {(pid) =>
                canPick ? (
                  <Combobox
                    id={pid}
                    value={path}
                    placeholder={listed.data!.secrets.length ? 'Choose a secret' : 'This store has no secrets'}
                    searchLabel="Search secrets"
                    onChange={(v) => {
                      setPath(v);
                      setKey(null);
                    }}
                    options={listed.data!.secrets.map((p) => ({ value: p, label: p }))}
                  />
                ) : (
                  <Input
                    id={pid}
                    className="font-mono"
                    spellCheck={false}
                    value={path}
                    disabled={!store || listed.isPending}
                    placeholder={store?.kind === 'kubernetes' ? 'orders-db' : 'cdc/orders-db'}
                    onChange={(e) => {
                      setPath(e.target.value.trim());
                      setKey(null);
                    }}
                  />
                )
              }
            </Field>
            <Field label="Key">
              {(kid) =>
                whole ? (
                  <Input id={kid} value="Whole value" readOnly />
                ) : keyNames.length ? (
                  <Select id={kid} value={key ?? ''} placeholder="Choose a key" onChange={setKey} options={keyNames.map((k) => ({ value: k, label: k }))} />
                ) : (
                  <Input
                    id={kid}
                    className="font-mono"
                    spellCheck={false}
                    value={key ?? ''}
                    disabled={!path}
                    placeholder={path ? 'password' : 'Pick a secret first'}
                    onChange={(e) => setKey(e.target.value.trim() || null)}
                  />
                )
              }
            </Field>
          </div>
          {listed.data && !listed.data.ok ? (
            <Notice tone="warn">{`${store?.name ?? 'This store'} can’t list its secrets (${listed.data.message}), so type the path.`}</Notice>
          ) : listed.data?.truncated ? (
            <Notice tone="info">This store has more than 1,000 secrets: type the path.</Notice>
          ) : null}
          {complete && check.data ? (
            <Notice tone={check.data.ok ? 'ok' : 'bad'}>
              {check.data.ok ? `Found in ${store!.name}. Only the reference ${ref} is saved.` : check.data.message}
            </Notice>
          ) : null}
          {typed || (isSet && !savedRef) ? <p className="m-0 text-sm text-muted">Switching to a store replaces the saved value when you save.</p> : null}
        </>
      )}
    </div>
  );
}
