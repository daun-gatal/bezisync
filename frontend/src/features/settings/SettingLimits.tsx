import { useMemo, useState } from 'react';
import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Pill } from '@/components/ui/feedback';
import { Check, Field, Input } from '@/components/ui/form';
import { Combobox, Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import { useDebounced } from '@/components/ui/hooks';
import { client, unwrap, type Schemas } from '@/lib/api';
import { useEngines } from '@/lib/engines';
import { settingError, type Setting } from '@/lib/settings';
import { settingText } from '@/components/SettingsEditor';

export type SettingLimit = Schemas['SettingLimit'];
type Kind = 'max' | 'min' | 'choices' | 'pin' | 'hide';

/** A limit in words ("At most 4", "Always 500", "Hidden"). */
export function limitText(l: SettingLimit, s?: Setting): string {
  const parts: string[] = [];
  if (l.hide) parts.push('Hidden from pipelines');
  if (l.pin) parts.push(`Always ${s ? settingText(s, l.pin) : l.pin}`);
  if (l.min) parts.push(`At least ${l.min}`);
  if (l.max) parts.push(`At most ${l.max}`);
  if (l.choices) parts.push(`Only ${l.choices.join(', ')}`);
  return parts.join(' · ');
}

/** Why a new limit can't be added as it is (the server checks the rest); null when it can. */
export function newLimitError(s: Setting | undefined, kind: Kind, value: string, choices: string[]): string | null {
  if (!s) return 'Choose a setting.';
  if ((kind === 'max' || kind === 'min') && !['int', 'fraction', 'duration'].includes(s.type)) return 'Only a number or a duration has a range.';
  if (kind === 'choices' && s.type !== 'enum') return 'Only a setting with choices can be limited to some.';
  if (kind === 'choices' && !choices.length) return 'Keep at least one choice, or hide the setting.';
  if ((kind === 'max' || kind === 'min' || kind === 'pin') && settingError(s, value)) return settingError(s, value);
  return null;
}

/**
 * The project's limits on typed settings (Admins): a range, fewer choices, a value every pipeline
 * gets, or hidden. Before saving it says how many pipelines a limit would refuse; a limit on a
 * setting BeziSync no longer offers is shown as obsolete and ignored.
 */
export function SettingLimits({
  projectId,
  value,
  onChange,
  disabled,
}: {
  projectId: string;
  value: Record<string, SettingLimit>;
  onChange: (v: Record<string, SettingLimit>) => void;
  disabled?: boolean;
}) {
  const settings = (useEngines().data ?? []).flatMap((e) => e.settings ?? []);
  const byKey = useMemo(() => new Map(settings.map((s) => [s.key, s])), [settings]);
  const asked = useDebounced(JSON.stringify(value), 400);
  const impact = useQuery({
    queryKey: ['limit-impact', projectId, asked],
    enabled: !disabled && asked !== '{}',
    placeholderData: keepPreviousData,
    queryFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/policies/setting-limits:impact', {
          params: { path: { project_id: projectId } },
          body: { setting_limits: JSON.parse(asked) } as never,
        }),
      ),
  });
  const counts = new Map((impact.data?.items ?? []).map((i) => [i.key, i]));
  const [key, setKey] = useState<string | null>(null);
  const [kind, setKind] = useState<Kind>('max');
  const [amount, setAmount] = useState('');
  const [choices, setChoices] = useState<string[]>([]);
  const chosen = key ? byKey.get(key) : undefined;
  const error = newLimitError(chosen, kind, amount, choices);
  const add = () => {
    if (!key || error) return;
    const l: SettingLimit = { ...(value[key] ?? {}) };
    if (kind === 'hide') {
      l.hide = true;
      delete l.pin;
    } else if (kind === 'choices') l.choices = choices;
    else l[kind] = amount;
    onChange({ ...value, [key]: l });
    setKey(null);
    setAmount('');
    setChoices([]);
  };
  const keys = Object.keys(value).sort();
  return (
    <div className="flex min-w-0 flex-col gap-4">
      {keys.length ? (
        <ul aria-label="Setting limits" className="m-0 flex list-none flex-col gap-2 p-0">
          {keys.map((k) => {
            const s = byKey.get(k);
            const hit = counts.get(k);
            return (
              <li key={k} className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-start gap-3 rounded-md border border-line px-3 py-2">
                <div className="flex min-w-0 flex-col gap-0.5">
                  <span className="flex flex-wrap items-center gap-2">
                    <strong className="text-sm">{s?.title ?? k}</strong>
                    {s ? null : <Pill tone="warn">Obsolete: ignored</Pill>}
                    {s ? <Meta>{s.engine === 'flink-cdc' ? 'Flink CDC' : 'Debezium'}</Meta> : null}
                  </span>
                  <Meta className="font-mono">{k}</Meta>
                  <span className="text-sm text-ink-2">{limitText(value[k]!, s)}</span>
                  {hit ? (
                    <span className="text-sm text-warn">
                      Would block {hit.pipelines} pipeline{hit.pipelines === 1 ? '' : 's'} at the next deploy: {hit.names.join(', ')}
                      {hit.pipelines > hit.names.length ? '…' : ''}
                    </span>
                  ) : null}
                </div>
                {disabled ? null : (
                  <Button
                    size="sm"
                    variant="ghost"
                    icon={<Trash2 />}
                    aria-label={`Remove the limit on ${s?.title ?? k}`}
                    onClick={() => {
                      const next = { ...value };
                      delete next[k];
                      onChange(next);
                    }}
                  />
                )}
              </li>
            );
          })}
        </ul>
      ) : (
        <Meta>No limits: pipelines may use every setting within its own bounds.</Meta>
      )}
      {disabled ? null : (
        <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,220px),1fr))] items-end gap-3">
          <Field label="Setting">
            {(id) => (
              <Combobox
                id={id}
                value={key}
                placeholder="Choose a setting…"
                searchLabel="Search settings"
                onChange={setKey}
                options={settings.map((s) => ({ value: s.key, label: s.title, text: `${s.title} ${s.key}`, description: `${s.engine === 'flink-cdc' ? 'Flink CDC' : 'Debezium'} · ${s.key}` }))}
              />
            )}
          </Field>
          <Field label="Limit">
            {(id) => (
              <Select
                id={id}
                value={kind}
                onChange={(k) => setKind(k as Kind)}
                options={[
                  { value: 'max', label: 'At most' },
                  { value: 'min', label: 'At least' },
                  { value: 'choices', label: 'Only some choices' },
                  { value: 'pin', label: 'Always this value' },
                  { value: 'hide', label: 'Hidden from pipelines' },
                ]}
              />
            )}
          </Field>
          {kind === 'choices' && chosen?.type === 'enum' ? (
            <fieldset className="m-0 flex flex-wrap gap-3 border-0 p-0">
              <legend className="sr-only">Choices pipelines may use</legend>
              {(chosen.choices ?? []).map((c) => (
                <Check key={c} checked={choices.includes(c)} label={c} onChange={(on) => setChoices(on ? [...choices, c] : choices.filter((x) => x !== c))} />
              ))}
            </fieldset>
          ) : kind === 'hide' || kind === 'choices' ? null : chosen?.type === 'enum' || chosen?.type === 'bool' ? (
            <Field label="Value">
              {(id) => (
                <Select
                  id={id}
                  value={amount}
                  onChange={setAmount}
                  options={(chosen.type === 'bool' ? ['true', 'false'] : (chosen.choices ?? [])).map((c) => ({ value: c, label: settingText(chosen, c) }))}
                />
              )}
            </Field>
          ) : (
            <Field label="Value" hint={chosen ? `${chosen.min ?? ''} to ${chosen.max ?? ''}` : undefined}>
              {(id) => <Input id={id} className="font-mono" value={amount} onChange={(e) => setAmount(e.target.value.trim())} />}
            </Field>
          )}
          <div className="flex flex-col gap-1">
            <Button onClick={add} disabled={Boolean(error)}>
              Add the limit
            </Button>
            {key && error ? <span className="text-xs text-bad">{error}</span> : null}
          </div>
        </div>
      )}
    </div>
  );
}
