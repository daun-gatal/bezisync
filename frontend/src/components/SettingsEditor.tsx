import { Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Pill } from '@/components/ui/feedback';
import { Field, Input, Segmented, Stepper } from '@/components/ui/form';
import { Combobox, Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import { IMPACT, STAGES, settingError, type Impact, type Setting } from '@/lib/settings';

/** What changing a setting does to a running pipeline: a glyph and words, never colour alone. */
export function ImpactPill({ impact }: { impact: Impact }) {
  const i = IMPACT[impact];
  return (
    <Pill tone={i.tone} title={i.long}>
      {i.label}
    </Pill>
  );
}

/** A setting's value as people read it ("on"/"off" for switches). */
export function settingText(s: Setting, value: string): string {
  if (s.type === 'bool') return value === 'true' ? 'on' : 'off';
  return value;
}

/**
 * Typed settings, grouped by stage: each with its control (bounded by the registry, or by `caps`
 * where a project limits it), its default and what changing it does. Settings not set yet are
 * added from a searchable list; removing one restores its default.
 */
export function SettingsEditor({
  value,
  onChange,
  settings,
  disabled,
  caps,
  addLabel = 'Add a setting',
  idBase,
}: {
  value: Record<string, string>;
  onChange: (v: Record<string, string>) => void;
  /** The settings this pipeline may tune. */
  settings: Setting[];
  disabled?: boolean;
  /** Tighter bounds a project sets (key → max), shown in the hint. */
  caps?: Record<string, number>;
  addLabel?: string;
  idBase: string;
}) {
  const put = (k: string, v: string) => onChange({ ...value, [k]: v });
  const drop = (k: string) => {
    const next = { ...value };
    delete next[k];
    onChange(next);
  };
  const set = settings.filter((s) => s.key in value);
  const unknown = Object.keys(value).filter((k) => !settings.some((s) => s.key === k));
  const unused = settings.filter((s) => !(s.key in value));
  return (
    <div className="flex min-w-0 flex-col gap-4">
      {STAGES.map(({ id, title }) => {
        const rows = set.filter((s) => s.stage === id);
        if (!rows.length) return null;
        return (
          <section key={id} aria-label={title} className="flex min-w-0 flex-col gap-3">
            <Meta className="font-semibold tracking-wide uppercase">{title}</Meta>
            {rows.map((s) => (
              <SettingRow
                key={s.key}
                idBase={idBase}
                setting={s}
                value={value[s.key] ?? s.default}
                cap={caps?.[s.key]}
                disabled={disabled}
                onChange={(v) => put(s.key, v)}
                onRemove={() => drop(s.key)}
              />
            ))}
          </section>
        );
      })}
      {unknown.map((k) => (
        <div key={k} className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
          <div className="flex min-w-0 flex-col">
            <Meta className="font-mono">{k}</Meta>
            <span className="text-sm text-bad">Not a setting this pipeline can use: remove it.</span>
          </div>
          <Button size="sm" variant="ghost" icon={<Trash2 />} aria-label={`Remove ${k}`} onClick={() => drop(k)} disabled={disabled} />
        </div>
      ))}
      {unused.length && !disabled ? (
        <Field label={addLabel} optional>
          {(id) => (
            <Combobox
              id={id}
              value={null}
              placeholder="Choose a setting…"
              searchLabel="Search settings"
              onChange={(k) => put(k, settings.find((s) => s.key === k)?.default ?? '')}
              options={unused.map((s) => ({
                value: s.key,
                label: s.title,
                text: `${s.title} ${s.key}`,
                description: `${STAGES.find((st) => st.id === s.stage)?.title ?? s.stage} · ${IMPACT[s.impact].label} · ${s.why}`,
              }))}
            />
          )}
        </Field>
      ) : null}
    </div>
  );
}

function SettingRow({
  idBase,
  setting: s,
  value,
  cap,
  disabled,
  onChange,
  onRemove,
}: {
  idBase: string;
  setting: Setting;
  value: string;
  cap?: number;
  disabled?: boolean;
  onChange: (v: string) => void;
  onRemove: () => void;
}) {
  const error = settingError(s, value);
  const max = cap !== undefined ? Math.min(cap, Number(s.max)) : Number(s.max);
  const capped = cap !== undefined && cap < Number(s.max) ? ` This project allows at most ${cap}.` : '';
  const hint = `${s.why} Default ${settingText(s, s.default)}.${capped}`;
  const id = `${idBase}-${s.key}`;
  return (
    <div className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-start gap-3">
      <Field label={s.title} hint={hint} error={error} group={s.type === 'bool'}>
        {() =>
          s.type === 'int' ? (
            <Stepper id={id} value={Number(value) || 0} min={Number(s.min)} max={max} onChange={(n) => onChange(String(n))} disabled={disabled} />
          ) : s.type === 'bool' ? (
            <Segmented
              label={s.title}
              value={value === 'true' ? 'true' : 'false'}
              onChange={onChange}
              options={[
                { value: 'false', label: 'Off' },
                { value: 'true', label: 'On' },
              ]}
            />
          ) : s.type === 'enum' ? (
            <Select id={id} value={value} disabled={disabled} onChange={onChange} options={(s.choices ?? []).map((c) => ({ value: c, label: c }))} />
          ) : (
            <Input id={id} className="font-mono" spellCheck={false} value={value} disabled={disabled} onChange={(e) => onChange(e.target.value)} />
          )
        }
      </Field>
      <div className="flex flex-col items-end gap-1.5 pt-6">
        <ImpactPill impact={s.impact} />
        <span className="max-w-48 truncate font-mono text-xs text-muted" title={s.key}>
          {s.key}
        </span>
        {disabled ? null : <Button size="sm" variant="ghost" icon={<Trash2 />} aria-label={`Remove ${s.title}`} onClick={onRemove} />}
      </div>
    </div>
  );
}
