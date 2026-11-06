import { Plus, Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Field, Input } from '@/components/ui/form';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import type { Schemas } from '@/lib/api';

export type PlacementProfile = Schemas['PlacementProfile'];
type Toleration = Schemas['Toleration'];

const NAME = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/;

/** Why a profile can't be saved as it is (the server says the rest); null when it can. */
export function profileError(p: PlacementProfile, all: PlacementProfile[]): string | null {
  if (!NAME.test(p.name)) return 'A name of lowercase letters, digits and dashes.';
  if (all.filter((x) => x.name === p.name).length > 1) return 'Another profile has this name.';
  const values = [...Object.entries(p.node_selector ?? {}), ...Object.entries(p.labels ?? {})].flat();
  if (values.some((v) => v.includes('${') || v.includes('{{'))) return 'Values are taken as they are: no ${…} or {{…}}.';
  return null;
}

/**
 * A Kubernetes runtime's placement profiles (Admins): each a name pipelines pick in Review, a node
 * selector, tolerations, a priority class and pod labels. Editing one moves its pipelines at their
 * next deploy; one in use can't be removed (the server lists who uses it).
 */
export function PlacementProfiles({ value, onChange, disabled }: { value: PlacementProfile[]; onChange: (v: PlacementProfile[]) => void; disabled?: boolean }) {
  const put = (i: number, p: PlacementProfile) => onChange(value.map((x, j) => (j === i ? p : x)));
  return (
    <section aria-label="Placement profiles" className="flex min-w-0 flex-col gap-3">
      <div className="flex flex-col gap-0.5">
        <strong className="text-sm font-semibold">Placement profiles</strong>
        <Meta>Where pipelines may run on this cluster. A pipeline picks one in Review; a change applies at its next deploy.</Meta>
      </div>
      {value.map((p, i) => (
        <div key={i} role="group" aria-label={`Profile ${p.name || i + 1}`} className="flex min-w-0 flex-col gap-3 rounded-md border border-line p-3">
          <div className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-end gap-3">
            <Field label="Profile name" error={profileError(p, value)}>
              {(id) => <Input id={id} className="font-mono" value={p.name} disabled={disabled} onChange={(e) => put(i, { ...p, name: e.target.value.trim() })} />}
            </Field>
            <Button variant="ghost" icon={<Trash2 />} aria-label={`Remove profile ${p.name || i + 1}`} disabled={disabled} onClick={() => onChange(value.filter((_, j) => j !== i))} />
          </div>
          <Pairs label="Node selector" idBase={`ps-${i}`} value={p.node_selector ?? {}} disabled={disabled} onChange={(node_selector) => put(i, { ...p, node_selector })} />
          <Tolerations idBase={`pt-${i}`} value={p.tolerations ?? []} disabled={disabled} onChange={(tolerations) => put(i, { ...p, tolerations })} />
          <Field label="Priority class" optional>
            {(id) => (
              <Input id={id} className="font-mono" value={p.priority_class ?? ''} disabled={disabled} onChange={(e) => put(i, { ...p, priority_class: e.target.value.trim() })} />
            )}
          </Field>
          <Pairs label="Pod labels" idBase={`pl-${i}`} value={p.labels ?? {}} disabled={disabled} onChange={(labels) => put(i, { ...p, labels })} />
        </div>
      ))}
      {disabled ? null : (
        <div>
          <Button size="sm" icon={<Plus />} onClick={() => onChange([...value, { name: `profile-${value.length + 1}`, node_selector: {}, tolerations: [], priority_class: '', labels: {} }])}>
            Add a profile
          </Button>
        </div>
      )}
    </section>
  );
}

/** Label-like key/value pairs, one row each. */
function Pairs({ label, idBase, value, onChange, disabled }: { label: string; idBase: string; value: Record<string, string>; onChange: (v: Record<string, string>) => void; disabled?: boolean }) {
  const rows = Object.entries(value);
  const set = (next: [string, string][]) => onChange(Object.fromEntries(next));
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <span className="text-sm font-medium text-ink-2">{label}</span>
      {rows.map(([k, v], i) => (
        <div key={i} className="grid min-w-0 grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] gap-2">
          <Input aria-label={`${label} ${i + 1} key`} id={`${idBase}-k${i}`} className="font-mono" value={k} disabled={disabled}
            onChange={(e) => set(rows.map((r, j) => (j === i ? [e.target.value.trim(), r[1]] : r)))} />
          <Input aria-label={`${label} ${i + 1} value`} id={`${idBase}-v${i}`} className="font-mono" value={v} disabled={disabled}
            onChange={(e) => set(rows.map((r, j) => (j === i ? [r[0], e.target.value.trim()] : r)))} />
          <Button variant="ghost" icon={<Trash2 />} aria-label={`Remove ${label.toLowerCase()} ${i + 1}`} disabled={disabled} onClick={() => set(rows.filter((_, j) => j !== i))} />
        </div>
      ))}
      {disabled ? null : (
        <div>
          <Button size="sm" variant="ghost" icon={<Plus />} onClick={() => set([...rows, [`key-${rows.length + 1}`, '']])}>
            Add to {label.toLowerCase()}
          </Button>
        </div>
      )}
    </div>
  );
}

function Tolerations({ idBase, value, onChange, disabled }: { idBase: string; value: Toleration[]; onChange: (v: Toleration[]) => void; disabled?: boolean }) {
  const put = (i: number, t: Toleration) => onChange(value.map((x, j) => (j === i ? t : x)));
  return (
    <div className="flex min-w-0 flex-col gap-1.5">
      <span className="text-sm font-medium text-ink-2">Tolerations</span>
      {value.map((t, i) => (
        <div key={i} className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,140px),1fr))] items-end gap-2">
          <Input aria-label={`Toleration ${i + 1} key`} id={`${idBase}-k${i}`} className="font-mono" placeholder="any key" value={t.key ?? ''} disabled={disabled}
            onChange={(e) => put(i, { ...t, key: e.target.value.trim() })} />
          <Select label={`Toleration ${i + 1} operator`} value={t.operator} disabled={disabled}
            onChange={(operator) => put(i, { ...t, operator, ...(operator === 'Exists' ? { value: '' } : {}) })}
            options={[{ value: 'Equal', label: 'equals' }, { value: 'Exists', label: 'exists' }]} />
          {t.operator === 'Equal' ? (
            <Input aria-label={`Toleration ${i + 1} value`} id={`${idBase}-v${i}`} className="font-mono" value={t.value ?? ''} disabled={disabled}
              onChange={(e) => put(i, { ...t, value: e.target.value.trim() })} />
          ) : null}
          <Select label={`Toleration ${i + 1} effect`} value={t.effect ?? ''} disabled={disabled} onChange={(effect) => put(i, { ...t, effect })}
            options={[{ value: '', label: 'Any effect' }, { value: 'NoSchedule', label: 'NoSchedule' }, { value: 'PreferNoSchedule', label: 'PreferNoSchedule' }, { value: 'NoExecute', label: 'NoExecute' }]} />
          <Button variant="ghost" icon={<Trash2 />} aria-label={`Remove toleration ${i + 1}`} disabled={disabled} onClick={() => onChange(value.filter((_, j) => j !== i))} />
        </div>
      ))}
      {disabled ? null : (
        <div>
          <Button size="sm" variant="ghost" icon={<Plus />} onClick={() => onChange([...value, { key: '', operator: 'Exists', value: '', effect: 'NoSchedule' }])}>
            Add a toleration
          </Button>
        </div>
      )}
    </div>
  );
}
