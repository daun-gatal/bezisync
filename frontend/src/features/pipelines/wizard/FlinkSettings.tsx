import { Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Field, Input, RadioCards } from '@/components/ui/form';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import type { Schemas } from '@/lib/api';
import type { FlinkChoices } from '@/lib/engines';

type Setting = Schemas['FlinkSetting'];

/** Destinations that load in batches (Freshness applies); Iceberg data appears at each save. */
export const BATCH_LOAD_SINKS = ['starrocks', 'doris'];

/**
 * A Flink CDC pipeline's run settings: what schema changes and new columns do, how often the
 * position is saved (and Iceberg commits), how fresh batch-loaded destinations are, and in Expert
 * mode the Flink settings from the reviewed catalogue.
 */
export function FlinkRunSettings({
  value,
  onChange,
  sinkType,
  expert,
  catalogue,
  deployed,
  events = false,
}: {
  value: FlinkChoices;
  onChange: (v: FlinkChoices) => void;
  sinkType: string | undefined;
  expert: boolean;
  catalogue: Setting[];
  deployed: boolean;
  /** Flink CDC writes this destination as change events (Kafka, ADR 0011). */
  events?: boolean;
}) {
  const set = <K extends keyof FlinkChoices>(k: K, v: FlinkChoices[K]) => onChange({ ...value, [k]: v });
  const iceberg = sinkType === 'iceberg';
  // StarRocks and Doris refuse source column defaults: BeziSync writes columns without them, which
  // needs every column known at deploy (the server refuses "send" for them too).
  const noDefaults = BATCH_LOAD_SINKS.includes(sinkType ?? '');
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <Field label="When a source table changes shape" group>
        {() => (
          <RadioCards
            label="When a source table changes shape"
            value={value.schema_changes}
            onChange={(v) => set('schema_changes', v)}
            options={[
              { value: 'apply', label: 'Apply', description: events ? 'New and renamed columns appear in the events; none are dropped from them.' : 'New and renamed columns reach the destination; nothing is dropped there.' },
              { value: 'ignore', label: 'Ignore', description: events ? 'Events keep their fields; changed columns stop updating.' : 'The destination keeps its columns; changed columns stop updating.' },
              { value: 'stop', label: 'Stop', description: 'The pipeline stops so someone can decide.' },
            ]}
          />
        )}
      </Field>
      <Field label="New columns" group>
        {() => (
          <RadioCards
            label="New columns"
            columns={2}
            value={value.new_columns}
            onChange={(v) => set('new_columns', v)}
            options={[
              { value: 'hold', label: 'Hold back', description: 'A new source column is sent from the next deploy, after someone reviews it.' },
              noDefaults
                ? {
                    value: 'send',
                    label: 'Send',
                    description: 'Not for this destination: it refuses the column defaults Flink CDC copies, so new columns wait for a deploy that writes them without.',
                    disabled: true,
                  }
                : { value: 'send', label: 'Send', description: 'New columns reach the destination as they appear.' },
            ]}
          />
        )}
      </Field>
      <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,280px),1fr))] gap-4">
        <Field
          label={iceberg ? 'Commit every' : 'Save the position every'}
          hint={iceberg ? 'New data becomes visible in Iceberg at each commit.' : events ? 'How much a restart may send again: consumers can see an event twice.' : 'How much a restart may replay (writes are idempotent by key).'}
        >
          {(id) => (
            <Select
              id={id}
              value={String(value.commit_seconds)}
              onChange={(v) => set('commit_seconds', Number(v) as FlinkChoices['commit_seconds'])}
              options={[
                { value: '30', label: '30 seconds' },
                { value: '60', label: '1 minute' },
                { value: '300', label: '5 minutes' },
              ]}
            />
          )}
        </Field>
        {sinkType && BATCH_LOAD_SINKS.includes(sinkType) ? (
          <Field label="Freshness" hint="Changes wait at most this long in a load batch.">
            {(id) => (
              <Select
                id={id}
                value={String(value.freshness_seconds)}
                onChange={(v) => set('freshness_seconds', Number(v) as FlinkChoices['freshness_seconds'])}
                options={[
                  { value: '2', label: '2 seconds' },
                  { value: '5', label: '5 seconds' },
                  { value: '30', label: '30 seconds' },
                ]}
              />
            )}
          </Field>
        ) : null}
      </div>
      {expert ? <FlinkConfEditor value={value.flink_conf} onChange={(c) => set('flink_conf', c)} catalogue={catalogue} deployed={deployed} /> : null}
    </div>
  );
}

/** Flink settings from the catalogue: pick a key, then its value (a list for choices). */
export function FlinkConfEditor({
  value,
  onChange,
  catalogue,
  deployed,
}: {
  value: Record<string, string>;
  onChange: (v: Record<string, string>) => void;
  catalogue: Setting[];
  deployed: boolean;
}) {
  const keys = Object.keys(value).sort();
  const unused = catalogue.filter((s) => !(s.key in value));
  const put = (k: string, v: string) => onChange({ ...value, [k]: v });
  const drop = (k: string) => {
    const next = { ...value };
    delete next[k];
    onChange(next);
  };
  return (
    <div className="flex min-w-0 flex-col gap-3">
      <Meta>
        Flink settings{deployed ? ' (applied at the next deploy)' : ''}: only settings that never change what a saved position holds are offered.
      </Meta>
      {keys.map((k) => {
        const s = catalogue.find((c) => c.key === k);
        return (
          <div key={k} className="grid min-w-0 grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] items-end gap-3">
            <Field label={s?.title ?? k} hint={s ? `${s.why} Default ${s.default}.` : 'Not a setting this version offers: remove it.'}>
              {(id) =>
                s?.type === 'enum' ? (
                  <Select id={id} value={value[k]} onChange={(v) => put(k, v)} options={(s.choices ?? []).map((c) => ({ value: c, label: c }))} />
                ) : (
                  <Input id={id} className="font-mono" spellCheck={false} value={value[k] ?? ''} onChange={(e) => put(k, e.target.value)} />
                )
              }
            </Field>
            <Meta className="pb-2 font-mono">{k}</Meta>
            <Button size="sm" variant="ghost" icon={<Trash2 />} aria-label={`Remove ${s?.title ?? k}`} onClick={() => drop(k)} />
          </div>
        );
      })}
      {unused.length ? (
        <Field label="Add a Flink setting" optional>
          {(id) => (
            <Select
              id={id}
              value={null}
              placeholder="Choose a setting…"
              onChange={(k) => put(k, catalogue.find((c) => c.key === k)?.default ?? '')}
              options={unused.map((s) => ({ value: s.key, label: s.title, description: s.key }))}
            />
          )}
        </Field>
      ) : null}
    </div>
  );
}
