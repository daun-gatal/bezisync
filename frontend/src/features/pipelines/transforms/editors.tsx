import type { SinkCaps } from '@/lib/connections';
import { useState, type ReactNode } from 'react';
import { Plus, X } from 'lucide-react';
import clsx from 'clsx';
import { Button } from '@/components/ui/button';
import { Notice, Pill } from '@/components/ui/feedback';
import { Check, Input } from '@/components/ui/form';
import { Select } from '@/components/ui/select';
import type { FilterOp, FilterRule, RuleValue, TransformStep, ValueFormats } from '../spec';
import {
  columnKind,
  fieldsBefore,
  FLINK_OPERATIONS,
  isNumericKind,
  isTextType,
  KNOWN_CLASSES,
  METADATA,
  metadataField,
  nameError,
  needsValue,
  needsValues,
  operatorsFor,
  OPERATIONS,
  parseRuleValue,
  SCRIPTED_CLASSES,
  textError,
  toggleFlinkOperation,
  TRUNCATE_SOURCES,
  type Caps,
  type TableInfo,
} from './model';
import type { FullSpec } from '../spec';

export interface EditorContext {
  sourceType: string;
  /** What the destination needs (required metadata, allowed delete handling). */
  sink: SinkCaps;
  tables: TableInfo[];
  caps: Caps;
  spec: FullSpec;
  steps: TransformStep[];
  index: number;
  expert: boolean;
  /** The pipeline runs on Flink CDC (rows written into tables, Flink CDC's operations). */
  flink: boolean;
}

/** Labelled row: the label column keeps editors aligned. */
export function Row({ label, children, htmlFor }: { label: string; children: ReactNode; htmlFor?: string }) {
  return (
    <div className="grid min-w-0 grid-cols-[minmax(0,1fr)] items-start gap-2 sm:grid-cols-[112px_minmax(0,1fr)]">
      <label htmlFor={htmlFor} className="pt-2 text-sm text-muted">
        {label}
      </label>
      <div className="flex min-w-0 flex-wrap items-center gap-2">{children}</div>
    </div>
  );
}

/** Two to four mutually exclusive choices. */
export function Segmented<T extends string>({
  label,
  value,
  options,
  onChange,
}: {
  label: string;
  value: T;
  options: { value: T; label: string }[];
  onChange: (v: T) => void;
}) {
  return (
    <div role="radiogroup" aria-label={label} className="inline-flex max-w-full flex-wrap gap-0.5 rounded-md border border-line-strong bg-subtle p-0.5">
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          role="radio"
          aria-checked={value === o.value}
          onClick={() => onChange(o.value)}
          className={clsx('h-7 rounded-[5px] px-2.5 text-sm font-medium', value === o.value ? 'bg-surface text-ink shadow-sm' : 'text-muted hover:text-ink')}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

/** Which captured tables a step applies to: all, or a chosen few. */
function TablesScope({ step, ctx, onChange }: { step: TransformStep; ctx: EditorContext; onChange: (s: TransformStep) => void }) {
  const all = ctx.spec.source.tables.include;
  if (all.length < 2) return null;
  const chosen = step.tables;
  return (
    <Row label="Applies to">
      <Segmented
        label="Applies to"
        value={chosen ? 'some' : 'all'}
        options={[
          { value: 'all', label: 'All tables' },
          { value: 'some', label: 'Some tables' },
        ]}
        onChange={(v) => onChange({ ...step, tables: v === 'all' ? undefined : [all[0]!] })}
      />
      {chosen ? (
        <div className="grid w-full grid-cols-[repeat(auto-fill,minmax(min(100%,200px),1fr))] gap-x-4 gap-y-1.5">
          {all.map((t) => (
            <Check
              key={t}
              label={<span className="font-mono text-sm">{t}</span>}
              checked={chosen.includes(t)}
              disabled={chosen.length === 1 && chosen.includes(t)}
              why="A step applies to at least one table."
              onChange={(on) => onChange({ ...step, tables: on ? [...chosen, t] : chosen.filter((x) => x !== t) })}
            />
          ))}
        </div>
      ) : null}
    </Row>
  );
}

/** The columns a step can refer to: those the scoped tables share at this point. */
function fieldOptions(step: TransformStep, ctx: EditorContext): string[] {
  const scoped = ctx.tables.filter((t) => !step.tables || step.tables.includes(t.name));
  const lists = scoped.map((t) => fieldsBefore(ctx.steps, ctx.index, t, ctx.spec));
  const union = [...new Set(lists.flat())];
  return union;
}

function kindOf(field: string, step: TransformStep, ctx: EditorContext): string {
  for (const t of ctx.tables.filter((t) => !step.tables || step.tables.includes(t.name))) {
    const c = t.columns.find((c) => c.name === field);
    if (c) return columnKind(ctx.sourceType, c.type);
  }
  return 'text';
}

export function ShapeEditor({ step, ctx, onChange }: { step: TransformStep; ctx: EditorContext; onChange: (s: TransformStep) => void }) {
  const fields = step.add_fields ?? [];
  const required = ctx.sink.unwrap?.add_fields ?? [];
  const allowedDeletes = ctx.sink.unwrap?.delete_handling.length ? ctx.sink.unwrap.delete_handling : ['rewrite', 'tombstone', 'drop'];
  const meta = METADATA.filter((m) => !m.postgresOnly || ctx.sourceType === 'postgresql').filter(
    (m) => m.value !== 'source.schema' || ['postgresql', 'sqlserver', 'oracle', 'cockroachdb'].includes(ctx.sourceType),
  );
  return (
    <div className="flex flex-col gap-3">
      <Row label="Deletes" htmlFor={`deletes-${step.id}`}>
        <div className="w-full max-w-md">
          <Select
            id={`deletes-${step.id}`}
            disabled={Boolean(ctx.sink.unwrap?.delete_handling_from)}
            value={step.delete_handling ?? 'rewrite'}
            onChange={(v) => onChange({ ...step, delete_handling: v as TransformStep['delete_handling'] })}
            options={[
              { value: 'rewrite', label: 'Keep as an event with __deleted = true' },
              { value: 'tombstone', label: 'Tombstone', description: 'An empty value; for compacted topics' },
              { value: 'drop', label: 'Drop delete events' },
            ].filter((o) => allowedDeletes.includes(o.value))}
          />
          {ctx.sink.unwrap?.delete_handling_from ? (
            <span className="mt-1 block text-xs text-muted">Set by the destination’s Deletes setting (Destination step).</span>
          ) : null}
        </div>
      </Row>
      <Row label="Add metadata">
        <div className="grid w-full grid-cols-[repeat(auto-fill,minmax(min(100%,220px),1fr))] gap-x-4 gap-y-1.5">
          {meta.map((m) => (
            <Check
              key={m.value}
              checked={fields.includes(m.value) || required.includes(m.value)}
              disabled={required.includes(m.value)}
              why={required.includes(m.value) ? 'The destination needs it.' : undefined}
              onChange={(on) => onChange({ ...step, add_fields: on ? [...fields, m.value] : fields.filter((f) => f !== m.value) })}
              label={
                <>
                  {m.label} <span className="font-mono text-xs text-muted">{metadataField(m.value)}</span>
                </>
              }
            />
          ))}
        </div>
      </Row>
    </div>
  );
}

const FORMAT_ROWS: { kind: keyof ValueFormats; columnKinds: string[]; label: string; options: Record<string, { label: string; example: string }> }[] = [
  {
    kind: 'decimals',
    columnKinds: ['decimal'],
    label: 'Decimals',
    options: {
      precise: { label: 'Exact (base64 bytes)', example: '"Mr4="' },
      double: { label: 'Number', example: '129.9' },
      string: { label: 'Exact text', example: '"129.90"' },
    },
  },
  {
    kind: 'timestamps',
    columnKinds: ['timestamp', 'timestamptz', 'date', 'time'],
    label: 'Timestamps',
    options: { epoch: { label: 'Epoch numbers', example: '1793351642118000' }, isostring: { label: 'ISO-8601 text', example: '"2026-10-30T09:14:02.118Z"' } },
  },
  {
    kind: 'binary',
    columnKinds: ['binary'],
    label: 'Binary',
    options: {
      bytes: { label: 'Base64 (bytes)', example: '"q83v"' },
      base64: { label: 'Base64 text', example: '"q83v"' },
      hex: { label: 'Hex text', example: '"abcdef"' },
    },
  },
];

export function ValuesEditor({ values, ctx, onChange }: { values: ValueFormats; ctx: EditorContext; onChange: (v: ValueFormats) => void }) {
  const kinds = new Set(ctx.tables.flatMap((t) => t.columns.map((c) => columnKind(ctx.sourceType, c.type))));
  const rows = FORMAT_ROWS.filter((r) => (ctx.caps.value_formats[r.kind]?.length ?? 0) > 0);
  return (
    <div className="flex flex-col gap-3">
      <table className="w-full border-collapse text-sm">
        <thead>
          <tr className="text-left text-xs text-muted">
            <th className="py-1 pr-3 font-medium">Values</th>
            <th className="py-1 pr-3 font-medium">Send as</th>
            <th className="py-1 font-medium">Example</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => {
            const allowed = ctx.caps.value_formats[r.kind] ?? [];
            const value = (values[r.kind] as string | undefined) ?? allowed[0]!;
            const count = ctx.tables.reduce((n, t) => n + t.columns.filter((c) => r.columnKinds.includes(columnKind(ctx.sourceType, c.type))).length, 0);
            return (
              <tr key={r.kind} className="border-t border-line">
                <td className="py-2 pr-3">
                  {r.label}{' '}
                  <span className="text-xs text-muted">
                    ({count} column{count === 1 ? '' : 's'}
                    {!r.columnKinds.some((k) => kinds.has(k)) ? ', none captured' : ''})
                  </span>
                </td>
                <td className="py-2 pr-3">
                  <Select
                    label={`${r.label} as`}
                    value={value}
                    onChange={(v) => onChange({ ...values, [r.kind]: v })}
                    options={allowed.map((m) => ({ value: m, label: `${r.options[m]?.label ?? m}${m === allowed[0] ? ' (default)' : ''}` }))}
                  />
                </td>
                <td className="py-2 font-mono text-xs wrap-anywhere">{r.options[value]?.example}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
      {values.decimals === 'double' ? (
        <Notice tone="info">
          Numbers can round very large or very precise decimals. Use <strong>Exact text</strong> for money if consumers can parse text.
        </Notice>
      ) : null}
      {(ctx.caps.value_formats.timestamps ?? []).length < 2 ? (
        <p className="m-0 text-xs text-muted">This source cannot send timestamps as ISO-8601 text.</p>
      ) : null}
    </div>
  );
}

/** Values typed one at a time (Enter or Add), shown as removable tokens. */
function TokenInput({ values, kind, label, onChange }: { values: RuleValue[]; kind: string; label: string; onChange: (v: RuleValue[]) => void }) {
  const [text, setText] = useState('');
  const parsed = parseRuleValue(kind, text);
  const add = () => {
    if (parsed === null || values.includes(parsed)) return;
    onChange([...values, parsed]);
    setText('');
  };
  return (
    <span className="flex min-w-0 flex-wrap items-center gap-1.5">
      {values.map((v) => (
        <span
          key={String(v)}
          className="inline-flex max-w-full min-w-0 items-center gap-1 rounded-md bg-accent-soft px-2 py-0.5 font-mono text-xs wrap-anywhere text-accent-ink"
        >
          {String(v)}
          <button type="button" aria-label={`Remove ${String(v)}`} onClick={() => onChange(values.filter((x) => x !== v))}>
            <X className="size-3" aria-hidden />
          </button>
        </span>
      ))}
      <Input
        aria-label={label}
        className="w-36 max-w-full font-mono"
        inputMode={isNumericKind(kind) ? 'decimal' : undefined}
        value={text}
        placeholder={isNumericKind(kind) ? 'number' : 'value'}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter') {
            e.preventDefault();
            add();
          }
        }}
      />
      <Button size="sm" variant="ghost" disabled={parsed === null} onClick={add}>
        Add
      </Button>
    </span>
  );
}

function RuleRow({
  rule,
  step,
  ctx,
  n,
  onChange,
  onRemove,
}: {
  rule: FilterRule;
  step: TransformStep;
  ctx: EditorContext;
  n: number;
  onChange: (r: FilterRule) => void;
  onRemove: () => void;
}) {
  const fields = fieldOptions(step, ctx);
  const kind = rule.field ? kindOf(rule.field, step, ctx) : 'text';
  const ops = operatorsFor(kind);
  const numeric = isNumericKind(kind);
  const [text, setText] = useState(rule.value === undefined ? '' : String(rule.value));
  return (
    <div className="flex min-w-0 flex-wrap items-center gap-2">
      <div className="min-w-0 flex-[1_1_160px] sm:max-w-56">
        <Select
          label={`Rule ${n} column`}
          value={rule.field}
          placeholder="Column…"
          className="font-mono"
          onChange={(v) => {
            const k = kindOf(v, step, ctx);
            const allowed = operatorsFor(k).map((o) => o.value);
            onChange({ field: v, op: allowed.includes(rule.op) ? rule.op : allowed[0]! });
          }}
          options={fields.map((f) => ({ value: f, label: f }))}
        />
      </div>
      <div className="min-w-0 flex-[0_1_160px]">
        <Select
          label={`Rule ${n} operator`}
          value={rule.op}
          onChange={(v) => {
            const op = v as FilterOp;
            onChange({ field: rule.field, op, ...(needsValues(op) ? { values: rule.values ?? [] } : needsValue(op) ? { value: rule.value } : {}) });
          }}
          options={ops.map((o) => ({ value: o.value, label: o.label }))}
        />
      </div>
      {needsValue(rule.op) ? (
        <Input
          aria-label={`Rule ${n} value`}
          className="w-36 max-w-full font-mono"
          inputMode={numeric ? 'decimal' : undefined}
          value={text}
          onChange={(e) => {
            setText(e.target.value);
            const v = parseRuleValue(kind, e.target.value);
            onChange({ field: rule.field, op: rule.op, ...(v === null ? {} : { value: v }) });
          }}
        />
      ) : null}
      {needsValues(rule.op) ? (
        <TokenInput
          label={`Rule ${n} values`}
          kind={kind}
          values={rule.values ?? []}
          onChange={(values) => onChange({ field: rule.field, op: rule.op, values })}
        />
      ) : null}
      {needsValue(rule.op) && text && parseRuleValue(kind, text) === null ? <span className="text-xs text-bad">Enter a number.</span> : null}
      <Button size="sm" variant="ghost" aria-label={`Remove rule ${n}`} onClick={onRemove}>
        <X className="size-3.5" aria-hidden />
      </Button>
    </div>
  );
}

export function FilterEditor({ step, ctx, onChange }: { step: TransformStep; ctx: EditorContext; onChange: (s: TransformStep) => void }) {
  if (step.condition) {
    return (
      <Notice tone="warn" title="Filter script">
        <code className="block whitespace-pre-wrap break-all font-mono text-xs">{step.condition}</code>
        <span className="mt-1 block text-xs">
          Scripts run inside the connector and need the project policy “Allow scripted transforms”. Edit them through YAML or the API, or remove this step and
          build the filter from rules.
        </span>
      </Notice>
    );
  }
  const ops = step.operations ?? [];
  const rules = step.rules ?? [];
  return (
    <div className="flex flex-col gap-3">
      <TablesScope step={step} ctx={ctx} onChange={onChange} />
      <Row label="Operations">
        {ctx.flink
          ? FLINK_OPERATIONS.map((o) => (
              <Check
                key={o.value}
                label={o.label}
                checked={o.ops.every((x) => ops.includes(x))}
                onChange={(on) => onChange({ ...step, operations: toggleFlinkOperation(ops, o.value, on) })}
              />
            ))
          : OPERATIONS.map((o) => (
              <Check
                key={o.value}
                label={o.label}
                checked={ops.includes(o.value)}
                onChange={(on) =>
                  onChange({
                    ...step,
                    operations: on ? OPERATIONS.map((x) => x.value).filter((x) => x === o.value || ops.includes(x)) : ops.filter((x) => x !== o.value),
                  })
                }
              />
            ))}
      </Row>
      {ctx.caps.row_filters ? (
        <Row label="Where">
          <div className="flex w-full flex-col gap-2">
            {rules.length > 1 ? (
              <span className="flex flex-wrap items-center gap-2 text-sm">
                <span className="w-28">
                  <Select
                    label="Match"
                    value={step.match ?? 'all'}
                    onChange={(v) => onChange({ ...step, match: v as 'all' | 'any' })}
                    options={[
                      { value: 'all', label: 'all' },
                      { value: 'any', label: 'any' },
                    ]}
                  />
                </span>
                of these are true:
              </span>
            ) : null}
            {rules.map((r, i) => (
              <RuleRow
                key={i}
                n={i + 1}
                rule={r}
                step={step}
                ctx={ctx}
                onChange={(next) => onChange({ ...step, rules: rules.map((x, j) => (j === i ? next : x)) })}
                onRemove={() => onChange({ ...step, rules: rules.filter((_, j) => j !== i), ...(rules.length <= 2 ? { match: 'all' } : {}) })}
              />
            ))}
            <div>
              <Button size="sm" variant="ghost" onClick={() => onChange({ ...step, rules: [...rules, { field: '', op: 'eq' }] })}>
                <Plus className="size-3.5" aria-hidden /> Condition
              </Button>
            </div>
          </div>
        </Row>
      ) : (
        <p className="m-0 text-xs text-muted">This source sends documents as JSON text, so filters can only check the operation.</p>
      )}
      <Notice tone="info">
        {ctx.flink
          ? 'Deletes are judged on the row before the delete. Rows a filter drops never reach the table: changing the filter later does not bring them back (Start over for that).'
          : 'Deletes are judged on the row before the delete. Dropped events are gone for this pipeline: changing the filter later does not bring them back (re-snapshot the table for that).'}
      </Notice>
      {ctx.expert ? (
        <p className="m-0 text-xs text-muted">
          {ctx.flink
            ? 'Flink CDC: a transform filter BeziSync generates (shown in the configuration on Review).'
            : 'Debezium: io.debezium.transforms.Filter with a condition BeziSync generates (shown in the configuration on Review).'}
        </p>
      ) : null}
    </div>
  );
}

export function RenameEditor({ step, ctx, onChange }: { step: TransformStep; ctx: EditorContext; onChange: (s: TransformStep) => void }) {
  const fields = fieldOptions(step, ctx);
  const renames = step.renames ?? [];
  return (
    <div className="flex flex-col gap-3">
      <TablesScope step={step} ctx={ctx} onChange={onChange} />
      <Row label="Renames">
        <div className="flex w-full flex-col gap-2">
          {renames.map((r, i) => {
            const taken = fields.filter((f) => f !== r.from && !renames.some((x) => x.from === f));
            const err = r.to ? nameError(r.to, taken) : null;
            return (
              <div key={i} className="flex flex-col gap-1">
                <div className="flex min-w-0 flex-wrap items-center gap-2">
                  <div className="min-w-0 flex-[1_1_160px] sm:max-w-56">
                    <Select
                      label={`Rename ${i + 1} from`}
                      value={r.from}
                      placeholder="Field…"
                      className="font-mono"
                      onChange={(v) => onChange({ ...step, renames: renames.map((x, j) => (j === i ? { ...x, from: v } : x)) })}
                      options={fields.map((f) => ({
                        value: f,
                        label: f,
                        disabled: f !== r.from && renames.some((x) => x.from === f),
                        why: 'Already renamed above.',
                      }))}
                    />
                  </div>
                  <span className="text-muted" aria-hidden>
                    →
                  </span>
                  <Input
                    aria-label={`Rename ${i + 1} to`}
                    aria-invalid={Boolean(err)}
                    className={clsx('min-w-0 flex-[1_1_140px] font-mono sm:max-w-48', err && 'border-bad')}
                    value={r.to}
                    placeholder="new_name"
                    onChange={(e) => onChange({ ...step, renames: renames.map((x, j) => (j === i ? { ...x, to: e.target.value } : x)) })}
                  />
                  {renames.length > 1 ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      aria-label={`Remove rename ${i + 1}`}
                      onClick={() => onChange({ ...step, renames: renames.filter((_, j) => j !== i) })}
                    >
                      <X className="size-3.5" aria-hidden />
                    </Button>
                  ) : null}
                </div>
                {err ? <span className="text-xs text-bad">{err}</span> : null}
              </div>
            );
          })}
          <div>
            <Button size="sm" variant="ghost" onClick={() => onChange({ ...step, renames: [...renames, { from: '', to: '' }] })}>
              <Plus className="size-3.5" aria-hidden /> Rename
            </Button>
          </div>
        </div>
      </Row>
      <p className="m-0 text-xs text-muted">The message key keeps the column’s name.</p>
    </div>
  );
}

export function AddFieldEditor({ step, ctx, onChange }: { step: TransformStep; ctx: EditorContext; onChange: (s: TransformStep) => void }) {
  const taken = fieldOptions(step, ctx);
  const nameErr = step.name ? nameError(step.name, taken) : null;
  const value = step.value ?? { kind: 'static', text: '' };
  const textErr = value.kind === 'static' && value.text ? textError(value.text) : null;
  return (
    <div className="flex flex-col gap-3">
      <TablesScope step={step} ctx={ctx} onChange={onChange} />
      <Row label="Field name" htmlFor={`name-${step.id}`}>
        <Input
          id={`name-${step.id}`}
          aria-invalid={Boolean(nameErr)}
          className={clsx('max-w-64 font-mono', nameErr && 'border-bad')}
          value={step.name ?? ''}
          placeholder="source_system"
          onChange={(e) => onChange({ ...step, name: e.target.value })}
        />
        {nameErr ? <span className="w-full text-xs text-bad">{nameErr}</span> : null}
      </Row>
      <Row label="Value">
        <Segmented
          label="Value"
          value={value.kind}
          options={[
            { value: 'static', label: 'Fixed text' },
            { value: 'pipeline', label: 'Pipeline name' },
          ]}
          onChange={(kind) => onChange({ ...step, value: kind === 'static' ? { kind, text: '' } : { kind } })}
        />
        {value.kind === 'static' ? (
          <Input
            aria-label="Value text"
            aria-invalid={Boolean(textErr)}
            className={clsx('min-w-0 flex-[1_1_160px] sm:max-w-64', textErr && 'border-bad')}
            value={value.text ?? ''}
            onChange={(e) => onChange({ ...step, value: { kind: 'static', text: e.target.value } })}
          />
        ) : null}
        {textErr ? <span className="w-full text-xs text-bad">{textErr}</span> : null}
      </Row>
      <p className="m-0 text-xs text-muted">Same value on every event. Do not put secrets here: it is stored in the pipeline’s configuration.</p>
    </div>
  );
}

export function FlattenEditor({ step, ctx, onChange }: { step: TransformStep; ctx: EditorContext; onChange: (s: TransformStep) => void }) {
  const d = step.delimiter ?? '_';
  return (
    <div className="flex flex-col gap-3">
      <TablesScope step={step} ctx={ctx} onChange={onChange} />
      <Row label="Separator">
        <Segmented
          label="Separator"
          value={d}
          options={[
            { value: '_', label: '_ underscore' },
            { value: '.', label: '. dot' },
          ]}
          onChange={(delimiter) => onChange({ ...step, delimiter })}
        />
        <code className="font-mono text-xs text-muted">
          address: {'{'} city {'}'} → address{d}city
        </code>
      </Row>
    </div>
  );
}

export function CustomEditor({ step, ctx, onChange }: { step: TransformStep; ctx: EditorContext; onChange: (s: TransformStep) => void }) {
  const known = (KNOWN_CLASSES as readonly string[]).includes(step.class ?? '');
  const [other, setOther] = useState(Boolean(step.class) && !known);
  const props = Object.entries(step.properties ?? {});
  const setProps = (entries: [string, string][]) => onChange({ ...step, properties: Object.fromEntries(entries) });
  return (
    <div className="flex flex-col gap-3">
      <Row label="Class" htmlFor={`class-${step.id}`}>
        <div className="w-full max-w-md">
          <Select
            id={`class-${step.id}`}
            className="font-mono"
            value={other ? '__other' : (step.class ?? '')}
            onChange={(v) => {
              if (v === '__other') {
                setOther(true);
                onChange({ ...step, class: '' });
              } else {
                setOther(false);
                onChange({ ...step, class: v });
              }
            }}
            options={[
              ...KNOWN_CLASSES.map((c) => ({ value: c, label: <span className="font-mono">{c}</span>, text: c })),
              { value: '__other', label: 'Another class in the image…' },
            ]}
          />
        </div>
        {other ? (
          <Input
            aria-label="Class name"
            className="max-w-md font-mono"
            value={step.class ?? ''}
            placeholder="com.acme.smt.MyTransform"
            onChange={(e) => onChange({ ...step, class: e.target.value.trim() })}
          />
        ) : null}
        {known ? <Pill tone="ok">ships with Debezium 3.7</Pill> : step.class ? <Pill tone="warn">your image must add it</Pill> : null}
        {SCRIPTED_CLASSES.includes(step.class ?? '') ? (
          <span className="w-full text-xs text-warn">This class runs a script: it also needs the policy “Allow scripted transforms”.</span>
        ) : null}
      </Row>
      <TablesScope step={step} ctx={ctx} onChange={onChange} />
      <Row label="Properties">
        <div className="flex w-full flex-col gap-2">
          {props.map(([k, v], i) => (
            <div key={i} className="flex min-w-0 flex-wrap items-center gap-2">
              <Input
                aria-label={`Property ${i + 1} name`}
                className="min-w-0 flex-[1_1_140px] font-mono sm:max-w-48"
                value={k}
                placeholder="field"
                onChange={(e) => setProps(props.map((p, j) => (j === i ? [e.target.value, p[1]] : p)))}
              />
              <span className="text-muted" aria-hidden>
                =
              </span>
              <Input
                aria-label={`Property ${i + 1} value`}
                className="min-w-0 flex-[1_1_160px] font-mono sm:max-w-56"
                value={v}
                onChange={(e) => setProps(props.map((p, j) => (j === i ? [p[0], e.target.value] : p)))}
              />
              <Button size="sm" variant="ghost" aria-label={`Remove property ${i + 1}`} onClick={() => setProps(props.filter((_, j) => j !== i))}>
                <X className="size-3.5" aria-hidden />
              </Button>
            </div>
          ))}
          <div>
            <Button size="sm" variant="ghost" onClick={() => setProps([...props, ['', '']])}>
              <Plus className="size-3.5" aria-hidden /> Property
            </Button>
          </div>
        </div>
      </Row>
      <p className="m-0 text-xs text-muted">Compiles to debezium.transforms.t_{step.id}.* · BeziSync cannot preview custom classes.</p>
    </div>
  );
}

export function KeysEditor({ keys, ctx, onChange }: { keys: Record<string, string[]>; ctx: EditorContext; onChange: (k: Record<string, string[]>) => void }) {
  // Rows being edited stay open while columns are ticked (tables without a key start open).
  const [editing, setEditing] = useState<Set<string>>(new Set([...Object.keys(keys), ...ctx.tables.filter((t) => !t.primaryKey.length).map((t) => t.name)]));
  const tables = [...ctx.tables].sort((a, b) => Number(a.primaryKey.length > 0) - Number(b.primaryKey.length > 0) || a.name.localeCompare(b.name));
  return (
    <table className="w-full border-collapse text-sm">
      <thead>
        <tr className="text-left text-xs text-muted">
          <th className="py-1 pr-3 font-medium">Table</th>
          <th className="py-1 font-medium">Key</th>
        </tr>
      </thead>
      <tbody>
        {tables.map((t) => {
          const cols = keys[t.name];
          const open = editing.has(t.name);
          return (
            <tr key={t.name} className="border-t border-line align-top">
              <td className="py-2 pr-3">
                <span className="flex flex-col items-start gap-1">
                  <span className="font-mono text-sm wrap-anywhere">{t.name}</span>
                  {!t.primaryKey.length ? <Pill tone="warn">no primary key</Pill> : null}
                </span>
              </td>
              <td className="py-2">
                {open ? (
                  <div className="flex flex-wrap gap-x-4 gap-y-1">
                    {t.columns.map((c) => (
                      <Check
                        key={c.name}
                        label={<span className="font-mono text-sm">{c.name}</span>}
                        checked={cols?.includes(c.name) ?? false}
                        onChange={(on) => {
                          const next = on ? [...(cols ?? []), c.name] : (cols ?? []).filter((x) => x !== c.name);
                          const out = { ...keys };
                          if (next.length) out[t.name] = next;
                          else delete out[t.name];
                          onChange(out);
                        }}
                      />
                    ))}
                    {t.primaryKey.length ? (
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => {
                          const out = { ...keys };
                          delete out[t.name];
                          onChange(out);
                          setEditing(new Set([...editing].filter((x) => x !== t.name)));
                        }}
                      >
                        Use the primary key
                      </Button>
                    ) : null}
                  </div>
                ) : (
                  <span className="text-muted">
                    Primary key ({t.primaryKey.join(', ')}){' '}
                    <Button size="sm" variant="ghost" onClick={() => setEditing(new Set([...editing, t.name]))}>
                      Change
                    </Button>
                  </span>
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

/** Truncate: text columns of the captured tables, each cut to at most a length. Masked or left-out
 *  columns aren't offered (a mask replaces the value whole). */
export function TruncateEditor({ step, ctx, onChange }: { step: TransformStep; ctx: EditorContext; onChange: (s: TransformStep) => void }) {
  const cols = step.columns ?? [];
  const put = (next: typeof cols) => onChange({ ...step, columns: next });
  const choices = ctx.tables.flatMap((t) => {
    const rules = ctx.spec.source.columns[t.name];
    return t.columns
      .filter((c) => isTextType(c.type) && !rules?.mask.includes(c.name) && !rules?.exclude.includes(c.name))
      .map((c) => ({ value: `${t.name}.${c.name}`, label: `${t.name}.${c.name}`, description: c.type }));
  });
  const unavailable = !ctx.flink && !TRUNCATE_SOURCES.includes(ctx.sourceType);
  return (
    <div className="flex flex-col gap-3">
      {unavailable ? <Notice tone="warn">Debezium truncates columns of PostgreSQL and MySQL sources only.</Notice> : null}
      {cols.map((c, i) => (
        <div key={i} className="grid min-w-0 grid-cols-[minmax(0,1fr)_8rem_auto] items-end gap-2">
          <Select
            label={`Column ${i + 1}`}
            value={c.column ? `${c.table}.${c.column}` : ''}
            onChange={(v) => {
              const at = v.lastIndexOf('.');
              put(cols.map((x, j) => (j === i ? { ...x, table: v.slice(0, at), column: v.slice(at + 1) } : x)));
            }}
            options={choices}
          />
          <Input
            aria-label={`Column ${i + 1} length`}
            type="number"
            min={1}
            max={65535}
            value={c.length}
            onChange={(e) => put(cols.map((x, j) => (j === i ? { ...x, length: Math.max(1, Math.min(65535, Number(e.target.value) || 1)) } : x)))}
          />
          <Button size="sm" variant="ghost" icon={<X />} aria-label={`Remove column ${i + 1}`} onClick={() => put(cols.filter((_, j) => j !== i))} />
        </div>
      ))}
      <div>
        <Button size="sm" icon={<Plus />} disabled={!choices.length || unavailable} onClick={() => put([...cols, { table: '', column: '', length: 255 }])}>
          Add a column
        </Button>
      </div>
      {!choices.length ? <span className="text-xs text-muted">No text columns to cut in the captured tables.</span> : null}
    </div>
  );
}
