import type { SinkCaps } from '@/lib/connections';
import { useState, type ReactNode } from 'react';
import { Plus, X } from 'lucide-react';
import clsx from 'clsx';
import { Button } from '@/components/ui/button';
import { Notice, Pill } from '@/components/ui/feedback';
import { Check, Input } from '@/components/ui/form';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import type { ComputedArg, ComputedFn, FilterOp, FilterRule, RuleValue, TransformStep, ValueFormats } from '../spec';
import {
  columnKind,
  fieldsBefore,
  argSlot,
  COMPUTED_FUNCTIONS,
  COMPUTED_GROUPS,
  DATE_PATTERNS,
  PARSE_DATE_PATTERNS,
  defaultArgs,
  kindFits,
  globMatch,
  routeTargets,
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
  flinkKeyProblem,
  keyChoices,
  KIND_NAMES,
  type Caps,
  type KeyChoice,
  type TableInfo,
} from './model';
import type { FullSpec } from '../spec';
import { effectiveDeletes, type ShapeCtx } from './model';
import { ExpressionInput } from './ExpressionInput';

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
  /** What the destination says about the event shape, for what deletes really become. */
  shape?: ShapeCtx;
  /** The pipeline, for checking expressions (ADR 0008). */
  projectId: string;
  pipelineId: string;
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
  // What a delete really becomes: Keep deleted rows or the destination may decide it.
  const deletes = effectiveDeletes(ctx.steps, ctx.spec.sink.options, ctx.shape ?? { unwrap: ctx.sink.unwrap ?? null });
  const meta = METADATA.filter((m) => !m.postgresOnly || ctx.sourceType === 'postgresql').filter(
    (m) => m.value !== 'source.schema' || ['postgresql', 'sqlserver', 'oracle', 'cockroachdb'].includes(ctx.sourceType),
  );
  return (
    <div className="flex flex-col gap-3">
      <Row label="Deletes" htmlFor={`deletes-${step.id}`}>
        <div className="w-full max-w-md">
          <Select
            id={`deletes-${step.id}`}
            disabled={Boolean(ctx.sink.unwrap?.delete_handling_from) || deletes.by === 'keep_deleted'}
            value={deletes.by === 'shape' ? (step.delete_handling ?? 'rewrite') : deletes.mode}
            onChange={(v) => onChange({ ...step, delete_handling: v as TransformStep['delete_handling'] })}
            options={[
              { value: 'rewrite', label: 'Keep as an event with __deleted = true' },
              { value: 'tombstone', label: 'Tombstone', description: 'An empty value; for compacted topics' },
              { value: 'drop', label: 'Drop delete events' },
            ].filter((o) => allowedDeletes.includes(o.value) || o.value === deletes.mode)}
          />
          {deletes.by === 'keep_deleted' ? (
            <span className="mt-1 block text-xs text-muted">Set by Keep deleted rows (step {deletes.step}). Remove that step to choose.</span>
          ) : ctx.sink.unwrap?.delete_handling_from ? (
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
  // On Flink CDC an Expert may write the condition as an expression (ADR 0008).
  const isExpression = step.expression !== undefined;
  const modeSwitch =
    ctx.flink && (ctx.expert || isExpression) ? (
      <Row label="Written as">
        <Segmented
          label="Filter written as"
          value={isExpression ? 'expression' : 'rules'}
          options={[
            { value: 'rules', label: 'Rules' },
            { value: 'expression', label: 'An expression' },
          ]}
          onChange={(v) => {
            const { expression: _e, operations: _o, rules: _r, match: _m, ...rest } = step;
            void _e;
            void _o;
            void _r;
            void _m;
            onChange(v === 'expression' ? { ...rest, expression: '' } : { ...rest, operations: ['insert', 'update', 'delete', 'snapshot'], rules: [] });
          }}
        />
      </Row>
    ) : null;
  if (isExpression) {
    const table = ctx.tables.find((t) => t.name === (step.tables?.[0] ?? ctx.spec.source.tables.include[0]));
    const rules = ctx.spec.source.columns[table?.name ?? ''];
    const readable = (table?.columns ?? []).filter((c) => !rules?.exclude?.includes(c.name) && !rules?.mask?.includes(c.name)).map((c) => ({ name: c.name, type: c.type }));
    return (
      <div className="flex flex-col gap-3">
        <TablesScope step={step} ctx={ctx} onChange={onChange} />
        {modeSwitch}
        <Row label="Keep rows where" htmlFor={`flt-expr-${ctx.index}`}>
          <ExpressionInput id={`flt-expr-${ctx.index}`} label="Keep rows where" value={step.expression ?? ''} use="filter" projectId={ctx.projectId} pipelineId={ctx.pipelineId}
            table={table?.name ?? ''} columns={readable} onChange={(v) => onChange({ ...step, expression: v })} />
        </Row>
        <Meta>The table keeps exactly the rows that match: a row an update makes match is added, one it stops matching is deleted.</Meta>
      </div>
    );
  }
  const ops = step.operations ?? [];
  const rules = step.rules ?? [];
  return (
    <div className="flex flex-col gap-3">
      <TablesScope step={step} ctx={ctx} onChange={onChange} />
      {modeSwitch}
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
                onRemove={() => {
                  const next = rules.filter((_, j) => j !== i);
                  // One rule needs no all/any; none takes no match at all (the server refuses match without rules).
                  const { match: _match, ...rest } = step;
                  onChange(next.length === 0 ? { ...rest, rules: next } : { ...step, rules: next, ...(next.length < 2 ? { match: 'all' } : {}) });
                }}
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

/**
 * Key columns per table (ADR 0010). On Flink CDC the destination's primary key, made of any column
 * the table writes: source columns under their written names, added fields and computed columns.
 * On Debezium the message key, made of source columns only (fields steps add are shown disabled,
 * with the reason). Each choice shows its type; a chosen column the table no longer writes stays
 * listed, ticked, so it can be removed, and the row says why it blocks.
 */
export function KeysEditor({
  keys,
  ctx,
  onChange,
  expressionWhy = {},
  expressionKind = {},
}: {
  keys: Record<string, string[]>;
  ctx: EditorContext;
  onChange: (k: Record<string, string[]>) => void;
  /** The server's answer for expression columns, table → column → why (undefined: checking). */
  expressionWhy?: Record<string, Record<string, string | null>>;
  /** The kind each expression column writes, from the same check. */
  expressionKind?: Record<string, Record<string, string>>;
}) {
  // Rows being edited stay open while columns are ticked (tables without a key start open).
  const [editing, setEditing] = useState<Set<string>>(new Set([...Object.keys(keys), ...ctx.tables.filter((t) => !t.primaryKey.length).map((t) => t.name)]));
  const tables = [...ctx.tables].sort((a, b) => Number(a.primaryKey.length > 0) - Number(b.primaryKey.length > 0) || a.name.localeCompare(b.name));
  const steps = ctx.steps;
  return (
    // One block per table, its name above its key: the choices carry types and need the width.
    <div className="flex min-w-0 flex-col text-sm">
        {tables.map((t) => {
          const cols = keys[t.name];
          const open = editing.has(t.name);
          const choices = keyChoices(t, ctx.spec, steps, ctx.flink, ctx.sourceType).map((c) => {
            if (c.expression === undefined) return c;
            if (!c.expression.trim()) return { ...c, why: 'Its expression is empty.' };
            c = { ...c, type: expressionKind[t.name]?.[c.value] ?? c.type };
            const why = expressionWhy[t.name]?.[c.value];
            return why === undefined || why === 'Checking its expression…' ? { ...c, checking: true } : why ? { ...c, why } : c;
          });
          const dangling = (cols ?? []).filter((c) => !choices.some((x) => x.value === c));
          const error = ctx.flink ? flinkKeyProblem(t, { ...ctx.spec, keys }, steps, expressionWhy[t.name]) : null;
          const computed = ctx.flink ? (cols ?? []).filter((c) => choices.some((x) => x.value === c && x.from === 'computed')) : [];
          // Flink CDC writes an empty key column as '' (seen on Iceberg): those rows share one key,
          // unless the key also holds the whole primary key (domain.FlinkKeyWarnings).
          const keepsPk = t.primaryKey.length > 0 && t.primaryKey.every((c) => cols?.includes(c));
          const nullable = ctx.flink && !keepsPk
            ? (cols ?? []).filter((c) => !t.primaryKey.includes(c) && t.columns.some((x) => x.name === c && x.nullable !== false) && choices.some((x) => x.value === c && x.from === 'column'))
            : [];
          const put = (next: string[]) => {
            const out = { ...keys };
            if (next.length) out[t.name] = next;
            else delete out[t.name];
            onChange(out);
          };
          const label = (c: KeyChoice) => (
            <span className="inline-flex flex-wrap items-baseline gap-x-1.5">
              <span className="font-mono text-sm">{c.label}</span>
              <span className="text-xs text-muted">
                {c.renamedFrom ? `from ${c.renamedFrom} · ` : c.from === 'added' ? 'added · ' : c.from === 'computed' ? 'computed · ' : ''}
                {c.from === 'column' ? c.type : (KIND_NAMES[c.type] ?? c.type)}
                {c.checking ? ' · checking…' : ''}
              </span>
            </span>
          );
          return (
            <div key={t.name} role="group" aria-label={`Key of ${t.name}`} className="flex min-w-0 flex-col gap-1.5 border-t border-line py-2 first:border-t-0">
              <span className="flex min-w-0 flex-wrap items-center gap-2">
                <span className="font-mono text-sm wrap-anywhere">{t.name}</span>
                {!t.primaryKey.length ? <Pill tone="warn">no primary key</Pill> : null}
              </span>
              <div className="min-w-0">
                {open ? (
                  <div className="flex flex-col gap-2">
                    <div className="flex flex-wrap gap-x-4 gap-y-1">
                      {choices.map((c) => {
                        const checked = cols?.includes(c.value) ?? false;
                        return (
                          <Check
                            key={`${c.from}:${c.value}`}
                            label={label(c)}
                            checked={checked}
                            // A chosen column that can't be a key stays ticked so it can be removed.
                            disabled={Boolean(c.why) && !checked}
                            why={c.why ?? null}
                            onChange={(on) => put(on ? [...(cols ?? []), c.value] : (cols ?? []).filter((x) => x !== c.value))}
                          />
                        );
                      })}
                      {dangling.map((c) => (
                        <Check
                          key={`gone:${c}`}
                          label={
                            <span className="inline-flex flex-wrap items-baseline gap-x-1.5">
                              <span className="font-mono text-sm">{c}</span>
                              <span className="text-xs text-bad">not written</span>
                            </span>
                          }
                          checked
                          onChange={() => put((cols ?? []).filter((x) => x !== c))}
                        />
                      ))}
                      {t.primaryKey.length ? (
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() => {
                            put([]);
                            setEditing(new Set([...editing].filter((x) => x !== t.name)));
                          }}
                        >
                          Use the primary key
                        </Button>
                      ) : null}
                    </div>
                    {error ? (
                      <span role="alert" className="text-xs text-bad wrap-anywhere">
                        {error}
                      </span>
                    ) : null}
                    {nullable.length ? (
                      <Meta>
                        {nullable.join(', ')} can be empty in the source: rows where {nullable.length === 1 ? 'it is' : 'they are'} empty would share one key and replace each other.
                      </Meta>
                    ) : null}
                    {computed.length ? (
                      <Meta>
                        {computed.join(', ')} {computed.length === 1 ? 'is' : 'are'} computed: a row whose key changes is written as a new row, and the old one
                        stays.
                      </Meta>
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
              </div>
            </div>
          );
        })}
    </div>
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

/** Routes: tables a pattern matches (schema.table, * any run) go to one destination of the name
 *  given; the first route a table matches wins and a table two routes match is refused (T10). Below,
 *  where each captured table lands, live. */
export function RouteEditor({ step, ctx, onChange }: { step: TransformStep; ctx: EditorContext; onChange: (s: TransformStep) => void }) {
  const routes = step.routes ?? [];
  const put = (next: typeof routes) => onChange({ ...step, routes: next });
  const captured = ctx.spec.source.tables.include;
  const steps = ctx.steps.map((t, j) => (j === ctx.index ? step : t));
  const targets = routeTargets(steps, captured);
  const what = ctx.flink ? 'table' : 'topic';
  return (
    <div className="flex flex-col gap-3">
      {routes.map((r, i) => {
        const n = captured.filter((t) => r.match && globMatch(r.match, t)).length;
        return (
          <div key={i} className="grid min-w-0 grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] items-end gap-2">
            <label className="flex min-w-0 flex-col gap-1 text-sm text-muted">
              Tables {i + 1}
              <Input aria-label={`Route ${i + 1} tables`} className="font-mono" placeholder="public.orders_*" value={r.match}
                onChange={(e) => put(routes.map((x, j) => (j === i ? { ...x, match: e.target.value.trim() } : x)))} />
            </label>
            <label className="flex min-w-0 flex-col gap-1 text-sm text-muted">
              Destination {what}
              <Input aria-label={`Route ${i + 1} destination`} className="font-mono" value={r.to}
                onChange={(e) => put(routes.map((x, j) => (j === i ? { ...x, to: e.target.value.trim() } : x)))} />
            </label>
            <Button size="sm" variant="ghost" icon={<X />} aria-label={`Remove route ${i + 1}`} disabled={routes.length === 1} onClick={() => put(routes.filter((_, j) => j !== i))} />
            <span className="col-span-3 text-xs text-muted">{r.match ? `Matches ${n} captured table${n === 1 ? '' : 's'}.` : 'A table name, or a pattern with *.'}</span>
          </div>
        );
      })}
      <div>
        <Button size="sm" icon={<Plus />} disabled={routes.length >= 20} onClick={() => put([...routes, { match: '', to: '' }])}>
          Add a route
        </Button>
      </div>
      <table aria-label="Where tables go" className="w-full text-left text-sm">
        <thead>
          <tr className="text-xs text-muted">
            <th className="py-1 font-medium">Table</th>
            <th className="py-1 font-medium">Goes to</th>
          </tr>
        </thead>
        <tbody>
          {captured.map((t) => (
            <tr key={t} className="border-t border-line">
              <td className="py-1 font-mono">{t}</td>
              <td className={clsx('py-1', targets.has(t) ? 'font-mono' : 'text-muted')}>{targets.get(t) || (targets.has(t) ? '…' : `its own ${what}`)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {[...new Set(targets.values())].some((to) => [...targets.values()].filter((x) => x === to).length > 1) ? (
        <Notice tone="info">Tables routed together must have the same key and columns; preflight checks it.</Notice>
      ) : null}
    </div>
  );
}

/** Computed column (Flink CDC): a new column of one table from a function of the fixed set, its
 *  arguments picked — columns of the kind the function takes (left-out ones aren't offered), typed
 *  numbers, a date pattern from a list, or a short text where the function joins texts. */
export function ComputedEditor({ step, ctx, onChange }: { step: TransformStep; ctx: EditorContext; onChange: (s: TransformStep) => void }) {
  const fn = step.function ?? 'upper';
  const f = COMPUTED_FUNCTIONS[fn];
  const args = step.args ?? [];
  const table = ctx.tables.find((t) => t.name === step.table);
  const excluded = ctx.spec.source.columns[step.table ?? '']?.exclude ?? [];
  const masked = ctx.spec.source.columns[step.table ?? '']?.mask ?? [];
  const put = (i: number, a: ComputedArg) => onChange({ ...step, args: args.map((x, j) => (j === i ? a : x)) });
  const columns = (i: number) =>
    (table?.columns ?? [])
      .filter((c) => !excluded.includes(c.name) && kindFits(argSlot(fn, i), columnKind(ctx.sourceType, c.type)))
      .map((c) => ({ value: c.name, label: c.name, description: c.type }));
  const taken = table ? fieldsBefore(ctx.steps, ctx.index, table, ctx.spec) : [];
  const nameErr = step.name ? nameError(step.name, taken) : null;
  const isExpression = step.expression !== undefined;
  // Writing an expression is for Experts (choose, don't type); a step that has one stays editable.
  const offerExpression = ctx.expert || isExpression;
  // Functions in their groups (Text, Numbers, …), as the picker lists them.
  const fnOptions = COMPUTED_GROUPS.flatMap((g) =>
    (Object.keys(COMPUTED_FUNCTIONS) as ComputedFn[])
      .filter((k) => COMPUTED_FUNCTIONS[k].group === g)
      .map((k) => ({ value: k, label: COMPUTED_FUNCTIONS[k].title, description: `${g} · ${COMPUTED_FUNCTIONS[k].help}` })),
  );
  const readable = (table?.columns ?? []).filter((c) => !excluded.includes(c.name) && !masked.includes(c.name)).map((c) => ({ name: c.name, type: c.type }));
  return (
    <div className="flex flex-col gap-3">
      <Row label="Table">
        <Select label="Table" value={step.table ?? ''} options={ctx.tables.map((t) => ({ value: t.name, label: t.name }))}
          onChange={(v) => onChange(isExpression ? { ...step, table: v } : { ...step, table: v, args: defaultArgs(fn) })} />
      </Row>
      <Row label="New column" htmlFor={`cc-name-${ctx.index}`}>
        <div className="flex flex-col gap-1">
          <Input id={`cc-name-${ctx.index}`} aria-label="New column" className="font-mono" value={step.name ?? ''} onChange={(e) => onChange({ ...step, name: e.target.value.trim() })} />
          {nameErr ? <span className="text-xs text-bad" role="alert">{nameErr}</span> : null}
        </div>
      </Row>
      {offerExpression ? (
        <Row label="Written as">
          <Segmented
            label="Written as"
            value={isExpression ? 'expression' : 'function'}
            options={[
              { value: 'function', label: 'A function' },
              { value: 'expression', label: 'An expression' },
            ]}
            onChange={(v) => {
              if (v === 'expression') {
                const { function: _f, args: _a, ...rest } = step;
                void _f;
                void _a;
                onChange({ ...rest, expression: '' });
              } else {
                const { expression: _e, ...rest } = step;
                void _e;
                onChange({ ...rest, function: 'upper', args: defaultArgs('upper') });
              }
            }}
          />
        </Row>
      ) : null}
      {isExpression ? (
        <Row label="Expression" htmlFor={`cc-expr-${ctx.index}`}>
          <ExpressionInput id={`cc-expr-${ctx.index}`} label="Expression" value={step.expression ?? ''} use="column" projectId={ctx.projectId} pipelineId={ctx.pipelineId}
            table={step.table ?? ''} columns={readable} onChange={(v) => onChange({ ...step, expression: v })} />
        </Row>
      ) : (
        <>
          <Row label="Function">
            <div className="flex flex-col gap-1">
              <Select label="Function" value={fn} options={fnOptions} onChange={(v) => onChange({ ...step, function: v as ComputedFn, args: defaultArgs(v as ComputedFn) })} />
            </div>
          </Row>
          {args.map((a, i) => {
            const slot = argSlot(fn, i);
            const label = `Argument ${i + 1}`;
            return (
              <Row key={i} label={label}>
                <div className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-end gap-2">
                  {slot === 'number' ? (
                    <Input aria-label={label} type="number" min={1} max={65535} value={a.number ?? 1}
                      onChange={(e) => put(i, { number: Math.max(1, Math.min(65535, Math.trunc(Number(e.target.value)) || 1)) })} />
                  ) : slot === 'digits' ? (
                    <Input aria-label={label} type="number" min={0} max={18} value={a.integer ?? 0}
                      onChange={(e) => put(i, { integer: Math.max(0, Math.min(18, Math.trunc(Number(e.target.value)) || 0)) })} />
                  ) : slot === 'pattern' || slot === 'date_pattern' ? (
                    <Select label={label} value={a.text ?? 'yyyy-MM-dd'} options={(slot === 'pattern' ? DATE_PATTERNS : PARSE_DATE_PATTERNS).map((p) => ({ value: p, label: p }))}
                      onChange={(v) => put(i, { text: v })} />
                  ) : slot === 'lit' ? (
                    <Input aria-label={label} value={a.text ?? ''} maxLength={200} onChange={(e) => put(i, { text: e.target.value })} />
                  ) : slot === 'any|lit' || slot === 'num|lit' ? (
                    <div className="grid min-w-0 grid-cols-[8rem_minmax(0,1fr)] gap-2">
                      <Select label={`${label} kind`} value={a.column !== undefined ? 'column' : 'value'}
                        options={[{ value: 'column', label: 'A column' }, { value: 'value', label: slot === 'num|lit' ? 'A number' : 'A text' }]}
                        onChange={(v) => put(i, v === 'column' ? { column: '' } : slot === 'num|lit' ? { decimal: '1' } : { text: '' })} />
                      {a.column !== undefined ? (
                        <Select label={label} value={a.column ?? ''} options={columns(i)} onChange={(v) => put(i, { column: v })} />
                      ) : slot === 'num|lit' ? (
                        <Input aria-label={label} inputMode="decimal" value={a.decimal ?? ''} maxLength={30} onChange={(e) => put(i, { decimal: e.target.value.trim() })} />
                      ) : (
                        <Input aria-label={label} value={a.text ?? ''} maxLength={200} onChange={(e) => put(i, { text: e.target.value })} />
                      )}
                    </div>
                  ) : (
                    <Select label={label} value={a.column ?? ''} options={columns(i)} onChange={(v) => put(i, { column: v })} />
                  )}
                  {f.max && args.length > (f.min ?? 0) ? (
                    <Button size="sm" variant="ghost" icon={<X />} aria-label={`Remove argument ${i + 1}`} onClick={() => onChange({ ...step, args: args.filter((_, j) => j !== i) })} />
                  ) : <span />}
                </div>
              </Row>
            );
          })}
          {f.max && args.length < f.max ? (
            <div>
              <Button size="sm" icon={<Plus />} onClick={() => onChange({ ...step, args: [...args, { column: '' }] })}>Add an argument</Button>
            </div>
          ) : null}
          {table && !columns(0).length && argSlot(fn, 0) !== 'lit' ? <span className="text-xs text-muted">{step.table} has no column {f.title.toLowerCase()} can read.</span> : null}
        </>
      )}
    </div>
  );
}

/** Keep deleted rows: what a delete becomes on this engine. It keeps rows; it is not a filter. */
export function SoftDeleteEditor({ ctx }: { ctx: EditorContext }) {
  return (
    <p className="m-0 text-sm text-ink-2">
      {ctx.flink
        ? 'Deletes arrive as rows with __op = -D; other rows carry their change type (+I, +U). The destination keeps every row: this step keeps rows, it doesn’t filter them.'
        : 'Deletes arrive as rows with __deleted = true, and no tombstone follows them. Consumers that remove rows on a delete now keep them.'}
    </p>
  );
}
