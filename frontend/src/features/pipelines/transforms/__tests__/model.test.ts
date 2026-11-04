import { describe, expect, it } from 'vitest';
import { fullSpec, type TransformStep } from '../../spec';
import { addStep, canMove, columnKind, DEBEZIUM_ONLY_STEPS, defaultStep, fieldsBefore, moveStep, nameError, operatorsFor, outputImpact, parseRuleValue, stepCaps, stepErrors, summarize, textError, type Caps, type TableInfo } from '../model';

const orders: TableInfo = { name: 'public.orders', columns: [{ name: 'id', type: 'bigint' }, { name: 'cust_id', type: 'bigint' }, { name: 'status', type: 'text' }], primaryKey: ['id'] };
const filter: TransformStep = { type: 'filter', id: 'keep', operations: ['insert'], match: 'all', rules: [] };
const unwrap: TransformStep = { type: 'unwrap', id: 'shape', delete_handling: 'rewrite', add_fields: ['op'], condition: null };
const rename: TransformStep = { type: 'rename', id: 'ren1', renames: [{ from: 'cust_id', to: 'customer_id' }] };
const add: TransformStep = { type: 'add_field', id: 'add1', name: 'source_system', value: { kind: 'static', text: 'orders-eu' } };

describe('step placement', () => {
  it('adds steps at the end of their stage', () => {
    expect(addStep([unwrap, rename], filter).map((t) => t.id)).toEqual(['keep', 'shape', 'ren1']);
    expect(addStep([filter, unwrap], add).map((t) => t.id)).toEqual(['keep', 'shape', 'add1']);
    expect(addStep([], filter).map((t) => t.id)).toEqual(['keep']);
  });
  it('moves only field changes, within their stage', () => {
    const steps = [filter, unwrap, rename, add];
    expect(moveStep(steps, 3, -1).map((t) => t.id)).toEqual(['keep', 'shape', 'add1', 'ren1']);
    expect(canMove(steps, 2, -1)).toBe(false);
    expect(canMove(steps, 0, 1)).toBe(false);
    expect(canMove(steps, 3, 1)).toBe(false);
  });
  it('gives fresh unique ids', () => {
    const s = defaultStep('flatten', [filter]);
    expect(s.id).toMatch(/^[a-z0-9]{4,12}$/);
    expect(s.id).not.toBe('keep');
  });
});

describe('step errors mirror the backend', () => {
  it('needs the row shape before field changes and filters before the shape', () => {
    expect(stepErrors([rename]).get(0)).toMatch(/Row after change/);
    expect(stepErrors([unwrap, filter]).get(1)).toMatch(/before the event shape/);
    expect(stepErrors([filter, unwrap, rename, add]).size).toBe(0);
  });
  it('checks names, values and rules', () => {
    expect(stepErrors([unwrap, { ...add, name: 'source system' }]).get(1)).toMatch(/Did you mean source_system/);
    expect(stepErrors([unwrap, { ...add, value: { kind: 'static', text: '${BZ_SRC_PASSWORD}' } }]).get(1)).toMatch(/\$\{/);
    expect(stepErrors([unwrap, { ...rename, renames: [{ from: 'a', to: 'x' }, { from: 'b', to: 'x' }] }]).get(1)).toMatch(/Two fields would be called x/);
    expect(stepErrors([{ ...filter, rules: [{ field: 'status', op: 'eq' }] }, unwrap]).get(0)).toMatch(/Enter a value/);
    expect(stepErrors([{ ...filter, rules: [{ field: 'status', op: 'in', values: [] }] }, unwrap]).get(0)).toMatch(/at least one value/);
    expect(stepErrors([{ type: 'filter', operations: [], rules: [] }]).get(0)).toMatch(/at least one operation/);
    expect(stepErrors([unwrap, { type: 'custom', class: 'Smt' }]).get(1)).toMatch(/Choose a class/);
  });
  it('validates names and text like the server', () => {
    expect(nameError('ok_1')).toBeNull();
    expect(nameError('1abc')).toMatch(/Did you mean _1abc/);
    expect(nameError('status', ['status'])).toMatch(/already has a field status/);
    expect(textError('a\tb')).toMatch(/line breaks/);
    expect(textError('x'.repeat(201))).toMatch(/200/);
  });
});

describe('columns', () => {
  it('classifies types like the backend', () => {
    expect(columnKind('postgresql', 'numeric')).toBe('decimal');
    expect(columnKind('postgresql', 'timestamp with time zone')).toBe('timestamptz');
    expect(columnKind('mysql', 'timestamp')).toBe('timestamptz');
    expect(columnKind('sqlserver', 'bit')).toBe('bool');
    expect(columnKind('mongodb', 'dict')).toBe('document');
    expect(columnKind('oracle', 'NUMBER')).toBe('decimal');
    expect(columnKind('oracle', 'DATE')).toBe('timestamp'); // Oracle's DATE has a time of day
    expect(columnKind('oracle', 'TIMESTAMP WITH LOCAL TIME ZONE')).toBe('timestamptz');
    expect(columnKind('oracle', 'RAW')).toBe('binary');
    expect(columnKind('oracle', 'VARCHAR2')).toBe('text');
    expect(columnKind('cockroachdb', 'bigint')).toBe('int');
    expect(columnKind('vitess', 'timestamp')).toBe('timestamptz');
    expect(columnKind('spanner', 'INT64')).toBe('int');
    expect(columnKind('spanner', 'TIMESTAMP')).toBe('timestamptz');
    expect(columnKind('spanner', 'DATE')).toBe('text');
    expect(columnKind('spanner', 'ARRAY<STRING>')).toBe('list');
    expect(operatorsFor('bool').map((o) => o.value)).toEqual(['is_true', 'is_false', 'is_null', 'is_not_null']);
    expect(operatorsFor('text').some((o) => o.value === 'gt')).toBe(false);
  });
  it('parses typed rule values', () => {
    expect(parseRuleValue('int', '10')).toBe(10);
    expect(parseRuleValue('decimal', '-1.5e2')).toBe(-150);
    expect(parseRuleValue('int', 'ten')).toBeNull();
    expect(parseRuleValue('text', ' paid ')).toBe('paid');
    expect(parseRuleValue('text', '  ')).toBeNull();
  });
  it('knows the fields at each point', () => {
    const spec = fullSpec({ source: { tables: { include: ['public.orders'] }, columns: { 'public.orders': { mask: [], exclude: ['status'] } } } } as never);
    const steps = [filter, unwrap, rename, add];
    expect(fieldsBefore(steps, 1, orders, spec)).toEqual(['id', 'cust_id']);
    expect(fieldsBefore(steps, 3, orders, spec)).toEqual(['id', 'customer_id', '__deleted', '__op']);
    expect(fieldsBefore(steps, 4, orders, spec)).toEqual(['id', 'customer_id', '__deleted', '__op', 'source_system']);
    expect(fieldsBefore([unwrap, { ...rename, tables: ['public.other'] }], 2, orders, spec)).toContain('cust_id');
  });
});

describe('plain language', () => {
  it('summarizes steps', () => {
    expect(summarize(unwrap)).toBe('Row after change · deletes kept with __deleted = true · adds __op');
    expect(summarize({ ...filter, tables: ['public.orders'], rules: [{ field: 'status', op: 'in', values: ['paid', 'shipped'] }] })).toBe('public.orders: insert, where status is one of paid, shipped');
    expect(summarize(add)).toBe('all tables: source_system = "orders-eu"');
    expect(summarize({ type: 'add_field', name: 'p', value: { kind: 'pipeline' } })).toBe('all tables: p = the pipeline name');
  });
});

describe('what changes for consumers', () => {
  const base = fullSpec({ source: { tables: { include: ['public.orders'] } }, transforms: [unwrap, rename] } as never);
  it('classifies changes', () => {
    const next = fullSpec({ ...base, transforms: [filter, unwrap, add], keys: { 'public.audit_log': ['event_id'] }, values: { decimals: 'double' } } as never);
    expect(outputImpact(base, next)).toEqual([
      { kind: 'breaking', text: 'New filter — all tables: insert. Events that do not match stop arriving.' },
      { kind: 'additive', text: 'New field source_system (all tables).' },
      { kind: 'breaking', text: 'Removed: Rename fields — all tables: cust_id → customer_id.' },
      { kind: 'breaking', text: 'Value formats change: decimals as numbers.' },
      { kind: 'key', text: 'public.audit_log: messages are keyed by event_id instead of the primary key. Kafka routes them to different partitions.' },
    ]);
  });
  it('a rename that keeps its output names is not breaking', () => {
    const next = fullSpec({ ...base, transforms: [unwrap, { ...rename, renames: [{ from: 'customer_ref', to: 'customer_id' }] }] } as never);
    expect(outputImpact(base, next)).toEqual([{ kind: 'additive', text: 'Rename now reads customer_ref; output names unchanged.' }]);
    const other = fullSpec({ ...base, transforms: [unwrap, { ...rename, renames: [{ from: 'cust_id', to: 'client' }] }] } as never);
    expect(outputImpact(base, other)[0]?.kind).toBe('breaking');
  });
  it('reports nothing for the same spec, and shape changes', () => {
    expect(outputImpact(base, base)).toEqual([]);
    expect(outputImpact(base, fullSpec({ ...base, transforms: [rename] } as never))[0]).toMatchObject({ kind: 'breaking', text: expect.stringMatching(/full change envelope/) });
    expect(outputImpact(base, fullSpec({ ...base, transforms: [{ ...unwrap, delete_handling: 'drop' }, rename] } as never))[0]?.text).toBe('Deletes change: deletes dropped.');
    expect(outputImpact(fullSpec({ ...base, transforms: [filter, unwrap] } as never), fullSpec({ ...base, transforms: [unwrap] } as never))[0]).toMatchObject({ kind: 'additive' });
  });
});

describe('step capabilities per engine', () => {
  const source: Caps = { value_formats: { decimals: ['precise', 'double', 'string'], timestamps: ['epoch', 'isostring'] }, key_columns: true, row_filters: true };
  it('offers no value formats on Flink CDC, which writes each column with its own type', () => {
    expect(stepCaps(source, false, true)).toEqual({ value_formats: {}, key_columns: true, row_filters: true });
    expect(stepCaps(source, true, true).value_formats).toEqual({});
  });
  it('keeps the source formats on Debezium, without epoch timestamps where the destination needs time precision', () => {
    expect(stepCaps(source, false, false)).toBe(source);
    expect(stepCaps(source, true, false).value_formats).toEqual({ decimals: ['precise', 'double', 'string'], timestamps: ['isostring'] });
  });
  it('marks the steps only Debezium runs', () => {
    expect([...DEBEZIUM_ONLY_STEPS].sort()).toEqual(['custom', 'flatten', 'outbox']);
  });
});
