import { describe, expect, it } from 'vitest';
import { fullSpec, type TransformStep } from '../../spec';
import { addStep, canMove, columnKind, DEBEZIUM_ONLY_STEPS, defaultStep, drawOrder, effectiveDeletes, sectionOf, shapeFields, summarizeShape, TYPE_TITLE, isTextType, fieldsBefore, flinkColumns, flinkKey, keyChoices, computedKind, KIND_NAMES, globMatch, routeTargets, computedError, defaultArgs, formulaText, kindFits, FLINK_ONLY_STEPS, moveStep, nameError, operatorsFor, outputImpact, parseRuleValue, stepCaps, stepErrors, summarize, textError, toggleFlinkOperation, type Caps, type TableInfo } from '../model';

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

describe('Flink CDC steps', () => {
  it('turns the first copy and inserts on and off together', () => {
    expect(toggleFlinkOperation([], 'new', true)).toEqual(['insert', 'snapshot']);
    expect(toggleFlinkOperation(['insert', 'update', 'snapshot'], 'new', false)).toEqual(['update']);
    expect(toggleFlinkOperation(['update'], 'delete', true)).toEqual(['update', 'delete']);
    // A Debezium-era filter with only inserts: turning "new rows" on adds the first copy.
    expect(toggleFlinkOperation(['insert'], 'new', true)).toEqual(['insert', 'snapshot']);
  });
  it('lists the columns a destination table gets: left out, renamed, added, keyed', () => {
    const spec = fullSpec({ source: { tables: { include: ['public.orders'] }, columns: { 'public.orders': { mask: [], exclude: ['status'] } } } } as never);
    const steps: TransformStep[] = [
      rename,
      { type: 'rename', id: 'other', tables: ['public.items'], renames: [{ from: 'id', to: 'item_id' }] },
      add,
      { type: 'add_field', id: 'add2', name: 'pipe', value: { kind: 'pipeline' } },
    ];
    expect(flinkColumns(orders, spec, steps, 'orders-lake')).toEqual([
      { name: 'id', from: { kind: 'column', column: 'id', renamed: false }, type: 'bigint', key: true },
      { name: 'customer_id', from: { kind: 'column', column: 'cust_id', renamed: true }, type: 'bigint', key: false },
      { name: 'source_system', from: { kind: 'added', value: 'orders-eu' }, type: 'text', key: false },
      { name: 'pipe', from: { kind: 'added', value: 'orders-lake' }, type: 'text', key: false },
    ]);
    // Key columns chosen for a table replace its primary key.
    const keyed = fullSpec({ source: { tables: { include: ['public.orders'] } }, keys: { 'public.orders': ['cust_id'] } } as never);
    expect(flinkColumns(orders, keyed, [], 'p').filter((c) => c.key).map((c) => c.name)).toEqual(['cust_id']);
    // Renamed, the key follows the written name.
    expect(flinkColumns(orders, keyed, [rename], 'p').filter((c) => c.key).map((c) => c.name)).toEqual(['customer_id']);
  });
  // ADR 0010: types in the destination table, and keys from any column the table writes.
  const upper: TransformStep = { type: 'computed', id: 'cc01', table: 'public.orders', name: 'status_up', function: 'upper', args: [{ column: 'status' }] };
  const absId: TransformStep = { type: 'computed', id: 'cc02', table: 'public.orders', name: 'id_abs', function: 'abs', args: [{ column: 'id' }] };
  const exprCol: TransformStep = { type: 'computed', id: 'cc03', table: 'public.orders', name: 'label', expression: "`status` || '-'" };
  it('types each destination column: the source type, a function result, an expression later', () => {
    const spec = fullSpec({ source: { tables: { include: ['public.orders'] } } } as never);
    const cut: TransformStep = { type: 'truncate', id: 'tr01', columns: [{ table: 'public.orders', column: 'status', length: 20 }] };
    const cols = flinkColumns(orders, spec, [cut, upper, absId, exprCol, add], 'p', 'postgresql');
    expect(cols.map((c) => [c.name, c.type])).toEqual([['id', 'bigint'], ['cust_id', 'bigint'], ['status', 'text'], ['status_up', 'text'], ['id_abs', 'int'], ['label', 'unknown'], ['source_system', 'text']]);
    expect(cols.find((c) => c.name === 'status')!.from).toEqual({ kind: 'column', column: 'status', renamed: false, cut: 20 });
    expect(computedKind(absId, orders, 'postgresql')).toBe('int');
    expect(KIND_NAMES.int).toBe('whole number');
  });
  it('resolves a Flink CDC key like the server: a source column first, then a step column', () => {
    const spec = (keys: string[], exclude: string[] = []) => ({ source: fullSpec({ source: { tables: { include: ['public.orders'] }, columns: { 'public.orders': { mask: [], exclude } } } } as never).source, keys: { 'public.orders': keys } });
    const steps = [rename, add, upper];
    expect(flinkKey(orders, spec(['id', 'cust_id', 'source_system', 'status_up']), steps)).toEqual({
      columns: [
        { name: 'id', written: 'id', from: 'column', source: 'id' },
        { name: 'cust_id', written: 'customer_id', from: 'column', source: 'cust_id' },
        { name: 'source_system', written: 'source_system', from: 'added' },
        { name: 'status_up', written: 'status_up', from: 'computed' },
      ],
      error: null,
    });
    expect(flinkKey(orders, spec(['status'], ['status']), steps).error).toBe('public.orders: the key column status is left out of the table. Keep it, or pick another key column.');
    // A rename's new name is that column (the server compiled it as it is).
    expect(flinkKey(orders, spec(['customer_id']), steps)).toEqual({ columns: [{ name: 'customer_id', written: 'customer_id', from: 'column', source: 'cust_id' }], error: null });
    expect(flinkKey(orders, spec(['gone']), steps).error).toBe('public.orders writes no column gone to key its rows by: pick the key columns again.');
    // A computed column of a left-out column's name is the key.
    const same: TransformStep = { ...upper, name: 'status' };
    expect(flinkKey(orders, spec(['status'], ['status']), [same]).columns).toEqual([{ name: 'status', written: 'status', from: 'computed' }]);
    // Columns not read yet: names as they are, through the renames.
    expect(flinkKey({ ...orders, columns: [] }, spec(['cust_id', 'gone', 'source_system']), steps).columns.map((c) => `${c.written}:${c.from}`)).toEqual(['customer_id:column', 'gone:column', 'source_system:added']);
  });
  it('offers every written column as a key on Flink CDC, source columns only on Debezium', () => {
    const spec = fullSpec({ source: { tables: { include: ['public.orders'] }, columns: { 'public.orders': { mask: [], exclude: ['status'] } } } } as never);
    const other: TransformStep = { type: 'add_field', id: 'add9', tables: ['public.items'], name: 'items_only', value: { kind: 'static', text: 'x' } };
    const flink = keyChoices(orders, spec, [rename, add, upper, other], true, 'postgresql');
    expect(flink.map((c) => [c.value, c.label, c.from, c.type, c.why ?? ''])).toEqual([
      ['id', 'id', 'column', 'bigint', ''],
      ['cust_id', 'customer_id', 'column', 'bigint', ''],
      ['status', 'status', 'column', 'text', 'Left out of the table.'],
      ['source_system', 'source_system', 'added', 'text', ''],
      // Discovery didn't say status is NOT NULL: the computed column can be empty (keys.test.ts).
      ['status_up', 'status_up', 'computed', 'text', 'Can be empty: it reads a column the source allows to be empty.'],
    ]);
    const debezium = keyChoices(orders, spec, [rename, add], false, 'postgresql');
    expect(debezium.map((c) => [c.value, c.label, c.why ? 'disabled' : ''])).toEqual([
      ['id', 'id', ''],
      ['cust_id', 'cust_id', ''],
      ['status', 'status', ''],
      ['source_system', 'source_system', 'disabled'],
    ]);
    // One choice per name: a computed column replaces the left-out source column of its name.
    const same: TransformStep = { ...upper, name: 'status' };
    expect(keyChoices(orders, spec, [same], true, 'postgresql').filter((c) => c.value === 'status').map((c) => c.from)).toEqual(['computed']);
  });
});

describe('soft delete', () => {
  const table: TableInfo = { name: 'public.orders', columns: [{ name: 'id', type: 'integer' }], primaryKey: ['id'] };
  it('adds the marker field once, says what it does, and counts as breaking', () => {
    const steps = [defaultStep('soft_delete', [])];
    expect(fieldsBefore(steps, 1, table, { source: { tables: { include: ['public.orders'] }, columns: {} } })).toEqual(['id', '__deleted']);
    expect(summarize(steps[0]!)).toContain('stays, marked deleted');
    const base = fullSpec({ source: { tables: { include: ['public.orders'] } } });
    const impact = outputImpact(base, { ...base, transforms: [...base.transforms, steps[0]!] });
    expect(impact.some((i) => i.kind === 'breaking' && i.text.includes('Deleted rows now stay'))).toBe(true);
  });
  it('runs on both engines', () => {
    expect(DEBEZIUM_ONLY_STEPS).not.toContain('soft_delete');
  });
});

describe('truncate', () => {
  it('cuts text only and says what it cuts', () => {
    expect(isTextType('character varying(200)')).toBe(true);
    expect(isTextType('text')).toBe(true);
    expect(isTextType('jsonb')).toBe(false);
    expect(isTextType('integer')).toBe(false);
    const step = { ...defaultStep('truncate', []), columns: [{ table: 'public.orders', column: 'notes', length: 100 }] };
    expect(summarize(step)).toBe('public.orders.notes to 100');
    expect(summarize(defaultStep('truncate', []))).toBe('No columns yet');
    expect(DEBEZIUM_ONLY_STEPS).not.toContain('truncate');
  });
  it('a row without a column is never saved nor previewed', () => {
    expect(stepErrors([defaultStep('truncate', [])]).get(0)).toBe('Add a column to cut.');
    expect(stepErrors([{ ...defaultStep('truncate', []), columns: [{ table: '', column: '', length: 255 }] }]).get(0)).toBe('Pick a column for every row.');
    expect(stepErrors([{ ...defaultStep('truncate', []), columns: [{ table: 'public.orders', column: 'notes', length: 9 }] }]).size).toBe(0);
  });
});

describe('route', () => {
  const tables = ['public.orders_2024', 'public.orders_2025', 'public.customers'];
  it('matches like the server (domain.GlobMatch)', () => {
    expect(globMatch('public.orders_*', 'public.orders_2024')).toBe(true);
    expect(globMatch('public.orders_*', 'public.customers')).toBe(false);
    expect(globMatch('*.customers', 'public.customers')).toBe(true);
    expect(globMatch('public.*_2025', 'public.orders_2025')).toBe(true);
    expect(globMatch('public.orders', 'public.orders_2024')).toBe(false);
    expect(globMatch('p*s*s', 'public.sales')).toBe(true);
    expect(globMatch('p*s*s', 'public.orders')).toBe(false);
    expect(globMatch('a*a', 'a')).toBe(false);
    expect(globMatch('a*a', 'aa')).toBe(true);
    expect(globMatch('*', 'public.x')).toBe(true);
    expect(globMatch('public.*o*o*', 'public.foo')).toBe(true);
  });
  it('sends each table to the first route it matches; the rest keep their own destination', () => {
    const step: TransformStep = { type: 'route', id: 's1', routes: [{ match: 'public.orders_*', to: 'orders' }] };
    const t = routeTargets([step], tables);
    expect([...t.entries()]).toEqual([['public.orders_2024', 'orders'], ['public.orders_2025', 'orders']]);
    expect(summarize(step)).toBe('public.orders_* → orders');
    expect(summarize({ type: 'route', routes: [] })).toBe('No routes yet');
    expect(DEBEZIUM_ONLY_STEPS).not.toContain('route');
  });
  it('refuses what the server refuses: bad patterns, bad names, and a table two routes match (T10)', () => {
    const err = (routes: { match: string; to: string }[], more: TransformStep[] = []) =>
      stepErrors([{ type: 'route', id: 's1', routes }, ...more], tables).get(0);
    expect(err([{ match: 'orders', to: 'x' }])).toMatch(/schema.table/);
    expect(err([{ match: 'public.orders_*', to: 'a b' }])).toMatch(/destination name/);
    expect(err([])).toBe('Add a route.');
    expect(err([{ match: 'public.orders_*', to: 'orders' }, { match: '*.orders_2025', to: 'other' }])).toMatch(/public.orders_2025 matches more than one route/);
    // Across two route steps too.
    expect(err([{ match: 'public.orders_*', to: 'orders' }], [{ type: 'route', id: 's2', routes: [{ match: 'public.*', to: 'all' }] }])).toMatch(/more than one route/);
    expect(err([{ match: 'public.orders_*', to: 'orders' }])).toBeUndefined();
  });
  it('a new or changed route breaks consumers', () => {
    const before = fullSpec({ source: { tables: { include: tables } } });
    const after = { ...before, transforms: [...before.transforms, { type: 'route' as const, id: 's1', routes: [{ match: 'public.orders_*', to: 'orders' }] }] };
    expect(outputImpact(before, after)).toEqual([{ kind: 'breaking', text: 'New routes — public.orders_* → orders. Matched tables arrive in another destination.' }]);
  });
});

describe('computed', () => {
  const step = (over: Partial<TransformStep>): TransformStep => ({ ...defaultStep('computed', []), table: 'public.orders', name: 'label', ...over });
  it('runs on Flink CDC only and starts with what its function takes', () => {
    expect(FLINK_ONLY_STEPS).toContain('computed');
    expect(defaultArgs('concat')).toEqual([{ column: '' }, { column: '' }]);
    expect(defaultArgs('substr')).toEqual([{ column: '' }, { number: 1 }, { number: 1 }]);
    expect(defaultArgs('date_format')).toEqual([{ column: '' }, { text: 'yyyy-MM-dd' }]);
  });
  it('offers only columns of the kind a function takes (T15)', () => {
    expect(kindFits('text', 'text')).toBe(true);
    expect(kindFits('text', 'int')).toBe(false);
    expect(kindFits('time', 'timestamp')).toBe(true);
    expect(kindFits('time', 'timestamptz')).toBe(true); // ADR 0008: verified on Flink CDC 3.7.0
    expect(kindFits('any', 'json')).toBe(false);
    expect(kindFits('any|lit', 'decimal')).toBe(true);
  });
  it('says what is missing, as the server would', () => {
    expect(computedError(step({ table: '' }))).toBe('Choose the table.');
    expect(computedError(step({ name: '1x' }))).toMatch(/letters, digits/);
    expect(computedError(step({ args: [{ column: '' }] }))).toBe('Pick a column for every argument.');
    expect(computedError(step({ function: 'substr', args: [{ column: 'notes' }, { number: 0 }, { number: 2 }] }))).toMatch(/from 1 to 65535/);
    expect(computedError(step({ function: 'concat', args: [{ column: 'a' }, { text: '${BZ_SRC_PASSWORD}' }] }))).toMatch(/\$\{/);
    expect(computedError(step({ args: [{ column: 'status' }] }))).toBeNull();
  });
  it('reads as a formula, adds its column and is additive', () => {
    const s = step({ function: 'concat', args: [{ column: 'status' }, { text: '-' }, { column: 'id' }] });
    expect(formulaText(s)).toBe('Join(status, "-", id)');
    expect(summarize(s)).toBe('public.orders: label = Join(status, "-", id)');
    const table: TableInfo = { name: 'public.orders', columns: [{ name: 'id', type: 'integer' }, { name: 'status', type: 'text' }], primaryKey: ['id'] };
    const spec = fullSpec({ source: { tables: { include: ['public.orders'] } } });
    expect(fieldsBefore([s, defaultStep('rename', [])], 1, table, spec)).toContain('label');
    expect(flinkColumns(table, spec, [s], 'p').at(-1)).toEqual({ name: 'label', from: { kind: 'computed', formula: 'Join(status, "-", id)' }, type: 'text', key: false });
    const after = { ...spec, transforms: [...spec.transforms, s] };
    expect(outputImpact(spec, after)).toEqual([{ kind: 'additive', text: 'New column label (public.orders).' }]);
  });
});

describe('Keep deleted rows (soft_delete): the server\'s shape boundary, drawing, and what deletes become', () => {
  const soft: TransformStep = { type: 'soft_delete', id: 'soft' };
  const soft2: TransformStep = { type: 'soft_delete', id: 'soft2' };
  const outbox: TransformStep = { type: 'outbox', id: 'obx' };
  const ids = (steps: TransformStep[]) => steps.map((t) => t.id);
  const errs = (steps: TransformStep[]) => [...stepErrors(steps).keys()];

  it.each([
    ['[soft_delete, filter]', [soft, filter], [1]],
    ['[filter, soft_delete]', [filter, soft], []],
    ['[soft_delete, rename]', [soft, rename], []],
    ['[rename, soft_delete]', [rename, soft], [0]],
    ['[unwrap, soft_delete, rename]', [unwrap, soft, rename], []],
  ] as const)('stepErrors %s → %j (the first stage-1 step is the boundary, as on the server)', (_n, steps, want) => {
    expect(errs([...steps])).toEqual(want);
  });

  it.each([
    ['[] + soft_delete', [], soft, ['soft']],
    ['[unwrap] + soft_delete', [unwrap], soft, ['shape', 'soft']],
    ['[filter, unwrap, rename] + soft_delete', [filter, unwrap, rename], soft, ['keep', 'shape', 'soft', 'ren1']],
    ['[soft_delete] + filter', [soft], filter, ['keep', 'soft']],
    ['[outbox] + soft_delete', [outbox], soft, ['obx', 'soft']],
    ['[unwrap, soft_delete] + rename', [unwrap, soft], rename, ['shape', 'soft', 'ren1']],
  ] as const)('addStep %s', (_n, steps, step, want) => {
    expect(ids(addStep([...steps], step))).toEqual(want);
  });

  it('draws every step type in exactly one section on each engine', () => {
    for (const type of Object.keys(TYPE_TITLE) as TransformStep['type'][]) {
      for (const flink of [false, true]) expect(['keep', 'shape', 'fields', 'other', 'hidden']).toContain(sectionOf({ type }, flink));
    }
    expect(sectionOf(soft, false)).toBe('shape');
    expect(sectionOf(soft, true)).toBe('keep');
    expect(sectionOf(unwrap, true)).toBe('hidden');
  });

  it('draws by section, the shape row before Keep deleted rows, without reordering the spec', () => {
    expect(drawOrder([rename, soft, filter, unwrap], false)).toEqual([2, 3, 1, 0]);
    expect(drawOrder([filter, unwrap, soft, rename], true)).toEqual([0, 2, 3]);
  });

  const jdbc = { required: false, add_fields: [], delete_handling: [], delete_handling_from: { option: 'deletes', map: { delete: 'tombstone', ignore: 'drop' } } };
  const iceberg = { required: true, add_fields: ['op'], delete_handling: ['rewrite'], delete_handling_from: null };
  it.each([
    ['Flink CDC with Keep deleted rows', [soft], {}, { flink: true }, { mode: 'op_marked', by: 'keep_deleted' }],
    ['Flink CDC without', [], {}, { flink: true }, { mode: 'flink_default', by: 'shape' }],
    ['Debezium, Keep deleted rows wins over the shape', [{ ...unwrap, delete_handling: 'drop' }, soft], {}, {}, { mode: 'rewrite', by: 'keep_deleted', step: 2 }],
    ['…and over the destination', [unwrap, soft], { deletes: 'delete' }, { unwrap: jdbc }, { mode: 'rewrite', by: 'keep_deleted', step: 2 }],
    ['the destination option maps deletes', [unwrap], { deletes: 'delete' }, { unwrap: jdbc }, { mode: 'tombstone', by: 'destination' }],
    ['…with its schema default when the option is left out', [unwrap], {}, { unwrap: jdbc, optionDefaults: { deletes: 'delete' } }, { mode: 'tombstone', by: 'destination' }],
    ['the shape step decides otherwise', [{ ...unwrap, delete_handling: 'drop' }], {}, {}, { mode: 'drop', by: 'shape' }],
    ['a required shape the compiler adds', [], {}, { unwrap: iceberg }, { mode: 'rewrite', by: 'shape' }],
    ['the full change envelope', [], {}, {}, { mode: 'envelope', by: 'shape' }],
  ] as const)('effectiveDeletes: %s', (_n, steps, options, ctx, want) => {
    expect(effectiveDeletes([...steps], options, ctx as never)).toEqual(want);
  });

  it('reports the first of two Keep deleted rows steps', () => {
    expect(effectiveDeletes([soft, soft2], {}).step).toBe(1);
  });

  it('describes the shape as it runs', () => {
    expect(summarizeShape([unwrap], {})).toBe('Row after change · deletes kept with __deleted = true · adds __op');
    expect(summarizeShape([{ ...unwrap, delete_handling: 'drop', add_fields: [] }, soft], {})).toBe('Row after change · deletes kept with __deleted = true (Keep deleted rows)');
    expect(summarizeShape([soft], {})).toBe('Row after change · deletes kept with __deleted = true (Keep deleted rows) · adds __op, __source_ts_ms');
    expect(summarizeShape([], {})).toBeNull();
  });

  it('says events become rows when Keep deleted rows is added to an envelope pipeline', () => {
    const before = fullSpec({ transforms: [] } as never);
    const after = fullSpec({ transforms: [soft] } as never);
    const texts = outputImpact(before, after).map((i) => i.text);
    expect(texts).toContain('Events become the row after the change instead of the full change envelope.');
    expect(texts.some((t) => t.startsWith('Deleted rows now stay'))).toBe(true);
    expect(texts.some((t) => t.startsWith('Deletes change'))).toBe(false);
  });

  it('gives one deletes line when Keep deleted rows joins an unwrap that drops deletes (regression guard)', () => {
    const before = fullSpec({ transforms: [{ ...unwrap, delete_handling: 'drop' }] } as never);
    const after = fullSpec({ transforms: [{ ...unwrap, delete_handling: 'drop' }, soft] } as never);
    const texts = outputImpact(before, after).map((i) => i.text);
    expect(texts.filter((t) => t.startsWith('Deleted rows now stay') || t.startsWith('Deletes change'))).toHaveLength(1);
  });
});

describe('a new keep-events filter', () => {
  it('has no match until it has rules (the server refuses match without rules)', () => {
    const f = defaultStep('filter', []);
    expect(f.match).toBeUndefined();
    expect(f.operations?.length).toBeGreaterThan(0);
    expect(stepErrors([f]).size).toBe(0);
  });
});

describe('the shape as the compiler builds it', () => {
  const jdbc = { required: false, add_fields: [], delete_handling: [], delete_handling_from: { option: 'deletes', map: { delete: 'tombstone', ignore: 'drop' } } };
  it('says the destination decides deletes (JDBC deletes: delete)', () => {
    expect(summarizeShape([{ type: 'unwrap', id: 'u', delete_handling: 'rewrite', add_fields: [] }], { deletes: 'delete' }, { unwrap: jdbc })).toBe(
      'Row after change · deletes become tombstones (the destination’s Deletes setting)',
    );
  });
  it('merges the fields a destination requires into the spec\'s unwrap (withSinkUnwrap)', () => {
    const iceberg = { required: true, add_fields: ['op', 'source.ts_ms'], delete_handling: ['rewrite'], delete_handling_from: null };
    expect(shapeFields([{ type: 'unwrap', id: 'u', add_fields: ['op'] }], { unwrap: iceberg })).toEqual(['op', 'source.ts_ms']);
    expect(shapeFields([{ type: 'soft_delete', id: 's' }])).toEqual(['op', 'source.ts_ms']);
    expect(shapeFields([], { unwrap: iceberg })).toEqual(['op', 'source.ts_ms']);
  });
});
