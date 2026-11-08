import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import type { ComputedArg, ComputedFn, TransformStep } from '../../spec';
import { COMPUTED_FUNCTIONS, computedKeyProblem, computedNeverEmpty, expressionKeyBody, expressionKeyProblem, flinkKeyProblem, KEY_KINDS, keyChoices, type TableInfo } from '../model';

// ADR 0010: the wizard mirrors which computed columns can key a Flink CDC table (domain.KeyKinds,
// expr.NeverNull of domain.ComputedTree), held equal by backend/testdata/key_columns.json.
const vectors = JSON.parse(readFileSync(resolve(__dirname, '../../../../../../backend/testdata/key_columns.json'), 'utf8')) as {
  key_kinds: string[];
  functions: Record<string, { kind: string; all: boolean; none: boolean; first: boolean }>;
};

/** The arguments the Go vector test builds: the fewest a function takes, columns c0, c1… in column slots. */
function sample(fn: ComputedFn): { step: TransformStep; cols: string[] } {
  const f = COMPUTED_FUNCTIONS[fn];
  const n = f.min ?? f.args.length;
  const cols: string[] = [];
  const args: ComputedArg[] = Array.from({ length: n }, (_, i) => {
    const slot = f.args[Math.min(i, f.args.length - 1)]!;
    if (slot === 'number') return { number: 1 };
    if (slot === 'digits') return { integer: 2 };
    if (slot === 'pattern' || slot === 'date_pattern') return { text: 'yyyy-MM-dd' };
    if (slot === 'lit') return { text: '-' };
    cols.push(`c${i}`);
    return { column: `c${i}` };
  });
  return { step: { type: 'computed', id: 'k', table: 'public.t', name: 'k', function: fn, args }, cols };
}

describe('computed key columns (shared vectors)', () => {
  it('match the server on kind and on being never empty', () => {
    expect(KEY_KINDS).toEqual(vectors.key_kinds);
    expect(Object.keys(COMPUTED_FUNCTIONS).sort()).toEqual(Object.keys(vectors.functions).sort());
    for (const [fn, v] of Object.entries(vectors.functions)) {
      const { step, cols } = sample(fn as ComputedFn);
      expect(computedNeverEmpty(step, () => true), `${fn} all`).toBe(v.all);
      expect(computedNeverEmpty(step, () => false), `${fn} none`).toBe(v.none);
      expect(computedNeverEmpty(step, (c) => c === cols[0]), `${fn} first`).toBe(v.first);
      // A kind the compile can make NOT NULL, the same on both sides.
      const out = COMPUTED_FUNCTIONS[fn as ComputedFn].out;
      expect(KEY_KINDS.includes(out), fn).toBe(vectors.key_kinds.includes(v.kind));
    }
  });
});

describe('unfit key columns', () => {
  const table: TableInfo = {
    name: 'public.t',
    columns: [
      { name: 'id', type: 'bigint', nullable: false },
      { name: 'code', type: 'text', nullable: false },
      { name: 'note', type: 'text', nullable: true },
      { name: 'made', type: 'date', nullable: false },
    ],
    primaryKey: ['id'],
  };
  const spec = (keys: string[]) => ({ source: { tables: { include: ['public.t'] }, columns: {} }, keys: { 'public.t': keys } }) as never;
  const cc = (name: string, fn: ComputedFn, args: ComputedArg[]): TransformStep => ({ type: 'computed', id: name, table: 'public.t', name, function: fn, args });
  it('says why a typed computed column can’t key the table', () => {
    expect(computedKeyProblem(cc('k', 'upper', [{ column: 'code' }]), table)).toBeNull();
    expect(computedKeyProblem(cc('k', 'upper', [{ column: 'note' }]), table)).toMatch(/source allows to be empty/);
    expect(computedKeyProblem(cc('k', 'to_integer', [{ column: 'code' }]), table)).toMatch(/no value for some rows/);
    expect(computedKeyProblem(cc('k', 'abs', [{ column: 'id' }]), table)).toMatch(/text or a whole number/);
    expect(computedKeyProblem(cc('k', 'year', [{ column: 'made' }]), table)).toBeNull();
    // A column without nullability from discovery counts as one that can be empty.
    expect(computedKeyProblem(cc('k', 'upper', [{ column: 'code' }]), { ...table, columns: table.columns.map((c) => ({ name: c.name, type: c.type })) })).toMatch(/can be empty/i);
  });
  it('blocks a cut key column, an unfit computed one, and waits for the server on expressions', () => {
    const cut: TransformStep = { type: 'truncate', id: 'tr', columns: [{ table: 'public.t', column: 'code', length: 4 }] };
    expect(flinkKeyProblem(table, spec(['code']), [cut])).toMatch(/cut to 4 characters/);
    expect(flinkKeyProblem(table, spec(['id', 'k']), [cc('k', 'upper', [{ column: 'note' }])])).toBe('public.t: the key column k: Can be empty: it reads a column the source allows to be empty.');
    expect(flinkKeyProblem(table, spec(['id', 'k']), [cc('k', 'lower', [{ column: 'code' }])])).toBeNull();
    // A key with a step column keeps the whole primary key (a month or a constant alone would merge rows).
    expect(flinkKeyProblem(table, spec(['k']), [cc('k', 'lower', [{ column: 'code' }])])).toMatch(/must keep the whole primary key \(id\)/);
    // Join with a separator over a column that can be empty: those rows would share a key.
    expect(computedKeyProblem(cc('k', 'join_with', [{ text: '-' }, { column: 'note' }, { column: 'code' }]), table)).toMatch(/can be empty/i);
    const e: TransformStep = { type: 'computed', id: 'e', table: 'public.t', name: 'e', expression: 'UPPER(`note`)' };
    expect(flinkKeyProblem(table, spec(['id', 'e']), [e], { e: 'Can be empty: …' })).toBe('public.t: the key column e: Can be empty: …');
    // Until the server answers, Continue waits; an empty expression can't be a key.
    expect(flinkKeyProblem(table, spec(['id', 'e']), [e], {})).toMatch(/Checking its expression/);
    expect(flinkKeyProblem(table, spec(['id', 'e']), [e], { e: null })).toBeNull();
    expect(flinkKeyProblem(table, spec(['id', 'e']), [{ ...e, expression: '' }], {})).toMatch(/is empty/);
    expect(expressionKeyBody(e, table).columns).toEqual([
      { name: 'id', type: 'bigint', nullable: false },
      { name: 'code', type: 'text', nullable: false },
      { name: 'note', type: 'text', nullable: true },
      { name: 'made', type: 'date', nullable: false },
    ]);
    expect(expressionKeyProblem(undefined)).toMatch(/Checking/);
    expect(expressionKeyProblem(undefined, true)).toMatch(/couldn’t check/);
    expect(expressionKeyProblem({ error: null, key_problem: null })).toBeNull();
    expect(expressionKeyProblem({ error: null, key_problem: 'it can be empty' })).toBe('it can be empty');
    expect(expressionKeyProblem({ error: { message: 'x' } })).toMatch(/error/);
    // The choice is offered disabled with the reason; an expression is asked of the server.
    const choices = keyChoices(table, spec([]), [cc('k', 'upper', [{ column: 'note' }]), e], true, 'postgresql');
    expect(choices.find((c) => c.value === 'k')?.why).toMatch(/can be empty/i);
    expect(choices.find((c) => c.value === 'e')).toMatchObject({ expression: 'UPPER(`note`)' });
  });
});
