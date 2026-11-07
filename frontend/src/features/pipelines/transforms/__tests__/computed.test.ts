import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { describe, expect, it } from 'vitest';
import type { TransformStep } from '../../spec';
import { COMPUTED_FUNCTIONS, computedError, DATE_PATTERNS, defaultArgs, formulaText, PARSE_DATE_PATTERNS, stepErrors, summarize } from '../model';

// The Go and TypeScript function lists are held equal (ADR 0008): both read this file.
const vectors = JSON.parse(readFileSync(resolve(__dirname, '../../../../../../backend/testdata/computed_functions.json'), 'utf8')) as {
  functions: Record<string, { args: string[]; min?: number; max?: number; out: string; group: string }>;
  date_patterns: string[];
  parse_date_patterns: string[];
};

describe('computed functions mirror domain.ComputedFunctions', () => {
  it('has the same functions, slots, counts, kinds and groups', () => {
    const mine = Object.fromEntries(
      Object.entries(COMPUTED_FUNCTIONS).map(([k, f]) => [k, { args: f.args, ...(f.min ? { min: f.min } : {}), ...(f.max ? { max: f.max } : {}), out: f.out, group: f.group }]),
    );
    expect(mine).toEqual(vectors.functions);
    expect(DATE_PATTERNS).toEqual(vectors.date_patterns);
    expect(PARSE_DATE_PATTERNS).toEqual(vectors.parse_date_patterns);
  });
});

describe('new arguments and expressions (ADR 0008)', () => {
  const base: TransformStep = { type: 'computed', table: 'shop.orders', name: 'x' };
  it('starts each slot with a valid value', () => {
    expect(defaultArgs('round')).toEqual([{ column: '' }, { integer: 2 }]);
    expect(defaultArgs('replace')).toEqual([{ column: '' }, { text: '' }, { text: '' }]);
    expect(defaultArgs('parse_date')).toEqual([{ column: '' }, { text: 'yyyy-MM-dd' }]);
  });
  it('checks digits, numbers and texts as the decoder does', () => {
    expect(computedError({ ...base, function: 'round', args: [{ column: 'total' }, { integer: 19 }] })).toMatch(/0 to 18/);
    expect(computedError({ ...base, function: 'multiply', args: [{ column: 'total' }, { decimal: '01.5' }] })).toMatch(/in full/);
    expect(computedError({ ...base, function: 'multiply', args: [{ column: 'total' }, { decimal: '-1.5' }] })).toBeNull();
    expect(computedError({ ...base, function: 'replace', args: [{ column: 's' }, { text: '${x}' }, { text: 'y' }] })).not.toBeNull();
    expect(formulaText({ ...base, function: 'multiply', args: [{ column: 'total' }, { decimal: '-1.5' }] })).toBe('Multiply(total, -1.5)');
  });
  it('takes an expression instead of a function', () => {
    expect(computedError({ ...base, expression: '' })).toMatch(/Write the expression/);
    expect(computedError({ ...base, expression: 'UPPER(`s`)' })).toBeNull();
    expect(formulaText({ ...base, expression: 'UPPER(`s`)' })).toBe('UPPER(`s`)');
  });
  it('validates and summarises an expression filter', () => {
    const f: TransformStep = { type: 'filter', id: 'ff01', expression: '' };
    expect(stepErrors([f], ['shop.orders']).get(0)).toMatch(/Write the condition/);
    expect(stepErrors([{ ...f, expression: '`id` > 1' }], ['shop.orders']).size).toBe(0);
    expect(summarize({ ...f, expression: '`id` > 1' })).toBe('all tables: where `id` > 1');
  });
});
