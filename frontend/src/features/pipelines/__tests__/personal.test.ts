import { describe, expect, it } from 'vitest';
import {
  canMask,
  choiceOf,
  defaultsFor,
  normalize,
  personalProblem,
  personalRows,
  personalSummary,
  rulesToSave,
  setChoice,
  type ColumnRulesMap,
} from '../wizard/personal';

const PG = ['character varying', 'character', 'text'];
const users = [
  { name: 'id', type: 'integer' },
  { name: 'email', type: 'text', pii: true },
  { name: 'birth_date', type: 'date', pii: true },
  { name: 'note', type: 'text' },
];
const columnsOf = (t: string) => (t === 'public.users' ? users : undefined);

describe('personal data per pipeline', () => {
  it('a newly picked table masks what can be hashed and removes the rest', () => {
    expect(defaultsFor(users, PG)).toEqual({ mask: ['email'], exclude: ['birth_date'] });
    // A source that can't hash at all (MongoDB) removes every personal-data field.
    expect(defaultsFor(users, [])).toEqual({ mask: [], exclude: ['email', 'birth_date'] });
  });

  it('only text columns of a masking source can be masked', () => {
    expect(canMask(PG, 'TEXT')).toBe(true);
    expect(canMask(PG, 'date')).toBe(false);
    expect(canMask(undefined, 'text')).toBe(false);
    expect(canMask(PG, null)).toBe(false);
  });

  it('one choice per column, and keep is recorded on purpose', () => {
    let r = setChoice(undefined, 'email', 'mask');
    r = setChoice(r, 'email', 'keep');
    expect(r).toEqual({ mask: [], exclude: [], keep: ['email'] });
    expect(choiceOf(r, 'email')).toBe('keep');
    r = setChoice(r, 'email', null);
    expect(r).toEqual({ mask: [], exclude: [] });
    // Lists from YAML that name a column twice: removing wins, then masking.
    expect(normalize({ mask: ['a', 'b'], exclude: ['a'], keep: ['b', 'c'] })).toEqual({ mask: ['b'], exclude: ['a'], keep: ['c'] });
  });

  it('lists detected columns, columns with a rule, and rules whose column is gone', () => {
    const rules: ColumnRulesMap = { 'public.users': { mask: ['note', 'old_email'], exclude: [] } };
    const rows = personalRows(['public.users'], columnsOf, rules, PG);
    expect(rows.map((r) => [r.column, r.detected, r.maskable, r.choice, r.type])).toEqual([
      ['email', true, true, null, 'text'],
      ['birth_date', true, false, null, 'date'],
      ['note', false, true, 'mask', 'text'],
      ['old_email', false, false, 'mask', null],
    ]);
  });

  it('blocks a mask that would leak and warns about undecided personal data', () => {
    const masked = personalRows(['public.users'], columnsOf, { 'public.users': { mask: ['email', 'birth_date'], exclude: [] } }, PG);
    expect(personalProblem(masked)).toEqual({ blocking: true, text: 'public.users.birth_date can’t be masked: choose Remove or Keep as is.' });
    const undecided = personalRows(['public.users'], columnsOf, {}, PG);
    expect(personalProblem(undecided)?.blocking).toBe(false);
    const decided = personalRows(['public.users'], columnsOf, { 'public.users': defaultsFor(users, PG) }, PG);
    expect(personalProblem(decided)).toBeNull();
  });

  it('saves untouched rules exactly as they were, so a visit never makes a new version', () => {
    const saved: ColumnRulesMap = { 'public.users': { mask: ['email', 'email'], exclude: [] }, 'public.gone': { mask: ['x'], exclude: [] } };
    const rules = { ...saved };
    expect(rulesToSave(rules, ['public.users'], saved)).toEqual({ 'public.users': { mask: ['email', 'email'], exclude: [] } });
    const touched = { ...rules, 'public.users': setChoice(rules['public.users'], 'email', 'remove') };
    expect(rulesToSave(touched, ['public.users'], saved)).toEqual({ 'public.users': { mask: [], exclude: ['email'] } });
    // Tables with nothing left are dropped.
    expect(rulesToSave({ 'public.users': { mask: [], exclude: [] } }, ['public.users'], {})).toEqual({});
  });

  it('summarises captured tables only', () => {
    const columns: ColumnRulesMap = { a: { mask: ['x'], exclude: ['y'], keep: ['z'] }, b: { mask: ['q'], exclude: [] } };
    expect(personalSummary(columns, ['a'])).toEqual({ masked: 1, removed: 1, kept: 1 });
  });
});
