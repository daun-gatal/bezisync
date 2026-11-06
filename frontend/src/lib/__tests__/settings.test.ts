import { canonicalSetting, heaviest, jsonbSorted, settingError, type Setting } from '../settings';

const base = { engine: 'debezium', scope: 'source', stage: 'snapshot', impact: 'restart', why: 'why', title: 'T', key: 'k' } as const;
const int = { ...base, type: 'int', default: '1', min: '1', max: '16' } as Setting;
const dur = { ...base, type: 'duration', default: '10 min', min: '1m', max: '1h' } as Setting;
const frac = { ...base, type: 'fraction', default: '0.05', min: '0', max: '0.9' } as Setting;
const bool = { ...base, type: 'bool', default: 'false' } as Setting;
const choice = { ...base, type: 'enum', default: 'a', choices: ['a', 'b'] } as Setting;

describe('settings', () => {
  it('checks values like the server (engine.CheckSetting)', () => {
    expect(settingError(int, '4')).toBeNull();
    expect(settingError(int, '+4')).toBeNull();
    expect(settingError(int, '0')).toBe('must be a whole number from 1 to 16');
    expect(settingError(int, '4.0')).not.toBeNull();
    expect(settingError(int, '1e1')).not.toBeNull();
    expect(settingError(dur, '15 min')).toBeNull();
    expect(settingError(dur, '90min')).toBe('must be between 1m and 1h');
    expect(settingError(dur, '30 sec')).toBe('must be a duration such as 30 s, 5 min or 500 ms');
    expect(settingError(frac, '0.1')).toBeNull();
    expect(settingError(frac, '1e-1')).not.toBeNull();
    expect(settingError(bool, 'TRUE')).toBe('must be true or false');
    expect(settingError(choice, 'c')).toBe('must be one of a, b');
  });
  it('writes values the one way the server stores them (engine.CanonicalSetting)', () => {
    expect(canonicalSetting(int, '04')).toBe('4');
    expect(canonicalSetting(int, '+4')).toBe('4');
    expect(canonicalSetting(dur, '15min')).toBe('15 min');
    expect(canonicalSetting(frac, '0.10')).toBe('0.1');
    expect(canonicalSetting(int, 'x')).toBeNull();
  });
  it('orders keys like PostgreSQL JSONB (shorter first, then bytewise)', () => {
    expect(Object.keys(jsonbSorted({ 'debezium.source.poll.interval.ms': 1, 'z.b': 2, 'a.b': 3, 'debezium.source.b': 4 }))).toEqual([
      'a.b',
      'z.b',
      'debezium.source.b',
      'debezium.source.poll.interval.ms',
    ]);
  });
  it('picks the heaviest impact', () => {
    expect(heaviest(['restart', 'next_copy', 'first_copy'])).toBe('next_copy');
    expect(heaviest([])).toBeUndefined();
  });
});
