import { describe, expect, it } from 'vitest';
import { limitText, newLimitError } from '../SettingLimits';
import type { Setting } from '@/lib/settings';

const threads = { key: 'debezium.source.snapshot.max.threads', title: 'Snapshot threads', type: 'int', min: '1', max: '16', default: '1' } as Setting;
const level = { key: 'quarkus.log.level', title: 'Log level', type: 'enum', choices: ['INFO', 'WARN', 'ERROR'], default: 'INFO' } as Setting;
const skip = { key: 'debezium.source.skip.messages.without.change', title: 'Skip unchanged updates', type: 'bool', default: 'false' } as Setting;

describe('setting limits', () => {
  it('says each limit in words', () => {
    expect(limitText({ max: '4' }, threads)).toBe('At most 4');
    expect(limitText({ pin: 'true' }, skip)).toBe('Always on');
    expect(limitText({ hide: true })).toBe('Hidden from pipelines');
    expect(limitText({ min: '2', max: '8' })).toBe('At least 2 · At most 8');
    expect(limitText({ choices: ['WARN', 'ERROR'] }, level)).toBe('Only WARN, ERROR');
  });
  it('refuses a limit its setting can’t take, as the server does', () => {
    expect(newLimitError(undefined, 'max', '4', [])).toBe('Choose a setting.');
    expect(newLimitError(threads, 'max', '4', [])).toBeNull();
    expect(newLimitError(threads, 'max', '0', [])).toContain('1 to 16');
    expect(newLimitError(level, 'max', 'WARN', [])).toContain('range');
    expect(newLimitError(threads, 'choices', '', [])).toContain('with choices');
    expect(newLimitError(level, 'choices', '', [])).toContain('at least one');
    expect(newLimitError(level, 'pin', 'DEBUG', [])).toContain('one of');
    expect(newLimitError(skip, 'hide', '', [])).toBeNull();
  });
});
