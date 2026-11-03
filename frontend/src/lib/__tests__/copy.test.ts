import { describe, expect, it } from 'vitest';
import { missingLabel, savedSecrets } from '../copy';
import type { Connection } from '../connections';

describe('copy to project', () => {
  it('says what a copy still needs', () => {
    expect(missingLabel(['password'])).toBe('Needs its password');
    expect(missingLabel(['password', 'ssh_key'])).toBe('Needs password, ssh_key');
  });

  it('counts only saved secrets as copyable: references stay references', () => {
    const c = { secret_fields_set: ['password', 'ssh_key'], secret_refs: { password: 'ref+vault://x#y' } } as unknown as Connection;
    expect(savedSecrets(c)).toEqual(['ssh_key']);
  });
});
