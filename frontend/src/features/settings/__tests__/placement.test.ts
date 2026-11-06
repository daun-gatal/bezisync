import { describe, expect, it } from 'vitest';
import { profileError } from '../PlacementProfiles';

describe('placement profiles', () => {
  const p = { name: 'cdc', node_selector: { pool: 'cdc' }, tolerations: [], priority_class: '', labels: {} };
  it('accepts a well-formed profile', () => {
    expect(profileError(p, [p])).toBeNull();
  });
  it('refuses bad names, twins and templates', () => {
    expect(profileError({ ...p, name: 'CDC pool' }, [p])).toContain('lowercase');
    expect(profileError(p, [p, { ...p }])).toContain('Another profile');
    expect(profileError({ ...p, node_selector: { pool: '${POOL}' } }, [p])).toContain('no ${');
    expect(profileError({ ...p, labels: { team: '{{ .team }}' } }, [p])).toContain('no ${');
  });
});
