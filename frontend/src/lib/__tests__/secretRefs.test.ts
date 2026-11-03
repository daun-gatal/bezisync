import { formatRef, guessKey, parseRef } from '../secretRefs';

describe('secret references', () => {
  it('parse and format like the API', () => {
    expect(parseRef('ref+vault://cdc/orders#password')).toEqual({ store: 'vault', path: 'cdc/orders', key: 'password' });
    expect(parseRef('ref+gcp://legacy-pg')).toEqual({ store: 'gcp', path: 'legacy-pg', key: null });
    expect(parseRef('hunter2')).toBeNull();
    expect(parseRef('ref+Bad://x')).toBeNull();
    expect(parseRef(undefined)).toBeNull();
    expect(formatRef({ store: 'k3s', path: 'redis', key: 'password' })).toBe('ref+k3s://redis#password');
    expect(formatRef({ store: 'aws', path: 'lake', key: null })).toBe('ref+aws://lake');
  });
});

describe('guessKey', () => {
  it.each([
    ['password', ['user', 'password'], 'password'],
    ['password', ['username', 'pwd'], 'pwd'],
    ['password', ['PASSWORD', 'host'], 'PASSWORD'],
    ['sasl_password', ['sasl_password', 'password'], 'sasl_password'],
    ['sasl_password', ['user', 'password'], 'password'],
    ['secret_access_key', ['accessKeyId', 'secretAccessKey'], 'secretAccessKey'],
    ['access_key_id', ['aws_access_key_id', 'aws_secret_access_key'], 'aws_access_key_id'],
    ['service_account_json', ['credentials'], 'credentials'],
    ['password', ['token'], 'token'],
  ])('%s from %j → %s', (field, keys, want) => {
    expect(guessKey(field, keys)).toBe(want);
  });

  it('leaves the choice to the person when nothing is clear', () => {
    expect(guessKey('password', ['host', 'port'])).toBeNull();
    expect(guessKey('password', [])).toBeNull();
  });
});
