import { describe, expect, it } from 'vitest';
import {
  draftBody,
  draftFromPreset,
  draftFromProvider,
  draftProblem,
  certExpiryDays,
  issuerError,
  lastOptionBlocker,
  passwordOffBlocker,
  PRESETS,
  presetFor,
  ruleSummary,
  type AuthSettings,
  type Provider,
} from '../signin';

const provider = (over: Partial<Provider> = {}): Provider => ({
  id: 'p1',
  label: 'Okta',
  kind: 'oidc',
  issuer: 'https://acme.okta.com',
  client_id: 'cid',
  scopes: ['openid', 'email'],
  groups_claim: 'groups',
  allow_unverified_email: false,
  role_rules: [],
  default_role: null,
  default_projects: [],
  enabled: true,
  last_test: { ok: true, message: 'ok', tested_at: '2026-11-01T00:00:00Z' },
  version: 1,
  created_at: '2026-11-01T00:00:00Z',
  updated_at: '2026-11-01T00:00:00Z',
  oidc: { email_claim: '', email_verified_claim: '', name_claim: '' },
  oauth2: null,
  saml: null,
  link_existing: false,
  ...over,
});
const settings = (over: Partial<AuthSettings> = {}): AuthSettings => ({
  password_login: true,
  password_login_in_force: true,
  redirect_uri: 'https://bz/api/v1/auth/oidc/callback',
  sso_admins: 1,
  saml_entity_id: 'https://bz/api/v1/auth/saml/metadata',
  saml_acs_url: 'https://bz/api/v1/auth/saml/acs',
  saml_metadata_url: 'https://bz/api/v1/auth/saml/metadata',
  ...over,
});
const google = PRESETS.find((p) => p.id === 'google')!;

describe('sign-in providers', () => {
  it('presets with placeholders leave the issuer for the Admin to fill', () => {
    const entra = PRESETS.find((p) => p.id === 'entra')!;
    expect(draftFromPreset(entra).issuer).toBe('');
    expect(issuerError(entra.issuer)).toBe('Replace the part in angle brackets.');
    expect(draftFromPreset(google).issuer).toBe('https://accounts.google.com');
  });

  it('recognises a saved Google provider (no groups claim)', () => {
    expect(presetFor(provider({ issuer: 'https://accounts.google.com/' }))?.noGroups).toBe(true);
    expect(presetFor(provider())).toBeNull();
  });

  it('accepts only http(s) issuers without query or fragment', () => {
    expect(issuerError('https://idp.example.com/realms/x')).toBeNull();
    expect(issuerError('ftp://idp')).not.toBeNull();
    expect(issuerError('https://idp?x=1')).not.toBeNull();
    expect(issuerError('')).toBe('Enter the issuer URL.');
  });

  it('keeps the saved secret unless a new one is typed', () => {
    const d = draftFromProvider(provider());
    expect(draftBody(d, true)).not.toHaveProperty('client_secret');
    expect(draftBody({ ...d, clientSecret: 'new' }, true).client_secret).toBe('new');
    expect(draftBody({ ...d, clientSecret: 's' }, false).client_secret).toBe('s');
  });

  it('sends Admin rules without a project and drops default projects without a default role', () => {
    const d = { ...draftFromProvider(provider()), defaultRole: null, defaultProjects: ['default'] };
    d.rules = [{ group: ' ops ', admin: true, project: '*', role: 'editor', approver: true }];
    const body = draftBody(d, true);
    expect(body.role_rules).toEqual([{ group: 'ops', admin: true, approver: false }]);
    expect(body.default_projects).toEqual([]);
  });

  it('says what blocks saving, in reading order', () => {
    const d = draftFromPreset(google);
    expect(draftProblem({ ...d, label: '' }, false, [])).toMatch(/Name the button/);
    expect(draftProblem(d, false, ['google'])).toMatch(/already uses this name/);
    expect(draftProblem(d, false, [])).toBe('Enter the client ID.');
    expect(draftProblem({ ...d, clientId: 'x' }, false, [])).toBe('Enter the client secret.');
    expect(draftProblem({ ...d, clientId: 'x', clientSecret: 'y' }, false, [])).toMatch(/Pick the projects/);
    expect(draftProblem({ ...d, clientId: 'x', clientSecret: 'y', defaultProjects: ['default'] }, false, [])).toBeNull();
    expect(draftProblem({ ...d, clientId: 'x', clientSecret: 'y', defaultRole: null, scopes: ['email'] }, false, [])).toBe('Keep the openid scope.');
  });

  it('summarises rules in plain words', () => {
    const name = (s: string) => (s === 'pay' ? 'Payments' : s);
    expect(ruleSummary({ group: 'g', admin: true, project: null, role: null, approver: false }, name)).toBe('Admin');
    expect(ruleSummary({ group: 'g', admin: false, project: 'pay', role: 'editor', approver: true }, name)).toBe('Editor in Payments, may approve');
    expect(ruleSummary({ group: 'g', admin: false, project: '*', role: 'viewer', approver: false }, name)).toBe('Viewer in every project');
  });

  it('GitHub fills its endpoints and fixed fields; its body has no claims and keeps user:email', () => {
    const github = PRESETS.find((p) => p.id === 'github')!;
    const d = { ...draftFromPreset(github), clientId: 'c', clientSecret: 's', defaultProjects: ['default'] };
    expect(d.userinfoUrl).toBe('https://api.github.com/user');
    expect(d.secretInBody).toBe(true);
    const body = draftBody(d, false);
    expect(body).toMatchObject({ kind: 'oauth2', flavor: 'github', secret_in_body: true });
    expect(body).not.toHaveProperty('subject_claim');
    expect(body).not.toHaveProperty('issuer');
    expect(draftProblem(d, false, [])).toBeNull();
    expect(draftProblem({ ...d, scopes: ['read:user'] }, false, [])).toMatch(/user:email/);
    expect(draftProblem({ ...d, userinfoUrl: 'https://api.github.com/users' }, false, [])).toMatch(/ends in \/user/);
    // On edit, the kind and flavor are never sent (they can't change).
    expect(draftBody(d, true)).not.toHaveProperty('kind');
    expect(draftBody(d, true)).not.toHaveProperty('flavor');
  });

  it('a generic OAuth2 provider needs its endpoints and field names', () => {
    const generic = PRESETS.find((p) => p.id === 'oauth2')!;
    const d = { ...draftFromPreset(generic), label: 'Gitea', clientId: 'c', clientSecret: 's', defaultRole: null };
    expect(draftProblem(d, false, [])).toBe('Enter the authorization URL.');
    const full = { ...d, authorizationUrl: 'https://g/a', tokenUrl: 'https://g/t', userinfoUrl: 'https://g/u' };
    expect(draftProblem(full, false, [])).toBeNull();
    expect(draftProblem({ ...full, subjectClaim: 'has space' }, false, [])).toMatch(/user’s id/);
    expect(draftBody(full, false)).toMatchObject({ subject_claim: 'sub', email_claim: 'email', groups_claim: 'groups', secret_in_body: false });
  });

  it('SAML takes one source of settings and never sends client fields', () => {
    const saml = PRESETS.find((p) => p.id === 'saml')!;
    const d = { ...draftFromPreset(saml), label: 'Okta SAML', defaultRole: null };
    expect(draftProblem(d, false, [])).toBe('Enter the metadata URL.');
    const byUrl = { ...d, metadataUrl: 'https://idp/md' };
    expect(draftProblem(byUrl, false, [])).toBeNull();
    const body = draftBody(byUrl, false);
    expect(body).toMatchObject({ kind: 'saml', metadata_url: 'https://idp/md' });
    for (const k of ['client_id', 'client_secret', 'scopes', 'issuer', 'metadata_xml', 'sso_url']) expect(body).not.toHaveProperty(k);
    const manual = { ...d, samlSource: 'manual' as const, idpEntityId: 'urn:idp', ssoUrl: 'https://idp/sso' };
    expect(draftProblem(manual, false, [])).toMatch(/BEGIN CERTIFICATE/);
    expect(draftProblem(manual, true, [])).toBeNull(); // editing keeps the certificate
    // Editing without choosing a new source sends none.
    const kept = draftBody({ ...manual, samlSource: null }, true);
    for (const k of ['metadata_url', 'metadata_xml', 'idp_entity_id', 'sso_url', 'certificate']) expect(kept).not.toHaveProperty(k);
  });

  it('maps OIDC claims; Entra knows its verified claim; GitHub checks its issuer', () => {
    const entra = { ...draftFromPreset(PRESETS.find((p) => p.id === 'entra')!), issuer: 'https://login.microsoftonline.com/t/v2.0', clientId: 'c', clientSecret: 's' };
    expect(draftBody(entra, false)).toMatchObject({ email_verified_claim: 'xms_edov', email_claim: '', name_claim: '' });
    expect(draftProblem({ ...entra, defaultProjects: ['default'], emailClaim: 'bad claim' }, false, [])).toMatch(/email claim/);
    expect(draftProblem({ ...entra, defaultProjects: ['default'], emailClaim: 'upn' }, false, [])).toBeNull();
    const github = draftFromPreset(PRESETS.find((p) => p.id === 'github')!);
    expect(draftBody(github, false).response_issuer).toBe('https://github.com/login/oauth');
    // Saved claim names come back into the form.
    expect(draftFromProvider(provider({ oidc: { email_claim: 'upn', email_verified_claim: 'xms_edov', name_claim: '' } })).emailClaim).toBe('upn');
  });

  it('warns before a SAML certificate expires', () => {
    const now = Date.parse('2026-11-01T00:00:00Z');
    const saml = provider({
      kind: 'saml',
      saml: {
        idp_entity_id: 'urn:idp',
        sso_url: 'https://idp/sso',
        metadata_url: '',
        certificates: [{ subject: 'CN=idp', not_after: '2026-11-11T00:00:00Z', sha256: 'ab' }],
        sign_requests: false,
        allow_idp_initiated: false,
        email_attribute: '',
        name_attribute: '',
        groups_attribute: '',
      },
    });
    expect(certExpiryDays(saml, now)).toBe(10);
    expect(certExpiryDays(provider(), now)).toBeNull();
  });

  it('never lets the last way in be switched off', () => {
    expect(passwordOffBlocker(settings(), [provider({ last_test: null })])).toMatch(/passes its test/);
    expect(passwordOffBlocker(settings({ sso_admins: 0 }), [provider()])).toMatch(/as an Admin/);
    expect(passwordOffBlocker(settings(), [provider()])).toBeNull();
    const off = settings({ password_login: false, password_login_in_force: false });
    expect(lastOptionBlocker(off, [provider()], provider())).toMatch(/only way to sign in/);
    expect(lastOptionBlocker(off, [provider(), provider({ id: 'p2' })], provider())).toBeNull();
    expect(lastOptionBlocker(settings(), [provider()], provider())).toBeNull();
  });
});
