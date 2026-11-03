import type { Schemas } from '@/lib/api';

export type Provider = Schemas['SSOProviderRead'];
export type AuthSettings = Schemas['AuthSettingsRead'];
export type Role = 'viewer' | 'operator' | 'editor';
export type Kind = 'oidc' | 'oauth2' | 'saml';
export type Flavor = 'generic' | 'github';
/** Where a SAML provider's identity provider settings come from. */
export type SamlSource = 'metadata_url' | 'metadata_xml' | 'manual';

/** One group rule as edited: Admin, or a role in a project ("*" = every project). */
export interface RuleDraft {
  group: string;
  admin: boolean;
  project: string;
  role: Role;
  approver: boolean;
}

export interface ProviderDraft {
  kind: Kind;
  label: string;
  // OpenID Connect and OAuth2
  issuer: string;
  clientId: string;
  /** Empty while editing keeps the saved secret. */
  clientSecret: string;
  scopes: string[];
  groupsClaim: string;
  allowUnverifiedEmail: boolean;
  // OAuth2
  flavor: Flavor;
  authorizationUrl: string;
  tokenUrl: string;
  userinfoUrl: string;
  subjectClaim: string;
  emailClaim: string;
  emailVerifiedClaim: string;
  nameClaim: string;
  secretInBody: boolean;
  /** The issuer identifier the provider names in its callback (RFC 9207); "" = not checked. */
  responseIssuer: string;
  // SAML
  /** null while editing: keep the identity provider settings as they are. */
  samlSource: SamlSource | null;
  metadataUrl: string;
  metadataXml: string;
  idpEntityId: string;
  ssoUrl: string;
  certificate: string;
  signRequests: boolean;
  allowIdpInitiated: boolean;
  emailAttribute: string;
  nameAttribute: string;
  groupsAttribute: string;
  // access
  /** People who sign in with another option (same email) move to this one. */
  linkExisting: boolean;
  rules: RuleDraft[];
  defaultRole: Role | null;
  defaultProjects: string[];
}

export interface Preset {
  id: string;
  kind: Kind;
  flavor?: Flavor;
  title: string;
  description: string;
  label: string;
  issuer: string;
  issuerHint: string;
  scopes: string[];
  groupsClaim?: string;
  /** The provider sends no groups: only the default access applies. */
  noGroups?: boolean;
  /** How groups are named (shown above the rules). */
  groupsHint?: string;
  /** OIDC: the claim that says the email is verified, when not email_verified. */
  emailVerifiedClaim?: string;
}

/** The generic OpenID Connect preset. */
export const OTHER_PRESET: Preset = {
  id: 'oidc',
  kind: 'oidc',
  title: 'Other OpenID Connect',
  description: 'Dex, Auth0, Authentik, Zitadel, …',
  label: '',
  issuer: '',
  issuerHint: 'The URL before /.well-known/openid-configuration.',
  scopes: ['openid', 'email', 'profile', 'groups'],
};

export const PRESETS: Preset[] = [
  {
    id: 'google',
    kind: 'oidc',
    title: 'Google',
    description: 'Google Workspace or any Google account',
    label: 'Google',
    issuer: 'https://accounts.google.com',
    issuerHint: 'Google’s issuer is always https://accounts.google.com.',
    scopes: ['openid', 'email', 'profile'],
    noGroups: true,
  },
  {
    id: 'entra',
    kind: 'oidc',
    title: 'Microsoft Entra ID',
    description: 'Azure AD: one tenant',
    label: 'Microsoft',
    issuer: 'https://login.microsoftonline.com/<tenant-id>/v2.0',
    issuerHint: 'Put your directory (tenant) ID in place of <tenant-id>.',
    scopes: ['openid', 'email', 'profile'],
    emailVerifiedClaim: 'xms_edov',
    groupsHint:
      'Entra sends group object IDs once the app registration emits groups; send only “groups assigned to the application”, since past 200 groups Entra sends a link instead and sign-in is refused. Add the optional claim xms_edov so BeziSync knows the email is verified.',
  },
  {
    id: 'okta',
    kind: 'oidc',
    title: 'Okta',
    description: 'An Okta org or custom authorization server',
    label: 'Okta',
    issuer: 'https://<your-org>.okta.com',
    issuerHint: 'Your Okta domain, or the issuer of a custom authorization server.',
    scopes: ['openid', 'email', 'profile', 'groups'],
  },
  {
    id: 'keycloak',
    kind: 'oidc',
    title: 'Keycloak',
    description: 'One Keycloak realm',
    label: 'Keycloak',
    issuer: 'https://<keycloak-host>/realms/<realm>',
    issuerHint: 'The realm URL, as shown in its OpenID endpoint configuration.',
    scopes: ['openid', 'email', 'profile'],
    groupsHint: 'Add a “Group Membership” mapper to the client (token claim name groups), or use realm_access.roles to grant from realm roles.',
  },
  {
    id: 'gitlab',
    kind: 'oidc',
    title: 'GitLab',
    description: 'GitLab.com or your own GitLab',
    label: 'GitLab',
    issuer: 'https://gitlab.com',
    issuerHint: 'https://gitlab.com, or your GitLab’s address.',
    scopes: ['openid', 'email', 'profile'],
    groupsClaim: 'groups_direct',
    groupsHint: 'Groups are GitLab group paths, such as acme/data-platform.',
  },
  {
    id: 'github',
    kind: 'oauth2',
    flavor: 'github',
    title: 'GitHub',
    description: 'GitHub.com or GitHub Enterprise Server',
    label: 'GitHub',
    issuer: '',
    issuerHint: '',
    scopes: ['read:user', 'user:email', 'read:org'],
    groupsHint: 'Groups are organizations (acme) and teams (acme/data-platform).',
  },
  OTHER_PRESET,
  {
    id: 'oauth2',
    kind: 'oauth2',
    flavor: 'generic',
    title: 'Other OAuth2',
    description: 'A provider without OpenID Connect (Gitea, …)',
    label: '',
    issuer: '',
    issuerHint: '',
    scopes: [],
  },
  {
    id: 'saml',
    kind: 'saml',
    title: 'SAML 2.0',
    description: 'Okta, Entra ID, ADFS, Google Workspace, JumpCloud, …',
    label: '',
    issuer: '',
    issuerHint: '',
    scopes: [],
    groupsHint: 'Groups come from the groups attribute (or the one you name).',
  },
];

const GITHUB = {
  authorizationUrl: 'https://github.com/login/oauth/authorize',
  tokenUrl: 'https://github.com/login/oauth/access_token',
  userinfoUrl: 'https://api.github.com/user',
};

const BLANK: Omit<ProviderDraft, 'kind' | 'label' | 'issuer' | 'scopes'> = {
  clientId: '',
  clientSecret: '',
  groupsClaim: 'groups',
  allowUnverifiedEmail: false,
  flavor: 'generic',
  authorizationUrl: '',
  tokenUrl: '',
  userinfoUrl: '',
  subjectClaim: 'sub',
  emailClaim: 'email',
  emailVerifiedClaim: 'email_verified',
  nameClaim: 'name',
  secretInBody: false,
  responseIssuer: '',
  samlSource: 'metadata_url',
  metadataUrl: '',
  metadataXml: '',
  idpEntityId: '',
  ssoUrl: '',
  certificate: '',
  signRequests: false,
  allowIdpInitiated: false,
  emailAttribute: '',
  nameAttribute: '',
  groupsAttribute: '',
  linkExisting: false,
  rules: [],
  defaultRole: 'viewer',
  defaultProjects: [],
};

/** The preset a saved provider came from, when its settings say so. */
export function presetFor(p: Pick<Provider, 'kind' | 'issuer' | 'oauth2'>): Preset | null {
  if (p.kind === 'saml') return PRESETS.find((x) => x.id === 'saml') ?? null;
  if (p.kind === 'oauth2') return PRESETS.find((x) => x.id === (p.oauth2?.flavor === 'github' ? 'github' : 'oauth2')) ?? null;
  return PRESETS.find((x) => x.kind === 'oidc' && x.issuer && !x.issuer.includes('<') && x.issuer === p.issuer.replace(/\/+$/, '')) ?? null;
}

export function draftFromPreset(p: Preset): ProviderDraft {
  const d: ProviderDraft = { ...BLANK, kind: p.kind, label: p.label, issuer: p.issuer.includes('<') ? '' : p.issuer, scopes: [...p.scopes] };
  if (p.groupsClaim) d.groupsClaim = p.groupsClaim;
  if (p.kind === 'oidc') Object.assign(d, { emailClaim: '', emailVerifiedClaim: p.emailVerifiedClaim ?? '', nameClaim: '' });
  if (p.kind === 'oauth2') {
    d.flavor = p.flavor ?? 'generic';
    if (d.flavor === 'github')
      Object.assign(d, GITHUB, { secretInBody: true, subjectClaim: 'id', emailVerifiedClaim: '', groupsClaim: '', responseIssuer: 'https://github.com/login/oauth' });
  }
  return d;
}

export function draftFromProvider(p: Provider): ProviderDraft {
  const o = p.oauth2;
  const s = p.saml;
  return {
    ...BLANK,
    kind: p.kind,
    label: p.label,
    issuer: p.kind === 'oidc' ? p.issuer : '',
    clientId: p.client_id,
    scopes: [...p.scopes],
    groupsClaim: p.groups_claim,
    allowUnverifiedEmail: p.allow_unverified_email,
    ...(p.oidc ? { emailClaim: p.oidc.email_claim, emailVerifiedClaim: p.oidc.email_verified_claim, nameClaim: p.oidc.name_claim } : {}),
    ...(o
      ? {
          flavor: o.flavor,
          authorizationUrl: o.authorization_url,
          tokenUrl: o.token_url,
          userinfoUrl: o.userinfo_url,
          subjectClaim: o.subject_claim,
          emailClaim: o.email_claim,
          emailVerifiedClaim: o.email_verified_claim,
          nameClaim: o.name_claim,
          secretInBody: o.secret_in_body,
          responseIssuer: o.response_issuer,
        }
      : {}),
    ...(s
      ? {
          samlSource: null,
          metadataUrl: s.metadata_url,
          idpEntityId: s.idp_entity_id,
          ssoUrl: s.sso_url,
          signRequests: s.sign_requests,
          allowIdpInitiated: s.allow_idp_initiated,
          emailAttribute: s.email_attribute,
          nameAttribute: s.name_attribute,
          groupsAttribute: s.groups_attribute,
        }
      : {}),
    linkExisting: p.link_existing,
    rules: p.role_rules.map((r) => ({ group: r.group, admin: r.admin, project: r.project ?? '*', role: (r.role ?? 'viewer') as Role, approver: r.approver })),
    defaultRole: (p.default_role ?? null) as Role | null,
    defaultProjects: [...p.default_projects],
  };
}

type Body = Schemas['SSOProviderCreate'];

/** The request body: only the fields the provider's kind uses; secrets only when typed. */
export function draftBody(d: ProviderDraft, editing: boolean): Body {
  const body: Body = {
    label: d.label.trim(),
    role_rules: d.rules.map((r) =>
      r.admin
        ? { group: r.group.trim(), admin: true, approver: false }
        : { group: r.group.trim(), admin: false, project: r.project, role: r.role, approver: r.approver },
    ),
    default_role: d.defaultRole,
    default_projects: d.defaultRole ? d.defaultProjects : [],
    link_existing: d.linkExisting,
  };
  if (!editing) body.kind = d.kind;
  if (d.kind === 'oidc' || d.kind === 'oauth2') {
    body.client_id = d.clientId.trim();
    body.scopes = d.scopes;
    body.allow_unverified_email = d.allowUnverifiedEmail;
    if (!editing || d.clientSecret) body.client_secret = d.clientSecret;
  }
  if (d.kind === 'oidc') {
    body.issuer = d.issuer.trim();
    body.groups_claim = d.groupsClaim.trim() || 'groups';
    body.email_claim = d.emailClaim.trim();
    body.email_verified_claim = d.emailVerifiedClaim.trim();
    body.name_claim = d.nameClaim.trim();
  }
  if (d.kind === 'oauth2') {
    if (!editing) body.flavor = d.flavor;
    body.authorization_url = d.authorizationUrl.trim();
    body.token_url = d.tokenUrl.trim();
    body.userinfo_url = d.userinfoUrl.trim();
    body.secret_in_body = d.secretInBody;
    body.response_issuer = d.responseIssuer.trim();
    if (d.flavor === 'generic') {
      body.subject_claim = d.subjectClaim.trim();
      body.email_claim = d.emailClaim.trim();
      body.email_verified_claim = d.emailVerifiedClaim.trim();
      body.name_claim = d.nameClaim.trim();
      body.groups_claim = d.groupsClaim.trim() || 'groups';
    }
  }
  if (d.kind === 'saml') {
    if (d.samlSource === 'metadata_url') body.metadata_url = d.metadataUrl.trim();
    if (d.samlSource === 'metadata_xml') body.metadata_xml = d.metadataXml.trim();
    if (d.samlSource === 'manual') {
      body.idp_entity_id = d.idpEntityId.trim();
      body.sso_url = d.ssoUrl.trim();
      if (d.certificate.trim()) body.certificate = d.certificate.trim();
    }
    body.sign_requests = d.signRequests;
    body.allow_idp_initiated = d.allowIdpInitiated;
    body.email_attribute = d.emailAttribute.trim();
    body.name_attribute = d.nameAttribute.trim();
    body.groups_attribute = d.groupsAttribute.trim();
  }
  return body;
}

/** An http(s) URL with a host and no fragment (what the server accepts for endpoints). */
export function urlError(v: string, what: string): string | null {
  const s = v.trim();
  if (!s) return `Enter the ${what}.`;
  if (s.includes('<') || s.includes('>')) return 'Replace the part in angle brackets.';
  try {
    const u = new URL(s);
    if ((u.protocol !== 'https:' && u.protocol !== 'http:') || !u.host || u.hash || u.username) throw new Error('bad');
  } catch {
    return `Enter the full ${what}, starting with https://.`;
  }
  return null;
}

/** An issuer the server accepts: http(s), a host, no query or fragment, no placeholder left. */
export function issuerError(issuer: string): string | null {
  const v = issuer.trim();
  if (!v) return 'Enter the issuer URL.';
  if (v.includes('<') || v.includes('>')) return 'Replace the part in angle brackets.';
  try {
    const u = new URL(v);
    if ((u.protocol !== 'https:' && u.protocol !== 'http:') || !u.host || u.search || u.hash) throw new Error('bad');
  } catch {
    return 'Enter a URL such as https://accounts.google.com.';
  }
  return null;
}

const CLAIM = /^[A-Za-z0-9_:./-]{1,100}$/;

/** The first thing keeping the form from being saved, in reading order. */
export function draftProblem(d: ProviderDraft, editing: boolean, labels: string[]): string | null {
  if (!d.label.trim()) return 'Name the button (for example Google).';
  if (labels.some((l) => l.toLowerCase() === d.label.trim().toLowerCase())) return 'Another sign-in option already uses this name.';
  if (d.kind === 'oidc') {
    const issuer = issuerError(d.issuer);
    if (issuer) return issuer;
    for (const [v, what] of [
      [d.emailClaim, 'email'],
      [d.emailVerifiedClaim, '“email verified”'],
      [d.nameClaim, 'name'],
    ] as const) {
      if (v.trim() && !CLAIM.test(v.trim())) return `The ${what} claim is a claim name such as email, or a path such as realm_access.roles.`;
    }
  }
  if (d.kind === 'oauth2') {
    for (const [v, what] of [
      [d.authorizationUrl, 'authorization URL'],
      [d.tokenUrl, 'token URL'],
      [d.userinfoUrl, 'user URL'],
    ] as const) {
      const e = urlError(v, what);
      if (e) return e;
    }
    if (d.flavor === 'github' && !/\/user\/?$/.test(d.userinfoUrl.trim())) return 'GitHub’s user URL ends in /user.';
    if (/\s/.test(d.responseIssuer.trim())) return 'The issuer identifier has no spaces, such as https://github.com/login/oauth.';
    if (d.flavor === 'generic') {
      for (const [v, what] of [
        [d.subjectClaim, 'id'],
        [d.emailClaim, 'email'],
        [d.nameClaim, 'name'],
      ] as const) {
        if (!CLAIM.test(v.trim())) return `Name the field that holds the user’s ${what}.`;
      }
      if (d.emailVerifiedClaim.trim() && !CLAIM.test(d.emailVerifiedClaim.trim())) return 'The “email verified” field is a field name such as email_verified.';
    }
  }
  if (d.kind === 'oidc' || d.kind === 'oauth2') {
    if (!d.clientId.trim()) return 'Enter the client ID.';
    if (!editing && !d.clientSecret) return 'Enter the client secret.';
    if (d.kind === 'oidc' && !d.scopes.includes('openid')) return 'Keep the openid scope.';
    if (d.kind === 'oauth2' && d.flavor === 'github' && !d.scopes.includes('user:email')) return 'Keep the user:email scope: BeziSync needs the verified email address.';
  }
  if (d.kind === 'saml') {
    if (d.samlSource === 'metadata_url') {
      const e = urlError(d.metadataUrl, 'metadata URL');
      if (e) return e;
    }
    if (d.samlSource === 'metadata_xml' && !d.metadataXml.includes('EntityDescriptor')) return 'Paste the identity provider’s metadata XML.';
    if (d.samlSource === 'manual') {
      if (!/^\S{1,500}$/.test(d.idpEntityId.trim())) return 'Enter the identity provider’s entity ID (issuer).';
      const e = urlError(d.ssoUrl, 'sign-in URL');
      if (e) return e;
      if (!editing && !d.certificate.includes('BEGIN CERTIFICATE')) return 'Paste the identity provider’s signing certificate (BEGIN CERTIFICATE).';
    }
  }
  if (d.rules.some((r) => !r.group.trim())) return 'Name the group of every rule.';
  if (d.defaultRole && d.defaultProjects.length === 0) return 'Pick the projects people get by default, or give no default access.';
  return null;
}

/** A scope is one word without spaces or quotes (RFC 6749). */
export function scopeError(v: string): string | null {
  return /^[\x21\x23-\x5b\x5d-\x7e]{1,100}$/.test(v) ? null : 'A scope is one word without spaces or quotes.';
}

export const KIND_LABEL: Record<Kind, string> = { oidc: 'OpenID Connect', oauth2: 'OAuth2', saml: 'SAML 2.0' };

const ROLE_NAMES: Record<Role, string> = { viewer: 'Viewer', operator: 'Operator', editor: 'Editor' };

export function roleName(r: string): string {
  return ROLE_NAMES[r as Role] ?? r;
}

/** What one rule grants, as a sentence fragment. */
export function ruleSummary(r: Schemas['SSORuleRead'], projectName: (slug: string) => string): string {
  if (r.admin) return 'Admin';
  const where = r.project === '*' || r.project === null ? 'every project' : projectName(r.project);
  return `${roleName(r.role ?? 'viewer')} in ${where}${r.approver ? ', may approve' : ''}`;
}

/** Who gets in when no rule matches. */
export function defaultSummary(p: Provider, projectName: (slug: string) => string): string {
  if (!p.default_role || p.default_projects.length === 0) return 'Everyone else: no access until an Admin adds them.';
  return `Everyone else: ${roleName(p.default_role)} in ${p.default_projects.map(projectName).join(', ')}.`;
}

/** Days until the SAML certificate that expires first (null: none or not SAML). */
export function certExpiryDays(p: Provider, now = Date.now()): number | null {
  const ends = (p.saml?.certificates ?? []).map((c) => new Date(c.not_after).getTime());
  if (!ends.length) return null;
  return Math.floor((Math.min(...ends) - now) / 86_400_000);
}

/** Why email and password sign-in cannot be turned off yet (null when it can). */
export function passwordOffBlocker(settings: AuthSettings, providers: Provider[]): string | null {
  if (!providers.some((p) => p.enabled && p.last_test?.ok)) return 'Add a sign-in provider that passes its test first.';
  if (settings.sso_admins === 0) return 'Sign in once with a provider as an Admin first, so someone can still manage BeziSync.';
  return null;
}

/** Why a provider cannot be turned off or removed (null when it can). */
export function lastOptionBlocker(settings: AuthSettings, providers: Provider[], p: Provider): string | null {
  if (settings.password_login || !p.enabled) return null;
  return providers.some((o) => o.id !== p.id && o.enabled) ? null : 'It is the only way to sign in: turn email and password sign-in on first.';
}
