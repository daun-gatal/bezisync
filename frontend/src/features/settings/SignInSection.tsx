import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { LogIn, Pencil, Plus, Power, Trash2, X } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Notice, Pill } from '@/components/ui/feedback';
import { Check, ChipsInput, Field, Input, Segmented, Switch, Textarea } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { CodeBlock, Confirm, Menu, Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { Ident, Meta } from '@/components/ui/text';
import { client, unwrap, type Schemas } from '@/lib/api';
import { ago } from '@/lib/format';
import {
  defaultSummary,
  draftBody,
  draftFromPreset,
  draftFromProvider,
  draftProblem,
  lastOptionBlocker,
  passwordOffBlocker,
  OTHER_PRESET,
  PRESETS,
  presetFor,
  ruleSummary,
  scopeError,
  type AuthSettings,
  type Preset,
  type Provider,
  type ProviderDraft,
  type Role,
  type RuleDraft,
  type SamlSource,
  KIND_LABEL,
  certExpiryDays,
} from './signin';

const providersKey = ['sso-providers'];
const settingsKey = ['auth-settings'];
const ROLE_OPTIONS: { value: Role; label: string }[] = [
  { value: 'viewer', label: 'Viewer' },
  { value: 'operator', label: 'Operator' },
  { value: 'editor', label: 'Editor' },
];

function RuleRow({
  rule,
  index,
  projects,
  onChange,
  onRemove,
}: {
  rule: RuleDraft;
  index: number;
  projects: { value: string; label: string }[];
  onChange: (r: RuleDraft) => void;
  onRemove: () => void;
}) {
  const n = index + 1;
  return (
    <div className="flex min-w-0 flex-col gap-3 rounded-md border border-line p-3">
      <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,180px),1fr))] items-end gap-3">
        <Field label={`Group (rule ${n})`}>
          {(id) => <Input id={id} value={rule.group} placeholder="data-platform" onChange={(e) => onChange({ ...rule, group: e.target.value })} />}
        </Field>
        <Field label="Grants">
          {(id) => (
            <Select
              id={id}
              value={rule.admin ? 'admin' : 'project'}
              onChange={(v) => onChange({ ...rule, admin: v === 'admin' })}
              options={[
                { value: 'project', label: 'A project role' },
                { value: 'admin', label: 'Admin', description: 'Manages everything in BeziSync' },
              ]}
            />
          )}
        </Field>
        {!rule.admin ? (
          <>
            <Field label="Project">
              {(id) => <Select id={id} value={rule.project} onChange={(v) => onChange({ ...rule, project: v })} options={projects} />}
            </Field>
            <Field label="Role">{(id) => <Select id={id} value={rule.role} onChange={(v) => onChange({ ...rule, role: v })} options={ROLE_OPTIONS} />}</Field>
          </>
        ) : null}
      </div>
      <div className="flex flex-wrap items-center justify-between gap-3">
        {!rule.admin ? (
          <Check checked={rule.approver} onChange={(on) => onChange({ ...rule, approver: on })} label="May approve deploys" />
        ) : (
          <Meta>Members of this group become Admins at their next sign-in.</Meta>
        )}
        <Button size="sm" variant="ghost" icon={<X />} onClick={onRemove} aria-label={`Remove rule ${n}`}>
          Remove
        </Button>
      </div>
    </div>
  );
}

const SAML_SOURCES: { value: SamlSource; label: string }[] = [
  { value: 'metadata_url', label: 'Metadata URL' },
  { value: 'metadata_xml', label: 'Paste metadata' },
  { value: 'manual', label: 'Enter by hand' },
];

/** BeziSync's side of SAML: what the identity provider needs. */
function SamlServiceProvider({ settings }: { settings: AuthSettings }) {
  return (
    <div className="flex min-w-0 flex-col gap-2 rounded-md border border-line bg-subtle p-3">
      <Meta>Register BeziSync with your identity provider as a SAML app (or give it the metadata URL):</Meta>
      <Field label="Entity ID (audience) and metadata URL">{() => <CodeBlock code={settings.saml_entity_id} label="SAML entity ID" dark={false} wrap />}</Field>
      <Field label="ACS URL (HTTP-POST)">{() => <CodeBlock code={settings.saml_acs_url} label="SAML ACS URL" dark={false} wrap />}</Field>
      <Meta>Send a persistent or email NameID, and the email, name and groups attributes.</Meta>
    </div>
  );
}

function ProviderForm({
  draft,
  onChange,
  editing,
  preset,
  projects,
  settings,
  current,
}: {
  draft: ProviderDraft;
  onChange: (d: ProviderDraft) => void;
  editing: boolean;
  preset: Preset | null;
  projects: { value: string; label: string }[];
  settings: AuthSettings | undefined;
  current?: Provider;
}) {
  const set = (patch: Partial<ProviderDraft>) => onChange({ ...draft, ...patch });
  const everyProject = [{ value: '*', label: 'Every project' }, ...projects];
  const github = draft.kind === 'oauth2' && draft.flavor === 'github';
  const text = (label: string, key: keyof ProviderDraft, props: { hint?: string; placeholder?: string; optional?: boolean; max?: number } = {}) => (
    <Field label={label} hint={props.hint} optional={props.optional}>
      {(id) => (
        <Input
          id={id}
          value={draft[key] as string}
          maxLength={props.max ?? 500}
          placeholder={props.placeholder}
          onChange={(e) => set({ [key]: e.target.value } as Partial<ProviderDraft>)}
        />
      )}
    </Field>
  );
  const secret = (
    <Field
      label="Client secret"
      optional={editing}
      hint={editing ? 'Saved and hidden. Enter a new one to replace it.' : 'Stored encrypted; it is never shown again.'}
    >
      {(id) => (
        <Input
          id={id}
          type="password"
          autoComplete="new-password"
          value={draft.clientSecret}
          placeholder={editing ? '••••••••' : undefined}
          onChange={(e) => set({ clientSecret: e.target.value })}
        />
      )}
    </Field>
  );
  const grid = 'grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,240px),1fr))] gap-4';
  return (
    <>
      <div className={grid}>
        {text('Button name', 'label', { hint: `The sign-in page shows “Sign in with ${draft.label.trim() || '…'}”.`, placeholder: 'Okta', max: 60 })}
        {draft.kind === 'oidc'
          ? text('Issuer URL', 'issuer', { hint: preset?.issuerHint || 'The URL before /.well-known/openid-configuration.', placeholder: preset?.issuer || 'https://idp.example.com' })
          : null}
        {draft.kind !== 'saml' ? text('Client ID', 'clientId', { hint: github ? 'From the GitHub OAuth app (not a GitHub App).' : undefined }) : null}
        {draft.kind !== 'saml' ? secret : null}
      </div>

      {draft.kind === 'oidc' ? (
        <div className="flex min-w-0 flex-col gap-3 border-t border-line pt-4">
          <h3 className="m-0 text-lg font-semibold">Claims</h3>
          <Meta as="p">Leave empty for the standard claims. A path such as realm_access.roles reads a nested claim.</Meta>
          <div className={grid}>
            {text('Email claim', 'emailClaim', { optional: true, max: 100, placeholder: 'email', hint: 'Entra ID without a mailbox: upn.' })}
            {text('“Email verified” claim', 'emailVerifiedClaim', {
              optional: true,
              max: 100,
              placeholder: 'email_verified',
              hint: 'Entra ID: xms_edov (add it as an optional claim).',
            })}
            {text('Name claim', 'nameClaim', { optional: true, max: 100, placeholder: 'name' })}
          </div>
        </div>
      ) : null}

      {draft.kind === 'oauth2' ? (
        <div className="flex min-w-0 flex-col gap-4 border-t border-line pt-4">
          <h3 className="m-0 text-lg font-semibold">{github ? 'GitHub' : 'Provider endpoints'}</h3>
          {github ? <Meta as="p">Using GitHub Enterprise Server? Put your server’s address in these URLs (its API is under /api/v3).</Meta> : null}
          <div className={grid}>
            {text('Authorization URL', 'authorizationUrl', { placeholder: 'https://git.example.com/login/oauth/authorize' })}
            {text('Token URL', 'tokenUrl', { placeholder: 'https://git.example.com/login/oauth/access_token' })}
            {text('User URL', 'userinfoUrl', { hint: github ? 'Ends in /user.' : 'Answers JSON about the signed-in user.', placeholder: 'https://git.example.com/api/v1/user' })}
          </div>
          {!github ? (
            <div className={grid}>
              {text('Field with the user’s id', 'subjectClaim', { max: 100 })}
              {text('Field with the email', 'emailClaim', { max: 100 })}
              {text('Field saying the email is verified', 'emailVerifiedClaim', {
                optional: true,
                max: 100,
                hint: 'Empty: the provider never says, and only “accept unverified email” lets people in.',
              })}
              {text('Field with the name', 'nameClaim', { max: 100 })}
            </div>
          ) : null}
          <Switch
            checked={draft.secretInBody}
            onChange={(secretInBody) => set({ secretInBody })}
            label="Send the client secret in the form"
            description="GitHub expects it there; most other providers expect HTTP Basic (off)."
          />
          {text('Issuer identifier (iss)', 'responseIssuer', {
            optional: true,
            placeholder: 'https://github.com/login/oauth',
            hint: github
              ? 'github.com names itself https://github.com/login/oauth in its callback. For GitHub Enterprise Server, clear it unless you know the value.'
              : 'When the provider names itself in its callback (RFC 9207), BeziSync refuses responses from anyone else.',
          })}
        </div>
      ) : null}

      {draft.kind === 'saml' ? (
        <div className="flex min-w-0 flex-col gap-4 border-t border-line pt-4">
          <h3 className="m-0 text-lg font-semibold">Identity provider</h3>
          {settings ? <SamlServiceProvider settings={settings} /> : null}
          {editing && current?.saml ? (
            <div className="flex min-w-0 flex-col gap-1 text-base">
              <span className="wrap-anywhere">
                <strong className="font-semibold">Entity ID:</strong> <Ident>{current.saml.idp_entity_id}</Ident>
              </span>
              <span className="wrap-anywhere">
                <strong className="font-semibold">Sign-in URL:</strong> <Ident>{current.saml.sso_url}</Ident>
              </span>
              {current.saml.certificates.map((c) => (
                <Meta key={c.sha256} className="wrap-anywhere">
                  Certificate {c.subject} · valid until {new Date(c.not_after).toLocaleDateString()}
                </Meta>
              ))}
            </div>
          ) : null}
          <Field label={editing ? 'Replace these with' : 'Settings come from'} group>
            {() => (
              <Segmented
                label="SAML settings source"
                value={draft.samlSource ?? 'keep'}
                onChange={(v) => set({ samlSource: v === 'keep' ? null : (v as SamlSource) })}
                options={[...(editing ? [{ value: 'keep' as const, label: 'Keep' }] : []), ...SAML_SOURCES]}
              />
            )}
          </Field>
          {draft.samlSource === 'metadata_url'
            ? text('Metadata URL', 'metadataUrl', {
                hint: 'BeziSync reads it now, and again at every Test, so rotated certificates are picked up.',
                placeholder: 'https://idp.example.com/app/…/sso/saml/metadata',
              })
            : null}
          {draft.samlSource === 'metadata_xml' ? (
            <Field label="Metadata XML">
              {(id) => <Textarea id={id} mono rows={6} value={draft.metadataXml} onChange={(e) => set({ metadataXml: e.target.value })} />}
            </Field>
          ) : null}
          {draft.samlSource === 'manual' ? (
            <>
              <div className={grid}>
                {text('Entity ID (issuer)', 'idpEntityId', { placeholder: 'http://www.okta.com/exk…' })}
                {text('Sign-in URL (HTTP-Redirect)', 'ssoUrl', { placeholder: 'https://idp.example.com/sso/saml' })}
              </div>
              <Field label="Signing certificate" optional={editing} hint={editing ? 'Empty keeps the current certificate.' : 'PEM, BEGIN CERTIFICATE …'}>
                {(id) => <Textarea id={id} mono rows={5} value={draft.certificate} onChange={(e) => set({ certificate: e.target.value })} />}
              </Field>
            </>
          ) : null}
          <div className={grid}>
            {text('Email attribute', 'emailAttribute', { optional: true, max: 200, hint: 'Empty: email, mail, or an email NameID.' })}
            {text('Name attribute', 'nameAttribute', { optional: true, max: 200, hint: 'Empty: name or displayName.' })}
            {text('Groups attribute', 'groupsAttribute', { optional: true, max: 200, hint: 'Empty: groups, memberOf or role.' })}
          </div>
          <Switch
            checked={draft.signRequests}
            onChange={(signRequests) => set({ signRequests })}
            label="Sign sign-in requests"
            description="Turn on if the identity provider requires signed requests; it reads BeziSync’s certificate from the metadata URL."
          />
          <Switch
            checked={draft.allowIdpInitiated}
            onChange={(allowIdpInitiated) => set({ allowIdpInitiated })}
            label="Allow sign-in started at the identity provider"
            description="Lets people open BeziSync from the identity provider’s app list. Leave off unless you need it: such sign-ins can’t be tied to the browser."
          />
        </div>
      ) : null}

      {draft.kind !== 'saml' ? (
        <Field label="Scopes" hint={github ? 'user:email gives the verified email; read:org gives organizations and teams.' : 'Add the scope that makes your provider send groups, if it has one.'}>
          {(id) => <ChipsInput id={id} values={draft.scopes} onChange={(scopes) => set({ scopes })} placeholder="groups" validate={scopeError} />}
        </Field>
      ) : null}

      <div className="flex min-w-0 flex-col gap-3 border-t border-line pt-4">
        <h3 className="m-0 text-lg font-semibold">Who gets in</h3>
        {preset?.noGroups ? (
          <Notice tone="info">{preset.title} sends no groups, so group rules never match: everyone gets the default access below.</Notice>
        ) : (
          <>
            <p className="m-0 text-base text-muted">
              Rules grant access from the groups your provider sends. They are applied at every sign-in; a role an Admin changes by hand in People is kept.
              {preset?.groupsHint ? ` ${preset.groupsHint}` : ''}
            </p>
            {draft.kind === 'oidc' || (draft.kind === 'oauth2' && !github)
              ? text(draft.kind === 'oidc' ? 'Groups claim' : 'Field with the groups', 'groupsClaim', {
                  max: 100,
                  hint: draft.kind === 'oidc' ? 'The ID token claim that lists the person’s groups.' : 'A list of group names in the user answer.',
                })
              : null}
            {draft.rules.map((r, i) => (
              <RuleRow
                key={i}
                rule={r}
                index={i}
                projects={everyProject}
                onChange={(next) => set({ rules: draft.rules.map((x, j) => (j === i ? next : x)) })}
                onRemove={() => set({ rules: draft.rules.filter((_, j) => j !== i) })}
              />
            ))}
            <div>
              <Button
                size="sm"
                icon={<Plus />}
                disabled={draft.rules.length >= 100}
                why="A provider has at most 100 rules."
                onClick={() => set({ rules: [...draft.rules, { group: '', admin: false, project: '*', role: 'viewer', approver: false }] })}
              >
                Add rule
              </Button>
            </div>
          </>
        )}
        <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,240px),1fr))] items-start gap-4">
          <Field label={preset?.noGroups ? 'Everyone gets' : 'Everyone else gets'}>
            {(id) => (
              <Select
                id={id}
                value={draft.defaultRole ?? 'none'}
                onChange={(v) => set({ defaultRole: v === 'none' ? null : (v as Role) })}
                options={[{ value: 'none', label: 'No access', description: 'An Admin adds them in People' }, ...ROLE_OPTIONS]}
              />
            )}
          </Field>
          {draft.defaultRole ? (
            <Field label="In these projects" group>
              {() => (
                <div className="flex min-w-0 flex-col gap-2">
                  {projects.map((p) => (
                    <Check
                      key={p.value}
                      checked={draft.defaultProjects.includes(p.value)}
                      onChange={(on) =>
                        set({ defaultProjects: on ? [...draft.defaultProjects, p.value] : draft.defaultProjects.filter((x) => x !== p.value) })
                      }
                      label={p.label}
                    />
                  ))}
                </div>
              )}
            </Field>
          ) : null}
        </div>
        <Switch
          checked={draft.linkExisting}
          onChange={(linkExisting) => set({ linkExisting })}
          label="Take over people who sign in with another option"
          description="When you move to this provider: someone who signed in with another option is moved here when the email matches. Off, they are refused, so one provider can’t take over another’s accounts."
        />
        {draft.kind !== 'saml' ? (
          <Switch
            checked={draft.allowUnverifiedEmail}
            onChange={(on) => set({ allowUnverifiedEmail: on })}
            label="Accept email addresses the provider has not verified"
            description="Leave off unless your provider never says whether an address is verified. Even on, an unverified address only creates a new account: it never signs in to an existing one."
          />
        ) : null}
      </div>
    </>
  );
}

export function SignInSection() {
  const qc = useQueryClient();
  const providers = useQuery({ queryKey: providersKey, queryFn: () => unwrap(client.GET('/api/v1/sso-providers')) });
  const settings = useQuery({ queryKey: settingsKey, queryFn: () => unwrap(client.GET('/api/v1/auth/settings')) });
  const projects = useQuery({ queryKey: ['projects'], queryFn: () => unwrap(client.GET('/api/v1/projects')) });
  const [editing, setEditing] = useState<{ preset: Preset | null; existing?: Provider } | null>(null);
  const [draft, setDraft] = useState<ProviderDraft | null>(null);
  const [deleting, setDeleting] = useState<Provider | null>(null);
  const [passwordOff, setPasswordOff] = useState(false);
  const refresh = () => {
    void qc.invalidateQueries({ queryKey: providersKey });
    void qc.invalidateQueries({ queryKey: settingsKey });
    void qc.invalidateQueries({ queryKey: ['auth-providers'] });
  };
  const projectOptions = (projects.data ?? []).map((p) => ({ value: p.slug, label: p.name }));
  const projectName = (slug: string) => projects.data?.find((p) => p.slug === slug)?.name ?? slug;

  const save = useMutation({
    mutationFn: async () => {
      if (!editing || !draft) throw new Error('nothing to save');
      const body = draftBody(draft, Boolean(editing.existing));
      if (editing.existing) {
        return unwrap(
          client.PATCH('/api/v1/sso-providers/{provider_id}', {
            params: { path: { provider_id: editing.existing.id }, header: { 'if-match': `"${editing.existing.version}"` } },
            body,
          }),
        );
      }
      return unwrap(client.POST('/api/v1/sso-providers', { body: body as Schemas['SSOProviderCreate'] }));
    },
    onSuccess: (p) => {
      toast.success(p.last_test?.ok ? `Saved ${p.label}: ${p.last_test.message}` : `Saved ${p.label}. It isn’t working yet: its card says why.`);
      setEditing(null);
      refresh();
    },
  });
  const toggle = useMutation({
    mutationFn: (p: Provider) =>
      unwrap(
        client.PATCH('/api/v1/sso-providers/{provider_id}', {
          params: { path: { provider_id: p.id }, header: { 'if-match': `"${p.version}"` } },
          body: { enabled: !p.enabled },
        }),
      ),
    onSuccess: (p) => {
      toast.success(p.enabled ? `People can sign in with ${p.label}` : `Turned off sign-in with ${p.label}`);
      refresh();
    },
  });
  const test = useMutation({
    mutationFn: (id: string) => unwrap(client.POST('/api/v1/sso-providers/{provider_id}/test', { params: { path: { provider_id: id } } })),
    onSuccess: refresh,
  });
  const remove = useMutation({
    mutationFn: (id: string) => unwrap(client.DELETE('/api/v1/sso-providers/{provider_id}', { params: { path: { provider_id: id } } })),
    onSuccess: () => {
      toast.success(`Removed sign-in with ${deleting?.label ?? 'the provider'}`);
      setDeleting(null);
      refresh();
    },
  });
  const password = useMutation({
    mutationFn: (on: boolean) => unwrap(client.PUT('/api/v1/auth/settings', { body: { password_login: on } })),
    onSuccess: (s) => {
      toast.success(s.password_login ? 'People can sign in with email and password' : 'Email and password sign-in is off');
      setPasswordOff(false);
      refresh();
    },
  });

  function open(preset: Preset | null, existing?: Provider) {
    setDraft(existing ? draftFromProvider(existing) : draftFromPreset(preset ?? OTHER_PRESET));
    save.reset();
    setEditing({ preset: preset ?? (existing ? presetFor(existing) : null), existing });
  }
  const labels = (providers.data ?? []).filter((p) => p.id !== editing?.existing?.id).map((p) => p.label);
  const problem = draft && editing ? draftProblem(draft, Boolean(editing.existing), labels) : null;

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="flex min-w-0 flex-[1_1_320px] flex-col gap-1">
          <h2 className="m-0 text-xl font-semibold">Sign-in</h2>
          <p className="m-0 text-base text-muted">How people sign in to BeziSync: your identity providers, and whether email and password still works.</p>
        </div>
        <Menu
          label="Add provider"
          variant="primary"
          items={PRESETS.map((p) => ({ label: p.title, description: p.description, onSelect: () => open(p) }))}
        />
      </div>

      <QueryState query={settings} doing="Couldn’t load the sign-in settings">
        {(s: AuthSettings) => {
          const blocker = passwordOffBlocker(s, providers.data ?? []);
          return (
            <Card title="Email and password">
              <Switch
                checked={s.password_login}
                onChange={(on) => (on ? password.mutate(true) : setPasswordOff(true))}
                disabled={password.isPending || (s.password_login && Boolean(blocker))}
                why={blocker}
                label="Allow sign-in with email and password"
                description="Turn it off to make everyone sign in with a provider. API tokens and MCP keep working."
              />
              {!s.password_login && s.password_login_in_force ? (
                <Notice tone="warn">No provider is turned on, so email and password sign-in still works until one is.</Notice>
              ) : null}
              <InlineError error={password.error} doing="Couldn’t change email and password sign-in" />
              <div className="flex min-w-0 flex-col gap-1.5">
                <Meta>Redirect URI to register with every provider</Meta>
                <CodeBlock code={s.redirect_uri} label="Redirect URI" dark={false} wrap />
              </div>
            </Card>
          );
        }}
      </QueryState>

      <QueryState query={providers} doing="Couldn’t load the sign-in providers">
        {(list) =>
          list.length === 0 ? (
            <Card>
              <Empty icon={<LogIn />} title="No sign-in providers">
                People sign in with an email and password an Admin sets. Add Google, Microsoft, Okta or any OpenID Connect provider to use your own accounts.
              </Empty>
            </Card>
          ) : (
            <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,360px),1fr))] gap-4">
              {list.map((p) => {
                const locked = settings.data ? lastOptionBlocker(settings.data, list, p) : null;
                return (
                  <Card
                    key={p.id}
                    title={p.label}
                    actions={
                      <>
                        <Button size="sm" loading={test.isPending && test.variables === p.id} onClick={() => test.mutate(p.id)}>
                          Test
                        </Button>
                        <Menu
                          label={`More actions for ${p.label}`}
                          icon="more"
                          variant="ghost"
                          size="sm"
                          items={[
                            { label: 'Edit', icon: <Pencil />, onSelect: () => open(null, p) },
                            {
                              label: p.enabled ? 'Turn off' : 'Turn on',
                              icon: <Power />,
                              disabled: Boolean(locked),
                              why: locked,
                              onSelect: () => toggle.mutate(p),
                            },
                            {
                              label: 'Remove',
                              icon: <Trash2 />,
                              danger: true,
                              disabled: Boolean(locked),
                              why: locked,
                              onSelect: () => {
                                remove.reset();
                                setDeleting(p);
                              },
                            },
                          ]}
                        />
                      </>
                    }
                  >
                    <div className="flex flex-wrap items-center gap-2">
                      {!p.enabled ? (
                        <Pill tone="grey">Off</Pill>
                      ) : p.last_test ? (
                        <Pill tone={p.last_test.ok ? 'ok' : 'bad'}>{p.last_test.ok ? 'Reachable' : 'Failing'}</Pill>
                      ) : (
                        <Pill tone="grey">Not tested</Pill>
                      )}
                      <Meta>{p.last_test ? `tested ${ago(p.last_test.tested_at)}` : 'not tested yet'}</Meta>
                    </div>
                    {p.last_test && !p.last_test.ok ? <p className="m-0 text-base text-bad wrap-break-word">{p.last_test.message}</p> : null}
                    <span className="flex min-w-0 flex-col gap-0.5">
                      <Meta>{p.kind === 'oauth2' && p.oauth2?.flavor === 'github' ? 'GitHub (OAuth2)' : KIND_LABEL[p.kind]}</Meta>
                      <Ident className="text-ink-2">{p.issuer}</Ident>
                    </span>
                    {(() => {
                      const days = certExpiryDays(p);
                      if (days === null || days >= 30) return null;
                      return (
                        <Notice tone={days < 0 ? 'bad' : 'warn'}>
                          {days < 0
                            ? 'The identity provider’s certificate has expired: nobody can sign in with it until it is replaced.'
                            : `The identity provider’s certificate expires in ${days} days: update it (or Test, if it comes from a metadata URL).`}
                        </Notice>
                      );
                    })()}
                    <ul className="m-0 flex list-none flex-col gap-1 p-0 text-base">
                      {p.role_rules.map((r, i) => (
                        <li key={i} className="wrap-anywhere">
                          <span className="font-medium">{r.group}</span>: {ruleSummary(r, projectName)}
                        </li>
                      ))}
                      <li className="text-muted">{defaultSummary(p, projectName)}</li>
                    </ul>
                    {test.error && test.variables === p.id ? <InlineError error={test.error} doing="Couldn’t test the provider" /> : null}
                    {toggle.error && toggle.variables?.id === p.id ? <InlineError error={toggle.error} doing="Couldn’t change the provider" /> : null}
                  </Card>
                );
              })}
            </div>
          )
        }
      </QueryState>

      <Modal
        open={editing !== null}
        onOpenChange={(o) => !o && setEditing(null)}
        size="lg"
        dirty={Boolean(draft && (draft.clientId || draft.clientSecret))}
        title={editing?.existing ? `Edit ${editing.existing.label}` : `Add ${editing?.preset?.title ?? 'a provider'}`}
        description={
          editing?.existing
            ? undefined
            : draft?.kind === 'saml'
              ? 'Add BeziSync to your identity provider as a SAML app with the entity ID and ACS URL below.'
              : 'Register BeziSync with your provider as a web application first, using the redirect URI on this page.'
        }
        why={problem}
        footer={
          <>
            <Button onClick={() => setEditing(null)}>Cancel</Button>
            <Button variant="primary" loading={save.isPending} disabled={Boolean(problem)} onClick={() => save.mutate()}>
              {editing?.existing ? 'Save' : 'Add provider'}
            </Button>
          </>
        }
      >
        {editing && draft ? (
          <>
            <ProviderForm
              draft={draft}
              onChange={setDraft}
              editing={Boolean(editing.existing)}
              preset={editing.preset}
              projects={projectOptions}
              settings={settings.data}
              current={editing.existing}
            />
            <InlineError error={save.error} doing="Couldn’t save the provider" />
          </>
        ) : null}
      </Modal>

      <Confirm
        open={deleting !== null}
        onOpenChange={(o) => !o && setDeleting(null)}
        title={`Remove sign-in with ${deleting?.label ?? ''}?`}
        effects={[
          'Its button leaves the sign-in page and its client secret is destroyed.',
          'People who signed in with it keep their accounts; they need another way to sign in.',
          'Roles it granted stay until an Admin changes them.',
        ]}
        confirmLabel="Remove"
        typeToConfirm={deleting?.label}
        pending={remove.isPending}
        error={remove.error}
        doing="Couldn’t remove the provider"
        onConfirm={() => deleting && remove.mutate(deleting.id)}
      />

      <Confirm
        open={passwordOff}
        onOpenChange={setPasswordOff}
        title="Turn off email and password sign-in?"
        effects={[
          'The sign-in page only offers your providers.',
          'People who only have a password can’t sign in until they use a provider with the same email.',
          'Signed-in sessions, API tokens and MCP keep working.',
          'If a provider breaks, run “bezisync enable-password-login” on the server.',
        ]}
        confirmLabel="Turn off"
        pending={password.isPending}
        error={password.error}
        doing="Couldn’t turn off email and password sign-in"
        onConfirm={() => password.mutate(false)}
      />
    </div>
  );
}
