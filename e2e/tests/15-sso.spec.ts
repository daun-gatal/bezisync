import { expect, request, test } from '@playwright/test';
import { ADMIN, Api, BASE_URL, choose, defaultProject, signIn } from './helpers';

const DEX = 'http://dex.bezisync-e2e-data.svc.cluster.local:30556/dex';
type Provider = { id: string; label: string; version: number; enabled: boolean };

/** Remove an Example IdP left by an earlier run (a quick run keeps the database). */
async function removeDex(api: Api) {
  await api.send('PUT', '/api/v1/auth/settings', { password_login: true });
  for (const p of await api.get<Provider[]>('/api/v1/sso-providers')) {
    if (p.label === 'Example IdP') await api.send('DELETE', `/api/v1/sso-providers/${p.id}`, undefined, 204);
  }
}

/** An Admin adds the Dex fixture in Settings → Sign-in; the other SSO journeys sign in with it. */
test('an Admin adds an identity provider in Settings and it appears on the sign-in page', async ({ page, browser }) => {
  await removeDex(await Api.signIn());
  await signIn(page);
  await page.goto('/p/default/settings/sign-in');
  await expect(page.getByText('Redirect URI to register with every provider')).toBeVisible();
  await page.getByRole('button', { name: 'Add provider' }).click();
  await page.getByRole('menuitem', { name: /Other OpenID Connect/ }).click();
  const dialog = page.getByRole('dialog');
  await dialog.getByLabel('Button name').fill('Example IdP');
  await dialog.getByLabel('Issuer URL').fill(DEX);
  await dialog.getByLabel('Client ID').fill('bezisync');
  await dialog.getByLabel('Client secret').fill('dex-e2e-secret');
  await dialog.getByRole('button', { name: 'Add rule' }).click();
  await dialog.getByLabel('Group (rule 1)').fill('authors');
  await choose(dialog.getByLabel('Project', { exact: true }), 'Default');
  await choose(dialog.getByLabel('Everyone else gets'), 'No access');
  await dialog.getByRole('button', { name: 'Add provider' }).click();
  await expect(dialog).toHaveCount(0);
  const card = page.getByRole('region', { name: 'Example IdP' });
  await expect(card.getByText('Reachable')).toBeVisible();
  await expect(card.getByText('Viewer in Default')).toBeVisible();
  // The secret never comes back.
  const listed = await (await Api.signIn()).get<unknown[]>('/api/v1/sso-providers');
  expect(JSON.stringify(listed)).not.toContain('dex-e2e-secret');

  const anon = await browser.newContext();
  const login = await anon.newPage();
  await login.goto('/login');
  await expect(login.getByRole('link', { name: 'Sign in with Example IdP' })).toBeVisible();
  await expect(login.getByLabel('Password')).toBeVisible();
  await anon.close();
});

/** Single sign-on through a real OIDC provider (Dex), with group-based roles. */
test('a user signs in with SSO and gets the role their group maps to', async ({ browser }) => {
  // Dex always signs in the same person: drop a hand-set role from an earlier run on this database.
  const reset = await Api.signIn();
  const base = `/api/v1/projects/${(await defaultProject(reset)).id}`;
  const earlier = (await reset.get<{ user_id: string; email: string }[]>(`${base}/members`)).find((m) => m.email === 'kilgore@kilgore.trout');
  if (earlier) await reset.send('DELETE', `${base}/members/${earlier.user_id}`);
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.goto('/login?next=%2Fp%2Fdefault%2Fconnections');
  await page.getByRole('link', { name: 'Sign in with Example IdP' }).click();
  // Dex's mock connector signs "Kilgore Trout" in and returns to BeziSync.
  await expect(page).toHaveURL(/\/p\/default\/connections/);
  await expect(page.getByRole('heading', { name: 'Connections', level: 1 })).toBeVisible();
  await expect(page.getByText('kilgore@kilgore.trout')).toBeVisible();
  // Group "authors" maps to viewer: no mutation controls.
  await expect(page.getByRole('button', { name: /Add source/ })).toHaveCount(0);
  await context.close();

  const admin = await Api.signIn();
  const project = await defaultProject(admin);
  const members = await admin.get<{ email: string; role: string }[]>(`/api/v1/projects/${project.id}/members`);
  expect(members.find((m) => m.email === 'kilgore@kilgore.trout')?.role).toBe('viewer');
  const events = await admin.get<{ items: { kind: string; summary: string }[] }>(`/api/v1/projects/${project.id}/events?limit=200`);
  expect(events.items.length).toBeGreaterThan(0);
});

test("an Admin changes an SSO user's role and approver right in People, and it survives their next sign-in", async ({ page, browser }) => {
  const ssoSignIn = async () => {
    const context = await browser.newContext();
    const p = await context.newPage();
    await p.goto('/login?next=%2Fp%2Fdefault%2Fpipelines');
    await p.getByRole('link', { name: 'Sign in with Example IdP' }).click();
    await expect(p).toHaveURL(/\/p\/default\/pipelines/);
    return { context, p };
  };
  const first = await ssoSignIn();
  await first.context.close();

  await signIn(page);
  const project = await defaultProject(await Api.signIn());
  await page.goto(`/p/default/settings/people?in=${project.id}`);
  const row = page.getByRole('listitem').filter({ hasText: 'kilgore@kilgore.trout' });
  await expect(row.getByText('Managed by SSO')).toBeVisible();
  await row.getByRole('checkbox', { name: /Approver/ }).check();
  await expect(row.getByText('Managed by SSO')).toHaveCount(0);
  await choose(row.getByLabel('Role for kilgore@kilgore.trout'), 'Operator');
  await expect(row.getByLabel('Role for kilgore@kilgore.trout')).toContainText('Operator');

  // Signing in again re-evaluates the group rules, which must not undo the admin's change.
  const again = await ssoSignIn();
  await expect(again.p.getByRole('link', { name: 'New pipeline' })).toHaveCount(0);
  await again.context.close();
  const admin = await Api.signIn();
  const members = await admin.get<{ email: string; role: string; can_approve: boolean; granted_by_sso: boolean }[]>(`/api/v1/projects/${project.id}/members`);
  const kilgore = members.find((m) => m.email === 'kilgore@kilgore.trout');
  expect(kilgore).toMatchObject({ role: 'operator', can_approve: true, granted_by_sso: false });
});

test('with email and password sign-in turned off, only the provider signs people in', async ({ page, browser }) => {
  const admin = await Api.signIn(); // this session outlives the switch
  // An Admin must have signed in with SSO first: Dex's user signs in and is made an Admin.
  const context = await browser.newContext();
  const sso = await context.newPage();
  await sso.goto('/login');
  await sso.getByRole('link', { name: 'Sign in with Example IdP' }).click();
  await expect(sso.getByRole('heading', { level: 1 })).toBeVisible();
  await context.close();
  const users = await admin.get<{ items: { id: string; email: string }[] }>('/api/v1/users?limit=200');
  const kilgore = users.items.find((u) => u.email === 'kilgore@kilgore.trout')!;
  await admin.send('PATCH', `/api/v1/users/${kilgore.id}`, { is_superuser: true });
  try {
    await signIn(page);
    await page.goto('/p/default/settings/sign-in');
    await page.getByRole('switch', { name: 'Allow sign-in with email and password' }).click();
    await page.getByRole('dialog').getByRole('button', { name: 'Turn off' }).click();
    await expect(page.getByRole('switch', { name: 'Allow sign-in with email and password' })).toHaveAttribute('aria-checked', 'false');
    // The last way in can't be removed.
    await page.getByRole('button', { name: 'More actions for Example IdP' }).click();
    await expect(page.getByRole('menuitem', { name: /Remove/ })).toBeDisabled();
    await page.keyboard.press('Escape');

    const anon = await browser.newContext();
    const login = await anon.newPage();
    await login.goto('/login');
    await expect(login.getByRole('link', { name: 'Sign in with Example IdP' })).toBeVisible();
    await expect(login.getByLabel('Password')).toHaveCount(0);
    const refused = await admin.rawWithoutCsrf('POST', '/api/v1/auth/login', { email: ADMIN.email, password: ADMIN.password });
    expect(refused.status()).toBe(403);
    await anon.close();
  } finally {
    await admin.send('PUT', '/api/v1/auth/settings', { password_login: true });
    await admin.send('PATCH', `/api/v1/users/${kilgore.id}`, { is_superuser: false });
  }
});

/** A test identity provider's signing certificate (public; valid until 2036). */
const SAML_CERT = 'MIIDDzCCAfegAwIBAgIUGvz1KRZWMGkmVdsiF6K6TJq/u50wDQYJKoZIhvcNAQELBQAwFzEVMBMGA1UEAwwMZTJlLXNhbWwtaWRwMB4XDTI2MTEwMTA0MDQzOVoXDTM2MTAyOTA0MDQzOVowFzEVMBMGA1UEAwwMZTJlLXNhbWwtaWRwMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1agd8jDlJg9inJFDxis6hWFQ1DXFTbmrbiFi/+FWyCtxTHLJuT3sS0sf9PBPiE0WYhQVDoU/ctuN0Tls1Uhg9Fy6bBOKSIjfwuxYnjgXvJxb0UxZ4M1GCbmDR8cOWWeER2X8pIBAd/LlNDJN3ygYFWSnocMFXZusZDHL1oOoe49sFqIvFXvEY15fXpku9ZZT4MhDueK8Iok0MqraPG1k0xjyOmRCBXKreku64zlE/6B9oWEfrkAm4XH5J+iB0mb86pqjp2EV5hm7Cs9ZxhuVF/HesLERhohgioxHm5Na+DqTufk/dztOWgEN3we/L3avMb3FvLkZ4Q95yXv8nweCLQIDAQABo1MwUTAdBgNVHQ4EFgQU0YJO7Fih2Pa1hvm/LPQeI277TlUwHwYDVR0jBBgwFoAU0YJO7Fih2Pa1hvm/LPQeI277TlUwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAlvpk60PZL1Sy7gvYEeoga0BhgazxQe9Xt+lo5cf/WyjZb8+6w44ZZm3V948L+ejuFv+Sg2luFPGUt7l/eeW+JmcMGjahh7hxWxLFUJ3BUYxHW9jMpqRiONfAfZmdafPaxwOrvwYlvl1zXhmy6C6jcLpuc/6C78uxipQ44m4LyDAmbZXfz7H7YgqwAlQ1AwteIdMV72XOrmQIigmjAz/4SGu2/N+we8F5xhAVU7rSPxEpKRX/OY/L49ZVkW5IcUJHoXkIIbg5cRcxxYVYK0jwsZCaSblGXJFlKICKhsykqbmEeNXs8ht9qqx4CmW540tz2HTHMJj4BGU/Kfyy1o1nCw==';
const SAML_METADATA = `<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="urn:e2e:saml-idp"><IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol"><KeyDescriptor use="signing"><KeyInfo xmlns="http://www.w3.org/2000/09/xmldsig#"><X509Data><X509Certificate>${SAML_CERT}</X509Certificate></X509Data></KeyInfo></KeyDescriptor><SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://idp.e2e.example/sso"/></IDPSSODescriptor></EntityDescriptor>`;

test('an Admin adds a SAML identity provider from its metadata and sees what to give it', async ({ page }) => {
  const admin = await Api.signIn();
  for (const p of await admin.get<Provider[]>('/api/v1/sso-providers')) {
    if (p.label === 'E2E SAML') await admin.send('DELETE', `/api/v1/sso-providers/${p.id}`, undefined, 204);
  }
  await signIn(page);
  await page.goto('/p/default/settings/sign-in');
  await page.getByRole('button', { name: 'Add provider' }).click();
  await page.getByRole('menuitem', { name: /SAML 2\.0/ }).click();
  const dialog = page.getByRole('dialog');
  // BeziSync's side, to register with the identity provider.
  await expect(dialog.getByLabel('SAML ACS URL')).toContainText('/api/v1/auth/saml/acs');
  await expect(dialog.getByLabel('SAML entity ID')).toContainText('/api/v1/auth/saml/metadata');
  await dialog.getByLabel('Button name').fill('E2E SAML');
  await dialog.getByRole('button', { name: 'Paste metadata' }).click();
  await dialog.getByLabel('Metadata XML').fill(SAML_METADATA);
  await choose(dialog.getByLabel('Everyone else gets'), 'No access');
  await dialog.getByRole('button', { name: 'Add provider' }).click();
  await expect(dialog).toHaveCount(0);
  const card = page.getByRole('region', { name: 'E2E SAML' });
  await expect(card.getByText('SAML 2.0')).toBeVisible();
  await expect(card.getByText('urn:e2e:saml-idp')).toBeVisible();
  // The service provider metadata is public, for identity providers to read.
  const md = await (await request.newContext({ baseURL: BASE_URL })).get('/api/v1/auth/saml/metadata');
  expect(md.status()).toBe(200);
  expect(await md.text()).toContain('AssertionConsumerService');
  const created = (await admin.get<Provider[]>('/api/v1/sso-providers')).find((p) => p.label === 'E2E SAML')!;
  await admin.send('DELETE', `/api/v1/sso-providers/${created.id}`, undefined, 204);
});

test('a plain OAuth2 provider signs people in, and moving between providers is an Admin choice', async ({ browser }) => {
  const admin = await Api.signIn();
  for (const p of await admin.get<Provider[]>('/api/v1/sso-providers')) {
    if (p.label === 'Dex OAuth2') await admin.send('DELETE', `/api/v1/sso-providers/${p.id}`, undefined, 204);
  }
  // Dex as a plain OAuth2 provider: no ID token is read, only its userinfo endpoint.
  const oauth = await admin.send<Provider>(
    'POST',
    '/api/v1/sso-providers',
    {
      label: 'Dex OAuth2',
      kind: 'oauth2',
      client_id: 'bezisync',
      client_secret: 'dex-e2e-secret',
      authorization_url: `${DEX}/auth`,
      token_url: `${DEX}/token`,
      userinfo_url: `${DEX}/userinfo`,
      scopes: ['openid', 'email', 'profile', 'groups'],
      role_rules: [{ group: 'authors', project: 'default', role: 'viewer' }],
      default_role: null,
    },
    201,
  );
  const signInWith = async (label: string) => {
    const context = await browser.newContext();
    const p = await context.newPage();
    await p.goto('/login?next=%2Fp%2Fdefault%2Fpipelines');
    await p.getByRole('link', { name: `Sign in with ${label}` }).click();
    await p.waitForURL(/\/(p\/default\/pipelines|login)/);
    const url = p.url();
    await context.close();
    return url;
  };
  // Kilgore already signs in with the OpenID Connect option: refused until an Admin allows the move.
  expect(decodeURIComponent(await signInWith('Dex OAuth2'))).toContain('another sign-in option');
  await admin.patch(`/api/v1/sso-providers/${oauth.id}`, { link_existing: true }, oauth.version);
  expect(await signInWith('Dex OAuth2')).toMatch(/\/p\/default\/pipelines/);
  // And back: the OpenID Connect option takes over again only when allowed.
  const oidc = (await admin.get<Provider[]>('/api/v1/sso-providers')).find((p) => p.label === 'Example IdP')!;
  await admin.patch(`/api/v1/sso-providers/${oidc.id}`, { link_existing: true }, oidc.version);
  expect(await signInWith('Example IdP')).toMatch(/\/p\/default\/pipelines/);
  await admin.send('DELETE', `/api/v1/sso-providers/${oauth.id}`, undefined, 204);
});

test('a failed SSO sign-in explains what went wrong', async ({ page }) => {
  await page.goto('/api/v1/auth/oidc/callback?code=bogus&state=forged');
  await expect(page).toHaveURL(/\/login\?error=/);
  await expect(page.getByText('The sign-in session expired. Start again.')).toBeVisible();
});

test('the audit log can be exported', async ({ page }) => {
  await signIn(page);
  await page.goto('/p/default/settings/audit');
  const [download] = await Promise.all([page.waitForEvent('download'), page.getByRole('link', { name: 'Export CSV' }).click()]);
  expect(download.suggestedFilename()).toBe('bezisync-audit-default.csv');
});
