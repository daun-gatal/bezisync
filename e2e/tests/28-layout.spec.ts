import { expect, test, type Page } from '@playwright/test';
import { Api, signIn } from './helpers';
import { KAFKA, SOURCE_PG } from './fixtures';
import { layoutProblems } from './layout';

/**
 * Layout sweep with stress data: names at their longest, with no spaces to break at. Every
 * main screen is checked at phone, tablet and laptop widths for overflow, sideways scrolling,
 * font sizes off the scale and squeezed text (see layout.ts).
 *
 * The fixtures live in their own project and are created once, so reruns reuse them.
 */
const SLUG = 'layout-stress';
const PROJECT = 'Payments platform engineering — EU West production (layout stress project)';
const SOURCE = 'shop-db-primary-cluster-eu-west-1-read-replica-for-cdc-capture'; // names stop at 63 characters
const SINK = 'analytics-kafka-cluster-eu-west-1-msk-serverless-private-link';
const PIPELINE = 'orders-postgres-primary-to-kafka-analytics-pii-masking-v2-eu1';
const OWNER = 'payments-platform-data-engineering-oncall-rotation-emea';

const WIDTHS = [390, 768, 1280];

interface Seed {
  pipelineId: string;
}

async function seed(): Promise<Seed> {
  const api = await Api.signIn();
  const projects = await api.get<{ id: string; slug: string }[]>('/api/v1/projects');
  const project = projects.find((p) => p.slug === SLUG) ?? (await api.send<{ id: string }>('POST', '/api/v1/projects', { slug: SLUG, name: PROJECT }, 201));
  const base = `/api/v1/projects/${project.id}`;
  const conns = await api.get<{ id: string; name: string }[]>(`${base}/connections`);
  const { password, ...pg } = SOURCE_PG;
  const source =
    conns.find((c) => c.name === SOURCE) ??
    (await api.send<{ id: string }>(
      'POST',
      `${base}/connections`,
      { kind: 'source', type: 'postgresql', name: SOURCE, config: { ...pg, ssl_mode: 'disable' }, secrets: { password } },
      201,
    ));
  const sink =
    conns.find((c) => c.name === SINK) ??
    (await api.send<{ id: string }>('POST', `${base}/connections`, { kind: 'sink', type: 'kafka', name: SINK, config: KAFKA }, 201));
  const pipes = await api.get<{ id: string; name: string }[]>(`${base}/pipelines`);
  const pipeline =
    pipes.find((p) => p.name === PIPELINE) ??
    (await api.send<{ id: string }>(
      'POST',
      `${base}/pipelines`,
      {
        name: PIPELINE,
        owner: OWNER,
        description: `Streams every order change from ${SOURCE} into ${SINK} with masking applied to customer_email_address_normalized.`,
        source_id: source.id,
        sink_id: sink.id,
        spec: { source: { tables: { include: ['shop.orders', 'shop.customers'] } } },
      },
      201,
    ));
  return { pipelineId: pipeline.id };
}

const SCREENS: { name: string; path: (s: Seed) => string }[] = [
  { name: 'pipelines list', path: () => `/p/${SLUG}/pipelines` },
  { name: 'pipeline overview', path: (s) => `/p/${SLUG}/pipelines/${s.pipelineId}` },
  { name: 'pipeline tables', path: (s) => `/p/${SLUG}/pipelines/${s.pipelineId}?tab=tables` },
  { name: 'pipeline config', path: (s) => `/p/${SLUG}/pipelines/${s.pipelineId}?tab=config` },
  { name: 'new pipeline', path: () => `/p/${SLUG}/pipelines/new` },
  { name: 'connections', path: () => `/p/${SLUG}/connections` },
  { name: 'alerts', path: () => `/p/${SLUG}/alerts` },
  { name: 'settings: people', path: () => `/p/${SLUG}/settings/people` },
  { name: 'settings: runtimes', path: () => `/p/${SLUG}/settings/runtimes` },
  { name: 'settings: policies', path: () => `/p/${SLUG}/settings/policies` },
  { name: 'settings: sign-in', path: () => `/p/${SLUG}/settings/sign-in` },
  { name: 'settings: mcp', path: () => `/p/${SLUG}/settings/mcp` },
  { name: 'account', path: () => `/p/${SLUG}/account` },
  { name: 'learn', path: () => `/p/${SLUG}/learn` },
];

test.describe.configure({ mode: 'serial' });

let fixtures: Seed;
test.beforeAll(async () => {
  fixtures = await seed();
});

async function sweep(page: Page, path: string): Promise<string[]> {
  const found: string[] = [];
  for (const width of WIDTHS) {
    await page.setViewportSize({ width, height: 900 });
    await page.goto(path);
    await page.waitForLoadState('networkidle');
    await expect(page.getByRole('heading', { level: 1 }).first()).toBeVisible();
    found.push(...(await layoutProblems(page)).map((p) => `${width}px ${p}`));
  }
  return found;
}

for (const screen of SCREENS) {
  test(`layout holds with long names: ${screen.name}`, async ({ page }) => {
    await signIn(page);
    const found = await sweep(page, screen.path(fixtures));
    if (found.length) console.log(`${screen.name}:\n  ${found.slice(0, 40).join('\n  ')}`);
    expect(found).toEqual([]);
  });
}

// Wide monitors: the root steps up (14 px base text at a laptop, 15 px from 1920 px, 16 px from
// 2560 px), every size follows it, and the page uses the width up to 120rem.
for (const [width, base] of [[1440, 14], [1920, 15], [2560, 16]] as const) {
  test(`wide screens scale the interface together: ${width} px`, async ({ page }) => {
    await signIn(page);
    await page.setViewportSize({ width, height: 1080 });
    const found: string[] = [];
    for (const path of [`/p/${SLUG}/pipelines`, `/p/${SLUG}/pipelines/${fixtures.pipelineId}`, `/p/${SLUG}/connections`, `/p/${SLUG}/settings/people`]) {
      await page.goto(path);
      await page.waitForLoadState('networkidle');
      await expect(page.getByRole('heading', { level: 1 }).first()).toBeVisible();
      found.push(...(await layoutProblems(page)).map((p) => `${path} ${p}`));
    }
    expect(found).toEqual([]);
    const body = await page.evaluate(() => parseFloat(getComputedStyle(document.body).fontSize));
    expect(body).toBeCloseTo(base, 1);
    // The content column fills the screen beside the sidebar instead of stopping at 1400 px.
    const main = await page.locator('#main > div').first().boundingBox();
    const rail = await page.locator('aside').first().boundingBox();
    expect(main!.width).toBeGreaterThan(width - (rail?.width ?? 0) - 100);
  });
}

test('layout holds with long names: sign-in page', async ({ page }) => {
  await page.goto('/login');
  const found: string[] = [];
  for (const width of WIDTHS) {
    await page.setViewportSize({ width, height: 900 });
    found.push(...(await layoutProblems(page)).map((p) => `${width}px ${p}`));
  }
  expect(found).toEqual([]);
});
