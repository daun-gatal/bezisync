import { defineConfig } from '@playwright/test';

const baseURL = process.env.BASE_URL ?? 'http://localhost:8080';

export default defineConfig({
  testDir: './tests',
  timeout: 120_000,
  expect: { timeout: 20_000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [['list'], ['html', { open: 'never', outputFolder: 'playwright-report' }]],
  use: {
    baseURL,
    // Lists page 20 rows by default. Specs share one growing database, so they read lists 100 rows
    // at a time (the viewer's own choice, bz.pageSize); 23-admin checks the default and the pagers.
    storageState: { cookies: [], origins: [{ origin: baseURL, localStorage: [{ name: 'bz.pageSize', value: '100' }] }] },
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    viewport: { width: 1440, height: 900 },
    // The SSO fixture's issuer is a cluster-internal name; reach it through the k3s node.
    launchOptions: { args: ['--host-resolver-rules=MAP dex.bezisync-e2e-data.svc.cluster.local bezisync-e2e-k3s'] },
  },
});
