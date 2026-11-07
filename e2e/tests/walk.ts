import { expect, type Page, type Request } from '@playwright/test';
import type { Api } from './helpers';

/** The wizard's steps, in order (Source is only shown for a new draft). */
export const WIZARD_STEPS = ['tables', 'sink', 'transform', 'review'] as const;
export type WizardStep = (typeof WIZARD_STEPS)[number];

interface SavedSpec { spec?: unknown }

/**
 * Opens every wizard step of a pipeline and proves autosave round-trips: no step shows a save
 * error, Review settles on "All changes saved", the last save's spec equals what the server
 * returns, and nothing saves again once settled (a client/server shape mismatch loops forever).
 */
export async function walkWizard(page: Page, api: Api, opts: { projectId: string; slug: string; pipelineId: string; steps?: readonly WizardStep[] }): Promise<void> {
  const saves: SavedSpec[] = [];
  const onRequest = (r: Request) => {
    if (r.method() === 'PATCH' && r.url().endsWith(`/projects/${opts.projectId}/pipelines/${opts.pipelineId}`)) {
      saves.push((r.postDataJSON() ?? {}) as SavedSpec);
    }
  };
  page.on('request', onRequest);
  try {
    for (const step of opts.steps ?? WIZARD_STEPS) {
      await page.goto(`/p/${opts.slug}/pipelines/new?draft=${opts.pipelineId}&step=${step}`);
      await expect(page).toHaveURL(new RegExp(`step=${step}`));
      await expect(page.getByRole('heading', { level: 1 })).toBeVisible();
      await expect(page.getByText(/Couldn.t save|Not saved/)).toHaveCount(0);
    }
    if ((opts.steps ?? WIZARD_STEPS).includes('review')) {
      await expect(page.getByText('All changes saved')).toBeVisible({ timeout: 20_000 });
    }
    const settled = saves.length;
    await page.waitForTimeout(4_000);
    expect(saves.length, 'the wizard kept saving after it settled (autosave loop)').toBe(settled);
    const last = [...saves].reverse().find((s) => s.spec !== undefined);
    if (last) {
      const server = await api.get<{ spec: unknown }>(`/api/v1/projects/${opts.projectId}/pipelines/${opts.pipelineId}`);
      expect(server.spec, 'the last autosave differs from what the server stored').toEqual(last.spec);
    }
    // Clicking Continue through every step of a settled pipeline saves nothing: each step's draft
    // equals the server's copy (key order included) when nothing changed (ADR 0007).
    if (!opts.steps) {
      const before = saves.length;
      await page.goto(`/p/${opts.slug}/pipelines/new?draft=${opts.pipelineId}&step=tables`);
      for (const next of ['sink', 'transform', 'review']) {
        await page.waitForLoadState('networkidle');
        await page.getByRole('button', { name: /^Continue/ }).click();
        await expect(page).toHaveURL(new RegExp(`step=${next}`));
      }
      await page.waitForLoadState('networkidle');
      expect(saves.slice(before).map((s) => JSON.stringify(s.spec ?? s).slice(0, 400)), 'a step saved an unchanged pipeline').toEqual([]);
    }
  } finally {
    page.off('request', onRequest);
  }
}
