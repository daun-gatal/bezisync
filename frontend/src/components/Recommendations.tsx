import { Button } from '@/components/ui/button';
import { Notice } from '@/components/ui/feedback';
import type { Schemas } from '@/lib/api';
import { settingText } from '@/components/SettingsEditor';
import type { Setting } from '@/lib/settings';

export type Recommendation = Schemas['Recommendation'];

/** How long a recommendation's evidence holds (services.RecommendationFreshness). */
export const FRESHNESS_MS = 10 * 60_000;

/** The recommendations still worth showing: fresh evidence, a setting this pipeline can use,
 *  and not already chosen. */
export function freshRecommendations(recs: Recommendation[] | undefined, settings: Setting[], tuning: Record<string, string>, now = Date.now()): Recommendation[] {
  return (recs ?? []).filter((r) => now - Date.parse(r.evidence_at) < FRESHNESS_MS && settings.some((s) => s.key === r.key) && !(r.key in tuning));
}

/** Suggested settings from preflight, each with why and one click to use it. */
export function Recommendations({ recs, settings, onUse }: { recs: Recommendation[]; settings: Setting[]; onUse: (key: string, value: string) => void }) {
  if (!recs.length) return null;
  return (
    <Notice tone="info" title={recs.length === 1 ? 'A suggested setting' : `${recs.length} suggested settings`}>
      <ul className="m-0 flex list-none flex-col gap-2 p-0">
        {recs.map((r) => {
          const s = settings.find((x) => x.key === r.key)!;
          return (
            <li key={r.key} className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
              <span className="min-w-0 flex-[1_1_260px]">
                <strong>
                  {s.title}: {settingText(s, r.value)}
                </strong>{' '}
                — {r.why}
              </span>
              <Button size="sm" aria-label={`Use ${s.title} ${settingText(s, r.value)}`} onClick={() => onUse(r.key, r.value)}>
                Use
              </Button>
            </li>
          );
        })}
      </ul>
    </Notice>
  );
}
