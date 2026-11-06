import { ImpactPill } from '@/components/SettingsEditor';
import { Meta } from '@/components/ui/text';
import type { Schemas } from '@/lib/api';
import { IMPACT, IMPACT_ORDER } from '@/lib/settings';

export type SettingsChange = Schemas['SettingsChange'];
type Change = SettingsChange['settings'][number];

const shown = (v: string | null | undefined) => (v === null || v === undefined ? 'default' : v);

/** One setting's change in words: "Snapshot threads: default → 4". */
export function changeText(c: Change): string {
  return `${c.title}: ${shown(c.from)} → ${shown(c.to)}${c.unverified ? ' (not verified by BeziSync)' : ''}`;
}

/** The lines a deploy confirmation lists for its settings changes, heaviest first. */
export function settingsChangeLines(change: SettingsChange | null | undefined): string[] {
  if (!change) return [];
  return [...change.settings]
    .sort((a, b) => IMPACT_ORDER.indexOf(b.impact) - IMPACT_ORDER.indexOf(a.impact))
    .map((c) => `${changeText(c)} — ${IMPACT[c.impact].long.toLowerCase()}.`);
}

/** What a deploy changes in the settings, grouped by what each change does, heaviest first. */
export function SettingsChangeList({ change }: { change: SettingsChange }) {
  const groups = [...IMPACT_ORDER].reverse().filter((i) => change.settings.some((c) => c.impact === i));
  return (
    <div className="flex min-w-0 flex-col gap-3">
      {groups.map((impact) => (
        <section key={impact} aria-label={IMPACT[impact].long} className="flex min-w-0 flex-col gap-1.5">
          <div className="flex flex-wrap items-center gap-2">
            <ImpactPill impact={impact} />
            <Meta>{IMPACT[impact].long}</Meta>
          </div>
          <ul className="m-0 flex list-disc flex-col gap-0.5 pl-5 text-sm text-ink-2 wrap-anywhere">
            {change.settings
              .filter((c) => c.impact === impact)
              .map((c) => (
                <li key={c.key}>
                  <strong className="font-semibold">{c.title}</strong>
                  {c.title !== c.key ? <span className="font-mono text-xs text-muted"> {c.key}</span> : null}: {shown(c.from)} → {shown(c.to)}
                  {c.unverified ? <span className="text-muted"> · not verified by BeziSync</span> : null}
                </li>
              ))}
          </ul>
        </section>
      ))}
    </div>
  );
}
