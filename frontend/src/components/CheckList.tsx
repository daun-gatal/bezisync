import { useState } from 'react';
import { CheckCircle2, CircleHelp, CircleSlash, Mail, RefreshCw, TriangleAlert, XCircle } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Pill } from '@/components/ui/feedback';
import { CodeBlock } from '@/components/ui/overlays';
import type { Schemas } from '@/lib/api';
import { ago } from '@/lib/format';

export type CheckReport = Schemas['CheckReport'];
export type Check = Schemas['Check'];

const ICON = {
  pass: <CheckCircle2 className="size-4 text-ok" aria-label="Passed" />,
  warn: <TriangleAlert className="size-4 text-warn" aria-label="Warning" />,
  fail: <XCircle className="size-4 text-bad" aria-label="Failed" />,
  skip: <CircleSlash className="size-4 text-muted" aria-label="Skipped" />,
  unknown: <CircleHelp className="size-4 text-grey" aria-label="Not verified" />,
} as const;

const CATEGORY: Record<string, string> = {
  network: 'network',
  auth: 'sign-in',
  cdc_setup: 'CDC setup',
  permissions: 'permissions',
  capacity: 'capacity',
  tables: 'tables',
  resilience: 'resilience',
  destination: 'destination',
  policy: 'project policy',
};

export function reportBadge(report: CheckReport | null | undefined) {
  if (!report) return <Pill tone="grey">Not checked</Pill>;
  if (!report.ok) return <Pill tone="bad">{report.summary}</Pill>;
  if (report.checks.some((c) => c.status === 'warn')) return <Pill tone="warn">{report.summary}</Pill>;
  // Something BeziSync can't verify is never shown as healthy.
  if (report.checks.some((c) => c.status === 'unknown')) return <Pill tone="grey">{report.summary}</Pill>;
  return <Pill tone="ok">Ready</Pill>;
}

function dbaMessage(check: Check, subject: string): string {
  return [
    `Hi — BeziSync needs a change on ${subject} before we can stream changes from it.`,
    '',
    `${check.title}: ${check.message}`,
    check.technical ? `Details: ${check.technical}` : '',
    check.fix ? `\nPlease run:\n\n${check.fix.code}\n${check.fix.restart_required ? '\n(This needs a restart — please schedule it.)' : ''}` : '',
  ]
    .filter(Boolean)
    .join('\n');
}

export function CheckList({
  report,
  expert,
  onRecheck,
  rechecking,
  subject,
  title = 'Readiness checks',
}: {
  report: CheckReport;
  expert: boolean;
  onRecheck?: () => void;
  rechecking?: boolean;
  subject: string;
  title?: string;
}) {
  const [showPassed, setShowPassed] = useState(false);
  const order = { fail: 0, warn: 1, unknown: 2, pass: 3, skip: 4 } as const;
  const sorted = [...report.checks].sort((a, b) => order[a.status] - order[b.status]);
  const visible = showPassed ? sorted : sorted.filter((c) => c.status !== 'pass' && c.status !== 'skip');
  const passed = report.checks.filter((c) => c.status === 'pass').length;

  return (
    <section aria-label={title} className="flex min-w-0 flex-col gap-1">
      <div className="flex flex-wrap items-center justify-between gap-2 pb-2">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <strong className="min-w-0 text-lg font-semibold wrap-anywhere">{title}</strong>
          {reportBadge(report)}
        </div>
        <div className="flex items-center gap-2 text-xs text-muted">
          checked {ago(report.checked_at)}
          {onRecheck ? (
            <Button size="sm" icon={<RefreshCw />} loading={rechecking} onClick={onRecheck}>
              Re-check
            </Button>
          ) : null}
        </div>
      </div>
      {visible.map((c) => (
        <article
          key={c.code}
          className={`grid grid-cols-[22px_minmax(0,1fr)] gap-x-2.5 gap-y-2 border-t border-line py-3 text-base ${c.status === 'fail' ? 'rounded-md border-transparent bg-bad-soft/40 px-2' : ''}`}
        >
          <span className="pt-0.5">{ICON[c.status]}</span>
          <div className="flex min-w-0 flex-col gap-1.5">
            <div className="flex flex-wrap items-baseline gap-x-2">
              <strong>{c.title}</strong>
              <span className="text-xs text-muted">
                {CATEGORY[c.category] ?? c.category}
                {c.blocking ? ' · blocks deploy' : ''}
              </span>
            </div>
            <span className="text-ink-2">{c.message}</span>
            {(expert || c.status === 'fail') && c.technical ? <span className="font-mono text-xs wrap-anywhere text-muted">{c.technical}</span> : null}
            {c.why && c.status !== 'pass' ? <span className="text-xs text-muted">{c.why}</span> : null}
            {c.doc && c.status !== 'pass' ? (
              <a href={c.doc} target="_blank" rel="noreferrer" className="text-xs font-medium text-accent hover:underline">
                Learn more about {c.code}
              </a>
            ) : null}
            {c.fix && c.status !== 'pass' ? (
              <div className="flex flex-col gap-2">
                <CodeBlock code={c.fix.code} label={`Fix for ${c.title}`} />
                <div className="flex flex-wrap items-center gap-2 text-xs text-muted">
                  {c.fix.restart_required ? <Pill tone="warn">needs a restart</Pill> : null}
                  <span>Usually done by: {c.fix.who}</span>
                  <Button
                    size="sm"
                    variant="ghost"
                    icon={<Mail />}
                    onClick={() => {
                      void navigator.clipboard?.writeText(dbaMessage(c, subject));
                      toast.success('Message for your DBA copied — paste it into email or chat.');
                    }}
                  >
                    Send to my DBA
                  </Button>
                </div>
              </div>
            ) : null}
          </div>
        </article>
      ))}
      {passed ? (
        <button
          type="button"
          onClick={() => setShowPassed((v) => !v)}
          className="self-start border-t-0 py-1 text-xs font-medium text-accent hover:text-accent-hover"
        >
          {showPassed ? 'Hide passed checks' : `Show ${passed} passed check${passed === 1 ? '' : 's'}`}
        </button>
      ) : null}
    </section>
  );
}
