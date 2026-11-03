import { Lightbulb, MessageSquare, ShieldAlert, TriangleAlert, type LucideIcon } from 'lucide-react';
import type { Schemas } from './api';

export type FeedbackKind = Schemas['FeedbackCreate']['kind'];
export type Feedback = Schemas['FeedbackRead'];

type Tone = 'bad' | 'warn' | 'info' | 'grey';

/** Mirrors services/feedback.LABELS on the backend, plus what the form says about each kind. */
export const FEEDBACK_KINDS: { kind: FeedbackKind; label: string; short: string; help: string; placeholder: string; tone: Tone; icon: LucideIcon }[] = [
  {
    kind: 'bug',
    label: 'Something is broken',
    short: 'Broken',
    help: 'An error, a wrong result, a dead end',
    placeholder: 'What did you do, what happened, and what did you expect?',
    tone: 'bad',
    icon: TriangleAlert,
  },
  {
    kind: 'security',
    label: 'Security issue',
    short: 'Security',
    help: 'Seeing or doing what you should not',
    placeholder: 'Steps to reproduce, what you could see or do, and how bad you think it is.',
    tone: 'warn',
    icon: ShieldAlert,
  },
  {
    kind: 'idea',
    label: 'Idea',
    short: 'Idea',
    help: 'Something you wish BeziSync did',
    placeholder: 'What were you trying to do? What would have helped?',
    tone: 'info',
    icon: Lightbulb,
  },
  {
    kind: 'other',
    label: 'Other',
    short: 'Other',
    help: 'Confusing wording, questions, praise',
    placeholder: 'Tell us anything.',
    tone: 'grey',
    icon: MessageSquare,
  },
];

export const MAX_FEEDBACK = 5000;

export function feedbackKind(kind: string) {
  return FEEDBACK_KINDS.find((k) => k.kind === kind) ?? FEEDBACK_KINDS[3]!;
}

const BROWSERS: [RegExp, string][] = [
  [/Edg\/(\d+)/, 'Edge'],
  [/OPR\/(\d+)/, 'Opera'],
  [/Firefox\/(\d+)/, 'Firefox'],
  [/Chrome\/(\d+)/, 'Chrome'],
  [/Version\/(\d+)[.\d]* .*Safari/, 'Safari'],
];
const SYSTEMS: [RegExp, string][] = [
  [/Android/, 'Android'],
  [/iPhone|iPad/, 'iOS'],
  [/Windows/, 'Windows'],
  [/Mac OS X|Macintosh/, 'macOS'],
  [/CrOS/, 'ChromeOS'],
  [/Linux/, 'Linux'],
];

/** "Chrome 131, macOS" from a user-agent string; unknown parts are left out. */
export function describeBrowser(userAgent: string): string {
  const browser = BROWSERS.map(([re, name]) => {
    const m = re.exec(userAgent);
    return m ? `${name} ${m[1]}` : null;
  }).find(Boolean);
  const system = SYSTEMS.find(([re]) => re.test(userAgent))?.[1];
  return [browser, system].filter(Boolean).join(', ') || 'Unknown browser';
}
