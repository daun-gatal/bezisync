import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';

/**
 * Layout and UX rules that are cheaper to enforce on the source than to find on screen.
 * Screens build on the components in components/ui, which are the only place allowed to
 * break these rules (they implement them). A failure names the file and how many times.
 */
export const RULES: Record<string, { why: string; find: (src: string) => number }> = {
  'bare-fr': {
    why: 'Grid tracks in fr without minmax(0, …) cannot shrink below their content, so long names push the layout wide.',
    find: (src) => [...src.matchAll(/grid-cols-\[([^\]]+)\]/g)].filter((m) => m[1]!.split('_').some((t) => /^\d*\.?\d+fr$/.test(t))).length,
  },
  'fixed-min-width': {
    why: 'A min-w-[…px] table forces sideways scrolling on laptops; columns hide by priority instead.',
    find: (src) => (src.match(/min-w-\[\d+px\]/g) ?? []).length,
  },
  'raw-h1': {
    why: 'Page titles go through PageHeader so they wrap, clamp and carry the full text as a tooltip.',
    find: (src) => (src.match(/<h1[\s>]/g) ?? []).length,
  },
  'toast-error': {
    why: 'Errors appear where they happened, with the request ID; toasts only confirm successes.',
    find: (src) => (src.match(/toast\.error\(/g) ?? []).length,
  },
  'arbitrary-text': {
    why: 'Type sizes come from the scale (text-xs … text-2xl); arbitrary sizes drift.',
    find: (src) => (src.match(/text-\[\d+(\.\d+)?px\]/g) ?? []).length,
  },
  'arbitrary-padding': {
    why: 'Padding comes from the components and the spacing scale (p-card, px-3 …), never arbitrary values, so cards and rows line up.',
    find: (src) => (src.match(/\b(p|px|py|pt|pb|pl|pr)-\[[^\]]+\]/g) ?? []).length,
  },
  'native-control': {
    why: 'Checkboxes, radios and selects use Check, Switch, RadioCards and Select, which explain why they are disabled.',
    find: (src) => (src.match(/type="(checkbox|radio)"|<select[\s>]/g) ?? []).length,
  },
  'link-as-button': {
    why: 'Links that look like buttons use ButtonLink, so there is one button style.',
    find: (src) => (src.match(/<(Link|a)\b[^>]*?className="[^"]*\b(h-8|h-9|h-10)\b[^"]*"/g) ?? []).length,
  },
};

const ROOT = join(__dirname, '..');

function files(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === '__tests__' || name === 'test' ? [] : files(path);
    return path.endsWith('.tsx') ? [path] : [];
  });
}

export function scan(): Record<string, Record<string, number>> {
  const found: Record<string, Record<string, number>> = {};
  for (const path of files(ROOT)) {
    const rel = relative(ROOT, path);
    if (rel.startsWith('components/ui/')) continue;
    const src = readFileSync(path, 'utf8');
    for (const [rule, { find }] of Object.entries(RULES)) {
      const n = find(src);
      if (n) (found[rule] ??= {})[rel] = n;
    }
  }
  return found;
}

describe('source rules', () => {
  const found = scan();
  for (const [rule, { why }] of Object.entries(RULES)) {
    it(`${rule}: ${why}`, () => {
      expect(found[rule] ?? {}).toEqual({});
    });
  }

  it('catches the patterns it is meant to', () => {
    expect(RULES['bare-fr']!.find('grid-cols-[320px_1fr]')).toBe(1);
    expect(RULES['bare-fr']!.find('grid-cols-[minmax(0,1fr)_auto]')).toBe(0);
    expect(RULES['arbitrary-text']!.find('text-[13.5px]')).toBe(1);
    expect(RULES['arbitrary-padding']!.find('px-3.5 p-[3px] px-4 py-[7px]')).toBe(2);
    expect(RULES['link-as-button']!.find('<Link to="/x" className="inline-flex h-9 rounded-lg">')).toBe(1);
    expect(RULES['native-control']!.find('<input type="checkbox" />')).toBe(1);
  });
});
