// Builds the BeziSync website into a static directory: the landing page and the docs.
//
//   bun src/build.ts [--release] [--ref <git ref>] [--out dist]
//
// The docs are this repository's own, read from the same checkout: docs/README.md (the index:
// its "##" sections and their links are the sidebar and the docs home's cards), docs/guide/*.md
// (one page each), docs/images/ (the screenshots), docs/reference/checks.md,
// docs/security/threat-model.md and CHANGELOG.md. Links between them become site URLs; links to
// any other file of the repository go to GitHub. --release says the checkout is a release (the
// pages say "Docs for vX.Y.Z" and link GitHub at its tag); without it they say the docs include
// unreleased changes and link GitHub at --ref (default preview). Anything that would publish a
// broken page (a link to a missing page or anchor, a missing or unused image, a guide page not in
// the index, an SEO override that no longer holds) fails the build.
import { Marked, type Token, type Tokens } from 'marked';
import hljs from 'highlight.js/lib/common';
import { copyFile, cp, mkdir, readFile, readdir, rm, stat, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { landing } from './landing';
import { SITE, REPO, layout, esc, type NavGroup } from './layout';
import { OVERRIDES, autoDescription, breadcrumbs, docTitle, jsonLdScript, overrideProblem, DESCRIPTION_MAX, DESCRIPTION_MIN } from './seo';

const argv = process.argv.slice(2);
const args = Object.fromEntries(argv.reduce<[string, string][]>((acc, a, i, all) => (a.startsWith('--') ? [...acc, [a.slice(2), all[i + 1] ?? '']] : acc), []));
const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..', '..'); // the repository
const SITE_DIR = path.join(ROOT, 'website');
const RELEASE = argv.includes('--release');
const REF = args.ref || 'preview';
const OUT = path.resolve(args.out || path.join(SITE_DIR, 'dist'));

const errors: string[] = [];
const fail = (msg: string) => errors.push(msg);

// --- the version the docs describe ----------------------------------------------------------

const versionGo = await readFile(path.join(ROOT, 'backend', 'internal', 'version', 'version.go'), 'utf8');
const appVersion = versionGo.match(/Version = "([^"]+)"/)?.[1];
if (!appVersion) fail('backend/internal/version/version.go: no Version');
const tag = `v${appVersion}`;
const GITHUB_REF = RELEASE ? tag : REF;
const version = RELEASE ? tag : `${tag} + unreleased changes`;
const changelogText = await readFile(path.join(ROOT, 'CHANGELOG.md'), 'utf8');
const latest = changelogText.match(/^## \[(v\d+\.\d+\.\d+)\]/m)?.[1];
// When the docs were released: the sitemap's lastmod (none for unreleased docs).
const released = RELEASE ? changelogText.match(new RegExp(`^## \\[${tag.replace(/\./g, '\\.')}\\] - (\\d{4}-\\d{2}-\\d{2})`, 'm'))?.[1] : undefined;
if (RELEASE && latest !== tag) fail(`--release: CHANGELOG.md's latest release is ${latest}, the app version is ${tag}`);

// --- headings and links -------------------------------------------------------------------

/** GitHub's heading anchors: lower case, punctuation dropped (not - or _), spaces to hyphens. */
function slugify(text: string): string {
  return text
    .toLowerCase()
    .trim()
    .replace(/<[^>]+>/g, '')
    .replace(/[^\p{L}\p{N}\s_-]/gu, '')
    .replace(/\s/g, '-');
}

/** A heading's text without markdown: links keep their text, code and emphasis lose their marks. */
function plain(md: string): string {
  return md
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/[`*~]/g, '')
    .replace(/<[^>]+>/g, '')
    .trim();
}

type Heading = { id: string; text: string; depth: number };
type Page = {
  url: string; // /docs/install/
  file: string; // repository path the content comes from
  title: string;
  group: string;
  tokens: Token[];
  headings: Heading[];
  description: string; // for search engines
  card: string; // on the /docs/ cards
  searchTitle: string; // the <title>
  parent?: { url: string; title: string }; // a page under another (a group of checks)
  source?: string; // the markdown an override must match, when not the page's own tokens
  checks?: string[]; // a check page: the names of its checks
  engines?: string; // a check page: which engines read or write it
  moved?: Record<string, string>; // anchors that used to be on this page → where they are now
};

/** Gives every heading of a document its GitHub anchor (unique within the document). */
function anchor(tokens: Token[]): void {
  const seen = new Map<string, number>();
  for (const t of tokens) {
    if (t.type !== 'heading') continue;
    const base = slugify(plain((t as Tokens.Heading).text));
    const n = seen.get(base) ?? 0;
    seen.set(base, n + 1);
    (t as Tokens.Heading & { id: string }).id = n === 0 ? base : `${base}-${n}`;
  }
}

function headingsOf(tokens: Token[]): Heading[] {
  return tokens
    .filter((t): t is Tokens.Heading & { id: string } => t.type === 'heading')
    .map((t) => ({ id: t.id, text: plain(t.text), depth: t.depth }));
}

function firstSentence(tokens: Token[]): string {
  const p = tokens.find((t) => t.type === 'paragraph' && !/^!\[/.test((t as Tokens.Paragraph).text)) as Tokens.Paragraph | undefined;
  const text = p ? plain(p.text).replace(/\s+/g, ' ') : '';
  const end = text.search(/[.!?](\s|$)/);
  return (end > 0 ? text.slice(0, end + 1) : text).slice(0, 200);
}

const lexer = new Marked({ gfm: true });

async function load(rel: string): Promise<Token[]> {
  const tokens = lexer.lexer(await readFile(path.join(ROOT, rel), 'utf8'));
  anchor(tokens);
  return [...tokens];
}

/** A document's body for the site: its "#" title and the GitHub-only "All documentation" footer dropped. */
function body(tokens: Token[]): Token[] {
  const out = tokens.filter((t) => !(t.type === 'heading' && t.depth === 1));
  const footer = (t: Token) => t.type === 'space' || t.type === 'hr' || /^\[All documentation\]\(\.\.\/README\.md\)$/.test(t.raw.trim());
  while (out.length && footer(out.at(-1)!)) out.pop();
  return out;
}

// --- the index: sections and pages --------------------------------------------------------

const INDEX = 'docs/README.md';
const indexTokens = await load(INDEX);
const intro: Token[] = [];
type Entry = { file: string; title: string; description: string; oldId?: string };
const sections: { title: string; id: string; entries: Entry[] }[] = [];
for (const t of indexTokens) {
  if (t.type === 'heading' && t.depth === 1) continue;
  if (t.type === 'heading' && t.depth === 2) {
    sections.push({ title: plain(t.text), id: (t as Tokens.Heading & { id: string }).id, entries: [] });
    continue;
  }
  if (!sections.length) {
    intro.push(t);
    continue;
  }
  if (t.type !== 'list') continue;
  for (const item of (t as Tokens.List).items) {
    // - <a id="old-anchor"></a>[Title](guide/page.md) — Description.
    const m = item.text.match(/^(?:<a id="([^"]+)"><\/a>)?\[([^\]]+)\]\(([^)#]+)\)\s+—\s+(.+)$/s);
    if (!m) {
      fail(`${INDEX}: a section item is not "[Title](file.md) — Description": ${item.text}`);
      continue;
    }
    sections.at(-1)!.entries.push({ oldId: m[1], title: m[2], file: path.posix.join('docs', m[3]), description: m[4].replace(/\s+/g, ' ').trim() });
  }
}

/** Where a docs file is published (reference pages keep the URLs they always had). */
const FIXED_URLS: Record<string, string> = {
  'docs/reference/checks.md': '/docs/reference/checks/',
  'docs/security/threat-model.md': '/docs/security/threat-model/',
  'CHANGELOG.md': '/docs/changelog/',
};
const urlOf = (file: string) => FIXED_URLS[file] ?? (/^docs\/guide\/[a-z0-9-]+\.md$/.test(file) ? `/docs/${path.posix.basename(file, '.md')}/` : undefined);

const pages: Page[] = [];
const nav: NavGroup[] = [];
const indexAnchors = new Map<string, string>(); // docs/README.md#<old section id> → page URL
for (const s of sections) {
  const group: NavGroup = { title: s.title, items: [] };
  for (const entry of s.entries) {
    const url = urlOf(entry.file);
    if (!url) continue; // listed in the index but read on GitHub (the ADRs)
    try {
      await stat(path.join(ROOT, entry.file));
    } catch {
      fail(`${INDEX}: lists ${entry.file}, which does not exist`);
      continue;
    }
    const tokens = body(await load(entry.file));
    pages.push({ url, file: entry.file, title: entry.title, group: s.title, tokens, headings: [], description: entry.description, card: entry.description, searchTitle: '' });
    group.items.push({ url, title: entry.title });
    if (entry.oldId) indexAnchors.set(entry.oldId, url);
  }
  nav.push(group);
}
// The changelog: not in the index (GitHub shows it at the repository's root), on the site under
// the last section, Reference.
const changelogPage: Page = {
  url: '/docs/changelog/', file: 'CHANGELOG.md', title: 'Changelog', group: 'Reference', tokens: body(await load('CHANGELOG.md')), headings: [],
  description: '', card: 'Every BeziSync release and what it changed.', searchTitle: '',
};
pages.push(changelogPage);
nav.at(-1)?.items.push({ url: changelogPage.url, title: changelogPage.title });

// Every guide page is in the index.
for (const f of await readdir(path.join(ROOT, 'docs', 'guide'))) {
  if (f.endsWith('.md') && !pages.some((p) => p.file === `docs/guide/${f}`)) fail(`docs/guide/${f} is not listed in ${INDEX}`);
}

// --- the preflight checks: one page per database or destination ---------------------------
//
// checks.md is generated from BeziSync's source: per source or destination ("##"), each check
// ("### `pg_wal_level`") with its name (**Logical replication**), why it matters and the fix.
// Each group with at least MIN_CHECKS checks gets its own page, its headings the checks' names
// (the id stays the anchor); /docs/reference/checks/ lists them all and keeps the smaller groups.

const CHECKS = 'docs/reference/checks.md';
const CHECKS_URL = '/docs/reference/checks/';
const MIN_CHECKS = 3;
const checkPages: Page[] = [];
/** Every check id (heading and HTML anchor) on a group page → that page, for old #links. */
const movedChecks: Record<string, string> = {};

const rawHtml = (raw: string): Token => ({ type: 'html', raw, text: raw, block: true, pre: false }) as Token;
const groupSlug = (name: string) => name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

/** A group's checks with their names as headings, shifted by `shift` levels. */
function namedChecks(tokens: Token[], shift: number): { tokens: Token[]; checks: { id: string; name: string }[] } {
  const out: Token[] = [];
  const checks: { id: string; name: string }[] = [];
  for (let i = 0; i < tokens.length; i++) {
    const t = tokens[i];
    if (t.type !== 'heading') {
      out.push(t);
      continue;
    }
    const h = t as Tokens.Heading & { id: string };
    let j = i + 1;
    while (tokens[j]?.type === 'space') j++;
    const name = tokens[j]?.type === 'paragraph' ? (tokens[j] as Tokens.Paragraph).text.match(/^\*\*([^*]+)\*\*$/)?.[1] : undefined;
    if (h.depth !== 3 || !name) {
      out.push({ ...h, depth: h.depth - shift } as Token);
      continue;
    }
    const code = plain(h.text);
    const named = lexer.lexer(`${'#'.repeat(3 - shift)} ${name} <code class="check-id">${esc(code)}</code>`)[0] as Tokens.Heading & { id: string };
    named.id = h.id;
    out.push(named);
    checks.push({ id: h.id, name });
    i = j; // the name is the heading now
  }
  return { tokens: out, checks };
}

const checksIndex = pages.find((p) => p.file === CHECKS);
if (!checksIndex) fail(`${INDEX} must list ${CHECKS}`);
else {
  checksIndex.source = checksIndex.tokens.map((t) => t.raw).join('');
  const groups: { name: string; heading: Token; tokens: Token[] }[] = [];
  const out: Token[] = [];
  for (const t of checksIndex.tokens) {
    if (t.type === 'heading' && t.depth === 2) groups.push({ name: plain(t.text), heading: t, tokens: [] });
    else (groups.at(-1)?.tokens ?? out).push(t);
  }
  for (const g of groups) {
    const count = g.tokens.filter((t) => t.type === 'heading' && t.depth === 3).length;
    // The reference's line saying which engines read or write it.
    const enginesToken = g.tokens.find((t) => t.type === 'paragraph' && /^\*(Read|Written) by .+\*$/.test((t as Tokens.Paragraph).text.trim()));
    const engines = enginesToken ? plain((enginesToken as Tokens.Paragraph).text) : undefined;
    out.push(g.heading);
    if (count < MIN_CHECKS) {
      out.push(...namedChecks(g.tokens, 0).tokens);
      continue;
    }
    if (enginesToken) out.push(enginesToken);
    const url = `${CHECKS_URL}${groupSlug(g.name)}/`;
    const { tokens, checks } = namedChecks(g.tokens, 1);
    for (const c of checks) movedChecks[c.id] = url;
    for (const t of g.tokens) for (const m of t.raw.matchAll(/<a id="([^"]+)"><\/a>/g)) movedChecks[m[1]] = url;
    const lead = rawHtml(
      `<p>The ${esc(g.name)} checks BeziSync runs before a pipeline deploys, each with why it matters and the fix. They are generated from BeziSync's source; <a href="${CHECKS_URL}">all preflight checks</a>.</p>`,
    );
    checkPages.push({
      url, file: CHECKS, title: `${g.name} checks`, group: 'Reference', tokens: [lead, ...tokens],
      headings: [], description: '', card: '', searchTitle: '', parent: { url: CHECKS_URL, title: checksIndex.title },
      checks: [...new Set(checks.map((c) => c.name))],
      engines,
    });
    out.push(
      rawHtml(
        `<p><a href="${url}">${count} ${esc(g.name)} checks and their fixes →</a></p><ul class="checks">${checks
          .filter((c, i) => checks.findIndex((o) => o.name === c.name) === i)
          .map((c) => `<li><a href="${url}#${c.id}">${esc(c.name)}</a></li>`)
          .join('')}</ul>`,
      ),
    );
  }
  checksIndex.moved = Object.fromEntries(Object.entries(movedChecks).map(([id, url]) => [id, `${url}#${id}`]));
  checksIndex.tokens = out;
}

// --- URLs that moved ------------------------------------------------------------------------
//
// The site used to cut one long guide into a page per section, at /docs/<section anchor>/. The
// guide's pages keep those URLs, except sections that were renamed (REDIRECTS: the old URL
// redirects) and sections whose parts became their own pages (SPLIT: their anchors forward from
// the page they used to be on).

const REDIRECTS: Record<string, string> = {
  '/docs/debezium-versions/': '/docs/engine-versions/',
  '/docs/flink-cdc-engine-optional/': '/docs/flink-cdc-engine/',
};
const SPLIT: Record<string, string[]> = {
  '/docs/install/': ['/docs/codespaces-lab/'],
  '/docs/destinations/': ['/docs/destinations-events/', '/docs/destinations-databases/', '/docs/destinations-lakehouse/', '/docs/destinations-analytics/'],
};

const index: Page = { url: '/docs/', file: INDEX, title: 'Documentation', group: '', tokens: intro, headings: [], description: '', card: '', searchTitle: '' };
const all = [index, ...pages, ...checkPages];
for (const p of all) p.headings = headingsOf(p.tokens);
for (const [from, targets] of Object.entries(SPLIT)) {
  const page = all.find((p) => p.url === from);
  if (!page) {
    fail(`SPLIT names ${from}, which is not a page`);
    continue;
  }
  page.moved ??= {};
  for (const url of targets) {
    const target = all.find((p) => p.url === url);
    if (!target) {
      fail(`SPLIT names ${url}, which is not a page`);
      continue;
    }
    page.moved[slugify(target.title)] = url;
    for (const h of target.headings) if (!page.headings.some((o) => o.id === h.id)) page.moved[h.id] ??= `${url}#${h.id}`;
  }
}
// The Codespaces lab was a section of Install under its full title.
(all.find((p) => p.url === '/docs/install/')?.moved ?? {})['run-your-own-lab-in-github-codespaces'] = '/docs/codespaces-lab/';

// --- titles and descriptions ----------------------------------------------------------------

for (const p of all) {
  if (!p.card) p.card = firstSentence(p.tokens) || p.title;
  if (!p.description) p.description = autoDescription(p.tokens, p.title);
  p.searchTitle = p === index ? 'BeziSync documentation' : docTitle(p.title);
  const o = OVERRIDES[p.url];
  if (!o) continue;
  const problem = overrideProblem(o, p.source ?? p.tokens.map((t) => t.raw).join(''));
  if (problem) {
    fail(`SEO override for ${p.url} (src/seo.ts): ${problem}`);
    continue;
  }
  p.description = o.description;
  if (o.title) p.searchTitle = o.title;
}
for (const url of Object.keys(OVERRIDES)) if (!all.some((p) => p.url === url)) fail(`SEO override for ${url} (src/seo.ts) matches no page`);
for (const p of pages) {
  if (p.description.length > DESCRIPTION_MAX || p.description.length < DESCRIPTION_MIN) {
    fail(`${p.url}: description is ${p.description.length} characters (${DESCRIPTION_MIN}-${DESCRIPTION_MAX}); change its line in ${INDEX} or add an override`);
  }
}
/** The source databases Debezium reads: their check pages say "for Debezium". */
const DEBEZIUM_SOURCES = new Set(['PostgreSQL', 'MySQL / MariaDB', 'SQL Server', 'MongoDB', 'Oracle', 'CockroachDB', 'Vitess', 'Google Cloud Spanner']);
/** Checks that read the same for every system: named last in a description. */
const GENERIC_CHECK = /^(Sign in|Reach |Talk to |Connect$|TLS$|Settings$|Credentials$|Events tab$|SSH)/;
for (const p of checkPages) {
  const name = p.title.replace(/ checks$/, '');
  const source = DEBEZIUM_SOURCES.has(name);
  const flink = /Flink CDC/.test(p.engines ?? '');
  const flinkOnly = /Flink CDC only/.test(p.engines ?? '');
  const engines = flinkOnly ? 'Flink CDC' : flink ? 'Debezium and Flink CDC' : 'Debezium';
  p.searchTitle = source
    ? `${name} for ${engines}: ${flink ? 'checks' : 'preflight checks'} and fixes`
    : flinkOnly
      ? `${name} for Flink CDC: preflight checks and fixes`
      : `${name}: CDC pipeline checks and fixes · BeziSync`;
  const names = p.checks ?? [];
  const specific = names.filter((n) => !GENERIC_CHECK.test(n));
  const ranked = specific.length >= 2 ? specific : [...specific, ...names.filter((n) => GENERIC_CHECK.test(n))];
  let d = source
    ? `Set up ${name} for ${engines}. What BeziSync checks before a pipeline deploys, with fixes: ${ranked[0]}`
    : `${name} checks BeziSync runs before a ${flinkOnly ? 'Flink CDC' : flink ? 'Debezium or Flink CDC' : 'CDC'} pipeline deploys, with fixes: ${ranked[0]}`;
  for (const n of ranked.slice(1)) if (`${d}, ${n}.`.length <= 160) d += `, ${n}`;
  p.description = `${d}.`;
}

// --- links ----------------------------------------------------------------------------------

// Where every anchor of every document lives on the site.
const anchors = new Map<string, string>(); // "file#id" -> "/docs/page/#id"
for (const p of [index, ...pages]) for (const h of p.headings) anchors.set(`${p.file}#${h.id}`, `${p.url}#${h.id}`);
for (const [id, url] of indexAnchors) anchors.set(`${INDEX}#${id}`, url);
for (const [id, url] of Object.entries(movedChecks)) anchors.set(`${CHECKS}#${id}`, `${url}#${id}`);
// Anchors a raw <a id="…"> sets in a page (the check reference has them).
for (const p of pages) for (const t of p.tokens) for (const m of t.raw.matchAll(/<a id="([^"]+)"><\/a>/g)) if (!anchors.has(`${p.file}#${m[1]}`)) anchors.set(`${p.file}#${m[1]}`, `${p.url}#${m[1]}`);
const fileUrls = new Map<string, string>([[INDEX, '/docs/'], ...pages.map((p): [string, string] => [p.file, p.url])]);

const IMAGES = 'docs/images/';
const images = new Set((await readdir(path.join(ROOT, IMAGES)).catch(() => [] as string[])).filter((f) => !f.startsWith('.') && !f.startsWith('_')));
const usedImages = new Set<string>();

/** A link or image in a document, made to work on the site. */
function resolve(href: string, from: string): string {
  if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith('//')) return href;
  const [rawPath, hash = ''] = href.split('#', 2);
  const target = rawPath ? path.posix.normalize(path.posix.join(path.posix.dirname(from), rawPath)) : from;
  if (target.startsWith(IMAGES)) {
    const name = target.slice(IMAGES.length);
    if (!images.has(name)) fail(`${from}: image ${href} does not exist`);
    usedImages.add(name);
    return `/docs/images/${name}`;
  }
  if (fileUrls.has(target)) {
    if (!hash) return fileUrls.get(target)!;
    const found = anchors.get(`${target}#${hash}`);
    if (found) return found;
    fail(`${from}: link to a missing anchor: ${href}`);
    return `${fileUrls.get(target)}#${hash}`;
  }
  if (/^docs\/(guide|reference|security)\//.test(target)) fail(`${from}: link to ${href}, which is not a page`);
  // Anything else in the repository: on GitHub, at the version the docs describe.
  return `${REPO}/blob/${GITHUB_REF}/${target.replace(/^\.\//, '')}${hash ? `#${hash}` : ''}`;
}

type Inline = { parser: { parseInline(t: Token[]): string } };
function renderer(from: string) {
  const md = new Marked({ gfm: true });
  md.use({
    renderer: {
      heading(this: Inline, token: Tokens.Heading & { id?: string }) {
        const inner = this.parser.parseInline(token.tokens);
        const id = token.id ?? slugify(plain(token.text));
        return `<h${token.depth} id="${id}"><a class="anchor" href="#${id}" aria-label="Link to this section">#</a>${inner}</h${token.depth}>\n`;
      },
      link(this: Inline, token: Tokens.Link) {
        const href = resolve(token.href, from);
        const external = /^https?:/.test(href) && !href.startsWith(SITE);
        const title = token.title ? ` title="${esc(token.title)}"` : '';
        const rel = external ? ' rel="noopener" target="_blank"' : '';
        return `<a href="${esc(href)}"${title}${rel}>${this.parser.parseInline(token.tokens)}</a>`;
      },
      image(token: Tokens.Image) {
        const src = resolve(token.href, from);
        // The screenshots are 1440×900 captures (scripts/docs-screenshots.sh).
        const size = src.startsWith('/docs/images/') ? ' width="1440" height="900"' : '';
        return `<img src="${esc(src)}" alt="${esc(token.text)}" loading="lazy"${size}>`;
      },
      paragraph(this: Inline, token: Tokens.Paragraph) {
        // A paragraph that is only an image is a figure, its alt text the caption.
        const only = token.tokens.length === 1 && token.tokens[0].type === 'image' ? (token.tokens[0] as Tokens.Image) : undefined;
        if (only) return `<figure class="shot">${this.parser.parseInline(token.tokens)}<figcaption>${esc(only.text)}</figcaption></figure>\n`;
        return `<p>${this.parser.parseInline(token.tokens)}</p>\n`;
      },
      code(token: Tokens.Code) {
        const lang = (token.lang ?? '').split(/\s/)[0];
        const name = lang === 'sh' || lang === 'shell' ? 'bash' : lang;
        const html = name && hljs.getLanguage(name) ? hljs.highlight(token.text, { language: name }).value : esc(token.text);
        return `<div class="code"><button class="copy" type="button" aria-label="Copy">Copy</button><pre><code class="hljs${lang ? ` language-${esc(lang)}` : ''}">${html}</code></pre></div>\n`;
      },
    },
  });
  return md;
}

function html(page: Page): string {
  const out = renderer(page.file).parser(page.tokens);
  return out.replace(/<table>/g, '<div class="table"><table>').replace(/<\/table>/g, '</table></div>');
}

/** Old links to an anchor that moved to another page land there. */
function forwardScript(moved: Record<string, string>): string {
  return `<script>(function(){var m=${JSON.stringify(moved).replace(/</g, '\\u003c')},t=m[decodeURIComponent(location.hash.slice(1))];if(t)location.replace(t)})()</script>`;
}

// --- output ---------------------------------------------------------------------------------

const order = nav.flatMap((g) => g.items);

/** The pages search engines should index: the sitemap lists exactly these. */
const indexable: string[] = [];
async function emit(url: string, content: string, opts: { index?: boolean } = {}): Promise<void> {
  if (opts.index) indexable.push(url);
  const file = path.join(OUT, url.endsWith('/') ? `${url}index.html` : url);
  await mkdir(path.dirname(file), { recursive: true });
  await writeFile(file, content);
}

await rm(OUT, { recursive: true, force: true });
await mkdir(OUT, { recursive: true });

for (const page of all) {
  const i = order.findIndex((o) => o.url === page.url);
  let content = html(page);
  if (page === index) {
    for (const s of sections) {
      const cards = s.entries
        .map((e) => {
          const url = pages.find((x) => x.file === e.file)?.url ?? resolve(path.posix.relative('docs', e.file), INDEX);
          return `<a class="card" href="${esc(url)}"><strong>${esc(e.title)}</strong><span>${esc(e.description)}</span></a>`;
        })
        .join('');
      content += `<h2 id="${s.id}"><a class="anchor" href="#${s.id}" aria-label="Link to this section">#</a>${esc(s.title)}</h2><div class="cards">${cards}</div>`;
    }
  }
  if (page.moved && Object.keys(page.moved).length) content += forwardScript(page.moved);
  await emit(
    page.url,
    layout({
      kind: 'docs',
      url: page.url,
      title: page.searchTitle,
      heading: page.title,
      description: page.description,
      body: content,
      nav,
      // A check page lists each check once, by its name (the variants of one check share it).
      toc: page === index
        ? sections.map((s) => ({ id: s.id, text: s.title }))
        : page.checks
          ? page.headings
              .filter((h) => h.depth === 2)
              .map((h) => ({ ...h, text: h.text.replace(/\s+[a-z0-9_]+$/, '') }))
              .filter((h, i, list) => list.findIndex((o) => o.text === h.text) === i)
          : page.headings.filter((h) => h.depth === 2),
      prev: i > 0 ? order[i - 1] : undefined,
      next: i >= 0 && i < order.length - 1 ? order[i + 1] : undefined,
      edit: `${REPO}/edit/preview/${page.file}`,
      version,
      parent: page.parent?.url,
      ogType: 'article',
      jsonLd: jsonLdScript([
        breadcrumbs([
          { name: 'Home', url: '/' },
          { name: 'Docs', url: '/docs/' },
          ...(page.parent ? [{ name: page.parent.title, url: page.parent.url }] : []),
          ...(page === index ? [] : [{ name: page.title, url: page.url }]),
        ]),
      ]),
    }),
    { index: true },
  );
}

// Search: every page's title, sections and text, read by the docs' search box.
const search = all.map((p) => ({
  url: p.url,
  title: p.title,
  headings: p.headings.filter((h) => h.depth >= 2).map((h) => ({ id: h.id, text: h.text })),
  text: plain(p.tokens.map((t) => ('text' in t && typeof t.text === 'string' ? t.text : '')).join(' ')).replace(/\s+/g, ' ').slice(0, 20000),
}));
await emit('/docs/search.json', JSON.stringify(search));

// The landing page: its screenshots are docs images too.
const SHOTS = ['pipelines.jpg', 'connection-checks.jpg', 'pipeline-overview.jpg', 'wizard-tables.jpg'];
for (const shot of SHOTS) {
  if (!images.has(shot)) fail(`the landing page shows ${IMAGES}${shot}, which does not exist`);
  usedImages.add(shot);
}
await emit('/', landing({ latest, docsVersion: version }), { index: true });

for (const [from, to] of Object.entries(REDIRECTS)) {
  if (all.some((p) => p.url === from)) fail(`REDIRECTS: ${from} is a page again`);
  if (!all.some((p) => p.url === to)) fail(`REDIRECTS: ${from} → ${to}, which is not a page`);
  await emit(
    from,
    `<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Moved · BeziSync docs</title><meta name="robots" content="noindex"><link rel="canonical" href="${SITE}${to}"><script>location.replace('${to}'+location.hash)</script><meta http-equiv="refresh" content="0; url=${to}"></head><body><p>This section moved to <a href="${to}">${to}</a>.</p></body></html>\n`,
  );
}

await emit(
  '/404.html',
  layout({
    kind: 'plain',
    url: '/404.html',
    title: 'Page not found · BeziSync',
    heading: 'Page not found',
    description: 'This page does not exist.',
    body: `<p>This page does not exist. Try the <a href="/docs/">documentation</a> or the <a href="/">home page</a>.</p>`,
    version,
  }),
);

// Static files: styles, scripts, fonts, the logo and the screenshots.
await cp(path.join(SITE_DIR, 'public'), OUT, { recursive: true });
await copyFile(path.join(SITE_DIR, 'src', 'styles.css'), path.join(OUT, 'styles.css'));
await copyFile(path.join(SITE_DIR, 'src', 'site.js'), path.join(OUT, 'site.js'));
await copyFile(path.join(ROOT, 'frontend', 'public', 'favicon.svg'), path.join(OUT, 'favicon.svg'));
await mkdir(path.join(OUT, 'docs', 'images'), { recursive: true });
for (const name of images) {
  if (!usedImages.has(name)) fail(`${IMAGES}${name} is not shown by any page: delete it or show it`);
  await copyFile(path.join(ROOT, IMAGES, name), path.join(OUT, 'docs', 'images', name));
}
await mkdir(path.join(OUT, 'fonts'), { recursive: true });
for (const [pkg, weights] of [['ibm-plex-sans', [400, 500, 600]], ['ibm-plex-mono', [400, 500]]] as const) {
  for (const w of weights) {
    const name = `${pkg}-latin-${w}-normal.woff2`;
    await copyFile(path.join(SITE_DIR, 'node_modules', '@fontsource', pkg, 'files', name), path.join(OUT, 'fonts', name));
  }
}
const lastmod = released ? `<lastmod>${released}</lastmod>` : '';
await writeFile(
  path.join(OUT, 'sitemap.xml'),
  `<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n${indexable.map((u) => `  <url><loc>${SITE}${u}</loc>${lastmod}</url>`).join('\n')}\n</urlset>\n`,
);
await writeFile(path.join(OUT, 'robots.txt'), `User-agent: *\nAllow: /\nSitemap: ${SITE}/sitemap.xml\n`);

if (errors.length) {
  for (const e of errors) console.error(`error: ${e}`);
  console.error(`${errors.length} problem(s): the site was not built`);
  await rm(OUT, { recursive: true, force: true });
  process.exit(1);
}
console.log(`built ${indexable.length} pages (docs for ${version}) into ${OUT}`);
