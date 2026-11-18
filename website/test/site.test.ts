// Builds the site from this checkout and checks every page: what search engines read, links,
// the URLs the site has always had, and the screenshots.
//
//   bun test
import { beforeAll, describe, expect, test } from 'bun:test';
import { mkdtemp, readFile, readdir, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Marked } from 'marked';
import { DESCRIPTION_MAX, DESCRIPTION_MIN, TITLE_MAX, autoDescription, clip, jsonLdScript } from '../src/seo';
import { SITE } from '../src/layout';

const SITE_DIR = path.join(import.meta.dir, '..');

const build = (out: string, ...args: string[]) => Bun.spawnSync(['bun', 'src/build.ts', ...args, '--out', out], { cwd: SITE_DIR });

let out = '';
let log = '';
type Page = { url: string; file: string; html: string };
let pages: Page[] = [];
let sitemap: string[] = [];

const decode = (s: string) =>
  s.replace(/&quot;/g, '"').replace(/&#39;/g, "'").replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&amp;/g, '&');
const one = (html: string, re: RegExp) => [...html.matchAll(re)].map((m) => decode(m[1]));
const isNoindex = (html: string) => /<meta name="robots" content="noindex"/.test(html);

async function htmlFiles(dir: string): Promise<string[]> {
  const found: string[] = [];
  for (const e of await readdir(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) found.push(...(await htmlFiles(p)));
    // Search Console's ownership file (public/google<id>.html) is not a page.
    else if (e.name.endsWith('.html') && !/^google[0-9a-f]+\.html$/.test(e.name)) found.push(p);
  }
  return found;
}

beforeAll(async () => {
  out = await mkdtemp(path.join(tmpdir(), 'bezisync-site-'));
  const run = build(out);
  log = run.stdout.toString() + run.stderr.toString();
  if (run.exitCode !== 0) throw new Error(`build failed:\n${log}`);
  for (const file of await htmlFiles(out)) {
    const rel = path.relative(out, file);
    const url = `/${rel.replace(/index\.html$/, '')}`;
    pages.push({ url, file, html: await readFile(file, 'utf8') });
  }
  sitemap = one(await readFile(path.join(out, 'sitemap.xml'), 'utf8'), /<loc>([^<]+)<\/loc>/g);
}, 120_000);

/** Pages search engines should index: everything but the 404 page and the redirects. */
const indexable = () => pages.filter((p) => p.url !== '/404.html' && !isNoindex(p.html));

describe('build', () => {
  test('reports no problems', () => {
    expect(log.split('\n').filter((l) => l.startsWith('error:'))).toEqual([]);
  });

  test('builds a release too (--release: "Docs for vX.Y.Z", GitHub links at the tag)', async () => {
    const dir = await mkdtemp(path.join(tmpdir(), 'bezisync-site-release-'));
    const run = build(dir, '--release');
    expect(run.exitCode, run.stderr.toString()).toBe(0);
    const install = await readFile(path.join(dir, 'docs', 'install', 'index.html'), 'utf8');
    expect(install).toMatch(/Docs for <strong>v\d+\.\d+\.\d+<\/strong>/);
    expect(install).toMatch(/github\.com\/daun-gatal\/bezisync\/blob\/v\d+\.\d+\.\d+\/deploy\/apparmor/);
  });

  test('the sitemap lists exactly the indexable pages', () => {
    expect([...sitemap].sort()).toEqual(indexable().map((p) => `${SITE}${p.url}`).sort());
    expect(sitemap.length).toBeGreaterThanOrEqual(27);
  });
});

describe('every indexable page', () => {
  test('has a unique title of at most TITLE_MAX characters', () => {
    const titles = indexable().map((p) => ({ url: p.url, t: one(p.html, /<title>([^<]*)<\/title>/g) }));
    for (const { url, t } of titles) {
      expect(t.length, url).toBe(1);
      expect(t[0].length, `${url}: ${t[0]}`).toBeLessThanOrEqual(TITLE_MAX);
    }
    const all = titles.map((x) => x.t[0]);
    expect(new Set(all).size).toBe(all.length);
  });

  test('has a unique description of DESCRIPTION_MIN to DESCRIPTION_MAX characters', () => {
    const all: string[] = [];
    for (const p of indexable()) {
      const d = one(p.html, /<meta name="description" content="([^"]*)">/g);
      expect(d.length, p.url).toBe(1);
      expect(d[0].length, `${p.url}: ${d[0]}`).toBeGreaterThanOrEqual(DESCRIPTION_MIN);
      expect(d[0].length, `${p.url}: ${d[0]}`).toBeLessThanOrEqual(DESCRIPTION_MAX);
      all.push(d[0]);
    }
    expect(new Set(all).size).toBe(all.length);
  });

  test('has a canonical URL equal to its sitemap entry', () => {
    for (const p of indexable()) {
      const c = one(p.html, /<link rel="canonical" href="([^"]+)">/g);
      expect(c, p.url).toEqual([`${SITE}${p.url}`]);
      expect(sitemap).toContain(c[0]);
    }
  });

  test('has one h1 and never skips a heading level', () => {
    for (const p of indexable()) {
      const main = p.html.slice(p.html.indexOf('<main'), p.html.indexOf('</main>'));
      const levels = [...main.matchAll(/<h([1-6])[\s>]/g)].map((m) => Number(m[1]));
      expect(levels.filter((l) => l === 1).length, p.url).toBe(1);
      for (let i = 1; i < levels.length; i++) {
        expect(levels[i] - levels[i - 1], `${p.url}: h${levels[i - 1]} then h${levels[i]}`).toBeLessThanOrEqual(1);
      }
    }
  });

  test('has JSON-LD that parses, and docs pages a breadcrumb ending at the page', () => {
    for (const p of indexable()) {
      const blocks = [...p.html.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)].map((m) => JSON.parse(m[1]));
      expect(blocks.length, p.url).toBeGreaterThan(0);
      for (const b of blocks) {
        expect(b['@context']).toBe('https://schema.org');
        expect(typeof b['@type']).toBe('string');
        expect(b.aggregateRating).toBeUndefined(); // never invented
      }
      if (p.url.startsWith('/docs/')) {
        const crumbs = blocks.find((b) => b['@type'] === 'BreadcrumbList');
        expect(crumbs, p.url).toBeDefined();
        expect(crumbs.itemListElement.at(-1).item).toBe(`${SITE}${p.url}`);
        expect(p.html).toContain('<meta property="og:type" content="article">');
      }
    }
    const home = pages.find((p) => p.url === '/')!;
    expect(home.html).toContain('"@type":"SoftwareApplication"');
    expect(home.html).toContain('"@type":"WebSite"');
  });

  test('links only to pages that exist', async () => {
    const broken: string[] = [];
    for (const p of pages) {
      for (const href of one(p.html, /href="(\/[^"#?]*)/g)) {
        if (href.startsWith('//')) continue;
        const file = path.join(out, href.endsWith('/') ? `${href}index.html` : href);
        if (!(await stat(file).catch(() => undefined))) broken.push(`${p.url} → ${href}`);
      }
    }
    expect(broken).toEqual([]);
  });
});

describe('assets', () => {
  // Cloudflare caches /styles.css for hours: pages must ask for the version they were built with.
  test('every page links the stylesheet and script by the hash of what was built', async () => {
    const { createHash } = await import('node:crypto');
    const hash = async (f: string) => createHash('sha256').update(await readFile(path.join(out, f))).digest('hex').slice(0, 12);
    const css = `/styles.css?v=${await hash('styles.css')}`;
    const js = `/site.js?v=${await hash('site.js')}`;
    for (const p of pages.filter((p) => p.html.includes('rel="stylesheet"'))) {
      expect(p.html, p.url).toContain(`<link rel="stylesheet" href="${css}">`);
      expect(p.html, p.url).toContain(`<script src="${js}" defer></script>`);
    }
  });
});

describe('the landing page', () => {
  test('says Debezium Server in its title, h1 and description', () => {
    const home = pages.find((p) => p.url === '/')!.html;
    expect(one(home, /<title>([^<]*)/g)[0]).toContain('Debezium Server');
    expect(one(home, /<h1>([\s\S]*?)<\/h1>/g)[0]).toContain('Debezium Server');
    expect(one(home, /<meta name="description" content="([^"]*)"/g)[0]).toContain('Debezium Server');
  });
});

describe('the landing page shows both engines', () => {
  test('Flink CDC is a second engine, not an add-on', () => {
    const home = pages.find((p) => p.url === '/')!.html;
    expect(home).not.toMatch(/Optional:|optional Flink CDC/i);
    expect(one(home, /<h1>([\s\S]*?)<\/h1>/g)[0]).toContain('Debezium Server or Flink CDC');
    // The hero stays short enough for a laptop's first screen: two sentences of lead.
    const lead = one(home, /<p class="lead">([\s\S]*?)<\/p>/g)[0];
    expect(lead.split(/[.;] /).length).toBeLessThanOrEqual(3);
    expect(home).toContain('<h2>Two engines, one control plane</h2>');
    expect(home).toContain('href="/docs/choose-an-engine/"');
    // Iceberg runs on both engines: one chip, marked, not a second row of Flink CDC destinations.
    expect(home.match(/>Apache Iceberg</g)?.length).toBe(1);
    expect(home).toContain('<span class="both">Apache Iceberg<small>both engines</small></span>');
    expect(home).toContain('<span class="flink">StarRocks<small>Flink CDC</small></span>');
  });
});

// Every URL the site served before the docs became pages of their own (one page per section of
// the old single-file guide), so links people saved and search engines indexed keep working.
const OLD_URLS = [
  'install', 'concepts', 'your-first-pipeline', 'choose-an-engine', 'sources', 'shape-events-transforms',
  'destinations', 'databases-behind-a-bastion-ssh', 'operate', 'manage-runtimes', 'remote-runtimes-agents',
  'google-gke-and-amazon-eks', 'engine-versions', 'flink-cdc-engine', 'personal-data',
  'guardrails-policies-approvals-templates', 'secrets-and-secret-stores', 'single-sign-on', 'ai-agents-mcp',
  'config-as-code-and-the-cli', 'adopt-an-existing-debezium-server', 'administration', 'configuration-reference',
  'reference/checks', 'security/threat-model', 'changelog', 'debezium-versions', 'flink-cdc-engine-optional',
].map((s) => `/docs/${s}/`);

describe('URLs the site has always had', () => {
  test('every old URL is still a page or redirects to one', () => {
    for (const url of OLD_URLS) expect(pages.some((p) => p.url === url), url).toBe(true);
  });

  test('renamed sections redirect, unindexed, to their new page', () => {
    for (const [from, to] of [['/docs/flink-cdc-engine-optional/', '/docs/flink-cdc-engine/'], ['/docs/debezium-versions/', '/docs/engine-versions/']]) {
      const old = pages.find((p) => p.url === from)!.html;
      expect(isNoindex(old)).toBe(true);
      expect(old).toContain(`<link rel="canonical" href="${SITE}${to}">`);
      expect(old).toContain(`location.replace('${to}'+location.hash)`);
      expect(sitemap).not.toContain(`${SITE}${from}`);
    }
    expect(one(pages.find((p) => p.url === '/docs/flink-cdc-engine/')!.html, /<title>([^<]*)/g)[0]).toBe('Flink CDC engine: sync tables to Iceberg, StarRocks, Doris');
  });

  test('sections that became their own pages are forwarded from where they were', () => {
    const destinations = pages.find((p) => p.url === '/docs/destinations/')!.html;
    expect(destinations).toContain('"apache-iceberg":"/docs/destinations-lakehouse/#apache-iceberg"');
    expect(destinations).toContain('"kafka":"/docs/destinations-events/#kafka"');
    const install = pages.find((p) => p.url === '/docs/install/')!.html;
    expect(install).toContain('"run-your-own-lab-in-github-codespaces":"/docs/codespaces-lab/"');
  });
});

describe('the docs', () => {
  test('the docs home has a card for every guide page, by section', () => {
    const home = pages.find((p) => p.url === '/docs/')!.html;
    for (const p of pages.filter((x) => /^\/docs\/[a-z0-9-]+\/$/.test(x.url) && !isNoindex(x.html) && x.url !== '/docs/changelog/')) {
      expect(home, p.url).toContain(`class="card" href="${p.url}"`);
    }
    expect(home).toContain('<h2 id="start-here">');
  });

  test('every screenshot is published and shown as a figure with its caption', async () => {
    const images = (await readdir(path.join(out, 'docs', 'images'))).filter((f) => f.endsWith('.jpg'));
    expect(images.length).toBeGreaterThanOrEqual(10);
    const html = pages.map((p) => p.html).join('\n');
    for (const img of images) expect(html, img).toContain(`/docs/images/${img}`);
    const first = pages.find((p) => p.url === '/docs/your-first-pipeline/')!.html;
    expect(first).toMatch(/<figure class="shot"><img src="\/docs\/images\/[a-z-]+\.jpg" alt="[^"]+" loading="lazy" width="1440" height="900"><figcaption>/);
  });

  test('the GitHub-only footer of each page is not on the site', () => {
    for (const p of pages) expect(p.html, p.url).not.toContain('All documentation</a>');
  });
});

describe('the preflight check pages', () => {
  const checks = () => pages.filter((p) => /^\/docs\/reference\/checks\/[^/]+\/$/.test(p.url));

  test('one page per source or destination, each check headed by its name', () => {
    expect(checks().length).toBeGreaterThanOrEqual(20);
    const pg = pages.find((p) => p.url === '/docs/reference/checks/postgresql/')!;
    expect(pg).toBeDefined();
    expect(pg.html).toContain('<h1>PostgreSQL checks</h1>');
    expect(pg.html).toMatch(/<h2 id="pg_wal_level">.*Logical replication <code class="check-id">pg_wal_level<\/code><\/h2>/);
    expect(pg.html).toContain('<a id="pg-wal-level"></a>'); // the anchor BeziSync's UI and links use
  });

  test('only the source databases Debezium reads say "for Debezium"', () => {
    const title = (url: string) => one(pages.find((p) => p.url === url)!.html, /<title>([^<]*)/g)[0];
    expect(title('/docs/reference/checks/postgresql/')).toContain('for Debezium');
    expect(title('/docs/reference/checks/oracle/')).toContain('for Debezium');
    expect(title('/docs/reference/checks/kafka/')).not.toContain('Debezium');
  });

  test('a check page names its engines once the reference says them, and lists each check once by name', () => {
    const pg = pages.find((p) => p.url === '/docs/reference/checks/postgresql/')!.html;
    const title = one(pg, /<title>([^<]*)/g)[0];
    if (pg.includes('Read by Debezium and by Flink CDC.')) {
      expect(title).toBe('PostgreSQL for Debezium and Flink CDC: checks and fixes');
      expect(pg).toContain('href="/docs/reference/checks/flink-cdc/#flink-replica-identity"');
      const sr = pages.find((p) => p.url === '/docs/reference/checks/starrocks/')!.html;
      expect(one(sr, /<title>([^<]*)/g)[0]).toBe('StarRocks for Flink CDC: preflight checks and fixes');
    } else {
      expect(title).toBe('PostgreSQL for Debezium: preflight checks and fixes');
    }
    const toc = pg.slice(pg.indexOf('<aside class="toc"'), pg.indexOf('</aside>', pg.indexOf('<aside class="toc"')));
    const items = one(toc, /<a [^>]*>([^<]*)<\/a>/g);
    expect(items.length).toBeGreaterThan(5);
    expect(new Set(items).size).toBe(items.length);
    for (const i of items) expect(i, i).not.toMatch(/\bpg_[a-z_]+$/);
  });

  test('the index lists every check page, and forwards links made before the split', () => {
    const index = pages.find((p) => p.url === '/docs/reference/checks/')!.html;
    for (const p of checks()) expect(index).toContain(`href="${p.url}"`);
    expect(index).toContain('"pg_wal_level":"/docs/reference/checks/postgresql/#pg_wal_level"');
    expect(index).toContain('"pg-wal-level":"/docs/reference/checks/postgresql/#pg-wal-level"');
  });

  test('a check page sits under Preflight checks in the breadcrumb and sidebar', () => {
    const pg = pages.find((p) => p.url === '/docs/reference/checks/postgresql/')!.html;
    expect(pg).toContain(`"name":"Preflight checks","item":"${SITE}/docs/reference/checks/"`);
    expect(pg).toContain('<a href="/docs/reference/checks/" aria-current="page">Preflight checks</a>');
  });
});

describe('seo helpers', () => {
  test('clip keeps whole sentences and never splits a version number', () => {
    const s = 'BeziSync 1.0 runs Debezium Server. '.repeat(3) + 'A'.repeat(200);
    const c = clip(s);
    expect(c.length).toBeLessThanOrEqual(DESCRIPTION_MAX);
    expect(c.endsWith('Debezium Server.')).toBe(true);
    expect(c).toContain('1.0');
  });

  test('clip cuts one long sentence at a word, with an ellipsis', () => {
    const c = clip(`${'word '.repeat(60)}end.`);
    expect(c.length).toBeLessThanOrEqual(DESCRIPTION_MAX);
    expect(c.endsWith('word…')).toBe(true);
  });

  test('autoDescription skips a too-short opening and falls back when there is no prose', () => {
    const lex = (md: string) => new Marked().lexer(md);
    expect(autoDescription(lex('Use an agent.\n\nThe agent dials out, so it works with private clusters and needs no cloud sign-in.'), 'X')).toBe(
      'Use an agent. The agent dials out, so it works with private clusters and needs no cloud sign-in.',
    );
    expect(autoDescription(lex('| a | b |\n|---|---|\n| 1 | 2 |'), 'Configuration reference')).toContain(
      'Configuration reference in BeziSync',
    );
  });

  test('jsonLdScript escapes "<" so no text closes the script element', () => {
    const s = jsonLdScript([{ name: '</script><script>alert(1)</script>' }]);
    expect(s.match(/<\/script>/g)?.length).toBe(1);
    expect(JSON.parse(s.replace(/^<script[^>]*>|<\/script>$/g, '')).name).toBe('</script><script>alert(1)</script>');
  });
});
