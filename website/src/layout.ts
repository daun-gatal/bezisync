// The page shell every page shares: head, header, the docs' sidebar and table of contents,
// and the footer.
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';

export const SITE = 'https://bezisync.dglabs-ai.dev';
export const REPO = 'https://github.com/daun-gatal/bezisync';

/**
 * An asset's URL with its content's hash: Cloudflare and browsers keep /styles.css for hours, so
 * a deploy that changes it gets a new URL instead of pairing new pages with an old stylesheet.
 */
const versioned = (file: string) =>
  `/${file}?v=${createHash('sha256').update(readFileSync(new URL(file, import.meta.url))).digest('hex').slice(0, 12)}`;
export const STYLES = versioned('styles.css');
export const SCRIPT = versioned('site.js');

export type NavItem = { url: string; title: string };
export type NavGroup = { title: string; items: NavItem[] };

export function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

const github = `<svg viewBox="0 0 16 16" aria-hidden="true"><path fill="currentColor" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>`;

export function header(active: 'home' | 'docs'): string {
  return `<header class="top">
  <div class="top-inner">
    <a class="brand" href="/"><img src="/favicon.svg" alt="" width="28" height="28"><span>BeziSync</span></a>
    <nav class="top-nav" aria-label="Site">
      <a href="/docs/"${active === 'docs' ? ' aria-current="page"' : ''}>Docs</a>
      <a href="/docs/changelog/">Changelog</a>
      <a href="${REPO}" class="icon-link" rel="noopener">${github}<span>GitHub</span></a>
      <button class="theme" type="button" aria-label="Switch between light and dark"><svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true"><circle cx="8" cy="8" r="6.25" fill="none" stroke="currentColor" stroke-width="1.5"/><path d="M8 1.75a6.25 6.25 0 0 1 0 12.5z" fill="currentColor"/></svg></button>
    </nav>
  </div>
</header>`;
}

export function footer(version: string): string {
  return `<footer class="foot">
  <div class="foot-inner">
    <div><a class="brand" href="/"><img src="/favicon.svg" alt="" width="22" height="22"><span>BeziSync</span></a>
      <p>Self-service change data capture on Debezium Server. Apache License 2.0.</p></div>
    <div class="foot-links">
      <a href="/docs/">Docs</a><a href="${REPO}">GitHub</a>
      <a href="${REPO}/releases">Releases</a><a href="${REPO}/blob/main/SECURITY.md">Security</a>
    </div>
  </div>
  <p class="foot-note">Docs for ${esc(version)}. BeziSync is an independent project, not affiliated with the Debezium project.</p>
</footer>`;
}

type Layout = {
  kind: 'docs' | 'plain' | 'home';
  url: string;
  title: string;
  heading: string;
  description: string;
  body: string;
  version: string;
  nav?: NavGroup[];
  toc?: { id: string; text: string }[];
  prev?: NavItem;
  next?: NavItem;
  edit?: string;
  /** Extra head markup (JSON-LD), and the Open Graph type (default website). */
  jsonLd?: string;
  ogType?: string;
  /** The sidebar entry a page belongs under, when it has none of its own. */
  parent?: string;
};

export function head(title: string, description: string, url: string, opts: { ogType?: string; jsonLd?: string } = {}): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Content-Security-Policy" content="default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self' 'unsafe-inline'; font-src 'self'; connect-src 'self'; base-uri 'self'; form-action 'none'">
<meta name="referrer" content="strict-origin-when-cross-origin">
<title>${esc(title)}</title>
<meta name="description" content="${esc(description)}">
<link rel="canonical" href="${SITE}${url}">
<meta property="og:title" content="${esc(title)}">
<meta property="og:description" content="${esc(description)}">
<meta property="og:url" content="${SITE}${url}">
<meta property="og:type" content="${opts.ogType ?? 'website'}">
<meta property="og:site_name" content="BeziSync">
<meta property="og:image" content="${SITE}/docs/images/pipelines.jpg">
<meta property="og:image:width" content="1440">
<meta property="og:image:height" content="900">
<meta name="twitter:card" content="summary_large_image">
<link rel="icon" href="/favicon.svg" type="image/svg+xml">
<link rel="preload" href="/fonts/ibm-plex-sans-latin-400-normal.woff2" as="font" type="font/woff2" crossorigin>
<link rel="stylesheet" href="${STYLES}">
<script>try{const t=localStorage.getItem('bz-theme');if(t)document.documentElement.dataset.theme=t}catch{}</script>
<script src="${SCRIPT}" defer></script>${opts.jsonLd ? `\n${opts.jsonLd}` : ''}
</head>`;
}

export function layout(p: Layout): string {
  if (p.kind !== 'docs') {
    return `${head(p.title, p.description, p.url, { ogType: p.ogType, jsonLd: p.jsonLd })}
<body>
${header('home')}
<main class="plain"><h1>${esc(p.heading)}</h1>${p.body}</main>
${footer(p.version)}
</body>
</html>`;
  }
  const sidebar = (p.nav ?? [])
    .map(
      (g) =>
        `<div class="side-group"><p>${esc(g.title)}</p><ul>${g.items
          .map((i) => `<li><a href="${i.url}"${i.url === p.url || i.url === p.parent ? ' aria-current="page"' : ''}>${esc(i.title)}</a></li>`)
          .join('')}</ul></div>`,
    )
    .join('');
  const toc = p.toc?.length
    ? `<aside class="toc" aria-label="On this page"><p>On this page</p><ul>${p.toc
        .map((h) => `<li><a href="#${h.id}">${esc(h.text)}</a></li>`)
        .join('')}</ul></aside>`
    : '<aside class="toc"></aside>';
  const pager =
    p.prev || p.next
      ? `<nav class="pager" aria-label="Previous and next">${p.prev ? `<a class="prev" href="${p.prev.url}"><span>Previous</span>${esc(p.prev.title)}</a>` : '<span></span>'}${
          p.next ? `<a class="next" href="${p.next.url}"><span>Next</span>${esc(p.next.title)}</a>` : ''
        }</nav>`
      : '';
  return `${head(p.title, p.description, p.url, { ogType: p.ogType, jsonLd: p.jsonLd })}
<body class="docs-page">
${header('docs')}
<div class="docs">
  <aside class="side" aria-label="Documentation">
    <button class="side-toggle" type="button" aria-expanded="false">Menu</button>
    <div class="side-inner">
      <div class="search"><input type="search" placeholder="Search the docs" aria-label="Search the docs" autocomplete="off"><ul class="results" hidden></ul></div>
      <p class="version">Docs for <strong>${esc(p.version)}</strong></p>
      ${sidebar}
    </div>
  </aside>
  <main class="content">
    <article class="prose">
      <h1>${esc(p.heading)}</h1>
      ${p.body}
    </article>
    ${pager}
    ${p.edit ? `<p class="edit"><a href="${p.edit}" rel="noopener">Edit this page on GitHub</a></p>` : ''}
  </main>
  ${toc}
</div>
${footer(p.version)}
</body>
</html>`;
}
