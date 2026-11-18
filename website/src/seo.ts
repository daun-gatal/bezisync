// What search engines read: page titles, descriptions and structured data (JSON-LD).
//
// The docs come from BeziSync at a release, so the text written here is tied to them: an
// override names phrases its section must contain, and is used only while it still does
// (otherwise the build warns and falls back to text taken from the docs themselves).
import type { Token, Tokens } from 'marked';
import { SITE, REPO } from './layout';

/** Longest title and description search engines show without cutting them. */
export const TITLE_MAX = 65;
export const DESCRIPTION_MAX = 160;
export const DESCRIPTION_MIN = 70;

const SUFFIX = ' · BeziSync for Debezium Server';
const SHORT_SUFFIX = ' · BeziSync docs';

/** "<Section> · BeziSync for Debezium Server", or the short suffix when that is too long. */
export function docTitle(title: string): string {
  if (/debezium/i.test(title)) return `${title} · BeziSync`;
  return `${title}${SUFFIX}`.length <= TITLE_MAX ? `${title}${SUFFIX}` : `${title}${SHORT_SUFFIX}`;
}

export type Override = {
  title?: string;
  description: string;
  /** Phrases the page's markdown must contain (case-insensitive) for the override to hold. */
  requires: string[];
  /** A section newer than some docs the site is built from: its absence is no warning. */
  newer?: boolean;
};

/** The pages people search for by task, keyed by URL. */
export const OVERRIDES: Record<string, Override> = {
  '/docs/': {
    title: 'BeziSync docs: a control plane for Debezium Server',
    description:
      'Install and run BeziSync, the open-source control plane for Debezium Server: pipelines, preflight checks, source protection, guardrails and operations.',
    requires: ['Debezium Server'],
  },
  '/docs/install/': {
    title: 'Install BeziSync for Debezium Server: Docker or Helm',
    description:
      'Install BeziSync with Docker Compose in two minutes, or on Kubernetes with its Helm chart. It runs and manages one Debezium Server per pipeline.',
    requires: ['docker compose', 'helm install'],
  },
  '/docs/adopt-an-existing-debezium-server/': {
    title: 'Migrate an existing Debezium Server (application.properties)',
    description:
      'Paste a Debezium Server application.properties: BeziSync maps its tables, snapshot mode, formats and SMTs into a managed pipeline with its own slot and offsets.',
    requires: ['application.properties', 'SMT', 'slot'],
  },
  '/docs/operate/': {
    title: 'Operate Debezium pipelines: lag, slots, binlog, snapshots',
    description:
      'Watch Debezium lag and throughput, replication slot WAL, binlog and oplog retention, run snapshots, pause, resume, restart and roll back pipelines.',
    requires: ['max_slot_wal_keep_size', 'binlog', 'oplog', 'snapshot', 'Pause / Resume / Restart', 'Roll back'],
  },
  '/docs/engine-versions/': {
    title: 'Upgrade Debezium Server versions across pipelines',
    description:
      'Choose the Debezium Server image each pipeline runs, add your own builds with extra converters or SMTs, and upgrade pipelines one at a time with rollback.',
    requires: ['Debezium Server images', 'Change Debezium version', 'rollback'],
  },
  '/docs/sources/': {
    title: 'Debezium sources: PostgreSQL, MySQL, Oracle, MongoDB and more',
    description:
      'Debezium sources in BeziSync: PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, MongoDB, CockroachDB, Vitess and Spanner, each checked with exact fixes.',
    requires: ['PostgreSQL', 'MySQL', 'Oracle', 'MongoDB', 'CockroachDB', 'Vitess', 'Spanner'],
  },
  '/docs/destinations/': {
    title: 'CDC destinations: Kafka, Iceberg, JDBC, StarRocks and more',
    description:
      'What every BeziSync destination shares: the names, event formats and shapes it takes, its checks, and which engine writes Kafka, Iceberg, JDBC or StarRocks.',
    requires: ['Kafka', 'Iceberg', 'JDBC', 'StarRocks', 'Elasticsearch'],
  },
  // Before BeziSync 0.5 the section was "Flink CDC engine (optional)": the override follows the rename.
  '/docs/flink-cdc-engine/': {
    title: 'Flink CDC engine: sync tables to Iceberg, StarRocks, Doris',
    description:
      'Flink CDC, the second engine next to Debezium Server: keep tables in sync in lakehouse and analytics stores, with parallel first copies and schema changes.',
    requires: ['Flink CDC', 'Iceberg', 'StarRocks', 'Doris', 'schema changes'],
  },
  '/docs/choose-an-engine/': {
    title: 'Debezium Server or Flink CDC: choose a CDC engine',
    description:
      'Debezium Server or Flink CDC for each pipeline: which sources and destinations each runs, what each costs, and which to pick for Iceberg and Kafka.',
    requires: ['Debezium', 'Flink CDC', 'Iceberg', 'Kafka', 'first deploy'],
    newer: true,
  },
  '/docs/google-gke-and-amazon-eks/': {
    title: 'Run Debezium Server on Google GKE and Amazon EKS',
    description:
      'Run Debezium Server pipelines on private GKE and EKS clusters through an outbound-only agent, or directly with Workload Identity or IRSA.',
    requires: ['GKE', 'EKS', 'agent', 'Workload Identity', 'IRSA'],
  },
  '/docs/reference/checks/': {
    title: 'Debezium preflight checks and fixes, per database',
    description:
      'Every check BeziSync runs before a Debezium pipeline deploys: wal_level, replication slots, binlog, LogMiner, SQL Server CDC, MongoDB, grants, with the fix.',
    requires: ['wal_level', 'pg_replication_slots', 'binlog', 'LogMiner', 'MongoDB', 'GRANT'],
  },
  '/docs/changelog/': {
    title: 'BeziSync changelog: releases and changes',
    description:
      'Every BeziSync release and what it changed: new sources and destinations, Debezium Server and Flink CDC features, changes and fixes.',
    requires: ['## ['],
  },
};

/** Why an override does not hold for a page, or undefined when it does. */
export function overrideProblem(o: Override, markdown: string): string | undefined {
  const text = markdown.toLowerCase();
  const missing = o.requires.filter((r) => !text.includes(r.toLowerCase()));
  if (missing.length) return `no longer mentions ${missing.map((m) => `"${m}"`).join(', ')}`;
  if (o.title && o.title.length > TITLE_MAX) return `title is ${o.title.length} characters (max ${TITLE_MAX})`;
  if (o.description.length > DESCRIPTION_MAX || o.description.length < DESCRIPTION_MIN)
    return `description is ${o.description.length} characters (${DESCRIPTION_MIN}-${DESCRIPTION_MAX})`;
  return undefined;
}

/**
 * Text without HTML tags. Tags are removed until none is left (a nested "<scr<b>ipt>" cannot
 * reassemble into one), then any "<" or ">" still there is dropped.
 */
export function stripTags(s: string): string {
  let out = s;
  for (let prev = ''; out !== prev; ) {
    prev = out;
    out = out.replace(/<[^<>]*>/g, '');
  }
  return out.replace(/[<>]/g, '');
}

/** Regular-expression text matching s literally. */
export function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/** Text a reader sees: no markdown, links or URLs (code keeps its text, without the marks). */
export function prose(md: string): string {
  const text = md
    .replace(/`([^`]*)`/g, '$1')
    .replace(/!?\[([^\]]*)\]\([^)]*\)/g, '$1')
    .replace(/https?:\/\/\S+/g, '');
  return stripTags(text)
    .replace(/[*_~]/g, '')
    .replace(/\s+/g, ' ')
    .replace(/\s+([,.;:])/g, '$1')
    .replace(/\(\s*\)/g, '')
    .trim();
}

/** Whole sentences up to max characters; a single longer sentence is cut at a word. */
export function clip(text: string, max = DESCRIPTION_MAX): string {
  if (text.length <= max) return text;
  // A sentence ends at . ! or ? followed by a space ("1.0" and "e.g" stay whole).
  const sentences = text.split(/(?<=[.!?])\s+/);
  let out = '';
  for (const s of sentences) {
    const next = out ? `${out} ${s}` : s;
    if (next.length > max) break;
    out = next;
  }
  if (out.length >= DESCRIPTION_MIN) return out;
  const cut = text.slice(0, max - 1);
  return `${cut.slice(0, cut.lastIndexOf(' ')).replace(/[,;:\s]+$/, '')}…`;
}

/**
 * A description from the docs: the first paragraphs with at least DESCRIPTION_MIN characters of
 * prose between them, clipped; or the title and what BeziSync is.
 */
export function autoDescription(tokens: Token[], title: string): string {
  let text = '';
  for (const t of tokens) {
    if (t.type === 'heading' || t.type === 'table' || t.type === 'code') {
      if (text) break; // the opening text ended
      continue;
    }
    const raw = t.type === 'paragraph' ? (t as Tokens.Paragraph).text : t.type === 'list' ? (t as Tokens.List).items.map((i) => i.text).join(' ') : '';
    const p = prose(raw);
    if (!p) continue;
    text = text ? `${text} ${p}` : p;
    if (text.length >= DESCRIPTION_MIN) break;
  }
  if (text.length >= DESCRIPTION_MIN) return clip(text);
  return `${title} in BeziSync, the open-source control plane for Debezium Server: how it works and how to set it up.`;
}

// --- structured data ----------------------------------------------------------------------

export type JsonLd = Record<string, unknown>;

/** A JSON-LD block. JSON, not HTML: "<" is escaped so no text can close the script element. */
export function jsonLdScript(data: JsonLd[]): string {
  return data.map((d) => `<script type="application/ld+json">${JSON.stringify(d).replace(/</g, '\\u003c')}</script>`).join('\n');
}

export function breadcrumbs(items: { name: string; url: string }[]): JsonLd {
  return {
    '@context': 'https://schema.org',
    '@type': 'BreadcrumbList',
    itemListElement: items.map((it, i) => ({ '@type': 'ListItem', position: i + 1, name: it.name, item: `${SITE}${it.url}` })),
  };
}

export function website(): JsonLd {
  return { '@context': 'https://schema.org', '@type': 'WebSite', name: 'BeziSync', url: `${SITE}/` };
}

/** BeziSync as software. No ratings or reviews: there are none to state honestly. */
export function software(version: string | undefined, description: string): JsonLd {
  return {
    '@context': 'https://schema.org',
    '@type': 'SoftwareApplication',
    name: 'BeziSync',
    description,
    url: `${SITE}/`,
    applicationCategory: 'DeveloperApplication',
    operatingSystem: 'Linux',
    ...(version ? { softwareVersion: version.replace(/^v/, '') } : {}),
    license: 'https://www.apache.org/licenses/LICENSE-2.0',
    downloadUrl: `${REPO}/releases`,
    sameAs: [REPO],
    isAccessibleForFree: true,
    offers: { '@type': 'Offer', price: '0', priceCurrency: 'USD' },
  };
}
