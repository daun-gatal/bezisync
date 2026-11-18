// The landing page: what BeziSync is, why, what it connects, and how to start.
import { REPO, esc, footer, head, header } from './layout';
import { jsonLdScript, software, website } from './seo';

/** latest: the newest release in CHANGELOG.md; docsVersion: what the footer says the docs describe. */
type Landing = { latest?: string; docsVersion: string };

const DESCRIPTION =
  'Open-source control plane and web UI for Debezium Server and Flink CDC: it runs CDC pipelines on Docker or Kubernetes and checks every source first.';

const features: [string, string, string][] = [
  ['🛫', 'Preflight checks with exact fixes', 'Every source, destination and runtime is checked before anything deploys, and each failure names the statement or setting that fixes it, per cloud provider.'],
  ['🛡️', 'Source protection', 'WAL, binlog and oplog retention in view, heartbeats and snapshot windows: the production database comes first.'],
  ['🔍', 'Honest health', 'Never green without fresh evidence. No data means Unknown, not Healthy, and every state says why.'],
  ['🖱️', 'One-click operations', 'Snapshots, offsets, pause, resume and restart. Every change is a version you can diff, promote and roll back.'],
  ['🔐', 'Secrets done right', 'Encrypted and write-only, or read from Vault, AWS, Google or Kubernetes secret stores. Never logged, never returned.'],
  ['🧑‍⚖️', 'Guardrails', 'Projects, roles, SSO, policies, approvals and a full audit trail, so self-service stays safe.'],
  ['🙈', 'Personal data, chosen per pipeline', 'Mask, remove or keep each column. Nothing personal leaves silently.'],
  ['🤖', 'AI agents over MCP', 'Agents work with exactly their person\'s permissions, and never see a credential.'],
];

const transforms: [string, string, string] = [
  '🧩',
  'Shape events without code',
  'Filters, renames, routes, keys and computed columns, picked from the table\'s own columns and previewed on a sample row before anything deploys.' +
    ' On Flink CDC, Experts can write expressions too: BeziSync parses each one and renders it NULL-safe, so no value can stop a job.',
];

const sources = ['PostgreSQL', 'MySQL', 'MariaDB', 'SQL Server', 'Oracle', 'MongoDB', 'CockroachDB', 'Vitess', 'Cloud Spanner'];
/** Debezium's destinations (Kafka and Iceberg first: Flink CDC writes them too). */
const destinations = ['Kafka', 'Apache Iceberg', 'PostgreSQL', 'MySQL', 'MariaDB', 'SQL Server', 'Pub/Sub', 'Kinesis', 'SQS', 'SNS', 'Event Hubs', 'Pulsar', 'Redis Streams', 'NATS', 'HTTP'];
/** The sources and destinations Flink CDC runs. */
const flinkSources = ['PostgreSQL', 'MySQL', 'MariaDB', 'SQL Server', 'Oracle'];
const flinkDestinations = ['Apache Iceberg', 'StarRocks', 'Apache Doris', 'Apache Fluss', 'Apache Paimon', 'OceanBase', 'Elasticsearch', 'Kafka'];
const flinkOnly = flinkDestinations.filter((d) => !destinations.includes(d));

/** "a, b and c" */
const list = (items: string[]) => (items.length < 2 ? items.join('') : `${items.slice(0, -1).join(', ')} and ${items.at(-1)}`);
const short = (d: string) => d.replace(/^Apache /, '');

/** A connector chip, marked with the engines that run it when Flink CDC does. */
function chip(name: string, debezium: boolean, flink: boolean): string {
  if (!flink) return `<span>${esc(name)}</span>`;
  const tag = debezium ? 'both engines' : 'Flink CDC';
  return `<span class="${debezium ? 'both' : 'flink'}">${esc(name)}<small>${tag}</small></span>`;
}

/** What people build: one source, one destination, one engine per pipeline. */
const useCases: [string, string, string, string][] = [
  ['📨', 'Events for your services', 'Every change as an event in Kafka, Pub/Sub, Kinesis, SQS, SNS, Event Hubs, Pulsar, Redis Streams, NATS or HTTP, with the event shape and format your consumers expect.', 'Debezium · Kafka also on Flink CDC'],
  ['🗄️', 'A live copy in another database', 'Tables kept in PostgreSQL, MySQL, MariaDB or SQL Server, or OceanBase, created from the source and upserted by key.', 'Debezium · OceanBase on Flink CDC'],
  ['🏞️', 'Lakehouse tables', 'One table per source table in Iceberg over REST or Glue catalogs, or in Paimon and Fluss, read by Trino, Snowflake, Databricks and other engines.', 'Debezium or Flink CDC'],
  ['📊', 'Real-time analytics', 'Fresh rows in StarRocks and Doris primary-key tables, loaded every few seconds, with source schema changes applied.', 'Flink CDC'],
  ['🔎', 'Search indexes', 'One Elasticsearch index per table, a document per row kept in step with the source: updated, replaced and deleted by key.', 'Flink CDC'],
];

const engines: [string, string, string][] = [
  ['🔄', 'Debezium Server', 'Reads every source and streams it to Kafka, queues, databases, Iceberg and HTTP, one Debezium Server per pipeline. The default: it masks personal data, offers every event format, the outbox and custom transforms, and copies one table again while the others stream.'],
  ['🌊', 'Flink CDC', `Reads ${list(flinkSources)} and keeps their tables in sync in ${list(flinkDestinations.map(short))}, one small Flink cluster per pipeline: large tables copied in parallel chunks, source schema changes applied to the destination tables, positions saved and restorable. An Admin makes it available by registering a Flink CDC image.`],
];

const card = ([icon, title, text]: [string, string, string], extra = '') =>
  `<div class="feature"><span class="icon" aria-hidden="true">${icon}</span><h3>${esc(title)}</h3><p>${esc(text)}</p>${extra}</div>`;

/** A docs screenshot (docs/images/, 1440×900). */
const shot = (file: string, alt: string, cls = '') =>
  `<figure class="shot ${cls}"><img src="/docs/images/${file}" alt="${esc(alt)}" loading="lazy" width="1440" height="900"></figure>`;

const story = (title: string, text: string, file: string, alt: string, link: string, flip = false) => `
    <div class="story${flip ? ' flip' : ''}">
      <div class="story-text"><h3>${title}</h3><p>${text}</p>${link}</div>
      ${shot(file, alt)}
    </div>`;

const more = (url: string, text: string) => ` <a class="more" href="${url}">${text} →</a>`;

export function landing({ latest, docsVersion }: Landing): string {
  return `${head('BeziSync: a control plane and UI for Debezium Server', DESCRIPTION, '/', {
    jsonLd: jsonLdScript([website(), software(latest, DESCRIPTION)]),
  })}
<body class="home">
${header('home')}
<main>
  <section class="hero">
    <div class="hero-inner">
      <p class="eyebrow">${latest ? `<a href="${REPO}/releases/tag/${latest}">${latest} is out</a> · ` : ''}Open source · Apache 2.0</p>
      <h1>CDC pipelines your whole team can run <em>safely</em>.<span class="h1-sub">On Debezium Server or Flink CDC, chosen per pipeline.</span></h1>
      <p class="lead">BeziSync is an open-source control plane and web UI for change data capture. Set up runtimes and guardrails once; then any engineer can stream a database to Kafka, Iceberg, ${list(flinkOnly.slice(0, 2).map(short).concat('more than a dozen other destinations'))}, with mistakes caught before they deploy.</p>
      <div class="cta">
        <a class="button" href="#get-started">Get started</a>
        <a class="button ghost" href="/docs/">Read the docs</a>
      </div>
    </div>
    ${shot('pipelines.jpg', 'BeziSync pipelines list with health, lag and the engine of each pipeline', 'hero-shot')}
  </section>

  <section class="band">
    <div class="section-head">
      <h2>Debezium, without the sharp edges</h2>
      <p>Debezium is powerful and easy to get wrong: a forgotten replication slot fills the disk with WAL, the binlog a connector needs is purged, a snapshot locks a table, a dashboard stays green while nothing flows. BeziSync turns that knowledge into the product.</p>
    </div>
    ${story('Checked before it deploys', 'Every source, destination and runtime is checked, and each failure says exactly what to change: the GRANT, the parameter group setting, the retention to raise, ready to send to your DBA. Nothing deploys until it passes.', 'connection-checks.jpg', 'Readiness checks of a PostgreSQL source, each failure with the exact fix', more('/docs/reference/checks/', 'Every check and its fix'))}
    ${story('Health you can trust', 'Status comes only from fresh evidence: offsets moving, heartbeats arriving, a canary row making the trip. Missing or stale data is Unknown, never green, and every state says why.', 'pipeline-overview.jpg', 'A pipeline with its live flow, throughput and evidence-based health', more('/docs/operate/', 'Operating pipelines'), true)}
    ${story('Personal data, decided per pipeline', 'Pick a table and every column that looks personal (email, phone, address, card, ID numbers) is found at once. Then choose for each one: mask it, remove it or keep it on purpose. The choice is part of the pipeline, versioned and audited like the rest.', 'wizard-tables.jpg', 'Personal columns found in the Tables step, each waiting for a choice', more('/docs/personal-data/', 'Personal data'))}
  </section>

  <section class="features engines">
    <div class="section-head"><h2>Two engines, one control plane</h2><p>Pick the engine for each pipeline: Debezium Server for events and every source, Flink CDC for tables kept in sync in lakehouse, analytics and search stores. Both get the same preflight checks, health, guardrails and audit.</p></div>
    <div class="grid">${engines.map((e) => card(e)).join('')}</div>
    <p class="section-more"><a class="more" href="/docs/choose-an-engine/">Which engine for which pipeline →</a></p>
  </section>

  <section class="features use-cases">
    <div class="section-head"><h2>What teams build with it</h2><p>One source, one destination, one engine per pipeline.</p></div>
    <div class="grid">${useCases.map(([icon, title, text, engine]) => card([icon, title, text], `<p class="engine">${esc(engine)}</p>`)).join('')}</div>
  </section>

  <section class="adopt">
    <div class="section-head"><h2>Already running Debezium Server?</h2><p>Paste its <code>application.properties</code>. BeziSync maps the tables, snapshot mode, naming, formats and SMTs into a pipeline, and says which properties it manages, keeps or refuses. Stop the old server, start the pipeline from "Only new changes", and drop the old slot afterwards.</p>
    <p><a class="button ghost" href="/docs/adopt-an-existing-debezium-server/">Adopt an existing Debezium Server</a></p></div>
  </section>

  <section class="features">
    <div class="section-head"><h2>Everything a CDC platform needs</h2></div>
    <div class="grid">${[...features, transforms].map((f) => card(f)).join('')}</div>
  </section>

  <section class="connectors">
    <div class="section-head"><h2>Connects what you run</h2><p>On Docker or Kubernetes, in your cluster or remote through an outbound-only agent. Untagged ones run on Debezium Server.</p></div>
    <div class="chips-row"><p><a href="/docs/sources/">Sources</a></p><div class="chips">${sources.map((s) => chip(s, true, flinkSources.includes(s))).join('')}</div></div>
    <div class="chips-row"><p><a href="/docs/destinations/">Destinations</a></p><div class="chips">${[...destinations, ...flinkOnly].map((d) => chip(d, destinations.includes(d), flinkDestinations.includes(d))).join('')}</div></div>
  </section>

  <section class="how">
    <div class="section-head"><h2>Small enough to run anywhere</h2><p>One Go binary (API, controller, agent and CLI) and a web UI. PostgreSQL is the only dependency, and it is the job queue too.</p></div>
    <div class="diagram" role="img" aria-label="Browser, CLI and AI agents talk to the BeziSync API, which stores state in PostgreSQL; the controller runs one Debezium Server or Flink CDC cluster per pipeline on Docker or Kubernetes, or on remote clusters through an agent.">
      <div class="node">Browser · CLI · AI agents (MCP)</div>
      <div class="arrow">↓</div>
      <div class="row"><div class="node accent">BeziSync API</div><div class="link">⟷</div><div class="node">PostgreSQL<small>metadata · jobs · audit · offsets</small></div></div>
      <div class="arrow">↓</div>
      <div class="row"><div class="node accent">Controller</div><div class="link">→</div><div class="node">Docker / Kubernetes<small>or a remote cluster via the agent</small></div><div class="link">→</div><div class="node">Debezium Server or Flink CDC<small>one per pipeline</small></div></div>
    </div>
  </section>

  <section class="start" id="get-started">
    <div class="section-head"><h2>Start in two minutes</h2></div>
    <div class="start-grid">
      <div class="start-card">
        <h3>Docker</h3>
        <p>A sample shop database with live orders, Kafka and a ready-made template.</p>
        <div class="code"><button class="copy" type="button" aria-label="Copy">Copy</button><pre><code>git clone ${REPO} && cd bezisync
docker compose -f deploy/compose/docker-compose.yml up -d --build</code></pre></div>
        <p class="small">Open http://localhost:8080 and sign in as <code>admin@bezisync.local</code> / <code>bezisync-demo</code>. Or <a href="/docs/codespaces-lab/">run your own lab in GitHub Codespaces</a>.</p>
      </div>
      <div class="start-card">
        <h3>Kubernetes</h3>
        <p>The Helm chart, with a bundled PostgreSQL for evaluation.</p>
        <div class="code"><button class="copy" type="button" aria-label="Copy">Copy</button><pre><code>helm install bezisync oci://ghcr.io/daun-gatal/charts/bezisync \\
  --namespace bezisync --create-namespace \\
  --set postgres.enabled=true \\
  --set secrets.masterKey="$(docker run --rm ghcr.io/daun-gatal/bezisync generate-master-key)"</code></pre></div>
        <p class="small">Production setup in the <a href="/docs/install/">install guide</a>.</p>
      </div>
    </div>
  </section>

  <section class="final">
    <h2>Open source, built in the open</h2>
    <p>BeziSync is Apache 2.0 licensed. Read the code, open an issue, or send a pull request.</p>
    <div class="cta"><a class="button" href="${REPO}">Star on GitHub</a><a class="button ghost" href="${REPO}/blob/main/README.md#contributing">Contribute</a></div>
  </section>
</main>
${footer(docsVersion)}
</body>
</html>`;
}
