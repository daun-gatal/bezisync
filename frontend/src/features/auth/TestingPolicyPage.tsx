import type { ReactNode } from 'react';
import { MessageSquare } from 'lucide-react';
import { ErrorPanel, FullPageLoading } from '@/app/ErrorPanel';
import { sessionState } from '@/app/guard';
import { ButtonLink } from '@/components/ui/button';
import { Title } from '@/components/ui/text';
import { useMeQuery, useProviders } from '@/lib/session';

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <section className="flex flex-col gap-2.5">
      <h2 className="m-0 text-xl font-semibold text-ink">{title}</h2>
      {children}
    </section>
  );
}

function List({ children }: { children: ReactNode }) {
  return <ul className="m-0 flex flex-col gap-1.5 pl-5">{children}</ul>;
}

/** The public source repository: issues, private vulnerability reports and pull requests. */
const REPO_URL = 'https://github.com/daun-gatal/bezisync';

function Ext({ href, children }: { href: string; children: ReactNode }) {
  return (
    <a href={href} target="_blank" rel="noreferrer" className="font-semibold text-accent hover:underline">
      {children}
    </a>
  );
}

function B({ children }: { children: ReactNode }) {
  return <strong className="font-semibold text-ink">{children}</strong>;
}

/** The accounts testers sign in with, from the lab's providers: "a Google or GitHub account". */
export function accountsPhrase(labels: string[]): string {
  const names = [...new Set(labels.map((l) => l.trim()).filter(Boolean))];
  if (!names.length) return 'an account';
  const list = names.length === 1 ? names[0]! : `${names.slice(0, -1).join(', ')} or ${names.at(-1)}`;
  return `${/^[aeiou]/i.test(list) ? 'an' : 'a'} ${list} account`;
}

/** Signed in: back to the app. Signed out: sign in. Unknown yet: nothing, so it never offers the wrong one. */
function HeaderAction() {
  const state = sessionState(useMeQuery());
  if (state === 'loading') return null;
  return state === 'signed-in' ? (
    <ButtonLink to="/" variant="primary">
      Back to BeziSync
    </ButtonLink>
  ) : (
    <ButtonLink to="/login" variant="primary">
      Sign in to try it
    </ButtonLink>
  );
}

/** Public: what lab testers may try and how to report it. Linked from sign-in and the feedback form. */
export function TestingPolicyPage() {
  const providers = useProviders();
  if (providers.isPending) return <FullPageLoading />;
  if (!providers.data?.lab) {
    return <ErrorPanel title="No testing policy here" detail="This BeziSync is not an open lab." />;
  }
  const accounts = accountsPhrase(providers.data.providers.map((p) => p.label));
  return (
    <div className="min-h-screen bg-bg text-ink">
      <header className="flex flex-wrap items-center justify-between gap-3 border-b border-line bg-surface px-6 py-3">
        <span className="flex items-center gap-2.5">
          <img src="/favicon.svg" alt="" className="size-7" />
          <span className="text-lg font-bold">BeziSync Lab</span>
        </span>
        <HeaderAction />
      </header>

      <main className="mx-auto flex max-w-[760px] flex-col gap-7 px-5 pt-10 pb-16 text-lg leading-relaxed text-ink-2">
        <div className="flex flex-col gap-2">
          <Title as="h1" className="text-2xl font-bold text-ink">
            Testing policy
          </Title>
          <p className="m-0 text-lg text-muted">
            BeziSync is open source (Apache 2.0) and still below 1.0. Anyone with {accounts} can try it here, and we want you to push on it. This page says
            what&apos;s fair game and how to tell us what you find.
          </p>
        </div>

        <Section title="What you get">
          <List>
            <li>Signing in with {accounts} puts you in the <B>Playground</B> project as an editor. You can build, deploy and operate pipelines there.</li>
            <li>
              The Playground&apos;s databases (PostgreSQL, MySQL, SQL Server, MongoDB, Oracle, CockroachDB, Vitess, the Spanner emulator) and destinations (Kafka, Pulsar, Redis, NATS, a
              PostgreSQL warehouse, RustFS with Iceberg tables, the Kinesis, Pub/Sub, AWS and Event Hubs emulators, an HTTP echo) are disposable lab services.
            </li>
            <li>All their data is <B>synthetic</B>. The names, emails and phone numbers are generated, not real people.</li>
            <li>The Playground is shared with every other tester. What you deploy, others can see.</li>
          </List>
        </Section>

        <section className="flex flex-col gap-2 rounded-xl bg-ok-soft p-card text-ok">
          <h2 className="m-0 text-lg font-semibold">Share it freely</h2>
          <p className="m-0">
            Screenshots, posts and links to the lab are welcome. Tell people about it. The source code is open on{' '}
            <Ext href={REPO_URL}>GitHub</Ext>: read it, run your own copy, and send fixes.
          </p>
        </section>

        <Section title="What you may test">
          <p className="m-0">Try to break BeziSync&apos;s security promises. In particular:</p>
          <List>
            <li><B>Reaching beyond the Playground:</B> seeing or changing another project, account-wide settings, or other people.</li>
            <li>
              <B>More access than an editor has:</B> managing members, runtimes, policies or the audit log, changing the Playground&apos;s managed
              connections, or becoming an Admin.
            </li>
            <li><B>Leaked secrets:</B> a password, token or key showing up in the UI, the API, logs, check messages or a pipeline&apos;s generated config.</li>
            <li><B>Sign-in and session flaws:</B> session theft, cross-site request forgery, bypassing sign-in, tokens that keep working after they should stop.</li>
            <li><B>Injection:</B> cross-site scripting, SQL injection, or sneaking in Debezium settings that should be refused.</li>
            <li><B>Making the server connect where it shouldn&apos;t:</B> using a connection&apos;s host to reach cloud metadata, loopback or internal addresses.</li>
            <li><B>Escaping a pipeline</B> onto the machine or cluster it runs on.</li>
          </List>
        </Section>

        <Section title="Please don't">
          <List>
            <li>
              Test anything except BeziSync itself (this site and its <code className="font-mono text-base">/api/v1</code>). Leave the network,
              servers, Google and GitHub alone. If a finding gives you that kind of access, stop and report it.
            </li>
            <li>Use anyone else&apos;s account, session or token.</li>
            <li>Flood or load-test the lab, or try to fill its disks or memory. Describe the problem and we&apos;ll reproduce it, or run your own copy of BeziSync and load-test that.</li>
            <li>Enter real credentials, or connect BeziSync to a real database or service of yours or your employer&apos;s.</li>
            <li>Delete or wreck shared Playground resources. Clean up what you create when you&apos;re done.</li>
          </List>
          <p className="m-0">
            If you come across data or secrets you shouldn&apos;t see, stop, don&apos;t keep or share them, and tell us. Testing in good faith within
            these rules is welcome, and we won&apos;t take action against you for it.
          </p>
        </Section>

        <Section title="How to report">
          <p className="m-0">Report security findings privately, in either of two ways:</p>
          <List>
            <li>
              Inside the app, open <B>Feedback</B> at the bottom of the sidebar and pick <B>Security issue</B>. Only BeziSync Admins can read those.
            </li>
            <li>
              On GitHub, open <Ext href={`${REPO_URL}/security/advisories/new`}>Security → Report a vulnerability</Ext>. It works for findings in
              your own copy of BeziSync too.
            </li>
          </List>
          <p className="m-0">Please don&apos;t open a public issue or post security findings before we&apos;ve fixed them.</p>
          <div className="flex flex-wrap items-start gap-3 rounded-xl border border-line bg-surface p-card">
            <span className="grid size-8 shrink-0 place-items-center rounded-lg bg-accent-soft text-accent-ink" aria-hidden>
              <MessageSquare className="size-4" />
            </span>
            <span className="min-w-0 flex-[1_1_240px] text-base">
              A good report says what you did, what happened, what you expected, and how bad you think it is. The app attaches the page you were on
              and your browser for you. Leave out any real credentials or personal data.
            </span>
          </div>
        </Section>

        <Section title="What happens next">
          <List>
            <li>
              We reply within <B>3 business days</B>: to the email of the account you signed in with, or on your GitHub report.
            </li>
            <li>
              Serious issues (leaked secrets, escaping the Playground, extra access, running code where you shouldn&apos;t) are fixed first, released
              with a security advisory, and the lab is redeployed.
            </li>
            <li>We tell you when it&apos;s fixed, and credit you in the release notes if you&apos;d like.</li>
            <li>There&apos;s no bug bounty at this stage.</li>
          </List>
        </Section>

        <Section title="Not about security?">
          <p className="m-0">
            Confusing screens, wrong wording, ideas, or a pipeline that won&apos;t do what you expect are just as valuable to us right now. Send them
            through Feedback too, or <Ext href={`${REPO_URL}/issues/new/choose`}>open an issue on GitHub</Ext>.
          </p>
        </Section>
      </main>
    </div>
  );
}
