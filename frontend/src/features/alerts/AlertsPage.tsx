import { useEffect, useRef, useState } from 'react';
import { useNavigate, useSearch } from '@tanstack/react-router';
import { BellRing, ChevronDown, ChevronRight, Pencil, Send, Trash2 } from 'lucide-react';
import { toast } from 'sonner';
import { Button, ButtonLink } from '@/components/ui/button';
import { Empty, InlineError, Notice, Pill } from '@/components/ui/feedback';
import { Check, ChipsInput, Field, Input, Switch } from '@/components/ui/form';
import { Card, Page, PageHeader, QueryState } from '@/components/ui/layout';
import { Pager, scrollBox, usePaged, useScrollTopOn } from '@/components/ui/pager';
import { Confirm, Menu, Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { DataTable, type Column } from '@/components/ui/table';
import { Tabs, TabPanel } from '@/components/ui/tabs';
import { Meta, plural, Title } from '@/components/ui/text';
import { cleanValues, SchemaForm, type FormValues, type JsonSchema } from '@/components/SchemaForm';
import type { Schemas } from '@/lib/api';
import { ago } from '@/lib/format';
import { useProject } from '@/lib/session';
import {
  useAcknowledge,
  useAlerts,
  useChannelKinds,
  useChannels,
  useCreateChannel,
  useDeleteChannel,
  useRules,
  useTestChannel,
  useUpdateChannel,
  useUpdateRule,
  type Alert,
  type Channel,
  type Rule,
} from '@/lib/alerts';

type Tab = 'firing' | 'rules' | 'channels';

function duration(seconds: number): string {
  if (seconds === 0) return 'Immediately';
  if (seconds < 60) return `${seconds} s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} min`;
  return `${(seconds / 3600).toFixed(1)} h`;
}

export function AlertsPage() {
  const { project } = useProject();
  const search = useSearch({ from: '/p/$projectSlug/alerts' });
  const navigate = useNavigate({ from: '/p/$projectSlug/alerts' });
  const tab: Tab = search.tab ?? 'firing';
  const open = useAlerts(project.id, 'open');
  return (
    <Page>
      <PageHeader title="Alerts" description="What needs a human now, who owns it, and what to do." />
      <Tabs
        idBase="alerts"
        label="Alert sections"
        value={tab}
        onChange={(t) => void navigate({ search: t === 'firing' ? {} : { tab: t } })}
        tabs={[
          { id: 'firing', label: 'Open', count: open.data ? open.data.length : null },
          { id: 'rules', label: 'Rules' },
          { id: 'channels', label: 'Channels' },
        ]}
      />
      <TabPanel idBase="alerts" value={tab}>
        {tab === 'firing' ? <OpenAlerts /> : tab === 'rules' ? <Rules /> : <Channels />}
      </TabPanel>
    </Page>
  );
}

function AlertCard({ alert }: { alert: Alert }) {
  const { project, can } = useProject();
  const ack = useAcknowledge(project.id);
  const resolved = alert.state === 'resolved';
  const delivered = alert.deliveries.filter((d) => (d as { ok?: boolean }).ok).length;
  const failed = alert.deliveries.some((d) => !(d as { ok?: boolean }).ok);
  return (
    <section
      aria-label={alert.title}
      data-tone={resolved ? undefined : alert.severity === 'critical' ? 'bad' : 'warn'}
      className="bz-striped flex min-w-0 flex-col gap-2.5 rounded-[10px] border border-line bg-surface p-card"
    >
      <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1.5">
        <Pill tone={resolved ? 'ok' : alert.severity === 'critical' ? 'bad' : 'warn'}>
          {resolved ? 'Resolved' : alert.severity === 'critical' ? 'Critical' : 'Warning'}
        </Pill>
        <Title as="strong" className="min-w-0 flex-[1_1_240px] text-lg font-semibold">
          {alert.title}
        </Title>
      </div>
      <Meta as="p" className="wrap-anywhere">
        {resolved ? `Resolved ${ago(alert.resolved_at)}` : `Firing for ${ago(alert.fired_at ?? alert.started_at).replace(' ago', '')}`}
        {alert.pipeline_owner ? ` · owner ${alert.pipeline_owner}` : ''} · rule “{alert.rule_name}”
        {alert.acknowledged_by ? ` · acknowledged by ${alert.acknowledged_by}` : ''}
      </Meta>
      {alert.message ? <p className="m-0 text-base text-ink-2 wrap-break-word">{alert.message}</p> : null}
      {alert.fix && !resolved ? (
        <p className="m-0 text-base wrap-break-word">
          <strong>What to do:</strong> {alert.fix}
        </p>
      ) : null}
      <div className="flex flex-wrap items-center gap-2">
        <ButtonLink
          size="sm"
          to="/p/$projectSlug/pipelines/$pipelineId"
          params={{ projectSlug: project.slug, pipelineId: alert.pipeline_id }}
          title={alert.pipeline_name}
        >
          Open pipeline
        </ButtonLink>
        {alert.state === 'firing' && can('alert.ack') ? (
          <Button size="sm" loading={ack.isPending} onClick={() => ack.mutate(alert.id, { onSuccess: () => toast.success('Acknowledged') })}>
            Acknowledge
          </Button>
        ) : null}
        {alert.deliveries.length ? (
          <Meta>
            Notified {delivered}×{failed ? ' · some deliveries failed' : ''}
          </Meta>
        ) : null}
      </div>
      <InlineError error={ack.error} doing="Couldn’t acknowledge the alert" />
    </section>
  );
}

function OpenAlerts() {
  const { project } = useProject();
  const search = useSearch({ from: '/p/$projectSlug/alerts' });
  const navigate = useNavigate({ from: '/p/$projectSlug/alerts' });
  const open = useAlerts(project.id, 'open');
  const resolved = useAlerts(project.id, 'resolved');
  const [showResolved, setShowResolved] = useState(false);
  return (
    <QueryState query={open} doing="Couldn’t load alerts">
      {(alerts) => (
        <div className="flex min-w-0 flex-col gap-4">
          {alerts.length === 0 ? (
            <Card>
              <Empty icon={<BellRing />} title="Nothing needs attention">
                Alerts appear here when a rule’s condition holds for its duration.
              </Empty>
            </Card>
          ) : (
            <AlertList
              label="Open alerts"
              noun="open alerts"
              alerts={alerts}
              page={search.page}
              onPage={(p) => void navigate({ search: (s) => ({ ...s, page: p > 1 ? p : undefined }) })}
            />
          )}
          {resolved.data?.length ? (
            <>
              <Button
                variant="ghost"
                className="self-start"
                aria-expanded={showResolved}
                icon={showResolved ? <ChevronDown /> : <ChevronRight />}
                onClick={() => setShowResolved(!showResolved)}
              >
                Recently resolved ({resolved.data.length})
              </Button>
              {showResolved ? <AlertList label="Recently resolved alerts" noun="resolved alerts" alerts={resolved.data} /> : null}
            </>
          ) : null}
        </div>
      )}
    </QueryState>
  );
}

/** A page of alert cards in their own scroll box, with the pager under it. */
function AlertList({ label, noun, alerts, page, onPage }: { label: string; noun: string; alerts: Alert[]; page?: number; onPage?: (p: number) => void }) {
  const paged = usePaged(alerts, { page, onPage });
  const box = useRef<HTMLDivElement>(null);
  useScrollTopOn(box, paged.page);
  return (
    <Card pad={false}>
      <div ref={box} role="region" aria-label={label} tabIndex={0} className={`${scrollBox('page')} flex min-w-0 flex-col gap-3 p-card`}>
        {paged.shown.map((a) => (
          <AlertCard key={a.id} alert={a} />
        ))}
      </div>
      <Pager total={paged.total} page={paged.page} onPage={paged.setPage} noun={noun} />
    </Card>
  );
}

type RuleBody = Schemas['RuleUpdate'];

function Rules() {
  const { project, can } = useProject();
  const rules = useRules(project.id);
  const channels = useChannels(project.id);
  const update = useUpdateRule(project.id);
  const manage = can('alert.manage');
  // Changes show at once (row-level state) and are confirmed in the row: "Saving…", "Saved", or why it failed.
  const [pending, setPending] = useState<Record<string, RuleBody>>({});
  const [status, setStatus] = useState<Record<string, 'saving' | 'saved' | { error: unknown }>>({});
  const save = (rule: Rule, body: RuleBody) => {
    setPending((m) => ({ ...m, [rule.id]: { ...m[rule.id], ...body } }));
    setStatus((m) => ({ ...m, [rule.id]: 'saving' }));
    update.mutate(
      { id: rule.id, body },
      {
        onSuccess: () => {
          setStatus((m) => ({ ...m, [rule.id]: 'saved' }));
          setPending(({ [rule.id]: _, ...rest }) => rest);
        },
        onError: (error) => {
          setStatus((m) => ({ ...m, [rule.id]: { error } }));
          setPending(({ [rule.id]: _, ...rest }) => rest);
        },
      },
    );
  };
  const view = (r: Rule): Rule => ({ ...r, ...(pending[r.id] as Partial<Rule>) });
  const chans = channels.data ?? [];

  const columns: Column<Rule>[] = [
    {
      id: 'rule',
      header: 'Rule',
      min: 220,
      cell: (raw) => {
        const r = view(raw);
        const st = status[r.id];
        return (
          <div className="flex min-w-0 flex-col gap-1">
            <Switch
              checked={r.enabled}
              disabled={!manage}
              why="Only people who manage alerts can change rules."
              onChange={(on) => save(raw, { enabled: on })}
              label={<strong>{r.name}</strong>}
              description={r.description}
            />
            {st === 'saving' ? <Meta className="pl-12">Saving…</Meta> : st === 'saved' ? <span className="pl-12 text-xs text-ok">Saved</span> : null}
            {st && typeof st === 'object' ? <InlineError error={st.error} doing={`Couldn’t save “${r.name}”`} /> : null}
          </div>
        );
      },
    },
    {
      id: 'severity',
      header: 'Severity',
      min: 130,
      cell: (raw) => (
        <Select
          label={`Severity of ${raw.name}`}
          value={view(raw).severity}
          disabled={!manage}
          onChange={(v) => save(raw, { severity: v as 'critical' | 'warning' })}
          options={[
            { value: 'critical', label: 'Critical', description: 'Pages the channels' },
            { value: 'warning', label: 'Warning', description: 'Inbox and channels, no paging' },
          ]}
        />
      ),
    },
    {
      id: 'after',
      header: 'Fires after',
      min: 140,
      cell: (raw) => {
        const cur = view(raw).for_seconds;
        return (
          <Select
            label={`Duration of ${raw.name}`}
            value={String(cur)}
            disabled={!manage}
            onChange={(v) => save(raw, { for_seconds: Number(v) })}
            options={[...new Set([0, 30, 60, 120, 300, 900, 1800, cur])].sort((a, b) => a - b).map((s) => ({ value: String(s), label: duration(s) }))}
          />
        );
      },
    },
    {
      id: 'notify',
      header: 'Notify',
      min: 200,
      wide: true,
      cell: (raw) => {
        const r = view(raw);
        return (
          <div className="flex min-w-0 flex-col gap-1.5 text-sm">
            {chans.map((c) => (
              <Check
                key={c.id}
                label={<span className="wrap-anywhere">{c.name}</span>}
                checked={r.channel_ids.includes(c.id)}
                disabled={!manage}
                onChange={(on) => save(raw, { channel_ids: on ? [...r.channel_ids, c.id] : r.channel_ids.filter((x) => x !== c.id) })}
              />
            ))}
            <Check label="Email the pipeline owner" checked={r.notify_owner} disabled={!manage} onChange={(on) => save(raw, { notify_owner: on })} />
            {chans.length === 0 ? <Meta>In-app inbox only. Add a channel to notify Slack, PagerDuty, email or a webhook.</Meta> : null}
          </div>
        );
      },
    },
  ];

  return (
    <Card pad={false}>
      <QueryState query={rules} doing="Couldn’t load alert rules">
        {(data) => <DataTable label="Alert rules" columns={columns} rows={data} rowKey={(r) => r.id} />}
      </QueryState>
    </Card>
  );
}

function Channels() {
  const { project, can } = useProject();
  const channels = useChannels(project.id);
  const rules = useRules(project.id);
  const remove = useDeleteChannel(project.id);
  const test = useTestChannel(project.id);
  const [editing, setEditing] = useState<{ existing?: Channel } | null>(null);
  const [deleting, setDeleting] = useState<Channel | null>(null);
  const [tests, setTests] = useState<Record<string, { ok: boolean; message: string } | { error: unknown }>>({});
  const manage = can('alert.manage');
  const usedBy = (c: Channel) => (rules.data ?? []).filter((r) => r.channel_ids.includes(c.id));

  const sendTest = (c: Channel) =>
    test.mutate(c.id, {
      onSuccess: (r) => setTests((m) => ({ ...m, [c.id]: { ok: r.ok, message: r.message } })),
      onError: (error) => setTests((m) => ({ ...m, [c.id]: { error } })),
    });

  return (
    <div className="flex min-w-0 flex-col gap-4">
      {manage ? (
        <div>
          <Button variant="primary" onClick={() => setEditing({})}>
            Add channel
          </Button>
        </div>
      ) : null}
      <QueryState query={channels} doing="Couldn’t load channels">
        {(list) =>
          list.length === 0 ? (
            <Card>
              <Empty title="No channels">Alerts still appear here and in everyone’s inbox. Add Slack, PagerDuty, email or a webhook to be paged.</Empty>
            </Card>
          ) : (
            <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,340px),1fr))] gap-4">
              {list.map((c) => {
                const t = tests[c.id];
                return (
                  <Card
                    key={c.id}
                    title={c.name}
                    actions={
                      manage ? (
                        <Menu
                          label={`Actions for ${c.name}`}
                          icon="more"
                          variant="ghost"
                          size="sm"
                          items={[
                            { label: 'Edit', icon: <Pencil />, onSelect: () => setEditing({ existing: c }) },
                            {
                              label: 'Delete',
                              icon: <Trash2 />,
                              danger: true,
                              onSelect: () => {
                                remove.reset();
                                setDeleting(c);
                              },
                            },
                          ]}
                        />
                      ) : null
                    }
                  >
                    <div className="flex flex-wrap items-center gap-2">
                      <Pill tone="grey">{c.kind}</Pill>
                      {!c.enabled ? <Pill tone="grey">Off</Pill> : null}
                      {c.last_error ? (
                        <Pill tone="bad">Last delivery failed</Pill>
                      ) : c.last_delivery_at ? (
                        <Pill tone="ok">{`Delivered ${ago(c.last_delivery_at)}`}</Pill>
                      ) : (
                        <Meta>Nothing sent yet</Meta>
                      )}
                    </div>
                    {c.last_error ? <p className="m-0 text-sm text-bad wrap-anywhere">{c.last_error}</p> : null}
                    <Meta>{usedBy(c).length ? `Used by ${plural(usedBy(c).length, 'rule')}` : 'No rule sends here yet'}</Meta>
                    {t ? (
                      'error' in t ? (
                        <InlineError error={t.error} doing="Couldn’t send the test" onRetry={() => sendTest(c)} />
                      ) : (
                        <Notice tone={t.ok ? 'ok' : 'bad'} title={t.ok ? 'Test delivered' : 'Test not delivered'}>
                          {t.message}
                        </Notice>
                      )
                    ) : null}
                    {manage ? (
                      <div>
                        <Button size="sm" icon={<Send />} loading={test.isPending && test.variables === c.id} onClick={() => sendTest(c)}>
                          Send test
                        </Button>
                      </div>
                    ) : null}
                  </Card>
                );
              })}
            </div>
          )
        }
      </QueryState>
      <ChannelDialog open={editing !== null} existing={editing?.existing} onOpenChange={(o) => !o && setEditing(null)} />
      <Confirm
        open={deleting !== null}
        onOpenChange={(o) => !o && setDeleting(null)}
        title={`Delete ${deleting?.name ?? ''}?`}
        effects={
          deleting
            ? usedBy(deleting).length
              ? [
                  `These rules stop notifying it: ${usedBy(deleting)
                    .map((r) => r.name)
                    .join(', ')}.`,
                  'Its stored secrets are destroyed.',
                ]
              : ['No rule sends here, so nothing stops paging.', 'Its stored secrets are destroyed.']
            : []
        }
        confirmLabel="Delete channel"
        pending={remove.isPending}
        error={remove.error}
        doing="Couldn’t delete the channel"
        onConfirm={() =>
          deleting &&
          remove.mutate(deleting.id, {
            onSuccess: () => {
              toast.success(`Deleted ${deleting.name}`);
              setDeleting(null);
            },
          })
        }
      />
    </div>
  );
}

const EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

/** Add or edit a channel. Secrets are write-only: when editing, blank keeps the stored value. */
function ChannelDialog({ open, onOpenChange, existing }: { open: boolean; onOpenChange: (open: boolean) => void; existing?: Channel }) {
  const { project, expert } = useProject();
  const kinds = useChannelKinds(project.id);
  const create = useCreateChannel(project.id);
  const update = useUpdateChannel(project.id);
  const [kind, setKind] = useState('slack');
  const [name, setName] = useState('');
  const [config, setConfig] = useState<FormValues>({});
  const [secrets, setSecrets] = useState<FormValues>({});
  const [recipients, setRecipients] = useState<string[]>([]);
  const [touched, setTouched] = useState(false);
  useEffect(() => {
    if (!open) return;
    const cfg = (existing?.config ?? {}) as FormValues;
    setKind(existing?.kind ?? 'slack');
    setName(existing?.name ?? '');
    setConfig(existing ? cfg : {});
    setSecrets({});
    setRecipients(existing?.kind === 'email' && Array.isArray(cfg.recipients) ? (cfg.recipients as string[]) : []);
    setTouched(false);
    create.reset();
    update.reset();
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps
  const def = kinds.data?.find((k) => k.kind === kind);
  const mutation = existing ? update : create;
  const missing = !name.trim() ? 'Give the channel a name.' : kind === 'email' && recipients.length === 0 ? 'Add at least one recipient.' : null;
  const change =
    <T,>(set: (v: T) => void) =>
    (v: T) => {
      set(v);
      setTouched(true);
    };

  function submit() {
    if (!def || missing) return;
    const cfg = kind === 'email' ? { recipients } : cleanValues(def.config_schema as JsonSchema, config);
    const sec = cleanValues(def.secret_schema as JsonSchema, secrets);
    const done = () => {
      toast.success(existing ? `Saved ${name}` : `Added ${name}`);
      onOpenChange(false);
    };
    if (existing) update.mutate({ id: existing.id, body: { name, config: cfg, ...(Object.keys(sec).length ? { secrets: sec } : {}) } }, { onSuccess: done });
    else create.mutate({ name, kind: kind as 'slack', config: cfg, secrets: sec, enabled: true }, { onSuccess: done });
  }

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      dirty={touched}
      title={existing ? `Edit ${existing.name}` : 'Add a notification channel'}
      why={missing}
      footer={
        <>
          <Button onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button variant="primary" disabled={Boolean(missing)} loading={mutation.isPending} onClick={submit}>
            {existing ? 'Save changes' : 'Add channel'}
          </Button>
        </>
      }
    >
      <Field label="Name">{(id) => <Input id={id} value={name} placeholder="Slack #cdc" onChange={(e) => change(setName)(e.target.value)} />}</Field>
      <Field label="Type" hint={existing ? 'The type can’t change. Add a new channel instead.' : undefined}>
        {(id) => (
          <Select
            id={id}
            value={kind}
            disabled={Boolean(existing)}
            onChange={(v) => {
              setKind(v);
              setConfig({});
              setSecrets({});
              setTouched(true);
            }}
            options={(kinds.data ?? []).map((k) => ({ value: k.kind, label: k.title }))}
          />
        )}
      </Field>
      {kind === 'email' ? (
        <Field label="Recipients" hint="Needs SMTP configured on the server.">
          {(id) => (
            <ChipsInput
              id={id}
              values={recipients}
              onChange={change(setRecipients)}
              placeholder="oncall@example.com"
              validate={(v) => (EMAIL.test(v) ? null : `“${v}” isn’t an email address.`)}
              hint="Press Enter after each address."
            />
          )}
        </Field>
      ) : def ? (
        <>
          <SchemaForm idPrefix="chc" schema={def.config_schema as JsonSchema} values={config} onChange={change(setConfig)} expert={expert} />
          <SchemaForm
            idPrefix="chs"
            schema={def.secret_schema as JsonSchema}
            values={secrets}
            onChange={change(setSecrets)}
            expert={expert}
            secret
            setSecrets={existing?.secret_fields_set ?? []}
          />
        </>
      ) : null}
      <InlineError error={mutation.error} doing={existing ? 'Couldn’t save the channel' : 'Couldn’t add the channel'} />
    </Modal>
  );
}
