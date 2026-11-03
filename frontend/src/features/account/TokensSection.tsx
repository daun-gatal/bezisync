import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { KeySquare } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Notice, Pill } from '@/components/ui/feedback';
import { Check, Field, Input } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { CodeBlock, Confirm, Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { List, ListRow } from '@/components/ui/table';
import { Meta, Title } from '@/components/ui/text';
import { client, unwrap, type Schemas } from '@/lib/api';
import { ago, formatDateTime } from '@/lib/format';
import { useProject } from '@/lib/session';
import { agentSetup } from '@/features/mcp/mcp';

type Token = Schemas['TokenRead'];

const EXPIRY = [
  { value: '30', label: '30 days' },
  { value: '90', label: '90 days', description: 'Recommended' },
  { value: '180', label: '180 days' },
  { value: '365', label: '1 year' },
  { value: 'never', label: 'Never', description: 'Not recommended: a leaked token works until revoked' },
];

export function TokensSection() {
  const { me, project } = useProject();
  const qc = useQueryClient();
  const tokens = useQuery({ queryKey: ['tokens'], queryFn: () => unwrap(client.GET('/api/v1/auth/tokens')) });
  const mcp = useQuery({ queryKey: ['mcp-info'], queryFn: () => unwrap(client.GET('/api/v1/mcp/info')) });
  const [creating, setCreating] = useState(false);
  const [name, setName] = useState('');
  const [scope, setScope] = useState(project.id);
  const [readOnly, setReadOnly] = useState(false);
  const [expiry, setExpiry] = useState('90');
  const [created, setCreated] = useState<Token | null>(null);
  const [copied, setCopied] = useState(false);
  const [revoking, setRevoking] = useState<Token | null>(null);
  const create = useMutation({
    mutationFn: () =>
      unwrap(
        client.POST('/api/v1/auth/tokens', {
          body: { name: name.trim(), project_id: scope || null, read_only: readOnly, expires_in_days: expiry === 'never' ? null : Number(expiry) },
        }),
      ),
    onSuccess: (t) => {
      setCreating(false);
      setCopied(false);
      setCreated(t);
      void qc.invalidateQueries({ queryKey: ['tokens'] });
    },
  });
  const revoke = useMutation({
    mutationFn: (id: string) => unwrap(client.DELETE('/api/v1/auth/tokens/{token_id}', { params: { path: { token_id: id } } })),
    onSuccess: () => {
      toast.success('Token revoked');
      setRevoking(null);
      void qc.invalidateQueries({ queryKey: ['tokens'] });
    },
  });
  const projectName = (id: string | null | undefined) => (id ? (me.projects.find((p) => p.id === id)?.name ?? 'a project') : 'all your projects');
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="flex min-w-0 flex-[1_1_320px] flex-col gap-1">
          <h2 className="m-0 text-xl font-semibold">API tokens</h2>
          <p className="m-0 text-base text-muted">
            Personal tokens for the CLI, CI (<code className="font-mono">bezisync remote login</code>) and AI agents (MCP). They act as you, limited
            to one project and optionally read-only.
          </p>
        </div>
        <Button
          variant="primary"
          onClick={() => {
            setName('');
            setScope(project.id);
            setReadOnly(false);
            setExpiry('90');
            create.reset();
            setCreating(true);
          }}
        >
          New token
        </Button>
      </div>
      <Card pad={false}>
        <QueryState query={tokens} doing="Couldn’t load your tokens">
          {(list) =>
            list.length === 0 ? (
              <Empty icon={<KeySquare />} title="No API tokens">
                Create one to use the CLI or apply pipelines from CI.
              </Empty>
            ) : (
              <List label="API tokens">
                {list.map((t) => (
                  <ListRow key={t.id} tone={t.expires_at && Date.parse(t.expires_at) < Date.now() ? 'grey' : null}>
                    <span className="flex min-w-0 flex-[1_1_260px] flex-col gap-0.5">
                      <span className="flex min-w-0 flex-wrap items-center gap-2">
                        <Title as="strong" lines={1} className="text-base font-semibold">
                          {t.name}
                        </Title>
                        <span className="font-mono text-xs text-muted">{t.prefix}…</span>
                        {t.read_only ? <Pill tone="grey">read-only</Pill> : null}
                        {!t.expires_at ? <Pill tone="warn">never expires</Pill> : null}
                      </span>
                      <Meta>
                        {projectName(t.project_id)} · {t.expires_at ? `expires ${formatDateTime(t.expires_at)}` : 'no expiry'} ·{' '}
                        {t.last_used_at ? `used ${ago(t.last_used_at)}` : 'never used'}
                      </Meta>
                    </span>
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => {
                        revoke.reset();
                        setRevoking(t);
                      }}
                      aria-label={`Revoke ${t.name}`}
                    >
                      Revoke
                    </Button>
                  </ListRow>
                ))}
              </List>
            )
          }
        </QueryState>
      </Card>
      <Modal
        open={creating}
        onOpenChange={setCreating}
        dirty={Boolean(name)}
        title="New API token"
        why={name.trim() ? null : 'Name the token.'}
        footer={
          <>
            <Button onClick={() => setCreating(false)}>Cancel</Button>
            <Button variant="primary" disabled={!name.trim()} loading={create.isPending} onClick={() => create.mutate()}>
              Create token
            </Button>
          </>
        }
      >
        <Field label="Name" hint="Where it is used, e.g. “orders repo CI”.">
          {(id) => <Input id={id} value={name} maxLength={200} onChange={(e) => setName(e.target.value)} />}
        </Field>
        <Field label="Project">
          {(id) => (
            <Select
              id={id}
              value={scope}
              onChange={setScope}
              options={[
                ...me.projects.map((p) => ({ value: p.id, label: p.name })),
                { value: '', label: 'All my projects', description: 'Broader than most CI needs' },
              ]}
            />
          )}
        </Field>
        <Field label="Expires after">{(id) => <Select id={id} value={expiry} onChange={setExpiry} options={EXPIRY} />}</Field>
        <Check checked={readOnly} onChange={setReadOnly} label="Read-only" description="Can view and export, not change." />
        <InlineError error={create.error} doing="Couldn’t create the token" />
      </Modal>
      <Modal
        open={created !== null}
        onOpenChange={() => undefined}
        closable={false}
        title={`Token “${created?.name ?? ''}”`}
        why={copied ? null : 'Copy it first: it won’t be shown again.'}
        footer={
          <Button variant="primary" disabled={!copied} onClick={() => setCreated(null)}>
            Done
          </Button>
        }
      >
        <Notice tone="warn" title="Copy it now">
          It is shown only once. Store it in your CI secret store or password manager.
        </Notice>
        {created?.token ? <CodeBlock label="API token" code={created.token} wrap /> : null}
        {created?.token ? (
          <CodeBlock label="CLI login" wrap code={`bezisync remote login --url ${window.location.origin} --project ${project.slug} --token ${created.token}`} />
        ) : null}
        {created?.token && mcp.data?.enabled && mcp.data.api_tokens_allowed ? (
          <div className="flex min-w-0 flex-col gap-1.5">
            <span className="text-sm font-medium text-ink-2">Or connect an AI agent (Claude Code, MCP)</span>
            <CodeBlock label="MCP setup with this token" wrap code={agentSetup('claude-code', mcp.data.url, 'token', created.token)!.code} />
          </div>
        ) : null}
        <Check checked={copied} onChange={setCopied} label="I’ve copied it and stored it somewhere safe" />
      </Modal>
      <Confirm
        open={revoking !== null}
        onOpenChange={(o) => !o && setRevoking(null)}
        title={`Revoke “${revoking?.name ?? ''}”?`}
        effects={['Anything using it stops working at once.']}
        confirmLabel="Revoke"
        pending={revoke.isPending}
        error={revoke.error}
        doing="Couldn’t revoke the token"
        onConfirm={() => revoking && revoke.mutate(revoking.id)}
      />
    </div>
  );
}
