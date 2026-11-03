import { useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { Bot } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { InlineError, Notice } from '@/components/ui/feedback';
import { Check, Field, Segmented } from '@/components/ui/form';
import { QueryState } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import { Meta, Title } from '@/components/ui/text';
import { client, unwrap } from '@/lib/api';
import { useMeQuery } from '@/lib/session';
import { grantDays } from './mcp';

/**
 * An MCP agent asks to use BeziSync as the signed-in person (OAuth consent). They choose which
 * projects it may open, whether it may only read, and for how long; the agent then never gets
 * more than their own role allows.
 */
export function ConsentPage({ request, error }: { request?: string; error?: string }) {
  const me = useMeQuery().data!;
  const info = useQuery({
    queryKey: ['oauth-request', request],
    enabled: Boolean(request),
    retry: false,
    queryFn: () => unwrap(client.GET('/api/v1/oauth/requests/{request_id}', { params: { path: { request_id: request! } } })),
  });
  const [project, setProject] = useState('');
  const [access, setAccess] = useState<'role' | 'read'>('role');
  const [days, setDays] = useState<string | null>(null);
  const [allowAgent, setAllowAgent] = useState(false);
  const go = (to: string) => window.location.assign(to);
  const approve = useMutation({
    mutationFn: (max: number) =>
      unwrap(
        client.POST('/api/v1/oauth/requests/{request_id}/approve', {
          params: { path: { request_id: request! } },
          body: { project_id: project || null, read_only: access === 'read', days: Number(days ?? Math.min(30, max)), allow_agent: allowAgent },
        }),
      ),
    onSuccess: (r) => go(r.redirect_to),
  });
  const deny = useMutation({
    mutationFn: () => unwrap(client.POST('/api/v1/oauth/requests/{request_id}/deny', { params: { path: { request_id: request! } } })),
    onSuccess: (r) => go(r.redirect_to),
  });

  return (
    <main className="flex min-h-screen flex-col items-center justify-center gap-4 bg-bg px-4 py-12">
      <div className="flex w-full max-w-md flex-col gap-5 rounded-2xl border border-line bg-surface p-7">
        <div className="flex items-center gap-3">
          <span className="grid size-9 shrink-0 place-items-center rounded-lg bg-accent-soft text-accent-ink">
            <Bot className="size-5" aria-hidden />
          </span>
          <Title as="h1" className="text-xl font-semibold">
            {info.data ? `${info.data.client_name} wants to use BeziSync` : 'Connect an agent'}
          </Title>
        </div>
        {error || !request ? (
          <Notice tone="bad">{error ?? 'This page is opened by an agent that asks to sign in. Start again from your agent.'}</Notice>
        ) : (
          <QueryState query={info} doing="Couldn’t load the sign-in request">
            {(r) => {
              const pending = r.client_status === 'pending';
              const blockedBy = pending && !me.user.is_superuser ? 'An Admin has to allow this agent first (Settings → MCP).' : null;
              const daysValue = days ?? String(Math.min(30, r.max_days));
              return (
                <>
                  <p className="m-0 text-base text-ink-2">
                    It will act as <strong className="font-semibold">{me.user.email}</strong> through BeziSync’s MCP tools, with no more than your role allows, and
                    returns to <code className="font-mono text-sm wrap-anywhere">{r.redirect_host}</code>.
                  </p>
                  <Field label="Projects it may open">
                    {(id) => (
                      <Select
                        id={id}
                        value={project}
                        onChange={setProject}
                        options={[
                          { value: '', label: 'Every project I can open' },
                          ...me.projects.map((p) => ({ value: p.id, label: p.name, description: p.slug })),
                        ]}
                      />
                    )}
                  </Field>
                  <Field label="What it may do" group>
                    {() => (
                      <Segmented
                        label="What it may do"
                        value={access}
                        onChange={setAccess}
                        options={[
                          { value: 'role', label: 'What my role allows' },
                          { value: 'read', label: 'Only read' },
                        ]}
                      />
                    )}
                  </Field>
                  <Field label="For" hint="It has to sign in again after that. You can disconnect it any time under Account.">
                    {(id) => <Select id={id} value={daysValue} onChange={setDays} options={grantDays(r.max_days)} />}
                  </Field>
                  {pending && me.user.is_superuser ? (
                    <Check
                      checked={allowAgent}
                      onChange={setAllowAgent}
                      label="Allow this agent for everyone"
                      description="It is new: new agents wait for an Admin. Only allow agents you trust."
                    />
                  ) : null}
                  {blockedBy ? <Notice tone="warn">{blockedBy}</Notice> : null}
                  <InlineError error={approve.error ?? deny.error} doing="Couldn’t finish connecting the agent" />
                  <div className="flex flex-wrap justify-end gap-2">
                    <Button loading={deny.isPending} onClick={() => deny.mutate()}>
                      Deny
                    </Button>
                    <Button
                      variant="primary"
                      loading={approve.isPending}
                      disabled={Boolean(blockedBy) || (pending && !allowAgent)}
                      why={blockedBy ?? 'Tick “Allow this agent for everyone” first.'}
                      onClick={() => approve.mutate(r.max_days)}
                    >
                      Allow
                    </Button>
                  </div>
                  {r.client_uri ? <Meta as="p">Agent’s site: {r.client_uri}</Meta> : null}
                </>
              );
            }}
          </QueryState>
        )}
      </div>
    </main>
  );
}
