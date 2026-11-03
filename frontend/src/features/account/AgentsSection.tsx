import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Bot } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Empty, InlineError } from '@/components/ui/feedback';
import { Segmented } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { CodeBlock } from '@/components/ui/overlays';
import { List, ListRow } from '@/components/ui/table';
import { Meta } from '@/components/ui/text';
import { client, unwrap, type Schemas } from '@/lib/api';
import { ago } from '@/lib/format';
import { AGENTS, agentSetup, checkSetup, connectModes, type Agent, type ConnectMode, type Setup } from '@/features/mcp/mcp';

type Grant = Schemas['OAuthGrantRead'];

/** Account → AI agents: how to connect one (MCP) and the ones you connected with sign-in. */
export function AgentsSection() {
  const qc = useQueryClient();
  const [picked, setPicked] = useState<ConnectMode | null>(null);
  const [agent, setAgent] = useState<Agent>('claude-code');
  const info = useQuery({ queryKey: ['mcp-info'], queryFn: () => unwrap(client.GET('/api/v1/mcp/info')) });
  const grants = useQuery({ queryKey: ['mcp-grants', 'mine'], queryFn: () => unwrap(client.GET('/api/v1/mcp/grants')) });
  const revoke = useMutation({
    mutationFn: (g: Grant) => unwrap(client.DELETE('/api/v1/mcp/grants/{grant_id}', { params: { path: { grant_id: g.id } } })),
    onSuccess: (_, g) => {
      toast.success(`Disconnected ${g.client_name}`);
      void qc.invalidateQueries({ queryKey: ['mcp-grants'] });
    },
  });
  return (
    <QueryState query={info} doing="Couldn’t load the MCP details">
      {(i) =>
        !i.enabled ? null : (
          <Card title="AI agents">
            <p className="m-0 text-base text-ink-2">
              Agents such as Claude can work with BeziSync for you through MCP: they see and do only what your role allows, and never handle passwords or keys.
            </p>
            {(() => {
              const modes = connectModes(i);
              if (!modes.length) return <Meta as="p">An Admin has turned off both ways to connect agents.</Meta>;
              const mode = picked && modes.includes(picked) ? picked : modes[0]!;
              // Agents that can't connect this way (ChatGPT only signs in) drop out of the picker.
              const agents = AGENTS.filter((a) => agentSetup(a.value, i.url, mode));
              const current = agents.some((a) => a.value === agent) ? agent : agents[0]!.value;
              const shown = agentSetup(current, i.url, mode)!;
              const setups: Setup[] = mode === 'token' ? [shown, checkSetup(i.url)] : [shown];
              return (
                <>
                  {modes.length > 1 ? (
                    <Segmented
                      label="How the agent connects"
                      value={mode}
                      onChange={setPicked}
                      options={[
                        { value: 'signin', label: 'Sign in' },
                        { value: 'token', label: 'API token' },
                      ]}
                    />
                  ) : null}
                  {mode === 'token' ? (
                    <Meta as="p">
                      Create a token under API tokens above and put it in place of &lt;your API token&gt;. A read-only token gives read-only tools, a
                      project token one project; revoking it disconnects the agent.
                    </Meta>
                  ) : null}
                  <Segmented label="Your agent" value={current} onChange={setAgent} options={agents} />
                  {setups.map((s) => (
                    <div key={s.label} className="flex min-w-0 flex-col gap-1.5">
                      <span className="text-sm font-medium text-ink-2">{s.label}</span>
                      <CodeBlock code={s.code} label={`${s.label} setup`} wrap />
                      <Meta>{s.note}</Meta>
                    </div>
                  ))}
                </>
              );
            })()}
            {i.oauth_enabled ? (
              <QueryState query={grants} doing="Couldn’t load your connected agents">
                {(list) =>
                  list.length === 0 ? (
                    <Empty icon={<Bot />} title="No agent connected">
                      Agents you sign in appear here, so you can disconnect them.
                    </Empty>
                  ) : (
                    <List label="Your connected agents">
                      {list.map((g) => (
                        <ListRow key={g.id}>
                          <div className="flex min-w-0 flex-[1_1_220px] flex-col gap-0.5">
                            <span className="font-semibold wrap-anywhere">{g.client_name}</span>
                            <Meta>
                              {g.project ? g.project.name : 'Every project you can open'} · {g.read_only ? 'read only' : 'your role'} · until{' '}
                              {new Date(g.expires_at).toLocaleDateString()}
                              {g.last_used_at ? ` · used ${ago(g.last_used_at)}` : ''}
                            </Meta>
                          </div>
                          <Button size="sm" loading={revoke.isPending && revoke.variables?.id === g.id} onClick={() => revoke.mutate(g)}>
                            Disconnect
                          </Button>
                        </ListRow>
                      ))}
                    </List>
                  )
                }
              </QueryState>
            ) : null}
            <InlineError error={revoke.error} doing="Couldn’t disconnect the agent" />
          </Card>
        )
      }
    </QueryState>
  );
}
