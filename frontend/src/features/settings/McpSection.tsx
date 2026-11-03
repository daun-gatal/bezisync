import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import clsx from "clsx";
import { Bot, ChevronRight, ShieldCheck, ShieldX, Trash2 } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  Empty,
  InlineError,
  Notice,
  Pill,
  type Tone,
} from "@/components/ui/feedback";
import { ChipsInput, Field, Search, Switch } from "@/components/ui/form";
import { Card, QueryState } from "@/components/ui/layout";
import { CodeBlock, Confirm, Menu } from "@/components/ui/overlays";
import { Select } from "@/components/ui/select";
import { List, ListRow } from "@/components/ui/table";
import { Meta, plural, Title } from "@/components/ui/text";
import { client, unwrap, type Schemas } from "@/lib/api";
import { ago } from "@/lib/format";
import {
  groupTools,
  hostError,
  KIND_LABEL,
  originError,
  toolOverrides,
  type McpTool,
} from "@/features/mcp/mcp";

type Settings = Schemas["MCPSettingsRead"];
type Agent = Schemas["OAuthClientRead"];
type Grant = Schemas["OAuthGrantRead"];

const KIND_TONE: Record<McpTool["kind"], Tone> = {
  read: "ok",
  write: "info",
  destructive: "bad",
};
const DAYS = [7, 30, 90, 180, 365].map((d) => ({
  value: String(d),
  label: d === 365 ? "1 year" : `${d} days`,
}));

/** Settings → MCP: the endpoint, how agents sign in, which tools exist, and who connected what. */
export function McpSection() {
  const qc = useQueryClient();
  const settings = useQuery({
    queryKey: ["mcp-settings"],
    queryFn: () => unwrap(client.GET("/api/v1/mcp/settings")),
  });
  const tools = useQuery({
    queryKey: ["mcp-tools"],
    queryFn: () => unwrap(client.GET("/api/v1/mcp/tools")),
  });
  const agents = useQuery({
    queryKey: ["mcp-clients"],
    queryFn: () => unwrap(client.GET("/api/v1/mcp/clients")),
  });
  const grants = useQuery({
    queryKey: ["mcp-grants", "all"],
    queryFn: () =>
      unwrap(
        client.GET("/api/v1/mcp/grants", { params: { query: { all: true } } }),
      ),
  });
  const [draft, setDraft] = useState<Settings | null>(null);
  const [toolState, setToolState] = useState<Record<string, boolean>>({});
  const [q, setQ] = useState("");
  const [openGroups, setOpenGroups] = useState<string[]>([]);
  const [removing, setRemoving] = useState<Agent | null>(null);
  useEffect(() => {
    if (settings.data && !draft) setDraft(settings.data);
  }, [settings.data, draft]);
  const refresh = () => {
    for (const k of [
      "mcp-settings",
      "mcp-tools",
      "mcp-clients",
      "mcp-grants",
      "mcp-info",
    ])
      void qc.invalidateQueries({ queryKey: [k] });
  };
  const overrides = useMemo(
    () => (tools.data ? toolOverrides(tools.data, toolState) : {}),
    [tools.data, toolState],
  );
  const dirty =
    Boolean(draft && settings.data) &&
    (JSON.stringify({ ...draft, tools: undefined }) !==
      JSON.stringify({ ...settings.data, tools: undefined }) ||
      JSON.stringify(overrides) !== JSON.stringify(settings.data?.tools ?? {}));

  const save = useMutation({
    mutationFn: () => {
      const { url: _url, ...rest } = draft!;
      return unwrap(
        client.PUT("/api/v1/mcp/settings", {
          body: { ...rest, tools: overrides },
        }),
      );
    },
    onSuccess: (s) => {
      toast.success("Saved the MCP settings");
      setDraft(s);
      setToolState({});
      refresh();
    },
  });
  const decide = useMutation({
    mutationFn: ({
      id,
      status,
    }: {
      id: string;
      status: "allowed" | "blocked";
    }) =>
      unwrap(
        client.PATCH("/api/v1/mcp/clients/{client_id}", {
          params: { path: { client_id: id } },
          body: { status },
        }),
      ),
    onSuccess: (a) => {
      toast.success(
        a.status === "allowed"
          ? `Allowed ${a.name}`
          : `Blocked ${a.name}: its access ended`,
      );
      refresh();
    },
  });
  const remove = useMutation({
    mutationFn: (id: string) =>
      unwrap(
        client.DELETE("/api/v1/mcp/clients/{client_id}", {
          params: { path: { client_id: id } },
        }),
      ),
    onSuccess: () => {
      toast.success(`Removed ${removing?.name ?? "the agent"}`);
      setRemoving(null);
      refresh();
    },
  });
  const revoke = useMutation({
    mutationFn: (g: Grant) =>
      unwrap(
        client.DELETE("/api/v1/mcp/grants/{grant_id}", {
          params: { path: { grant_id: g.id } },
        }),
      ),
    onSuccess: (_, g) => {
      toast.success(`Disconnected ${g.client_name} for ${g.user_email}`);
      refresh();
    },
  });

  const set = (patch: Partial<Settings>) =>
    draft && setDraft({ ...draft, ...patch });
  const isOn = (t: McpTool) => toolState[t.name] ?? t.enabled;
  const shown = (tools.data ?? []).filter(
    (t) =>
      !q ||
      `${t.title} ${t.name} ${t.description}`
        .toLowerCase()
        .includes(q.toLowerCase()),
  );

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="flex min-w-0 flex-[1_1_320px] flex-col gap-1">
          <h2 className="m-0 text-xl font-semibold">MCP</h2>
          <p className="m-0 text-base text-muted">
            Let AI agents (Claude, Cursor, …) work with BeziSync through the
            Model Context Protocol. An agent only ever does what the person who
            connected it may do; passwords and keys never go through it.
          </p>
        </div>
        <Button
          variant="primary"
          loading={save.isPending}
          disabled={!dirty}
          why="Nothing changed."
          onClick={() => save.mutate()}
        >
          Save
        </Button>
      </div>
      <InlineError error={save.error} doing="Couldn’t save the MCP settings" />

      <QueryState query={settings} doing="Couldn’t load the MCP settings">
        {() =>
          draft ? (
            <Card title="Endpoint">
              <Switch
                checked={draft.enabled}
                onChange={(enabled) => set({ enabled })}
                label="Turn on MCP"
                description="Off: the endpoint answers “not found” and no agent can connect."
              />
              <div className="flex min-w-0 flex-col gap-1.5">
                <Meta>Agents connect to</Meta>
                <CodeBlock
                  code={draft.url}
                  label="MCP endpoint"
                  dark={false}
                  wrap
                />
              </div>
              <Switch
                checked={draft.allow_api_tokens}
                onChange={(allow_api_tokens) => set({ allow_api_tokens })}
                label="Accept API tokens"
                description="People paste a token from Account → API tokens into their agent. Read-only tokens give read-only tools."
              />
              <Switch
                checked={draft.oauth_enabled}
                onChange={(oauth_enabled) => set({ oauth_enabled })}
                label="Let agents sign in"
                description="Agents open BeziSync to sign in (OAuth): the person picks the projects, read-only or not, and for how long."
              />
              {draft.oauth_enabled ? (
                <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,240px),1fr))] items-start gap-4">
                  <Field label="New agents">
                    {(id) => (
                      <Select
                        id={id}
                        value={draft.oauth_registration}
                        onChange={(oauth_registration) =>
                          set({ oauth_registration })
                        }
                        options={[
                          {
                            value: "open",
                            label: "Can be used at once",
                            description:
                              "Anyone signed in may connect any agent",
                          },
                          {
                            value: "approval",
                            label: "Wait for an Admin",
                            description: "You allow each new agent below",
                          },
                        ]}
                      />
                    )}
                  </Field>
                  <Field label="Access lasts at most">
                    {(id) => (
                      <Select
                        id={id}
                        value={String(draft.oauth_max_grant_days)}
                        onChange={(v) =>
                          set({ oauth_max_grant_days: Number(v) })
                        }
                        options={
                          DAYS.some(
                            (d) =>
                              d.value === String(draft.oauth_max_grant_days),
                          )
                            ? DAYS
                            : [
                                ...DAYS,
                                {
                                  value: String(draft.oauth_max_grant_days),
                                  label: `${draft.oauth_max_grant_days} days`,
                                },
                              ]
                        }
                      />
                    )}
                  </Field>
                  <Field
                    label="Agents may return to"
                    optional
                    hint="Hosts for https redirects. Empty: any https host; agents on people’s computers (localhost) always work."
                  >
                    {(id) => (
                      <ChipsInput
                        id={id}
                        values={draft.oauth_redirect_hosts}
                        onChange={(oauth_redirect_hosts) =>
                          set({ oauth_redirect_hosts })
                        }
                        placeholder="claude.ai"
                        validate={hostError}
                      />
                    )}
                  </Field>
                </div>
              ) : null}
              <Field
                label="Browser origins allowed"
                optional
                hint="Only for agents running in a web page. Desktop and command-line agents send no origin."
              >
                {(id) => (
                  <ChipsInput
                    id={id}
                    values={draft.allowed_origins}
                    onChange={(allowed_origins) => set({ allowed_origins })}
                    placeholder="https://agent.example.com"
                    validate={originError}
                  />
                )}
              </Field>
              {draft.enabled &&
              !draft.allow_api_tokens &&
              !draft.oauth_enabled ? (
                <Notice tone="warn">
                  With API tokens and sign-in both off, no agent can connect.
                </Notice>
              ) : null}
            </Card>
          ) : null
        }
      </QueryState>

      <QueryState query={tools} doing="Couldn’t load the MCP tools">
        {(list) => (
          <Card
            title={`Tools · ${list.filter(isOn).length} of ${list.length} on`}
            pad={false}
          >
            <div className="flex flex-col gap-3 border-b border-line px-card py-3">
              <Meta as="p">
                Each person sees only the tools their role allows. Tools that
                delete or reset are off until you turn them on. Tools that would
                take or show a credential (API tokens, runtime and secret-store
                keys, sign-in providers) don’t exist.
              </Meta>
              <div>
                <Search
                  label="Search tools"
                  placeholder="Search tools…"
                  value={q}
                  onChange={setQ}
                />
              </div>
            </div>
            {groupTools(shown).map(([group, items]) => {
              const open = Boolean(q) || openGroups.includes(group);
              return (
                <div
                  key={group}
                  className="border-b border-line last:border-b-0"
                >
                  <button
                    type="button"
                    aria-expanded={open}
                    onClick={() =>
                      setOpenGroups((g) =>
                        g.includes(group)
                          ? g.filter((x) => x !== group)
                          : [...g, group],
                      )
                    }
                    className="flex min-h-row w-full min-w-0 items-center gap-3 px-card py-2 text-left hover:bg-hover"
                  >
                    <ChevronRight
                      className={clsx(
                        "size-4 shrink-0 text-muted transition-transform",
                        open && "rotate-90",
                      )}
                      aria-hidden
                    />
                    <span className="font-semibold">{group}</span>
                    <Meta className="ml-auto shrink-0">
                      {items.filter(isOn).length} of {items.length} on
                    </Meta>
                  </button>
                  {open ? (
                    <List label={`${group} tools`}>
                      {items.map((t) => (
                        <ListRow key={t.name}>
                          <div className="flex min-w-0 flex-[1_1_260px] flex-col gap-0.5">
                            <Switch
                              checked={isOn(t)}
                              onChange={(on) =>
                                setToolState((s) => ({ ...s, [t.name]: on }))
                              }
                              label={
                                <>
                                  {t.title}{" "}
                                  <code className="font-mono text-xs text-muted">
                                    {t.name}
                                  </code>
                                </>
                              }
                              description={t.description}
                            />
                          </div>
                          <span className="flex shrink-0 flex-wrap gap-1.5">
                            <Pill tone={KIND_TONE[t.kind]}>
                              {KIND_LABEL[t.kind]}
                            </Pill>
                            {t.admin_only ? (
                              <Pill tone="grey">Admins</Pill>
                            ) : null}
                          </span>
                        </ListRow>
                      ))}
                    </List>
                  ) : null}
                </div>
              );
            })}
          </Card>
        )}
      </QueryState>

      <QueryState query={agents} doing="Couldn’t load the agents">
        {(list) => (
          <Card title="Agents" pad={list.length === 0}>
            {list.length === 0 ? (
              <Empty icon={<Bot />} title="No agent has signed in yet">
                Agents register themselves the first time someone connects them
                with sign-in.
              </Empty>
            ) : (
              <List label="Agents">
                {list.map((a) => (
                  <ListRow
                    key={a.id}
                    tone={a.status === "pending" ? "warn" : null}
                  >
                    <div className="flex min-w-0 flex-[1_1_240px] flex-col gap-0.5">
                      <Title lines={1} className="font-semibold">
                        {a.name}
                      </Title>
                      <Meta>
                        {plural(a.active_grants, "connection")} · registered{" "}
                        {ago(a.created_at)}
                        {a.last_used_at ? ` · used ${ago(a.last_used_at)}` : ""}
                      </Meta>
                      <Meta className="wrap-anywhere">
                        Returns to {a.redirect_uris.join(", ")}
                      </Meta>
                    </div>
                    <Pill
                      tone={
                        a.status === "allowed"
                          ? "ok"
                          : a.status === "pending"
                            ? "warn"
                            : "bad"
                      }
                    >
                      {a.status === "allowed"
                        ? "Allowed"
                        : a.status === "pending"
                          ? "Waiting for you"
                          : "Blocked"}
                    </Pill>
                    <Menu
                      label={`Actions for ${a.name}`}
                      icon="more"
                      variant="ghost"
                      size="sm"
                      items={[
                        a.status !== "allowed"
                          ? {
                              label: "Allow",
                              icon: <ShieldCheck />,
                              onSelect: () =>
                                decide.mutate({ id: a.id, status: "allowed" }),
                            }
                          : null,
                        a.status !== "blocked"
                          ? {
                              label: "Block",
                              description: "Ends every connection it has",
                              icon: <ShieldX />,
                              onSelect: () =>
                                decide.mutate({ id: a.id, status: "blocked" }),
                            }
                          : null,
                        {
                          label: "Remove",
                          icon: <Trash2 />,
                          danger: true,
                          onSelect: () => setRemoving(a),
                        },
                      ]}
                    />
                  </ListRow>
                ))}
              </List>
            )}
            <InlineError
              error={decide.error}
              doing="Couldn’t change the agent"
            />
          </Card>
        )}
      </QueryState>

      <QueryState query={grants} doing="Couldn’t load the connected agents">
        {(list) => (
          <Card
            title={`Connected agents · ${list.length}`}
            pad={list.length === 0}
          >
            {list.length === 0 ? (
              <Meta as="p">Nobody has an agent connected with sign-in.</Meta>
            ) : (
              <List label="Connected agents">
                {list.map((g) => (
                  <ListRow key={g.id}>
                    <div className="flex min-w-0 flex-[1_1_240px] flex-col gap-0.5">
                      <span className="font-semibold wrap-anywhere">
                        {g.client_name} · {g.user_email}
                      </span>
                      <Meta>
                        {g.project
                          ? g.project.name
                          : "Every project they can open"}{" "}
                        · {g.read_only ? "read only" : "their role"} · until{" "}
                        {new Date(g.expires_at).toLocaleDateString()}
                        {g.last_used_at ? ` · used ${ago(g.last_used_at)}` : ""}
                      </Meta>
                    </div>
                    <Button
                      size="sm"
                      loading={
                        revoke.isPending && revoke.variables?.id === g.id
                      }
                      onClick={() => revoke.mutate(g)}
                    >
                      Disconnect
                    </Button>
                  </ListRow>
                ))}
              </List>
            )}
            <InlineError
              error={revoke.error}
              doing="Couldn’t disconnect the agent"
            />
          </Card>
        )}
      </QueryState>

      <Confirm
        open={removing !== null}
        onOpenChange={(o) => !o && setRemoving(null)}
        title={`Remove ${removing?.name ?? ""}?`}
        effects={[
          "Every connection it has ends at once.",
          "It can register again; with “Wait for an Admin”, it then waits for you.",
        ]}
        confirmLabel="Remove"
        pending={remove.isPending}
        error={remove.error}
        doing="Couldn’t remove the agent"
        onConfirm={() => removing && remove.mutate(removing.id)}
      />
    </div>
  );
}
