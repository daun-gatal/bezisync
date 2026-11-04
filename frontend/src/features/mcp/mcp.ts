import type { Schemas } from '@/lib/api';

export type McpInfo = Schemas['MCPInfo'];
export type McpTool = Schemas['MCPToolRead'];

/** How an agent proves who it is: signing in (OAuth) or an API token in a header. */
export type ConnectMode = 'signin' | 'token';

/** The token placeholder in setup snippets when the real one isn't at hand. */
export const TOKEN_PLACEHOLDER = '<your API token>';

/** Ways to connect: the ones an Admin left on, sign-in first. */
export function connectModes(info: Pick<McpInfo, 'oauth_enabled' | 'api_tokens_allowed'>): ConnectMode[] {
  return [...(info.oauth_enabled ? (['signin'] as const) : []), ...(info.api_tokens_allowed ? (['token'] as const) : [])];
}

/** Agents with their own setup steps; "other" is any client configured with JSON. */
export type Agent = 'claude-code' | 'claude' | 'chatgpt' | 'codex' | 'gemini' | 'cursor' | 'vscode' | 'windsurf' | 'other';

export const AGENTS: { value: Agent; label: string }[] = [
  { value: 'claude-code', label: 'Claude Code' },
  { value: 'claude', label: 'Claude Desktop' },
  { value: 'chatgpt', label: 'ChatGPT' },
  { value: 'codex', label: 'Codex' },
  { value: 'gemini', label: 'Gemini CLI' },
  { value: 'cursor', label: 'Cursor' },
  { value: 'vscode', label: 'VS Code' },
  { value: 'windsurf', label: 'Windsurf' },
  { value: 'other', label: 'Other' },
];

export type Setup = { label: string; code: string; note: string };

const json = (v: unknown) => JSON.stringify(v, null, 2);

/**
 * How to point an agent at BeziSync's MCP endpoint, signing in or with an API token; null when
 * the agent can't connect that way (ChatGPT only signs in).
 */
export function agentSetup(agent: Agent, url: string, mode: ConnectMode, token = TOKEN_PLACEHOLDER): Setup | null {
  const signin = mode === 'signin';
  const bearer = `Bearer ${token}`;
  const headers = signin ? {} : { headers: { Authorization: bearer } };
  const label = AGENTS.find((a) => a.value === agent)!.label;
  switch (agent) {
    case 'claude-code':
      return {
        label,
        code: `claude mcp add --transport http bezisync ${url}${signin ? '' : ` --header "Authorization: ${bearer}"`}`,
        note: signin ? 'Claude Code opens BeziSync in your browser to sign in.' : 'The agent acts as you, within the token’s project and read-only setting.',
      };
    case 'claude':
      return signin
        ? { label, code: url, note: 'In Claude (desktop or claude.ai), open Settings → Connectors → Add custom connector and paste this URL; Claude opens BeziSync to sign in.' }
        : {
            label,
            code: json({
              mcpServers: {
                bezisync: { command: 'npx', args: ['-y', 'mcp-remote', url, '--header', 'Authorization:${BEZISYNC_AUTH}'], env: { BEZISYNC_AUTH: bearer } },
              },
            }),
            note: 'Add to claude_desktop_config.json (Settings → Developer → Edit Config); it needs Node.js. Custom connectors sign in instead.',
          };
    case 'chatgpt':
      return signin
        ? { label, code: url, note: 'In ChatGPT, turn on developer mode under Settings → Apps & Connectors, then create a connector with this URL; ChatGPT opens BeziSync to sign in.' }
        : null;
    case 'codex':
      return signin
        ? { label, code: `codex mcp add bezisync --url ${url}\ncodex mcp login bezisync`, note: 'Codex opens BeziSync in your browser to sign in.' }
        : {
            label,
            code: `export BEZISYNC_TOKEN=${token}\ncodex mcp add bezisync --url ${url} --bearer-token-env-var BEZISYNC_TOKEN`,
            note: 'Codex reads the token from BEZISYNC_TOKEN each time it starts, so keep it set in your shell profile.',
          };
    case 'gemini':
      return {
        label,
        code: `gemini mcp add --transport http bezisync ${url}${signin ? '' : ` --header "Authorization: ${bearer}"`}`,
        note: signin ? 'Gemini CLI opens BeziSync to sign in the first time it connects (or run /mcp auth bezisync).' : 'The agent acts as you, within the token’s project and read-only setting.',
      };
    case 'cursor':
      return { label, code: json({ mcpServers: { bezisync: { url, ...headers } } }), note: 'Add to ~/.cursor/mcp.json, or under Cursor Settings → MCP.' };
    case 'vscode':
      return {
        label,
        code: json({ servers: { bezisync: { type: 'http', url, ...headers } } }),
        note: 'Run “MCP: Open User Configuration” and add this for GitHub Copilot’s agent mode; keep tokens out of a workspace’s .vscode/mcp.json.',
      };
    case 'windsurf':
      return { label, code: json({ mcpServers: { bezisync: { serverUrl: url, ...headers } } }), note: 'Add to ~/.codeium/windsurf/mcp_config.json, or under Windsurf Settings → MCP servers.' };
    case 'other':
      return {
        label: 'Any MCP client (JSON)',
        code: json({ mcpServers: { bezisync: { type: 'http', url, ...headers } } }),
        note: signin ? 'Clients that support sign-in (OAuth) open BeziSync to sign in.' : 'Most clients take this, or the same URL and header, in their MCP settings.',
      };
  }
}

/** A curl call that lists the tools an API token may use. */
export function checkSetup(url: string, token = TOKEN_PLACEHOLDER): Setup {
  return {
    label: 'Check the connection',
    code: `curl -s ${url} -H "Authorization: Bearer ${token}" -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'`,
    note: 'Lists the tools this token may use.',
  };
}

/** Durations offered on the consent page: never more than an Admin allows. */
export function grantDays(max: number): { value: string; label: string }[] {
  const all = [1, 7, 30, 90, 180, 365].filter((d) => d <= max);
  if (!all.includes(max)) all.push(max);
  return all.map((d) => ({ value: String(d), label: d === 1 ? '1 day' : d === 365 ? '1 year' : `${d} days` }));
}

export const KIND_LABEL: Record<McpTool['kind'], string> = { read: 'Reads', write: 'Changes', destructive: 'Deletes or resets' };

/** The order areas are listed in: day-to-day work first, administration last. */
export const GROUP_ORDER = [
  'Pipelines',
  'Connections',
  'Runtimes',
  'Approvals',
  'Alerts',
  'Templates',
  'Engine versions',
  'Secret stores',
  'Projects',
  'People',
  'General',
];

/** Tools grouped by area (GROUP_ORDER, unknown areas last), catalog order within an area. */
export function groupTools(tools: McpTool[]): [string, McpTool[]][] {
  const groups = new Map<string, McpTool[]>();
  for (const t of tools) groups.set(t.group, [...(groups.get(t.group) ?? []), t]);
  const rank = (g: string) => (GROUP_ORDER.includes(g) ? GROUP_ORDER.indexOf(g) : GROUP_ORDER.length);
  return [...groups.entries()].sort(([a], [b]) => rank(a) - rank(b));
}

/** The tools map to save: only choices that differ from each tool's default. */
export function toolOverrides(tools: McpTool[], enabled: Record<string, boolean>): Record<string, boolean> {
  const out: Record<string, boolean> = {};
  for (const t of tools) {
    const on = enabled[t.name] ?? t.enabled;
    if (on !== t.default_enabled) out[t.name] = on;
  }
  return out;
}

/** A host name for redirect URIs ("agent.example.com" or "*.example.com"). */
export function hostError(v: string): string | null {
  return /^(\*\.)?[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$/.test(v.toLowerCase())
    ? null
    : 'Use a host name such as agent.example.com or *.example.com.';
}

/** A browser origin ("https://agent.example.com"). */
export function originError(v: string): string | null {
  return /^https?:\/\/[^/?#\s]+$/.test(v.replace(/\/+$/, '')) ? null : 'Use an origin such as https://agent.example.com.';
}
