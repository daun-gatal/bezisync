import { describe, expect, it } from 'vitest';
import { AGENTS, agentSetup, checkSetup, connectModes, grantDays, groupTools, hostError, originError, toolOverrides, type McpTool } from '../mcp';

const tool = (name: string, over: Partial<McpTool> = {}): McpTool => ({
  name,
  title: name,
  description: '',
  group: 'Pipelines',
  kind: 'read',
  admin_only: false,
  permission: null,
  enabled: true,
  default_enabled: true,
  ...over,
});

describe('MCP settings', () => {
  it('keeps only choices that differ from each tool’s default', () => {
    const tools = [tool('list_pipelines'), tool('delete_pipeline', { kind: 'destructive', enabled: false, default_enabled: false })];
    expect(toolOverrides(tools, {})).toEqual({});
    expect(toolOverrides(tools, { delete_pipeline: true, list_pipelines: true })).toEqual({ delete_pipeline: true });
    expect(toolOverrides(tools, { list_pipelines: false })).toEqual({ list_pipelines: false });
  });

  it('groups tools in catalog order', () => {
    const groups = groupTools([tool('a'), tool('b', { group: 'Alerts' }), tool('c')]);
    expect(groups.map(([g, ts]) => [g, ts.map((t) => t.name)])).toEqual([
      ['Pipelines', ['a', 'c']],
      ['Alerts', ['b']],
    ]);
  });

  it('never offers longer access than an Admin allows', () => {
    expect(grantDays(30).map((d) => d.value)).toEqual(['1', '7', '30']);
    expect(grantDays(45).map((d) => d.value)).toEqual(['1', '7', '30', '45']);
    expect(grantDays(365).at(-1)?.label).toBe('1 year');
  });

  it('validates redirect hosts and origins like the server', () => {
    expect(hostError('claude.ai')).toBeNull();
    expect(hostError('*.example.com')).toBeNull();
    expect(hostError('https://claude.ai')).not.toBeNull();
    expect(originError('https://agent.example.com')).toBeNull();
    expect(originError('agent.example.com')).not.toBeNull();
  });

  it('sets agents up to sign in, or with an API token in a header', () => {
    const url = 'https://bz/api/v1/mcp';
    expect(agentSetup('claude-code', url, 'signin')!.code).toBe('claude mcp add --transport http bezisync https://bz/api/v1/mcp');
    expect(JSON.parse(agentSetup('other', url, 'signin')!.code).mcpServers.bezisync.headers).toBeUndefined();
    expect(agentSetup('claude-code', url, 'token')!.code).toBe(
      'claude mcp add --transport http bezisync https://bz/api/v1/mcp --header "Authorization: Bearer <your API token>"',
    );
    expect(JSON.parse(agentSetup('other', url, 'token')!.code).mcpServers.bezisync.headers).toEqual({ Authorization: 'Bearer <your API token>' });
    expect(checkSetup(url).code).toContain('"method":"tools/list"');
    // A freshly created token is filled in.
    expect(agentSetup('claude-code', url, 'token', 'bzt_abc')!.code).toContain('Bearer bzt_abc');
  });

  it('sets up every major agent in the shape its client expects', () => {
    const url = 'https://bz/api/v1/mcp';
    for (const { value } of AGENTS) {
      const s = agentSetup(value, url, 'signin')!;
      expect(s.code).toContain(url);
      expect(s.code).not.toContain('Bearer');
    }
    // Every agent but ChatGPT (sign-in only) takes a token, and only ever in the header it sends.
    for (const { value } of AGENTS) {
      const s = agentSetup(value, url, 'token', 'bzt_abc');
      if (value === 'chatgpt') expect(s).toBeNull();
      else expect(s!.code).toContain('bzt_abc');
    }
    expect(JSON.parse(agentSetup('vscode', url, 'token', 't')!.code).servers.bezisync).toEqual({ type: 'http', url, headers: { Authorization: 'Bearer t' } });
    expect(JSON.parse(agentSetup('cursor', url, 'signin')!.code).mcpServers.bezisync).toEqual({ url });
    expect(JSON.parse(agentSetup('windsurf', url, 'signin')!.code).mcpServers.bezisync).toEqual({ serverUrl: url });
    expect(JSON.parse(agentSetup('claude', url, 'token', 't')!.code).mcpServers.bezisync.env).toEqual({ BEZISYNC_AUTH: 'Bearer t' });
    expect(agentSetup('codex', url, 'token', 't')!.code).toContain('--bearer-token-env-var BEZISYNC_TOKEN');
    expect(agentSetup('gemini', url, 'token', 't')!.code).toBe(`gemini mcp add --transport http bezisync ${url} --header "Authorization: Bearer t"`);
  });

  it('offers the ways an Admin left on, sign-in first', () => {
    expect(connectModes({ oauth_enabled: true, api_tokens_allowed: true })).toEqual(['signin', 'token']);
    expect(connectModes({ oauth_enabled: false, api_tokens_allowed: true })).toEqual(['token']);
    expect(connectModes({ oauth_enabled: false, api_tokens_allowed: false })).toEqual([]);
  });
});
