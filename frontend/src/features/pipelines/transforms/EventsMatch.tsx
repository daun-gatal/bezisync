import { Pill as Badge } from '@/components/ui/feedback';
import { useDiscovery } from '@/lib/connections';
import { useVersions, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { useTransformPreview } from '@/lib/transforms';
import { fullSpec } from '../spec';

/** Compares delivered events with the preview of the deployed steps (field names only). */
export function EventsMatch({ pipeline: p, table, values }: { pipeline: Pipeline; table: string; values: unknown[] }) {
  const { project } = useProject();
  const versions = useVersions(project.id, p.id);
  const deployed = versions.data?.find((v) => v.number === p.deployed_version);
  const spec = deployed ? fullSpec(deployed.spec) : null;
  // Only plain-JSON rows have fields fully decided by the steps; MongoDB documents vary.
  const comparable =
    Boolean(spec?.transforms.some((t) => t.type === 'unwrap')) &&
    p.source.type !== 'mongodb' &&
    spec?.sink.format.type === 'json' &&
    !spec.sink.format.include_schema;
  const discovery = useDiscovery(project.id, comparable ? p.source.id : undefined);
  const info = discovery.data?.tables.find((t) => `${t.schema}.${t.name}` === table);
  const body =
    comparable && spec && info
      ? {
          spec: spec as never,
          table,
          operation: 'insert' as const,
          columns: (info.columns ?? []).map((c) => ({ name: c.name, type: c.type, nullable: c.nullable, pii: c.pii })),
          primary_key: info.primary_key ?? [],
          replica_identity: info.replica_identity ?? null,
        }
      : null;
  const preview = useTransformPreview(project.id, p.id, body);
  if (!comparable || !preview.data?.fields) return null;
  const expected = new Set(preview.data.fields);
  const rows = values.filter((v): v is Record<string, unknown> => Boolean(v) && typeof v === 'object' && !Array.isArray(v));
  if (rows.length === 0) return <Badge tone="grey">Not compared with the preview yet</Badge>;
  const missing = new Set<string>();
  const extra = new Set<string>();
  for (const r of rows) {
    const got = new Set(Object.keys(r));
    expected.forEach((f) => {
      if (!got.has(f)) missing.add(f);
    });
    got.forEach((f) => {
      if (!expected.has(f)) extra.add(f);
    });
  }
  if (!missing.size && !extra.size)
    return (
      <Badge tone="ok">
        Matches preview · checked {rows.length} event{rows.length === 1 ? '' : 's'}
      </Badge>
    );
  return (
    <Badge tone="warn">
      Differs from preview{missing.size ? ` · missing ${[...missing].join(', ')}` : ''}
      {extra.size ? ` · unexpected ${[...extra].join(', ')}` : ''}
    </Badge>
  );
}
