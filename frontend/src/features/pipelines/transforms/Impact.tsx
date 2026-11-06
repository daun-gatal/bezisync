import { Pill as Badge } from '@/components/ui/feedback';
import { defaultsOf, type JsonSchema } from '@/components/SchemaForm';
import { sinkCapsOf, useConnectorTypes } from '@/lib/connections';
import { useVersions, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { fullSpec, optionsSchemaFor, type FullSpec } from '../spec';
import { outputImpact, summarize, summarizeShape, summarizeValues, TYPE_TITLE, type ShapeCtx } from './model';

/** What a pipeline's destination says about the event shape, for describing deletes as they run. */
export function useShapeCtx(pipeline: Pick<Pipeline, 'engine' | 'sink'>): ShapeCtx {
  const { project } = useProject();
  const caps = sinkCapsOf(useConnectorTypes(project.id).data, pipeline.sink?.type);
  const schema = optionsSchemaFor(caps.options_schema as JsonSchema | null, pipeline.engine);
  return { flink: pipeline.engine === 'flink-cdc', unwrap: caps.unwrap ?? null, optionDefaults: schema ? defaultsOf(schema) : {} };
}

const LABEL = {
  breaking: { tone: 'risk', text: 'Breaking' },
  key: { tone: 'warn', text: 'Key change' },
  additive: { tone: 'grey', text: 'Additive' },
} as const;

/** What consumers will see change, between the deployed version and `version` (default: latest). */
export function OutputImpact({ pipeline, version, spec }: { pipeline: Pipeline; version?: number; spec?: FullSpec }) {
  const { project } = useProject();
  const versions = useVersions(project.id, pipeline.id);
  const ctx = useShapeCtx(pipeline);
  if (!pipeline.deployed_version || !versions.data) return null;
  const deployed = versions.data.find((v) => v.number === pipeline.deployed_version);
  const target = spec ? null : versions.data.find((v) => v.number === (version ?? pipeline.current_version));
  // Never compare against a version that is not loaded yet: that would report wrong changes.
  if (!deployed || (!spec && !target)) return null;
  const next = spec ?? fullSpec(target!.spec);
  const impact = outputImpact(fullSpec(deployed.spec), next, ctx);
  return (
    <section aria-label="What changes for consumers" className="flex flex-col gap-2">
      <strong className="text-base">What changes for consumers</strong>
      {impact.length === 0 ? (
        <p className="m-0 text-sm text-muted">Nothing: events keep the same shape, fields and keys.</p>
      ) : (
        <ul className="m-0 grid list-none gap-1.5 p-0 text-sm">
          {impact.map((i) => (
            <li key={i.text} className="flex items-baseline gap-2">
              <Badge tone={LABEL[i.kind].tone} className="shrink-0">
                {LABEL[i.kind].text}
              </Badge>
              <span>{i.text}</span>
            </li>
          ))}
        </ul>
      )}
      {impact.length ? (
        <p className="m-0 text-xs text-muted">
          Events already delivered do not change. To resend existing rows in the new shape, re-snapshot the tables after deploying (Offsets tab).
        </p>
      ) : null}
    </section>
  );
}

/** The steps in plain language (Review and Overview). */
export function StepsSummary({ spec, flink = false, ctx = {} }: { spec: FullSpec; flink?: boolean; ctx?: ShapeCtx }) {
  const values = summarizeValues(spec.values);
  const keys = Object.entries(spec.keys ?? {});
  if (flink) {
    // Flink CDC writes tables: no event shape, keys are the destination's primary key.
    const items = [
      'Tables are kept in sync, row by row, by primary key.',
      ...spec.transforms.filter((t) => t.type !== 'unwrap').map((t) => `${TYPE_TITLE[t.type]}: ${summarize(t)}.`),
      ...keys.map(([t, c]) => `Primary key of ${t}: ${c.join(', ')}.`),
    ];
    return (
      <ol className="m-0 flex flex-col gap-1 pl-5 text-sm">
        {items.map((s) => (
          <li key={s}>{s}</li>
        ))}
      </ol>
    );
  }
  const items = [
    ...(values ? [`Value formats: ${values}.`] : []),
    ...spec.transforms.map((t) => `${t.type === 'unwrap' ? 'Event shape' : TYPE_TITLE[t.type]}: ${t.type === 'unwrap' ? (summarizeShape(spec.transforms, spec.sink.options, ctx) ?? summarize(t)) : summarize(t)}.`),
    ...keys.map(([t, c]) => `Message key of ${t}: ${c.join(', ')}.`),
  ];
  if (!spec.transforms.some((t) => t.type === 'unwrap')) {
    // The compiler adds the row shape for Keep deleted rows (and for a destination that needs it).
    const shape = summarizeShape(spec.transforms, spec.sink.options, { ...ctx, flink: false });
    items.unshift(
      shape === null
        ? 'Events are the full change envelope (before, after and source).'
        : spec.transforms.some((t) => t.type === 'soft_delete')
          ? 'Event shape: Row after change, added by BeziSync for Keep deleted rows.'
          : `Event shape: ${shape}, added by BeziSync for the destination.`,
    );
  }
  return (
    <ol className="m-0 flex flex-col gap-1 pl-5 text-sm">
      {items.map((s) => (
        <li key={s}>{s}</li>
      ))}
    </ol>
  );
}
