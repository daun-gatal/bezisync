import { useEffect, useImperativeHandle, useMemo, useRef, useState, type Ref } from 'react';
import { Plus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Notice, Skeleton } from '@/components/ui/feedback';
import { Check, Field, Input, RadioCards } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import { Meta } from '@/components/ui/text';
import { CheckList, reportBadge } from '@/components/CheckList';
import { ConnectionDialog } from '@/features/connections/ConnectionDialog';
import { allows, usePolicies } from '@/lib/policies';
import { ENGINE_TITLE, engineAllowed, engineChoices, enginePitch, hiddenLines, pickEngine, sinkFit, useEngines, writesEvents, type EngineId, type SinkFit } from '@/lib/engines';
import { SchemaForm, cleanValues, defaultsOf, type FormValues, type JsonSchema } from '@/components/SchemaForm';
import { DEFAULT_SINK_CAPS, sinkCapsOf, TYPE_LABEL, useCheckConnection, useConnections, useConnectorTypes, useDiscovery } from '@/lib/connections';
import type { Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { deriveSinkOptions, flinkEventFormat, fullSpec, keepPartitionBy, optionsSchemaFor, sinkDestinationFor, sinkNamingError, specForEngine, type FullSpec } from '../spec';
import { useSaveSpec, type StepHandle } from '../useSaveSpec';
import { ConnectionCards, StepHeader } from './shared';
import { partitionByFor, partitionError, writtenByTable, type PartitionBy } from '../transforms/PartitionBy';

type Format = FullSpec['sink']['format'];

const FORMATS: { value: Format['type']; label: string; description: string }[] = [
  { value: 'json', label: 'JSON', description: 'Plain JSON events. Debezium: JsonConverter' },
  { value: 'avro', label: 'Avro', description: 'Compact, typed events. Needs a schema registry.' },
  { value: 'protobuf', label: 'Protobuf', description: 'Typed events. Needs a schema registry.' },
  { value: 'cloudevents', label: 'CloudEvents (JSON)', description: 'The CloudEvents envelope around each change.' },
];

/** Step 3: where changes go, how destinations are named, and the event format. */
export function SinkStep({
  pipeline,
  handle,
  onReady,
  onBack,
  onNext,
}: {
  pipeline: Pipeline;
  handle: Ref<StepHandle>;
  onReady: (ready: boolean) => void;
  onBack: () => void;
  onNext: () => void;
}) {
  const { project, can, expert } = useProject();
  const connections = useConnections(project.id);
  const check = useCheckConnection(project.id);
  const spec = useMemo(() => fullSpec(pipeline.spec), [pipeline.spec]);
  const [sinkId, setSinkId] = useState<string | undefined>(pipeline.sink?.id);
  const [adding, setAdding] = useState(false);
  const [naming, setNaming] = useState(spec.sink.naming);
  const [format, setFormat] = useState<Format>(spec.sink.format);
  const [options, setOptions] = useState<FormValues>(spec.sink.options);
  const types = useConnectorTypes(project.id);
  const engines = useEngines();
  const { save, pending, error } = useSaveSpec(pipeline);
  const policyQuery = usePolicies(project.id);
  const policies = policyQuery.data;
  // Unreadable policies hide nothing here: the server refuses saving with its own message.
  const readable = policies && !policies.unreadable ? policies : undefined;
  const sourceType = pipeline.source.type;
  // Each destination is judged by the engines that run this source to it, the engines the project
  // allows and whether they are on (ADR 0011). The pipeline's saved destination always shows.
  const fits = new Map<string, SinkFit>();
  const hidden: { name: string; why: string }[] = [];
  const sinks = (connections.data ?? []).filter((c) => {
    if (c.kind !== 'sink') return false;
    if (c.id === pipeline.sink?.id) return true;
    if (!allows(readable?.allowed_sink_types, c.type)) return false;
    const fit = sinkFit(engines.data, sourceType, c.type, readable);
    fits.set(c.id, fit);
    if (fit.state === 'hidden') hidden.push({ name: c.name, why: fit.why });
    return fit.state !== 'hidden';
  });
  const cardWhy = (c: { id: string }) => {
    const fit = fits.get(c.id);
    return fit?.state === 'unavailable' ? fit.why : null;
  };
  const current = sinks.find((c) => c.id === sinkId);
  const report = check.data && check.variables?.id === current?.id ? check.data : current?.last_check;
  const caps = current ? sinkCapsOf(types.data, current.type) : DEFAULT_SINK_CAPS;
  const tables = spec.source.tables.include;
  const nameError = sinkNamingError(naming, caps, tables, pipeline.name);
  const deployed = Boolean(pipeline.deployed_version);
  // The engines that can run this source to the chosen destination; a deployed pipeline keeps its own.
  const choices = current ? engineChoices(engines.data, sourceType, current.type) : [];
  const [picked, setPicked] = useState<EngineId | null>(null);
  const engine: EngineId = deployed ? pipeline.engine : (picked ?? pickEngine(choices, pipeline.engine, readable));
  const flink = engine === 'flink-cdc';
  // Flink CDC writes Kafka as change events and the other destinations as tables (ADR 0011).
  const events = writesEvents(engines.data, engine, current?.type);
  // Debezium is a real choice for a change log only before the first deploy, where it runs this pair.
  const debeziumOffered = !deployed && choices.some((e) => e.id === 'debezium' && e.available && engineAllowed(readable, e.id));
  const optionsSchema = optionsSchemaFor(caps.options_schema as JsonSchema | null, engine, { debeziumOffered });
  const masked = Object.values(spec.source.columns).reduce((n, r) => n + (r.mask?.length ?? 0), 0);
  const formats = FORMATS.filter((f) => caps.formats.includes(f.value));
  // A sink with one format (Iceberg reads Kafka Connect structs) uses it whatever the spec says.
  const forcedFormat = caps.formats.length === 1;
  // Flink CDC writes tables, or Kafka in its one format: there is no event format to choose.
  const showFormat = !forcedFormat && !flink && caps.formats.length > 0;
  const registry = showFormat && (format.type === 'avro' || format.type === 'protobuf');
  const discovery = useDiscovery(project.id, pipeline.source.id);
  // Partition by (Iceberg, both engines) is chosen in Transform (ADR 0007): this step keeps the
  // stored value as it is, and drops it only for a destination that isn't Iceberg.
  const storedPartitionBy = spec.sink.options.partition_by as PartitionBy | undefined;
  const partitioned = current?.type === 'iceberg';
  const partitions = storedPartitionBy ?? {};
  const written = writtenByTable(tables, discovery.data?.tables, spec, engine, pipeline.source.type, options.write_mode ?? spec.sink.options.write_mode);
  // A write-mode or engine change here can make saved fields invalid: said here, fixed in Transform.
  const partitionProblem = partitioned ? partitionError(partitionByFor(partitions, engine), written) : null;
  const savedOptions = (() => {
    // A destination without options keeps none of another destination's (Partition by included).
    if (!optionsSchema) return current?.type === pipeline.sink?.type ? spec.sink.options : {};
    return keepPartitionBy(deriveSinkOptions(cleanValues(optionsSchema, options), optionsSchema, tables, spec.keys ?? {}, discovery.data?.tables), storedPartitionBy, partitioned);
  })();

  // Choosing another destination: start from its naming default (unless the user wrote their
  // own), a format it takes and its option defaults.
  const lastType = useRef(pipeline.sink?.type);
  useEffect(() => {
    if (!current || lastType.current === current.type || deployed) return;
    const previous = sinkCapsOf(types.data, lastType.current);
    lastType.current = current.type;
    setNaming((n) => (n === previous.naming.default || n === DEFAULT_SINK_CAPS.naming.default ? caps.naming.default : n));
    setFormat((f) => (caps.formats.includes(f.type) || forcedFormat ? f : { ...f, type: 'json' }));
    setOptions(optionsSchema ? defaultsOf(optionsSchema) : {});
  }, [current, caps, types.data, deployed, forcedFormat, optionsSchema]);
  const chosen = choices.find((e) => e.id === engine);
  // On Flink CDC Kafka the format is its JSON (specForEngine): compared and saved as such.
  const savedFormat = flink && events ? flinkEventFormat(format) : format;
  const missing = !sinkId
    ? 'Choose a destination.'
    : current && engines.data && choices.length === 0
      ? `No engine runs ${TYPE_LABEL[pipeline.source.type] ?? pipeline.source.type} to ${TYPE_LABEL[current.type] ?? current.type}.`
      : current && engines.data && !chosen?.available
        ? (choices.find((e) => !e.available)?.reason ?? 'Its engine isn’t available.')
      : current && engines.data && !engineAllowed(readable, engine)
        ? `This project doesn’t allow pipelines on ${ENGINE_TITLE[engine]}.`
      : nameError
      ? 'Fix the destination naming.'
      : flink && optionsSchema?.properties?.write_mode?.ui?.unavailable?.[String(options.write_mode)]
        ? 'Choose how rows are written: Flink CDC can’t append every change.'
      : registry && !format.registry_url
        ? 'Enter the schema registry URL.'
        : null;
  const body = () => ({
    sink_id: sinkId,
    ...(engine !== pipeline.engine ? { engine } : {}),
    spec: specForEngine({ ...spec, sink: { ...spec.sink, naming, format, options: savedOptions } }, engine, { events }),
  });
  const dirty = () =>
    sinkId !== pipeline.sink?.id ||
    engine !== pipeline.engine ||
    naming !== spec.sink.naming ||
    JSON.stringify(savedFormat) !== JSON.stringify(spec.sink.format) ||
    JSON.stringify(savedOptions) !== JSON.stringify(spec.sink.options);
  useImperativeHandle(handle, () => ({ dirty, commit: async () => (missing ? false : dirty() ? save(body()) : true) }));
  const isReady = !missing;
  useEffect(() => onReady(isReady), [isReady, onReady]);

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <StepHeader
        title="Where should changes go?"
        description={caps.naming.single ? 'Every table goes to the one destination this connection names.' : 'Each table gets its own destination.'}
      />
      {policies?.unreadable ? (
        <Notice tone="warn">This project’s policies can’t be read, so changes are blocked until an Admin fixes them in Settings → Policies.</Notice>
      ) : readable?.allowed_sink_types ? (
        <Notice tone="info">This project allows these destinations: {readable.allowed_sink_types.map((t) => TYPE_LABEL[t] ?? t).join(', ')}.</Notice>
      ) : null}
      {deployed ? <Notice tone="info">The destination of a deployed pipeline can’t change.</Notice> : null}
      <QueryState query={connections} doing="Couldn’t load the destinations">
        {() =>
          // What fits is judged once the engines and policies are known, so cards don't disappear.
          (engines.isPending || policyQuery.isPending) && !deployed ? (
            <Skeleton lines={2} />
          ) : sinks.length === 0 ? (
            <Card>
              <Empty title={`No destination takes ${TYPE_LABEL[sourceType] ?? sourceType} here`}>
                {can('connection.manage') ? 'Add a destination below: it lists the types this source can go to.' : 'Ask an editor or an Admin to add a destination this source can go to.'}
              </Empty>
            </Card>
          ) : (
            <ConnectionCards label="Destination" items={sinks} selected={sinkId} onSelect={(c) => setSinkId(c.id)} disabled={deployed} why={cardWhy} />
          )
        }
      </QueryState>
      {hidden.length && !deployed ? <HiddenSinks hidden={hidden} source={TYPE_LABEL[sourceType] ?? sourceType} /> : null}
      {can('connection.manage') && !deployed ? (
        <div>
          <Button icon={<Plus />} onClick={() => setAdding(true)}>
            Add a destination
          </Button>
        </div>
      ) : null}
      {current ? (
        report && !report.ok ? (
          <Card>
            <CheckList report={report} expert={expert} subject={current.name} onRecheck={() => check.mutate({ id: current.id })} rechecking={check.isPending} />
          </Card>
        ) : (
          <div className="flex flex-wrap items-center gap-2 text-sm text-muted">
            Last check: {reportBadge(report)}
            <Button size="sm" variant="ghost" loading={check.isPending} onClick={() => check.mutate({ id: current.id })}>
              Check again
            </Button>
          </div>
        )
      ) : null}
      <InlineError error={check.error} doing="Couldn’t check the destination" />
      {current && choices.length > 1 ? (
        <Card title="Engine">
          <RadioCards
            label="Engine"
            value={engine}
            onChange={(v) => setPicked(v as EngineId)}
            options={choices.map((e) => ({
              value: e.id,
              label: e.id === 'debezium' ? `${ENGINE_TITLE[e.id]} (default)` : ENGINE_TITLE[e.id],
              description: enginePitch(e.id, writesEvents(engines.data, e.id, current.type)),
              disabled: (deployed || !e.available || !engineAllowed(readable, e.id)) && e.id !== engine,
              why: deployed ? 'A deployed pipeline keeps its engine.' : !engineAllowed(readable, e.id) ? 'Not allowed in this project.' : (e.reason ?? null),
            }))}
          />
        </Card>
      ) : current && flink ? (
        <Notice tone="info" title="Runs on Flink CDC">
          {TYPE_LABEL[current.type] ?? current.type} is written by Flink CDC: {lowerFirst(enginePitch('flink-cdc', events))}
        </Notice>
      ) : null}
      {current && flink && events ? (
        <Meta className="block">Records are Flink CDC’s Debezium-style JSON (no schema): it has no other format.</Meta>
      ) : null}
      {flink && masked > 0 ? (
        <Notice tone="warn" title="Masked columns">
          Flink CDC has no hash function: go back to Tables and choose Remove (or Keep as is) for the {masked === 1 ? 'masked column' : `${masked} masked columns`}.
        </Notice>
      ) : null}
      <Card>
        <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,280px),1fr))] gap-4">
          {caps.naming.single ? null : (
            <Field
              label="Destination naming"
              error={nameError}
              hint={
                tables.length
                  ? `Preview: ${tables
                      .slice(0, 3)
                      .map((t) => sinkDestinationFor(naming, t, pipeline.name, caps))
                      .join(', ')}${tables.length > 3 ? ', …' : ''}`
                  : 'Use {schema}, {table} and {pipeline}.'
              }
            >
              {(id) => (
                <Input id={id} className="font-mono" spellCheck={false} value={naming} invalid={Boolean(nameError)} onChange={(e) => setNaming(e.target.value)} />
              )}
            </Field>
          )}
          {!showFormat ? null : (
            <Field label="Event format">
              {(id) => <Select id={id} value={format.type} onChange={(v) => setFormat({ ...format, type: v })} options={formats} />}
            </Field>
          )}
          {registry ? (
            <>
              <Field label="Schema registry URL">
                {(id) => (
                  <Input
                    id={id}
                    value={format.registry_url ?? ''}
                    placeholder="http://registry:8080/apis/registry/v2"
                    onChange={(e) => setFormat({ ...format, registry_url: e.target.value || null })}
                  />
                )}
              </Field>
              <Field label="Registry">
                {(id) => (
                  <Select
                    id={id}
                    value={format.registry_kind}
                    onChange={(v) => setFormat({ ...format, registry_kind: v as 'apicurio' | 'confluent' })}
                    options={[
                      { value: 'apicurio', label: 'Apicurio' },
                      { value: 'confluent', label: 'Confluent' },
                    ]}
                  />
                )}
              </Field>
            </>
          ) : null}
          {expert && showFormat && format.type === 'json' ? (
            <div className="col-span-full">
              <Check
                checked={format.include_schema}
                onChange={(on) => setFormat({ ...format, include_schema: on })}
                label="Embed the Kafka Connect schema in every event"
              />
            </div>
          ) : null}
        </div>
      </Card>
      {current && optionsSchema ? (
        <Card title={`${TYPE_LABEL[current.type] ?? current.type} settings for this pipeline`}>
          <SchemaForm idPrefix="sink-opt" schema={optionsSchema} values={options} onChange={setOptions} expert={expert} />
          {/* Keep deleted rows (Transform) decides deletes over this destination's own Deletes option. */}
          {caps.unwrap?.delete_handling_from && !flink && spec.transforms.some((t) => t.type === 'soft_delete') ? (
            <Meta className="mt-3 block">Deletes: kept, marked deleted. Set by Keep deleted rows in Transform.</Meta>
          ) : null}
        </Card>
      ) : null}
      {partitioned ? (
        partitionProblem ? (
          <Notice tone="warn" title="Partition by needs a change">
            {partitionProblem} Fix it in the Transform step, next.
          </Notice>
        ) : (
          <Meta className="block">Partition by is chosen in Transform, after the key and renames.</Meta>
        )
      ) : null}
      <InlineError error={error} doing="Couldn’t save the destination" />
      <div className="flex flex-wrap justify-between gap-3">
        <Button onClick={onBack}>← Back</Button>
        <Button variant="primary" disabled={Boolean(missing)} why={missing} loading={pending} onClick={onNext}>
          Continue to transform →
        </Button>
      </div>
      <ConnectionDialog
        open={adding}
        onOpenChange={setAdding}
        kind="sink"
        fitting={{ sourceType, policy: policies }}
        onSaved={(c) => {
          setSinkId(c.id);
          check.mutate({ id: c.id });
        }}
      />
    </div>
  );
}

function lowerFirst(s: string): string {
  return s.charAt(0).toLowerCase() + s.slice(1);
}

function HiddenSinks({ hidden, source }: { hidden: { name: string; why: string }[]; source: string }) {
  return (
    <div className="flex flex-col gap-0.5 text-sm text-muted" data-testid="hidden-sinks">
      <span>Not shown for {source}:</span>
      {hiddenLines(hidden).map((l) => (
        <span key={l} className="wrap-anywhere">
          {l}
        </span>
      ))}
    </div>
  );
}
