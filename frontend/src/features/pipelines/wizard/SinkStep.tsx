import { useEffect, useImperativeHandle, useMemo, useRef, useState, type Ref } from 'react';
import { Plus } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Notice } from '@/components/ui/feedback';
import { Check, Field, Input, RadioCards } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { Select } from '@/components/ui/select';
import { CheckList, reportBadge } from '@/components/CheckList';
import { ConnectionDialog } from '@/features/connections/ConnectionDialog';
import { allows, usePolicies } from '@/lib/policies';
import { ENGINE_PITCH, ENGINE_TITLE, engineChoices, pickEngine, useEngines, type EngineId } from '@/lib/engines';
import { SchemaForm, cleanValues, defaultsOf, type FormValues, type JsonSchema } from '@/components/SchemaForm';
import { DEFAULT_SINK_CAPS, sinkCapsOf, TYPE_LABEL, useCheckConnection, useConnections, useConnectorTypes, useDiscovery } from '@/lib/connections';
import type { Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import { deriveSinkOptions, fullSpec, optionsSchemaFor, sinkDestinationFor, sinkNamingError, specForEngine, type FullSpec } from '../spec';
import { useSaveSpec, type StepHandle } from '../useSaveSpec';
import { ConnectionCards, StepHeader } from './shared';
import { cleanPartitionBy, partitionError, PartitionByEditor, type PartitionBy } from './PartitionBy';
import { jsonbSorted } from '@/lib/settings';

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
  const policies = usePolicies(project.id).data;
  const sinks = (connections.data ?? []).filter((c) => c.kind === 'sink' && (allows(policies?.allowed_sink_types, c.type) || c.id === pipeline.sink?.id));
  const current = sinks.find((c) => c.id === sinkId);
  const report = check.data && check.variables?.id === current?.id ? check.data : current?.last_check;
  const caps = current ? sinkCapsOf(types.data, current.type) : DEFAULT_SINK_CAPS;
  const tables = spec.source.tables.include;
  const nameError = sinkNamingError(naming, caps, tables, pipeline.name);
  const deployed = Boolean(pipeline.deployed_version);
  // The engines that can run this source to the chosen destination; a deployed pipeline keeps its own.
  const choices = current ? engineChoices(engines.data, pipeline.source.type, current.type) : [];
  const [picked, setPicked] = useState<EngineId | null>(null);
  const engine: EngineId = deployed ? pipeline.engine : (picked ?? pickEngine(choices, pipeline.engine));
  const flink = engine === 'flink-cdc';
  const optionsSchema = optionsSchemaFor(caps.options_schema as JsonSchema | null, engine);
  const masked = Object.values(spec.source.columns).reduce((n, r) => n + (r.mask?.length ?? 0), 0);
  const formats = FORMATS.filter((f) => caps.formats.includes(f.value));
  // A sink with one format (Iceberg reads Kafka Connect structs) uses it whatever the spec says.
  const forcedFormat = caps.formats.length === 1;
  // Flink CDC writes tables: there is no event format to choose.
  const showFormat = !forcedFormat && !flink && caps.formats.length > 0;
  const registry = showFormat && (format.type === 'avro' || format.type === 'protobuf');
  const discovery = useDiscovery(project.id, pipeline.source.id);
  // Partition by is Flink CDC's on Iceberg, edited by its own card and kept out of the form.
  const [partitionBy, setPartitionBy] = useState<PartitionBy>((spec.sink.options.partition_by as PartitionBy | undefined) ?? {});
  const partitioned = flink && current?.type === 'iceberg';
  const partitions = cleanPartitionBy(partitionBy);
  const savedOptions = (() => {
    if (!optionsSchema) return spec.sink.options;
    const { partition_by: _pb, ...rest } = deriveSinkOptions(cleanValues(optionsSchema, options), optionsSchema, tables, spec.keys ?? {}, discovery.data?.tables);
    void _pb;
    return partitioned && Object.keys(partitions).length ? jsonbSorted({ ...rest, partition_by: partitions }) : rest;
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
  const missing = !sinkId
    ? 'Choose a destination.'
    : current && engines.data && choices.length === 0
      ? `No engine runs ${TYPE_LABEL[pipeline.source.type] ?? pipeline.source.type} to ${TYPE_LABEL[current.type] ?? current.type}.`
      : current && engines.data && !chosen?.available
        ? (choices.find((e) => !e.available)?.reason ?? 'Its engine isn’t available.')
      : nameError
      ? 'Fix the destination naming.'
      : partitioned && partitionError(partitionBy)
        ? partitionError(partitionBy)
      : registry && !format.registry_url
        ? 'Enter the schema registry URL.'
        : null;
  const body = () => ({
    sink_id: sinkId,
    ...(engine !== pipeline.engine ? { engine } : {}),
    spec: specForEngine({ ...spec, sink: { ...spec.sink, naming, format, options: savedOptions } }, engine),
  });
  const dirty = () =>
    sinkId !== pipeline.sink?.id ||
    engine !== pipeline.engine ||
    naming !== spec.sink.naming ||
    JSON.stringify(format) !== JSON.stringify(spec.sink.format) ||
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
      {policies?.allowed_sink_types ? (
        <Notice tone="info">This project allows these destinations: {policies.allowed_sink_types.map((t) => TYPE_LABEL[t] ?? t).join(', ')}.</Notice>
      ) : null}
      {deployed ? <Notice tone="info">The destination of a deployed pipeline can’t change.</Notice> : null}
      <QueryState query={connections} doing="Couldn’t load the destinations">
        {() =>
          sinks.length === 0 && !can('connection.manage') ? (
            <Card>
              <Empty title="No destinations available">Ask an editor or an Admin to add a destination.</Empty>
            </Card>
          ) : (
            <ConnectionCards label="Destination" items={sinks} selected={sinkId} onSelect={(c) => setSinkId(c.id)} disabled={deployed} />
          )
        }
      </QueryState>
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
              description: ENGINE_PITCH[e.id],
              disabled: (deployed || !e.available) && e.id !== engine,
              why: deployed ? 'A deployed pipeline keeps its engine.' : (e.reason ?? null),
            }))}
          />
        </Card>
      ) : current && flink ? (
        <Notice tone="info" title="Runs on Flink CDC">
          {TYPE_LABEL[current.type] ?? current.type} is written by Flink CDC: {ENGINE_PITCH['flink-cdc'].charAt(0).toLowerCase() + ENGINE_PITCH['flink-cdc'].slice(1)}
        </Notice>
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
        </Card>
      ) : null}
      {partitioned ? (
        <Card>
          <PartitionByEditor value={partitionBy} onChange={setPartitionBy} tables={tables} discovered={discovery.data?.tables} sourceType={pipeline.source.type}
            excluded={Object.fromEntries(Object.entries(spec.source.columns).map(([t, r]) => [t, r.exclude ?? []]))} />
        </Card>
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
        onSaved={(c) => {
          setSinkId(c.id);
          check.mutate({ id: c.id });
        }}
      />
    </div>
  );
}
