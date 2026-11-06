import { engineChoices, engineGate, flinkChoices, pickEngine, runs, type Engine, flinkSpecBlock } from '../engines';
import type { Schemas } from '../api';

const base = { kind: 'sink', type: 'x', title: 'X', description: '', presets: [], config_schema: {}, secret_schema: {}, supports_masking: false } as const;

describe('engineGate', () => {
  it('is null for destinations every engine writes', () => {
    expect(engineGate({ ...base } as unknown as Schemas['ConnectorType'])).toBeNull();
  });
  it('says whether the engine that writes it is on', () => {
    expect(engineGate({ ...base, engines: ['flink-cdc'], available: false, unavailable_reason: 'off' } as unknown as Schemas['ConnectorType'])).toEqual({
      engines: ['flink-cdc'],
      available: false,
      reason: 'off',
    });
    expect(engineGate({ ...base, engines: ['flink-cdc'] } as unknown as Schemas['ConnectorType'])?.available).toBe(true);
  });
});


const debezium = { id: 'debezium', title: 'Debezium', available: true, reason: null, default_image: 'x', sources: null, sinks: null, except_sinks: ['starrocks', 'doris', 'fluss'] } as Engine;
const flink = (available: boolean) =>
  ({ id: 'flink-cdc', title: 'Flink CDC', available, reason: available ? null : 'off', default_image: null, sources: ['mysql', 'postgresql', 'sqlserver'],
    sinks: ['iceberg', 'starrocks', 'doris', 'fluss'], except_sinks: null }) as Engine;

describe('engine choices (the backend engine.Supports and ChooseEngine)', () => {
  it('knows what each engine runs', () => {
    expect(runs(debezium, 'mongodb', 'kafka')).toBe(true);
    expect(runs(debezium, 'mysql', 'starrocks')).toBe(false);
    expect(runs(flink(true), 'mongodb', 'iceberg')).toBe(false);
    expect(runs(flink(true), 'mysql', 'kafka')).toBe(false);
    expect(runs(flink(true), 'postgresql')).toBe(true);
  });
  it('offers both for Iceberg from a Flink CDC source, Debezium first', () => {
    const both = engineChoices([debezium, flink(true)], 'mysql', 'iceberg');
    expect(both.map((e) => e.id)).toEqual(['debezium', 'flink-cdc']);
    expect(pickEngine(both, 'debezium')).toBe('debezium');
    expect(pickEngine(both, 'flink-cdc')).toBe('flink-cdc');
  });
  it('picks Flink CDC where only it fits, and keeps an engine that is off out', () => {
    expect(pickEngine(engineChoices([debezium, flink(true)], 'mysql', 'starrocks'), 'debezium')).toBe('flink-cdc');
    expect(pickEngine(engineChoices([debezium, flink(false)], 'mysql', 'iceberg'), 'flink-cdc')).toBe('debezium');
    expect(engineChoices([debezium, flink(true)], 'mongodb', 'starrocks')).toEqual([]);
  });
  it('fills Flink CDC choices with the backend defaults', () => {
    expect(flinkChoices(undefined)).toEqual({ schema_changes: 'apply', new_columns: 'hold', freshness_seconds: 5, commit_seconds: 60, flink_conf: {} });
    expect(flinkChoices({ new_columns: 'send' }).new_columns).toBe('send');
  });
});

describe('flinkSpecBlock', () => {
  // What the server stores and returns (domain dumpFlinkCDC): no empty flink_conf, its keys in
  // JSONB order (shorter first, then bytewise), which differs from alphabetical order.
  it('writes the block back exactly as the server returns it, so autosave settles', () => {
    const stored = { schema_changes: 'apply', new_columns: 'hold', freshness_seconds: 5, commit_seconds: 60 } as const;
    expect(JSON.stringify(flinkSpecBlock(flinkChoices(stored)))).toBe(JSON.stringify(stored));
    expect(JSON.stringify(flinkSpecBlock(flinkChoices(undefined)))).toBe(JSON.stringify(stored));
    const withConf = { ...stored, flink_conf: { 'heartbeat.timeout': '60 s', 'restart-strategy.type': 'fixed-delay', 'pekko.ask.timeout': '30 s' } };
    expect(JSON.stringify(flinkSpecBlock(flinkChoices(withConf)))).toBe(
      JSON.stringify({ ...stored, flink_conf: { 'heartbeat.timeout': '60 s', 'pekko.ask.timeout': '30 s', 'restart-strategy.type': 'fixed-delay' } }),
    );
    const jsonb = { ...stored, flink_conf: { 'taskmanager.memory.managed.fraction': '0.1', 'pekko.ask.timeout': '30 s' } };
    expect(Object.keys(flinkSpecBlock(flinkChoices(jsonb)).flink_conf!)).toEqual(['pekko.ask.timeout', 'taskmanager.memory.managed.fraction']);
    const sameLength = { ...stored, flink_conf: { 'b.x': '1', 'a.y': '2', 'aa': '3' } };
    expect(Object.keys(flinkSpecBlock(flinkChoices(sameLength)).flink_conf!)).toEqual(['aa', 'a.y', 'b.x']);
  });

  it('keeps a custom cluster, last and in the order the server dumps it', () => {
    const stored = { schema_changes: 'apply', new_columns: 'hold', freshness_seconds: 5, commit_seconds: 60 } as const;
    const cluster = {
      parallelism: 4,
      slot: { cpu: '1', memory: '2Gi' },
      job_manager: { cpu: '1', memory: '1Gi' },
      shape: 'manual' as const,
      task_managers: { count: 2, slots: 2, cpu: '2', memory: '4Gi' },
    };
    const shuffled = {
      task_managers: { memory: '4Gi', cpu: '2', slots: 2, count: 2 },
      shape: 'manual' as const,
      job_manager: { memory: '1Gi', cpu: '1' },
      slot: { memory: '2Gi', cpu: '1' },
      parallelism: 4,
    };
    expect(JSON.stringify(flinkSpecBlock(flinkChoices({ ...stored, cluster: shuffled })))).toBe(JSON.stringify({ ...stored, cluster }));
    // A streaming slot size goes right after slot.
    const shrinking = { parallelism: 4, job_manager: { cpu: '1', memory: '1Gi' }, shape: 'auto' as const, streaming_slot: { memory: '512Mi', cpu: '0.5' }, slot: { cpu: '1', memory: '2Gi' } };
    expect(Object.keys(flinkSpecBlock(flinkChoices({ ...stored, cluster: shrinking })).cluster!)).toEqual(['parallelism', 'slot', 'streaming_slot', 'job_manager', 'shape']);
    // An automatic shape has no task_managers.
    const auto = { parallelism: 2, slot: { cpu: '1', memory: '1Gi' }, job_manager: { cpu: '1', memory: '1Gi' }, shape: 'auto' as const };
    expect(JSON.stringify(flinkSpecBlock(flinkChoices({ ...stored, cluster: { ...auto, task_managers: null } })).cluster)).toBe(JSON.stringify(auto));
  });
});
