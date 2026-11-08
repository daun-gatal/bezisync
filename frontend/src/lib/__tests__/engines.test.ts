import { enginePitch, engineAllowed, engineChoices, engineGate, flinkChoices, pickEngine, runs, sinkFit, sourceFit, writesEvents, type Engine, flinkSpecBlock } from '../engines';
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

// ADR 0011: the Destination step offers what an allowed, available engine runs from the source.
describe('sinkFit and the engine policy', () => {
  const pg = (available = true) => ({ ...flink(available), sources: ['mysql', 'postgresql', 'sqlserver', 'oracle'], sinks: ['iceberg', 'starrocks', 'doris', 'fluss', 'kafka'], event_sinks: ['kafka'] }) as Engine;
  const all = [debezium, pg()];
  it('hides a destination no engine runs from the source', () => {
    expect(sinkFit(all, 'mongodb', 'starrocks')).toEqual({ state: 'hidden', why: 'no engine runs this source to them' });
    expect(sinkFit(all, 'mongodb', 'kafka').state).toBe('ok');
  });
  it('keeps a destination either engine writes when one of them is off or disallowed', () => {
    // Kafka and Iceberg: Debezium and Flink CDC both write them.
    expect(sinkFit([debezium, pg(false)], 'postgresql', 'kafka')).toMatchObject({ state: 'ok', engines: [{ id: 'debezium' }] });
    expect(sinkFit(all, 'postgresql', 'iceberg', { allowed_engines: ['flink-cdc'] })).toMatchObject({ state: 'ok', engines: [{ id: 'flink-cdc' }] });
    expect(sinkFit(all, 'postgresql', 'iceberg', { allowed_engines: ['debezium'] })).toMatchObject({ state: 'ok', engines: [{ id: 'debezium' }] });
    expect(sinkFit(all, 'postgresql', 'kafka')).toMatchObject({ state: 'ok', engines: [{ id: 'debezium' }, { id: 'flink-cdc' }] });
  });
  it('hides what only a disallowed engine writes, and says so', () => {
    const f = sinkFit(all, 'postgresql', 'starrocks', { allowed_engines: ['debezium'] });
    expect(f).toEqual({ state: 'hidden', why: 'this project doesn’t allow Flink CDC, which writes them' });
    // Debezium only, Kafka from MongoDB: fine; Flink CDC only, Kafka from MongoDB: Flink CDC can't read it.
    expect(sinkFit(all, 'mongodb', 'kafka', { allowed_engines: ['flink-cdc'] }).state).toBe('hidden');
  });
  it('disables (not hides) what only an engine that is off writes', () => {
    expect(sinkFit([debezium, pg(false)], 'postgresql', 'starrocks')).toEqual({ state: 'unavailable', why: 'off' });
    // Flink CDC only and off: Kafka is written by Debezium too, but the policy leaves Flink CDC, which is off.
    expect(sinkFit([debezium, pg(false)], 'postgresql', 'kafka', { allowed_engines: ['flink-cdc'] })).toEqual({ state: 'unavailable', why: 'off' });
  });
  it('ignores unreadable or missing policies (the server refuses saving itself)', () => {
    expect(sinkFit(all, 'postgresql', 'starrocks', { allowed_engines: [], unreadable: true }).state).toBe('ok');
    expect(engineAllowed(undefined, 'flink-cdc')).toBe(true);
    expect(engineAllowed({ allowed_engines: null }, 'flink-cdc')).toBe(true);
    expect(engineAllowed({ allowed_engines: [] }, 'debezium')).toBe(false);
  });
  it('judges nothing before /engines answers', () => {
    expect(sinkFit(undefined, 'mongodb', 'starrocks').state).toBe('ok');
  });
  it('picks an allowed engine first, like ChooseEngine', () => {
    const both = engineChoices(all, 'postgresql', 'kafka');
    expect(pickEngine(both, 'debezium', { allowed_engines: ['flink-cdc'] })).toBe('flink-cdc');
    expect(pickEngine(both, 'flink-cdc', { allowed_engines: ['debezium'] })).toBe('debezium');
    expect(pickEngine(both, 'debezium', { allowed_engines: ['debezium'] })).toBe('debezium');
    // No allowed engine on: the old order (the policy refuses it with its own message).
    expect(pickEngine(engineChoices([debezium, pg(false)], 'postgresql', 'kafka'), 'debezium', { allowed_engines: ['flink-cdc'] })).toBe('debezium');
  });
  it('says when no allowed engine reads the source', () => {
    expect(sourceFit(all, 'mongodb')).toBeNull();
    expect(sourceFit(all, 'mongodb', { allowed_engines: ['flink-cdc'] })).toBe('This project allows only Flink CDC, which can’t read this source.');
    expect(sourceFit(all, 'postgresql', { allowed_engines: ['flink-cdc'] })).toBeNull();
  });
});

describe('writesEvents', () => {
  const fl = { ...flink(true), event_sinks: ['kafka'] } as Engine;
  it('is the engine and destination pair, not the destination alone', () => {
    expect(writesEvents([debezium, fl], 'debezium', 'kafka')).toBe(true);
    expect(writesEvents([debezium, fl], 'debezium', 'iceberg')).toBe(true);
    expect(writesEvents([debezium, fl], 'flink-cdc', 'kafka')).toBe(true);
    expect(writesEvents([debezium, fl], 'flink-cdc', 'iceberg')).toBe(false);
    expect(writesEvents(undefined, 'flink-cdc', 'kafka')).toBe(true);
  });
  it('describes Flink CDC by what it writes there', () => {
    expect(enginePitch('flink-cdc', true)).not.toMatch(/upsert|tables in sync/);
    expect(enginePitch('flink-cdc', false)).toMatch(/tables in sync/);
  });
});
