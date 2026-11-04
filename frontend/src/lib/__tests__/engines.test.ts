import { engineChoices, engineGate, flinkChoices, pickEngine, runs, type Engine } from '../engines';
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
