import { engineGate } from '../engines';
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
