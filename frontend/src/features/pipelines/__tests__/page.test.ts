import { copyEstimate, LARGE_ROWS, LARGE_TABLES } from '../page/SnapshotsTab';
import { filterLogs } from '../page/StreamTabs';

describe('log filter', () => {
  const lines = [
    '2026-10-31 10:00:00 INFO  io.debezium Starting',
    '2026-10-31 10:00:01 ERROR io.debezium Slot is gone',
    'java.lang.IllegalStateException: replication slot missing',
    '\tat io.debezium.Foo.bar(Foo.java:1)',
    '2026-10-31 10:00:02 DEBUG io.debezium Polling',
    'continuation of the debug line',
    '2026-10-31 10:00:03 WARN  io.debezium Lag growing',
  ];

  it('keeps a stack trace with the error it belongs to', () => {
    expect(filterLogs(lines, 'ERROR', '')).toEqual(lines.slice(1, 4));
  });

  it('drops lines without a level when the line they continue is filtered out', () => {
    expect(filterLogs(lines, 'WARN', '')).toEqual([...lines.slice(1, 4), lines[6]]);
    expect(filterLogs(lines, 'INFO', '')).not.toContain('continuation of the debug line');
  });

  it('shows everything at DEBUG and searches within what is shown', () => {
    expect(filterLogs(lines, 'DEBUG', '')).toEqual(lines);
    expect(filterLogs(lines, 'DEBUG', 'SLOT')).toEqual([lines[1], lines[2]]);
  });
});

describe('re-copy estimate', () => {
  it('adds up rows and size, counts unknown tables, and flags large copies', () => {
    const e = copyEstimate([
      { table: 'a', rows: 1_000, bytes: 2_000 },
      { table: 'b', rows: null, bytes: null },
    ]);
    expect(e).toMatchObject({ rows: 1_000, bytes: 2_000, unknown: 1, minutes: 1, large: false });
  });

  it('is large above the table or row threshold', () => {
    const many = Array.from({ length: LARGE_TABLES + 1 }, (_, i) => ({ table: `t${i}`, rows: 1, bytes: 1 }));
    expect(copyEstimate(many).large).toBe(true);
    expect(copyEstimate([{ table: 'big', rows: LARGE_ROWS + 1, bytes: 1 }]).large).toBe(true);
    expect(copyEstimate([{ table: 'small', rows: LARGE_ROWS, bytes: 1 }]).large).toBe(false);
  });

  it('gives no time when no table has a known size', () => {
    expect(copyEstimate([{ table: 'x', rows: null, bytes: null }]).minutes).toBeNull();
  });
});

import { positionLabel, startOverEffects } from '../page/FlinkPositions';
import { logComponents } from '../page/StreamTabs';
import { flinkLine } from '../page/Flow';

describe('Flink CDC on the pipeline page', () => {
  it('names saved positions', () => {
    const at = { id: 1, flink_id: 1, size: 1, taken_at: 't', current: true };
    expect(positionLabel({ ...at, kind: 'savepoint', reason: 'stop' })).toBe('Saved when it stopped');
    expect(positionLabel({ ...at, kind: 'checkpoint', reason: 'periodic' })).toBe('Periodic save');
    expect(positionLabel({ ...at, kind: 'checkpoint', reason: 'restored from #4' })).toBe('Restored copy: #4');
  });
  it('says what each way to start over does, per source', () => {
    expect(startOverEffects('skip_to_now', 'postgresql').some((e) => e.includes('replication slot'))).toBe(true);
    expect(startOverEffects('skip_to_now', 'mysql').some((e) => e.includes('replication slot'))).toBe(false);
    expect(startOverEffects('resnapshot', 'mysql')[1]).toContain('copied again');
  });
  it('lists a cluster’s log sources', () => {
    expect(logComponents(undefined).map((c) => c.value)).toEqual(['jobmanager', 'taskmanager-1']);
    expect(
      logComponents([
        { name: 'a', role: 'jobmanager', state: 'running', restarts: 0 },
        { name: 'b', role: 'taskmanager', state: 'running', restarts: 0 },
        { name: 'c', role: 'taskmanager', state: 'running', restarts: 0 },
        { name: 'd', role: 'tunnel', state: 'running', restarts: 0 },
      ]).map((c) => c.value),
    ).toEqual(['jobmanager', 'taskmanager-1', 'taskmanager-2', 'tunnel']);
  });
  it('shows the engine and its TaskManagers in the flow strip', () => {
    expect(flinkLine({ taskmanagers: 1, taskmanagers_wanted: 2 })).toBe('Flink CDC · 1 of 2 TaskManagers');
    expect(flinkLine(undefined)).toBe('Flink CDC');
  });
});
