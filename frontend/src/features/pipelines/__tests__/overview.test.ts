import { peaks, readoutAt, type Series } from '@/lib/series';
import type { SourceImpact } from '@/lib/status';
import { formatLatency, formatWindow, latest, protectionView, transformView } from '../page/overview';
import { fullSpec, type TransformStep } from '../spec';

const GB = 1024 ** 3;
const at = '2026-10-31T11:00:00Z';

describe('source protection', () => {
  it('never shows an unmeasured probe as OK', () => {
    const v = protectionView('postgresql', { at, slot: 'bz_abc', summary: 'The replication slot does not exist yet.' });
    expect(v.pill).toEqual({ tone: 'grey', label: 'Not measured yet' });
    expect(v.facts).toEqual([{ label: 'Replication slot', value: 'bz_abc', note: 'not created yet', mono: true }]);
    expect(v.meter).toBeUndefined();
  });

  it('shows the error when the probe failed', () => {
    const v = protectionView('mysql', { at, error: 'connection refused', summary: 'Could not measure the source.' });
    expect(v.pill).toEqual({ tone: 'grey', label: 'Unknown' });
    expect(v.note).toBe('Couldn’t measure: connection refused');
  });

  it('PostgreSQL: slot, WAL status, room and the limit bar', () => {
    const v = protectionView('postgresql', {
      at, slot: 'bz_abc', slot_active: true, log_status: 'reserved', retained_bytes: GB, headroom_bytes: 4 * GB, limit_bytes: 10 * GB,
    });
    expect(v.pill.label).toBe('OK');
    expect(v.facts.map((f) => [f.label, f.value, f.note])).toEqual([
      ['Replication slot', 'bz_abc', 'in use'],
      ['WAL status', 'Kept normally', undefined],
      ['Room before the slot is lost', '4.0 GB', undefined],
    ]);
    expect(v.meter).toEqual({ label: 'WAL kept for this pipeline', ratio: 0.1, text: '1.0 GB of 10 GB limit', tone: 'ok' });
    expect(v.note).toBeUndefined();
  });

  it('PostgreSQL: no cap is said plainly, and an old server is unknown, never unlimited', () => {
    const noCap = protectionView('postgresql', { at, slot: 's', slot_active: true, log_status: 'extended', retained_bytes: 1, no_cap: true, limit_bytes: GB });
    expect(noCap.facts[2]).toMatchObject({ value: 'No cap' });
    const old = protectionView('postgresql', { at, slot: 's', slot_active: false, retained_bytes: 1, limit_bytes: GB });
    expect(old.facts.map((f) => f.value)).toEqual(['s', 'Unknown', 'Unknown']);
    expect(old.facts[0]!.note).toBe('not in use');
    expect(JSON.stringify(old)).not.toMatch(/unlimited/i);
  });

  it('PostgreSQL: lost WAL is bad, and the bar warns near and past the limit', () => {
    const lost = protectionView('postgresql', { at, slot: 's', slot_active: false, log_status: 'lost', lost: true, risk: 'gone', retained_bytes: 2 * GB, limit_bytes: GB });
    expect(lost.pill).toEqual({ tone: 'bad', label: 'Lost' });
    expect(lost.facts[1]).toMatchObject({ value: 'Removed', tone: 'bad' });
    expect(lost.meter).toMatchObject({ ratio: 2, tone: 'risk' });
    const near = protectionView('postgresql', { at, slot: 's', slot_active: true, log_status: 'reserved', retained_bytes: 0.85 * GB, limit_bytes: GB });
    expect(near.meter?.tone).toBe('warn');
    const at80 = protectionView('postgresql', { at, slot: 's', slot_active: true, log_status: 'reserved', retained_bytes: 0.8 * GB, limit_bytes: GB });
    expect(at80.meter?.tone).toBe('warn');
  });

  it('MySQL: file, position and what is unread', () => {
    const ok = protectionView('mysql', { at, reading_file: 'mysql-bin.000042', log_status: 'ok', retained_bytes: 24 * 1024 ** 2, limit_bytes: 10 * GB });
    expect(ok.facts.map((f) => [f.label, f.value])).toEqual([
      ['Reading binlog file', 'mysql-bin.000042'],
      ['Position', 'Safe from the next purge'],
      ['Not read yet', '24 MB'],
    ]);
    expect(ok.meter).toMatchObject({ label: 'Binlog waiting for this pipeline', text: '24 MB of 10 GB limit', tone: 'ok' });
    const oldest = protectionView('mysql', { at, reading_file: 'b.1', log_status: 'oldest', retained_bytes: 1, limit_bytes: GB, risk: 'next purge' });
    expect(oldest.pill.label).toBe('At risk');
    expect(oldest.facts[1]).toMatchObject({ value: 'Oldest file kept', tone: 'risk' });
  });

  it('MySQL: a purged file and a position not stored yet', () => {
    const purged = protectionView('mysql', { at, reading_file: 'b.1', lost: true, risk: 'purged', summary: 'The binlog file at the stored position was purged.' });
    expect(purged.pill.tone).toBe('bad');
    expect(purged.facts).toEqual([{ label: 'Reading binlog file', value: 'b.1', mono: true, note: 'purged from the server', tone: 'bad' }]);
    expect(purged.meter).toBeUndefined();
    const early = protectionView('mysql', { at, summary: '3 binlog files on the server; position not stored yet.' });
    expect(early.facts).toEqual([]);
    expect(early.note).toBe('3 binlog files on the server; position not stored yet.');
  });

  it('MongoDB: the window, not a size "for this pipeline", and no limit bar', () => {
    const full = protectionView('mongodb', { at, log_status: 'window 52 h', window_seconds: 52 * 3600 + 600, retained_bytes: 95, headroom_bytes: 5 });
    expect(full.facts.map((f) => [f.label, f.value, f.note])).toEqual([
      ['Oplog window', '2 d 4 h', undefined],
      ['Oplog used', '95 B of 100 B', 'shared by every reader'],
    ]);
    expect(full.meter).toBeUndefined();
    const filling = protectionView('mongodb', { at, log_status: 'filling', retained_bytes: 10 });
    expect(filling.facts.map((f) => [f.value, f.note])).toEqual([
      ['Still filling', 'holds every change since it was created'],
      ['10 B', 'capacity unknown'],
    ]);
    const short = protectionView('mongodb', { at, window_seconds: 5 * 3600, log_status: 'window 5 h', retained_bytes: 1, risk: 'short' });
    expect(short.facts[0]!.tone).toBe('risk');
  });

  it('SQL Server: retention, position and the log, each once', () => {
    const ok = protectionView('sqlserver', {
      at, window_seconds: 3 * 86400, position_kept: true, log_status: 'nothing', log_used_bytes: 64 * 1024 ** 2, log_size_bytes: 512 * 1024 ** 2,
    });
    expect(ok.pill.label).toBe('OK');
    expect(ok.facts.map((f) => [f.label, f.value])).toEqual([
      ['CDC keeps changes for', '3 d'],
      ['Pipeline position', 'Inside that window'],
    ]);
    expect(ok.meter).toEqual({ label: 'Transaction log used', ratio: 0.125, text: '64 MB of 512 MB', tone: 'ok' });
    expect(JSON.stringify(ok)).not.toMatch(/for this pipeline|oldest/i);
    const waiting = protectionView('sqlserver', { at, position_kept: true, log_status: 'replication', log_used_bytes: 9, log_size_bytes: 10, risk: 'full' });
    expect(waiting.meter).toMatchObject({ text: '9 B of 10 B · waits on replication', tone: 'risk' });
    expect(waiting.facts[0]).toMatchObject({ value: 'Unknown', note: 'reading the cleanup job needs db_owner' });
    const lost = protectionView('sqlserver', { at, window_seconds: 3600, position_kept: false, lost: true, risk: 'lost' });
    expect(lost.pill.tone).toBe('bad');
    expect(lost.facts[1]).toMatchObject({ value: 'Cleaned up', tone: 'bad' });
    expect(lost.meter).toBeUndefined();
    const nothing = protectionView('sqlserver', { at, summary: 'Nothing could be measured (needs db_owner on the database).' });
    expect(nothing.pill.label).toBe('Not measured yet');
    expect(nothing.facts).toEqual([]);
    expect(nothing.note).toBe('Nothing could be measured (needs db_owner on the database).');
    const early = protectionView('sqlserver', { at, window_seconds: 3 * 86400, log_status: 'nothing' });
    expect(early.facts[1]).toMatchObject({ value: 'Unknown' });
    expect(early.pill).toEqual({ tone: 'grey', label: 'Unknown' });
  });

  it('another source shows what it kept, and nothing about slots or binlogs', () => {
    const v = protectionView('db2', { at, retained_bytes: 2048 } as SourceImpact);
    expect(v.facts).toEqual([{ label: 'Log kept for this pipeline', value: '2.0 KB', mono: true }]);
  });

  it('a time-kept history shows the window, the position against it, and is OK only when both are known', () => {
    const ok = protectionView('cockroachdb', { at, behind_seconds: 30, retention_seconds: 4 * 3600 });
    expect(ok.pill).toEqual({ tone: 'ok', label: 'OK' });
    expect(ok.facts).toEqual([
      { label: 'Changes kept (GC TTL)', value: '4 h' },
      { label: 'Pipeline position', value: 'under a minute behind', tone: undefined },
    ]);
    expect(ok.meter).toMatchObject({ label: 'Window used', text: 'under a minute of 4 h', tone: 'ok' });

    const late = protectionView('spanner', { at, behind_seconds: 20 * 3600, retention_seconds: 24 * 3600, risk: 'late' });
    expect(late.pill.label).toBe('At risk');
    expect(late.facts[1]).toMatchObject({ value: '20 h behind', tone: 'risk' });
    expect(late.meter?.tone).toBe('warn');

    const lost = protectionView('oracle', { at, behind_seconds: 3 * 86400, retention_seconds: 86400, risk: 'gone', lost: true });
    expect(lost.pill).toEqual({ tone: 'bad', label: 'Lost' });
    expect(lost.facts[1]).toMatchObject({ tone: 'bad' });
    expect(lost.meter?.tone).toBe('risk');

    const half = protectionView('oracle', { at, behind_seconds: 60 });
    expect(half.pill).toEqual({ tone: 'grey', label: 'Unknown' });
    expect(half.facts[0]).toEqual({ label: 'Archived logs reach back', value: 'Unknown' });
    expect(half.meter).toBeUndefined();

    const fresh = protectionView('spanner', { at, retention_seconds: 86400 });
    expect(fresh.pill.label).toBe('Unknown');
    expect(fresh.facts[1]).toEqual({ label: 'Pipeline position', value: 'Not stored yet' });

    const nothing = protectionView('vitess', { at, summary: 'Binlog retention is not visible through VTGate.' });
    expect(nothing.pill.label).toBe('Not measured yet');
    expect(nothing.note).toBe('Binlog retention is not visible through VTGate.');
  });

  it('formats an oplog window', () => {
    expect(formatWindow(0)).toBe('0 min');
    expect(formatWindow(45 * 60)).toBe('45 min');
    expect(formatWindow(3600)).toBe('1 h');
    expect(formatWindow(26 * 3600 + 5 * 60)).toBe('26 h 5 min');
    expect(formatWindow(47 * 3600 + 59 * 60)).toBe('47 h 59 min');
    expect(formatWindow(48 * 3600)).toBe('2 d');
    expect(formatWindow(3 * 86400)).toBe('3 d');
    expect(formatWindow(52 * 3600 + 600)).toBe('2 d 4 h');
  });
});

describe('transforms card', () => {
  const unwrap: TransformStep = { type: 'unwrap', delete_handling: 'rewrite', add_fields: ['op', 'source.ts_ms'], condition: null };

  it('the default pipeline: row after change, deletes kept, two extra fields, no steps', () => {
    const { output, steps } = transformView(fullSpec({ transforms: [unwrap], values: { decimals: 'double' } } as never));
    expect(output.map((r) => r.title)).toEqual(['The row after the change', 'Deletes are kept', '2 extra fields', 'Value formats']);
    expect(output[1]!.code).toEqual(['__deleted = true']);
    expect(output[2]).toMatchObject({ detail: 'Operation · Change time', code: ['__op', '__source_ts_ms'] });
    expect(output[3]!.detail).toBe('Decimals as numbers');
    expect(steps).toEqual([]);
  });

  it('each delete handling and extra-field count reads correctly', () => {
    const one = transformView(fullSpec({ transforms: [{ ...unwrap, delete_handling: 'tombstone', add_fields: ['op'] }] } as never)).output;
    expect(one.map((r) => r.title)).toEqual(['The row after the change', 'Deletes become tombstones', 'One extra field']);
    const none = transformView(fullSpec({ transforms: [{ ...unwrap, delete_handling: 'drop', add_fields: [] }] } as never)).output;
    expect(none.map((r) => r.title)).toEqual(['The row after the change', 'Deletes are dropped']);
    const unknown = transformView(fullSpec({ transforms: [{ ...unwrap, add_fields: ['source.weird'] }] } as never)).output;
    expect(unknown[2]).toMatchObject({ detail: 'source.weird', code: ['__source_weird'] });
  });

  it('without an event-shape step, consumers get the full envelope', () => {
    const { output } = transformView(fullSpec({ transforms: [] } as never));
    expect(output.map((r) => r.title)).toEqual(['The full change envelope']);
  });

  it('outbox events replace the row shape; keys and other steps are listed', () => {
    const { output, steps } = transformView(
      fullSpec({
        transforms: [
          { type: 'filter', id: 'f1', operations: ['insert'], match: 'all', rules: [] },
          { type: 'outbox', id: 'o1' },
          { type: 'rename', id: 'r1', renames: [{ from: 'a', to: 'b' }] },
        ],
        keys: { 'shop.orders': ['customer_id'], 'shop.items': ['order_id', 'sku'] },
      } as never),
    );
    expect(output.map((r) => r.title)).toEqual(['Outbox events', 'Custom message keys on 2 tables']);
    expect(output[1]!.detail).toBe('shop.orders by customer_id · shop.items by order_id, sku');
    expect(steps.map((r) => [r.icon, r.title])).toEqual([
      ['filter', 'Keep only some events'],
      ['rename', 'Rename fields'],
    ]);
  });
});

describe('chart readout', () => {
  const series: Series[] = [
    { label: 'a', values: [1, 5, null], color: '--x', format: String },
    { label: 'b', values: [null, null, null], color: '--y', format: String },
  ];

  it('shows the last point, or the hovered one, and unknown as null', () => {
    expect(readoutAt(series, [10, 20, 30], null)).toEqual({ at: 30, values: [null, null] });
    expect(readoutAt(series, [10, 20, 30], 1)).toEqual({ at: 20, values: [5, null] });
    expect(readoutAt(series, [], null)).toEqual({ at: null, values: [null, null] });
    expect(readoutAt(series, [10, 20, 30], 9)).toEqual({ at: null, values: [null, null] });
  });

  it('peaks ignore gaps and are null for a series with no data', () => {
    expect(peaks(series)).toEqual([5, null]);
  });

  it('latest is the last point, never an older one', () => {
    expect(latest([1, 2, null])).toBeNull();
    expect(latest([1, 2])).toBe(2);
    expect(latest([])).toBeNull();
  });

  it('latency ticks do not repeat at small values', () => {
    expect([0, 0.5, 1, 1.5, 2].map(formatLatency)).toEqual(['0 ms', '0.5 ms', '1 ms', '1.5 ms', '2 ms']);
    expect(formatLatency(250)).toBe('250 ms');
    expect(formatLatency(1500)).toBe('1.5 s');
    expect(formatLatency(90_000)).toBe('2 min');
    expect(formatLatency(5_400_000)).toBe('1.5 h');
  });
});

import { flinkView, shapeView } from '../page/overview';

describe('flinkView', () => {
  const ago = (iso: string) => `ago(${iso})`;
  it('says what the cluster is doing and how far the first copy got', () => {
    const v = flinkView(
      { phase: 'running', streaming: false, taskmanagers: 1, taskmanagers_wanted: 2, chunks: { 'shop.orders': 12, 'shop.customers': 1 },
        last_saved: { id: 9, kind: 'checkpoint', reason: 'periodic', taken_at: 't1', size: 1 }, checkpoints_failed: 2 },
      ago,
    )!;
    expect(v.job).toBe('Running · copying existing rows');
    expect(v.taskmanagers).toBe('1 of 2 connected');
    expect(v.saved).toBe('ago(t1) · 2 failed saves');
    expect(v.copy).toEqual([{ table: 'shop.customers', chunks: 1 }, { table: 'shop.orders', chunks: 12 }]);
    expect(v.problem).toBeNull();
  });
  it('drops the copy once streaming, and reports a store failure', () => {
    const v = flinkView({ phase: 'running', streaming: true, chunks: { a: 1 }, store_error: 'db down' }, ago)!;
    expect(v.job).toBe('Running · streaming changes');
    expect(v.copy).toBeNull();
    expect(v.saved).toBeNull();
    expect(v.problem).toBe('Saving positions failed: db down');
    expect(flinkView(undefined, ago)).toBeNull();
    expect(flinkView({ phase: 'config_error' }, ago)!.job).toBe('Can’t start: the image or settings');
  });
});

describe('transformView on Flink CDC', () => {
  it('describes tables kept in sync, never Debezium events', () => {
    const spec = fullSpec({
      source: { tables: { include: ['s.t'] }, columns: { 's.t': { mask: [], exclude: ['email'] } } },
      transforms: [{ type: 'unwrap', delete_handling: 'rewrite', add_fields: ['op'] }],
      keys: { 's.t': ['id'] },
    } as never);
    const { output, steps } = transformView(spec, { sinkType: 'starrocks' });
    expect(output.map((r) => r.title)).toEqual(['Tables kept in sync', 'Deletes remove the row', 'One column left out', 'Chosen primary key']);
    expect(steps).toEqual([]);
    const iceberg = transformView(spec, { sinkType: 'iceberg' }).output;
    expect(iceberg[1]).toMatchObject({ title: 'Deleted rows are kept', code: ['__op = -D'] });
  });
});

describe('shapeView', () => {
  const size = { task_managers: 2, slots: 3, task_manager_cpu: '3', task_manager_memory: '6656Mi' };
  const streaming_size = { task_managers: 1, slots: 6, task_manager_cpu: '3', task_manager_memory: '3584Mi' };
  const agoOf = () => '2 h ago';
  it('is nothing for a cluster that never shrinks', () => {
    expect(shapeView({}, agoOf)).toBeNull();
    expect(shapeView({ flink_shape: { phase: 'copy', size } }, agoOf)).toBeNull();
    expect(shapeView(null, agoOf)).toBeNull();
  });
  it('says which shape it runs and offers Shrink now only when ready', () => {
    expect(shapeView({ flink_shape: { phase: 'copy', size, streaming_size } }, agoOf)).toEqual({
      text: 'Copy shape, shrinks after the first copy · 2 TaskManagers × 3 slots, 3 CPU and 6656Mi each',
      ready: false,
      problem: null,
    });
    expect(shapeView({ flink_shape: { phase: 'copy', size, streaming_size, shrink_ready: true } }, agoOf)?.ready).toBe(true);
    expect(shapeView({ flink_shape: { phase: 'streaming', size, streaming_size, shrunk_at: '2026-11-06T10:00:00Z' } }, agoOf)?.text).toBe(
      'Streaming shape, shrank 2 h ago · 1 TaskManager × 6 slots, 3 CPU and 3584Mi each',
    );
  });
  it('says when it stopped trying', () => {
    const v = shapeView({ flink_shape: { phase: 'copy', size, streaming_size, shrink_off: true, shrink_failures: 3, shrink_ready: true } }, agoOf);
    expect(v?.ready).toBe(false);
    expect(v?.problem).toContain('failed 3 times');
  });
});
