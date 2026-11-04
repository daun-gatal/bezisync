import { useState } from 'react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Pill } from '@/components/ui/feedback';
import { Field, Input } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { Confirm } from '@/components/ui/overlays';
import { List, ListRow } from '@/components/ui/table';
import { Meta } from '@/components/ui/text';
import { ago, formatBytes, formatDateTime } from '@/lib/format';
import { useResetOffsets, useRestorePosition, useSavedPositions, type Pipeline, type ResetMode, type SavedPosition } from '@/lib/pipelines';
import { useProject } from '@/lib/session';

/** What a saved position is, in words. */
export function positionLabel(s: SavedPosition): string {
  if (s.kind === 'savepoint') return s.reason === 'stop' ? 'Saved when it stopped' : `Savepoint (${s.reason})`;
  if (s.reason.startsWith('restored from')) return `Restored copy: ${s.reason.slice('restored from '.length)}`;
  return 'Periodic save';
}

/** The effects of each way to start over, by source (PostgreSQL's slot moves forward). */
export function startOverEffects(mode: ResetMode, sourceType: string, at?: string): string[] {
  const aside = 'Every saved position is set aside (it stays listed here for a week and can still be restored).';
  switch (mode) {
    case 'resnapshot':
      return [aside, 'On resume, every captured table is copied again, then streaming continues.', 'Rows already in the destination are written again by key.'];
    case 'skip_to_now':
      return [
        aside,
        'On resume, the pipeline reads changes from now: changes made since it stopped are never delivered.',
        ...(sourceType === 'postgresql' ? ['The replication slot moves forward to now.'] : []),
        'Existing rows are not copied.',
      ];
    case 'from_time':
      return [aside, `On resume, the pipeline reads changes made since ${at ? formatDateTime(at) : 'the time you choose'}.`, 'Existing rows are not copied.'];
  }
}

/**
 * A Flink CDC pipeline's saved positions (newest first) and how to restore one or start over.
 * Positions change only while the pipeline is paused and its cluster has stopped.
 */
export function FlinkPositions({ pipeline: p }: { pipeline: Pipeline }) {
  const { project, can } = useProject();
  const deployed = Boolean(p.deployed_version);
  const positions = useSavedPositions(project.id, p.id, deployed);
  const restore = useRestorePosition(project.id);
  const reset = useResetOffsets(project.id);
  const [restoring, setRestoring] = useState<SavedPosition | null>(null);
  const [mode, setMode] = useState<ResetMode | null>(null);
  const [from, setFrom] = useState('');
  const paused = p.desired_state === 'paused';
  const why = paused ? null : 'Pause the pipeline first: its position changes only while it is stopped.';
  const allowed = can('offsets.reset') && deployed;
  const fromISO = from ? new Date(from).toISOString() : '';
  const future = Boolean(from) && new Date(from).getTime() > Date.now();
  return (
    <Card title="Saved positions" pad={false}>
      <div className="flex flex-col gap-2 px-card pb-2">
        <Meta as="p">
          Flink CDC saves the pipeline’s position at every commit and when it stops, encrypted in the BeziSync state database. It resumes from the
          newest one.
        </Meta>
      </div>
      {!deployed ? (
        <Meta as="p" className="px-card pb-card">
          Nothing saved yet: the pipeline hasn’t run.
        </Meta>
      ) : (
        <QueryState query={positions} doing="Couldn’t load the saved positions" lines={3}>
          {(data) =>
            data.positions.length === 0 ? (
              <Meta as="p" className="px-card pb-card">
                Nothing saved yet{p.desired_state === 'running' ? ': the first save comes once the pipeline runs.' : '.'}
              </Meta>
            ) : (
              <List label="Saved positions">
                {data.positions.map((s, i) => (
                  <ListRow key={s.id} tone={s.current ? null : 'grey'}>
                    <span className="flex min-w-0 flex-[1_1_260px] flex-col gap-1">
                      <span className="flex flex-wrap items-center gap-2">
                        <strong className="text-base font-medium">{positionLabel(s)}</strong>
                        {i === 0 && s.current ? <Pill tone="ok">Resumes from here</Pill> : null}
                        {!s.current ? <Pill tone="grey">Set aside</Pill> : null}
                      </span>
                      <Meta>
                        #{s.id} · {formatDateTime(s.taken_at)} ({ago(s.taken_at)}) · {formatBytes(s.size)}
                      </Meta>
                    </span>
                    {allowed && !(i === 0 && s.current) ? (
                      <Button
                        size="sm"
                        disabled={!paused}
                        why={why}
                        aria-label={`Restore position ${s.id}`}
                        onClick={() => {
                          restore.reset();
                          setRestoring(s);
                        }}
                      >
                        Restore…
                      </Button>
                    ) : null}
                  </ListRow>
                ))}
              </List>
            )
          }
        </QueryState>
      )}
      {allowed ? (
        <div className="flex flex-col gap-2 border-t border-line p-card">
          <strong className="text-sm font-semibold">Start over</strong>
          <div className="flex flex-wrap gap-2">
            <Button disabled={!paused} why={why} onClick={() => {
                reset.reset();
                setMode('resnapshot');
              }}>
              Copy every table again…
            </Button>
            <Button
              disabled={!paused || !p.caps.start_from_time}
              why={!p.caps.start_from_time ? (p.caps.why?.start_from_time ?? null) : why}
              onClick={() => {
                reset.reset();
                setFrom('');
                setMode('from_time');
              }}
            >
              Changes since a time…
            </Button>
            <Button disabled={!paused} why={why} variant="danger" onClick={() => {
                reset.reset();
                setMode('skip_to_now');
              }}>
              Changes from now…
            </Button>
          </div>
          {!paused ? <Meta>{why}</Meta> : null}
        </div>
      ) : null}
      <Confirm
        open={restoring !== null}
        onOpenChange={(o) => !o && setRestoring(null)}
        tone="primary"
        title={`Restore the position saved ${restoring ? formatDateTime(restoring.taken_at) : ''}?`}
        effects={[
          'On resume, the pipeline continues from this position.',
          'Changes after it are read again: rows already in the destination are written again by key.',
          'Newer positions stay listed and can be restored instead.',
        ]}
        confirmLabel="Restore position"
        typeToConfirm={p.name}
        pending={restore.isPending}
        error={restore.error}
        doing="Couldn’t restore the position"
        onConfirm={() =>
          restoring &&
          restore.mutate(
            { id: p.id, positionId: restoring.id, confirm: p.name },
            {
              onSuccess: () => {
                toast.success('Position restored. Resume the pipeline to continue from it.');
                setRestoring(null);
              },
            },
          )
        }
      />
      <Confirm
        open={mode !== null}
        onOpenChange={(o) => !o && setMode(null)}
        title={mode === 'resnapshot' ? 'Copy every table again?' : mode === 'from_time' ? 'Read changes since a time?' : 'Read changes from now?'}
        effects={mode ? startOverEffects(mode, p.source.type, fromISO) : []}
        confirmLabel={mode === 'skip_to_now' ? 'Skip changes' : 'Start over'}
        typeToConfirm={p.name}
        disabled={mode === 'from_time' && (!from || future)}
        why={mode === 'from_time' ? (!from ? 'Choose the time.' : future ? 'Choose a time in the past.' : null) : null}
        pending={reset.isPending}
        error={reset.error}
        doing="Couldn’t start over"
        onConfirm={() =>
          mode &&
          reset.mutate(
            { id: p.id, mode, confirm: p.name, fromTime: fromISO },
            {
              onSuccess: () => {
                toast.success('Done. Resume the pipeline to start over.');
                setMode(null);
              },
            },
          )
        }
      >
        {mode === 'from_time' ? (
          <Field label="Since (your local time)" hint="The source must still hold its change log from then.">
            {(id) => <Input id={id} type="datetime-local" value={from} onChange={(e) => setFrom(e.target.value)} />}
          </Field>
        ) : null}
      </Confirm>
    </Card>
  );
}
