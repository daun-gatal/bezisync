import { useState } from 'react';
import { ShieldCheck, X } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Notice } from '@/components/ui/feedback';
import { Card } from '@/components/ui/layout';
import { Menu } from '@/components/ui/overlays';
import { Combobox, Select } from '@/components/ui/select';
import { DataTable } from '@/components/ui/table';
import { Meta, plural, Short } from '@/components/ui/text';
import { personalProblem, safeChoice, type Choice, type PersonalRow } from './personal';

const LABEL: Record<Choice, string> = { mask: 'Mask', remove: 'Remove', keep: 'Keep as is' };

/**
 * Step 2's personal data choices for the captured tables: one row per column that looks like
 * personal data (or has a rule), each masked, removed or kept on purpose.
 */
export function PersonalDataCard({
  rows,
  candidates,
  sourceCanMask,
  sourceTitle,
  noMaskWhy = null,
  expert,
  onChoose,
}: {
  rows: PersonalRow[];
  /** Other columns of the captured tables that can get a rule ("table.column" → type). */
  candidates: { table: string; column: string; type: string; maskable: boolean }[];
  sourceCanMask: boolean;
  sourceTitle: string;
  /** Why nothing can be masked, when it isn't the source (a Flink CDC pipeline). */
  noMaskWhy?: string | null;
  expert: boolean;
  onChoose: (table: string, column: string, choice: Choice | null) => void;
}) {
  const [adding, setAdding] = useState<string | null>(null);
  const detected = rows.filter((r) => r.detected).length;
  const problem = personalProblem(rows);
  const maskWhy = (r: PersonalRow) =>
    !sourceCanMask
      ? (noMaskWhy ?? `${sourceTitle} values can’t be hashed.`)
      : r.type === null
        ? 'The column is not in the table any more.'
        : `Only text columns can be masked (this one is ${r.type}).`;
  const options = (r: PersonalRow) => [
    {
      value: 'mask' as const,
      label: LABEL.mask,
      description: 'A salted hash: equal values still match for joins.',
      disabled: !r.maskable,
      why: r.maskable ? null : maskWhy(r),
    },
    { value: 'remove' as const, label: LABEL.remove, description: 'Left out of every event.' },
    // A column you added stops being protected with its × button instead.
    ...(r.detected ? [{ value: 'keep' as const, label: LABEL.keep, description: 'Sent as it is: you decided it may leave the database.' }] : []),
  ];
  const applyAll = (v: Choice) => {
    for (const r of rows) {
      if (r.type === null) continue;
      onChoose(r.table, r.column, v === 'mask' && !r.maskable ? 'remove' : v === 'keep' && !r.detected ? null : v);
    }
  };

  return (
    <Card
      title="Personal data"
      actions={
        rows.length > 1 ? (
          <Menu
            label="Choose for all"
            size="sm"
            items={[
              { label: 'Mask all', description: 'Columns that can’t be hashed are removed instead.', onSelect: () => applyAll('mask') },
              { label: 'Remove all', onSelect: () => applyAll('remove') },
              { label: 'Keep all as is', onSelect: () => applyAll('keep') },
            ]}
          />
        ) : null
      }
    >
      <p className="m-0 text-base text-muted">
        {detected
          ? `${plural(detected, 'column')} look like personal data. Choose what reaches the destination; masking keeps joins working.`
          : 'No column looks like personal data. You can still mask or remove any column.'}
        {expert ? ' Debezium: column.mask.hash.v2.SHA-256.with.salt and column.exclude.list.' : ''}
      </p>
      {rows.length ? (
        <DataTable
          label="Personal data choices"
          rows={rows}
          rowKey={(r) => `${r.table}.${r.column}`}
          paging="card"
          columns={[
            {
              id: 'column',
              header: 'Column',
              min: 200,
              cell: (r) => (
                <div className="flex min-w-0 flex-col gap-0.5">
                  <Short max={40} mono>{`${r.table}.${r.column}`}</Short>
                  <Meta>{r.type === null ? 'not in the table any more' : `${r.type}${r.detected ? ' · looks like personal data' : ''}`}</Meta>
                </div>
              ),
            },
            {
              id: 'choice',
              header: 'Send',
              min: 170,
              cell: (r) =>
                r.type === null ? (
                  <Button size="sm" variant="ghost" icon={<X />} onClick={() => onChoose(r.table, r.column, null)} aria-label={`Drop the rule for ${r.table}.${r.column}`}>
                    Drop rule
                  </Button>
                ) : (
                  <div className="flex min-w-0 items-center gap-2">
                    <Select
                      label={`What to send for ${r.table}.${r.column}`}
                      value={r.choice}
                      placeholder="Choose…"
                      onChange={(v) => onChoose(r.table, r.column, v)}
                      options={options(r)}
                      invalid={(r.choice === 'mask' && !r.maskable) || (r.detected && r.choice === null)}
                    />
                    {!r.detected ? (
                      <Button
                        size="sm"
                        variant="ghost"
                        icon={<X />}
                        label={`Stop protecting ${r.table}.${r.column}`}
                        onClick={() => onChoose(r.table, r.column, null)}
                      />
                    ) : null}
                  </div>
                ),
            },
          ]}
        />
      ) : null}
      {problem ? <Notice tone={problem.blocking ? 'bad' : 'warn'}>{problem.text}</Notice> : null}
      {candidates.length ? (
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <ShieldCheck className="size-4 shrink-0 text-muted" aria-hidden />
          <label htmlFor="protect-column" className="text-sm text-ink-2">
            Protect another column
          </label>
          <div className="min-w-0 flex-[1_1_240px]">
            <Combobox
              id="protect-column"
              value={adding}
              placeholder="Choose a column…"
              searchLabel="Search columns"
              onChange={(v) => {
                const c = candidates.find((x) => `${x.table}.${x.column}` === v);
                if (c) onChoose(c.table, c.column, safeChoice(c.maskable));
                setAdding(null);
              }}
              options={candidates.map((c) => ({
                value: `${c.table}.${c.column}`,
                label: `${c.table}.${c.column}`,
                description: `${c.type} · ${c.maskable ? 'will be masked' : 'will be removed (can’t be hashed)'}`,
              }))}
            />
          </div>
        </div>
      ) : null}
    </Card>
  );
}
