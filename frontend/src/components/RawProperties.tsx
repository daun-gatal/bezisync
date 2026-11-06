import { Plus, Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Pill } from '@/components/ui/feedback';
import { Input } from '@/components/ui/form';
import { Meta } from '@/components/ui/text';
import type { Schemas } from '@/lib/api';

export type RawRow = { key: string; value: string };
export type Verdict = Schemas['ClassifyRead'];

const CLASS: Record<Verdict['class'], { label: string; tone: 'grey' | 'bad' }> = {
  allowed: { label: 'Not verified', tone: 'grey' },
  invalid: { label: 'Can’t be set', tone: 'bad' },
  credential: { label: 'Credential', tone: 'bad' },
  forbidden: { label: 'Not allowed', tone: 'bad' },
  modelled: { label: 'Set elsewhere', tone: 'bad' },
  owned: { label: 'Set by BeziSync', tone: 'bad' },
};

/** Where a refused property's value is set instead, as a link label. */
export const FIX_LABEL: Record<string, string> = {
  tables: 'Tables step',
  sink: 'Destination step',
  transform: 'Transform step',
  review: 'Review',
  connection: 'the connection',
};

/**
 * Raw (advanced) properties as key/value rows. What BeziSync makes of each key comes from the
 * server (`verdicts`, by key); a key that is also a typed setting offers to become one. Empty rows
 * are allowed while typing and never saved.
 */
export function RawProperties({
  rows,
  onChange,
  verdicts,
  onUseSetting,
  disabled,
  keyPlaceholder,
  idBase,
}: {
  rows: RawRow[];
  onChange: (rows: RawRow[]) => void;
  verdicts: Record<string, Verdict | undefined>;
  onUseSetting?: (row: RawRow) => void;
  disabled?: boolean;
  keyPlaceholder: string;
  idBase: string;
}) {
  const set = (i: number, patch: Partial<RawRow>) => onChange(rows.map((r, j) => (j === i ? { ...r, ...patch } : r)));
  const keys = rows.map((r) => r.key.trim());
  return (
    <div className="flex min-w-0 flex-col gap-3">
      {rows.map((row, i) => {
        const key = row.key.trim();
        const v = key ? verdicts[key] : undefined;
        const dup = key !== '' && keys.indexOf(key) !== i;
        const status = dup ? { label: 'Twice', tone: 'bad' as const } : v ? CLASS[v.class] : key ? { label: 'Checking…', tone: 'grey' as const } : null;
        const message = dup ? 'This key is already set above.' : v && v.class !== 'allowed' ? v.message : v?.setting ? `BeziSync lists this one as “${v.setting.title}”.` : null;
        return (
          <div key={i} className="flex min-w-0 flex-col gap-1">
            <div className="grid min-w-0 grid-cols-[minmax(0,3fr)_minmax(0,2fr)_auto_auto] items-center gap-2">
              <Input
                id={`${idBase}-key-${i}`}
                aria-label={`Property ${i + 1} name`}
                className="font-mono"
                spellCheck={false}
                placeholder={keyPlaceholder}
                value={row.key}
                disabled={disabled}
                invalid={Boolean(dup || (v && v.class !== 'allowed'))}
                onChange={(e) => set(i, { key: e.target.value })}
              />
              <Input
                id={`${idBase}-value-${i}`}
                aria-label={`Property ${i + 1} value`}
                className="font-mono"
                spellCheck={false}
                placeholder="value"
                value={row.value}
                disabled={disabled}
                onChange={(e) => set(i, { value: e.target.value })}
              />
              <span className="min-w-24">{status ? <Pill tone={status.tone}>{status.label}</Pill> : null}</span>
              <Button
                size="sm"
                variant="ghost"
                icon={<Trash2 />}
                aria-label={`Remove property ${i + 1}`}
                disabled={disabled}
                onClick={() => onChange(rows.filter((_, j) => j !== i))}
              />
            </div>
            {message ? (
              <div className="flex flex-wrap items-center gap-2">
                <Meta className={v && v.class !== 'allowed' ? 'text-bad' : undefined}>
                  {message}
                  {v?.fix && v.class !== 'allowed' ? ` Set it on the ${FIX_LABEL[v.fix] ?? v.fix}.` : ''}
                </Meta>
                {v?.setting && onUseSetting && !disabled ? (
                  <Button size="sm" onClick={() => onUseSetting(row)}>
                    Use the typed setting
                  </Button>
                ) : null}
              </div>
            ) : null}
          </div>
        );
      })}
      {disabled ? null : (
        <div>
          <Button size="sm" icon={<Plus />} onClick={() => onChange([...rows, { key: '', value: '' }])}>
            Add a property
          </Button>
        </div>
      )}
    </div>
  );
}
