import { Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/layout';
import { Meta } from '@/components/ui/text';
import type { ReviewProblem } from './review';

/**
 * The engine settings and raw properties that block saving, where the full editor isn't drawn
 * (Guided mode, or a pipeline with no typed settings to offer): each with why and Remove, so a
 * blocking value is never on a field the person can't see.
 */
export function BlockingSettings({ problems, onRemove, expert, card = true }: { problems: ReviewProblem[]; onRemove: (p: ReviewProblem) => void; expert: boolean; card?: boolean }) {
  if (!problems.length) return null;
  const group = (field: 'tuning' | 'advanced') => {
    const list = problems.filter((p) => p.field === field);
    return list.length ? (
      <div className="flex min-w-0 flex-col gap-3" data-review-field={field}>
        {list.map((p) => (
          <div key={`${p.field}:${p.key}`} className="grid min-w-0 grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
            <div className="flex min-w-0 flex-col">
              <Meta className="font-mono break-all">{p.key}</Meta>
              <span className="text-sm text-bad wrap-break-word">{p.message}</span>
            </div>
            <Button size="sm" variant="ghost" icon={<Trash2 />} aria-label={`Remove ${p.key}`} onClick={() => onRemove(p)} />
          </div>
        ))}
      </div>
    ) : null;
  };
  const rows = (
    <div className="flex min-w-0 flex-col gap-3">
      {expert ? null : <Meta>Switch to Expert mode (account menu) to edit these.</Meta>}
      {group('tuning')}
      {group('advanced')}
    </div>
  );
  return card ? <Card title="Settings that block saving">{rows}</Card> : rows;
}
