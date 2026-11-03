import { Plus } from 'lucide-react';
import { Combobox } from '@/components/ui/select';
import { usePipelines } from '@/lib/pipelines';
import { useProject } from '@/lib/session';

/** Pick a team already owning pipelines in this project, or add one explicitly. */
export function OwnerPicker({ id, value, onChange }: { id: string; value: string; onChange: (owner: string) => void }) {
  const { project } = useProject();
  const pipelines = usePipelines(project.id);
  const owners = [...new Set([value, ...(pipelines.data ?? []).map((p) => p.owner)].filter(Boolean))].sort((a, b) => a.localeCompare(b));
  return (
    <Combobox
      id={id}
      value={value}
      onChange={onChange}
      placeholder="No owner yet"
      searchLabel="Search or add a team"
      options={[{ value: '', label: 'No owner', description: 'Alerts reach only the project’s channels' }, ...owners.map((o) => ({ value: o, label: o }))]}
      footer={(close, query) => {
        const name = query.trim();
        if (!name || owners.includes(name)) return null;
        return (
          <button
            type="button"
            disabled={name.length > 200}
            onClick={() => {
              onChange(name);
              close();
            }}
            className="flex w-full min-w-0 items-center gap-2 rounded-md px-2 py-1.5 text-left text-base text-accent hover:bg-hover disabled:text-muted"
          >
            <Plus className="size-4 shrink-0" aria-hidden />
            <span className="min-w-0 wrap-anywhere">{name.length > 200 ? 'Owner names stop at 200 characters' : `Use “${name}”`}</span>
          </button>
        );
      }}
    />
  );
}
