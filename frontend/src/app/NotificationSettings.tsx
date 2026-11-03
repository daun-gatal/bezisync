import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { InlineError } from '@/components/ui/feedback';
import { Check } from '@/components/ui/form';
import { Modal } from '@/components/ui/overlays';
import { useProject, useUpdateMe } from '@/lib/session';

/** Mirrors services/inbox.KINDS on the backend. */
const KINDS = [
  { kind: 'alert_fired', label: 'An alert fires' },
  { kind: 'alert_resolved', label: 'An alert resolves' },
  { kind: 'approval_requested', label: 'A deploy waits for my approval' },
  { kind: 'approval_decided', label: 'My deploy request is approved or rejected' },
  { kind: 'feedback_received', label: 'Someone sends feedback', adminOnly: true },
] as const;

type Kind = (typeof KINDS)[number]['kind'];

/** Per-user choice of which events land in the inbox (channels such as Slack are per rule). Each box saves at once. */
export function NotificationSettings({ open, onOpenChange }: { open: boolean; onOpenChange: (open: boolean) => void }) {
  const { me } = useProject();
  const update = useUpdateMe();
  const saved = (me.user.muted_notifications ?? []) as Kind[];
  // Optimistic: the box changes at once; a failed save puts it back and says why.
  const [muted, setMuted] = useState<Set<Kind>>(() => new Set(saved));
  useEffect(() => {
    if (open) {
      setMuted(new Set(saved));
      update.reset();
    }
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps
  const toggle = (kind: Kind, on: boolean) => {
    const next = new Set(muted);
    if (on) next.delete(kind);
    else next.add(kind);
    setMuted(next);
    update.mutate({ muted_notifications: [...next] }, { onError: () => setMuted(new Set(saved)) });
  };
  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      title="Notification settings"
      description="What lands in your inbox. Each choice saves at once. Alert channels (Slack, email, …) are set per alert rule."
      size="sm"
      footer={
        <Button variant="primary" onClick={() => onOpenChange(false)}>
          Done
        </Button>
      }
    >
      <fieldset className="m-0 flex flex-col gap-3 border-0 p-0">
        <legend className="mb-1 text-sm font-medium text-ink-2">Notify me when</legend>
        {KINDS.filter((k) => !('adminOnly' in k) || me.user.is_superuser).map((k) => (
          <Check key={k.kind} checked={!muted.has(k.kind)} onChange={(on) => toggle(k.kind, on)} label={k.label} />
        ))}
      </fieldset>
      <InlineError error={update.error} doing="Couldn’t save your choice" />
    </Modal>
  );
}
