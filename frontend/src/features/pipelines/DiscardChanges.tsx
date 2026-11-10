import { toast } from 'sonner';
import { Confirm } from '@/components/ui/overlays';
import { useDiscardChanges, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';

/**
 * Whether a pipeline's undeployed change can be discarded (ADR 0017): it was deployed, its
 * current version isn't the deployed one, and nothing waits for approval.
 */
export function canDiscard(p: Pipeline | undefined, canEdit: boolean): p is Pipeline {
  return Boolean(canEdit && p && p.deployed_version && p.has_undeployed_changes && !p.pending_change);
}

/** What discarding does, said the same way wherever it is offered. */
export function discardEffects(p: Pipeline): string[] {
  return [
    `Settings go back to v${p.deployed_version}, the version the connector runs.`,
    `The connector keeps running v${p.deployed_version}: nothing restarts.`,
    `v${p.current_version} stays in History; deploy it from there to bring it back.`,
    'Description, owner, labels and the runtime aren’t part of a version: they stay as they are.',
  ];
}

export function discardedToast(p: Pipeline) {
  toast.success(`Discarded v${p.current_version}; v${p.deployed_version} stays deployed`);
}

/** Confirms and discards a pipeline's undeployed change. */
export function DiscardDialog({
  pipeline: p,
  open,
  onOpenChange,
  onDone,
}: {
  pipeline: Pipeline;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onDone?: () => void;
}) {
  const { project } = useProject();
  const discard = useDiscardChanges(project.id);
  return (
    <Confirm
      open={open}
      onOpenChange={(o) => {
        if (!o) discard.reset();
        onOpenChange(o);
      }}
      title={`Discard v${p.current_version}?`}
      effects={discardEffects(p)}
      confirmLabel={`Discard v${p.current_version}`}
      pending={discard.isPending}
      error={discard.error}
      doing="Couldn’t discard the change"
      onConfirm={() =>
        discard.mutate(p, {
          onSuccess: () => {
            discardedToast(p);
            onOpenChange(false);
            onDone?.();
          },
        })
      }
    />
  );
}
