import { useUpdatePipeline, type Pipeline } from '@/lib/pipelines';
import { useProject } from '@/lib/session';
import type { FullSpec } from './spec';
import type { EngineId } from '@/lib/engines';

export interface SaveBody {
  spec?: FullSpec;
  sink_id?: string;
  runtime_id?: string;
  name?: string;
  owner?: string;
  /** Only when the person picks one; otherwise the server keeps or picks the engine. */
  engine?: EngineId;
}

/**
 * Saves wizard edits (optimistic concurrency on the pipeline). The server keeps one version per
 * change: saves update the author's draft version until it is deployed or sent for approval.
 * `save` resolves to whether it worked; the error stays in `error` for the step to show inline.
 */
export function useSaveSpec(pipeline: Pipeline) {
  const { project } = useProject();
  const update = useUpdatePipeline(project.id);
  return {
    pending: update.isPending,
    error: update.error,
    reset: update.reset,
    save: async (body: SaveBody): Promise<boolean> => {
      try {
        await update.mutateAsync({ pipeline, body: { ...body, spec: body.spec as never, note: '' } });
        return true;
      } catch {
        return false;
      }
    },
  };
}

/** What every wizard step gives the wizard, so leaving a step (by any route) saves it first. */
export interface StepHandle {
  dirty: () => boolean;
  /** Save the step; false when it can't be saved (invalid, or the save failed: the step shows why). */
  commit: () => Promise<boolean>;
}
