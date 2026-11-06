import { keepPreviousData, useQuery } from '@tanstack/react-query';
import { useMemo } from 'react';
import { useDebounced } from '@/components/ui/hooks';
import { client, unwrap, type Schemas } from './api';
import type { FlinkCluster } from './engines';

/** How the server would pack a custom cluster on the pipeline's runtime now (POST …/cluster:pack). */
export type PackedCluster = Schemas['ClusterPackRead'];

/** CPU in quarters from lo to hi, written as the spec stores it ("0.25", "1.5", "2"). */
function cpus(lo: number, hi: number): string[] {
  const out: string[] = [];
  for (let m = lo; m <= hi; m += 250) out.push(String(m / 1000));
  return out;
}

/** Memory from lo to hi MiB in steps, whole GiB written as Gi (domain.ParseMemoryAny). */
function memories(lo: number, hi: number, step: number): string[] {
  const out: string[] = [];
  for (let m = lo; m <= hi; m += step) out.push(m % 1024 === 0 ? `${m / 1024}Gi` : `${m}Mi`);
  return out;
}

/** The choices the server accepts (domain/cluster.go bounds). TaskManager memory is offered in
 *  512 MiB steps (the server takes 256). */
export const SLOT_CPUS = cpus(250, 4000);
export const SLOT_MEMORIES = memories(512, 8192, 256);
export const JM_CPUS = cpus(250, 2000);
export const JM_MEMORIES = memories(768, 8192, 256);
export const TM_CPUS = cpus(500, 16000);
export const TM_MEMORIES = memories(1024, 65536, 512);
export const MAX_PARALLELISM = 128;
export const MAX_TASK_MANAGERS = 16;
export const MAX_SLOTS = 8;

/** MiB of a quantity ("2Gi", "1536Mi"). */
export function mib(q: string): number {
  if (q.endsWith('Gi')) return Number(q.slice(0, -2)) * 1024;
  if (q.endsWith('Mi')) return Number(q.slice(0, -2));
  return NaN;
}

/** A memory quantity as people read it ("1.5 GiB", "768 MiB"). */
export function memoryLabel(q: string): string {
  const m = mib(q);
  if (!Number.isFinite(m)) return q;
  return m >= 1024 && m % 256 === 0 ? `${Number((m / 1024).toFixed(2))} GiB` : `${m} MiB`;
}

/** A CPU quantity as the spec writes it ("500m" → "0.5"). */
function cpuOf(q: string): string {
  return q.endsWith('m') ? String(Number(q.slice(0, -1)) / 1000) : q;
}

/** A custom cluster that starts from a preset: the same readers, each slot what the preset's
 *  TaskManager gives a slot (its memory less the TaskManager's 512 MiB, shared by its 2 slots). */
export function clusterFromPreset(size: Schemas['FlinkSize'] | undefined): FlinkCluster {
  if (!size) return { parallelism: 2, slot: { cpu: '0.5', memory: '768Mi' }, job_manager: { cpu: '0.5', memory: '1Gi' }, shape: 'auto' };
  const slots = Math.max(1, Math.round(size.parallelism / size.taskmanagers));
  const slotMiB = Math.max(512, Math.floor((mib(size.taskmanager_memory) - 512) / slots / 256) * 256);
  const slotCPU = Math.max(250, Math.floor((Number(cpuOf(size.taskmanager_cpu)) * 1000) / slots / 250) * 250);
  return {
    parallelism: size.parallelism,
    slot: { cpu: String(slotCPU / 1000), memory: slotMiB % 1024 === 0 ? `${slotMiB / 1024}Gi` : `${slotMiB}Mi` },
    job_manager: { cpu: cpuOf(size.jobmanager_cpu), memory: size.jobmanager_memory },
    shape: 'auto',
  };
}

/** A streaming slot size that starts below the copy slot: about half, never bigger. */
export function streamingFrom(slot: { cpu: string; memory: string }): { cpu: string; memory: string } {
  const cpu = Math.max(250, Math.floor((Number(slot.cpu) * 1000) / 2 / 250) * 250);
  const mem = Math.max(512, Math.floor(mib(slot.memory) / 2 / 256) * 256);
  return { cpu: String(cpu / 1000), memory: mem % 1024 === 0 ? `${mem / 1024}Gi` : `${mem}Mi` };
}

/** Why a streaming slot size can't be used (mirrors decodeCluster); null when it can. The first of `streamingErrors`. */
export function streamingError(c: FlinkCluster): string | null {
  const e = streamingErrors(c);
  return e.cpu ?? e.memory ?? null;
}

/** Why a manual shape can't hold the readers (mirrors decodeCluster); null when it can. The first of `manualErrors`. */
export function manualError(c: FlinkCluster): string | null {
  const e = manualErrors(c);
  return e.taskManagers ?? e.memory ?? null;
}

/** How the server packs a cluster for the pipeline's runtime, asked once it stopped changing. */
export function usePackCluster(projectId: string, pipelineId: string, cluster: FlinkCluster | undefined, enabled: boolean) {
  // Debounce a string: a fresh object each render would never settle.
  const asked = useDebounced(cluster ? JSON.stringify(cluster) : '', 300);
  const body = useMemo(() => (asked ? (JSON.parse(asked) as FlinkCluster) : null), [asked]);
  return useQuery({
    queryKey: ['cluster-pack', projectId, pipelineId, asked],
    enabled: enabled && body !== null,
    staleTime: 60_000,
    placeholderData: keepPreviousData,
    retry: false,
    queryFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/pipelines/{pipeline_id}/cluster:pack', {
          params: { path: { project_id: projectId, pipeline_id: pipelineId } },
          body: { cluster: body! },
        }),
      ),
  });
}

/**
 * `streamingError` per field: a bigger CPU or memory than while copying on that field, the same
 * size (both equal) on both. A manual shape is never resized and its streaming fields aren't
 * drawn (switching to Manual clears the streaming size), so it has none. Mirrors decodeCluster.
 */
export function streamingErrors(c: FlinkCluster): { cpu?: string; memory?: string } {
  const s = c.streaming_slot;
  if (!s || c.shape === 'manual') return {};
  const bigger = 'While streaming a reader needs no more than while copying.';
  const sameCpu = Number(s.cpu) === Number(c.slot.cpu);
  const sameMem = mib(s.memory) === mib(c.slot.memory);
  if (sameCpu && sameMem) {
    const same = 'The same size as while copying: there is nothing to shrink to.';
    return { cpu: same, memory: same };
  }
  const out: { cpu?: string; memory?: string } = {};
  if (Number(s.cpu) > Number(c.slot.cpu)) out.cpu = bigger;
  if (mib(s.memory) > mib(c.slot.memory)) out.memory = bigger;
  return out;
}

/** `manualError` per field: too few slots on TaskManagers, too little memory a slot on TaskManager memory. Only a manual shape has them. */
export function manualErrors(c: FlinkCluster): { taskManagers?: string; memory?: string } {
  const tm = c.task_managers;
  if (c.shape !== 'manual' || !tm) return {};
  if (tm.count * tm.slots < c.parallelism) return { taskManagers: `${tm.count} × ${tm.slots} slots can’t hold ${c.parallelism} readers: add TaskManagers or slots.` };
  if (mib(tm.memory) / tm.slots < 512) return { memory: 'Each slot needs at least 512 MiB: add memory or remove slots.' };
  return {};
}
