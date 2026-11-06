import { describe, expect, it } from 'vitest';
import { clusterSummary, readersPerTaskManager } from '@/components/ClusterDiagram';
import { copyMinutes } from '@/features/pipelines/wizard/FlinkClusterEditor';
import { clusterFromPreset, JM_CPUS, JM_MEMORIES, manualError, memoryLabel, SLOT_CPUS, SLOT_MEMORIES, TM_MEMORIES } from '../cluster';

const small = { jobmanager_cpu: '500m', jobmanager_memory: '1Gi', taskmanagers: 1, taskmanager_cpu: '1', taskmanager_memory: '2Gi', parallelism: 2, total_memory_mib: 3072 };
const large = { jobmanager_cpu: '1', jobmanager_memory: '1536Mi', taskmanagers: 4, taskmanager_cpu: '1', taskmanager_memory: '2Gi', parallelism: 8, total_memory_mib: 9728 };

describe('custom Flink CDC clusters', () => {
  it('offers exactly the values the server accepts, canonically written', () => {
    expect(SLOT_CPUS[0]).toBe('0.25');
    expect(SLOT_CPUS.at(-1)).toBe('4');
    expect(SLOT_CPUS).toContain('1.5');
    expect(SLOT_MEMORIES[0]).toBe('512Mi');
    expect(SLOT_MEMORIES).toContain('1Gi');
    expect(SLOT_MEMORIES).not.toContain('1024Mi');
    expect(SLOT_MEMORIES.at(-1)).toBe('8Gi');
    expect(JM_CPUS.at(-1)).toBe('2');
    expect(JM_MEMORIES[0]).toBe('768Mi');
    // Whole GiB are written as Gi, the rest as Mi (the server refuses "2048Mi").
    expect(TM_MEMORIES.every((m) => (m.endsWith('Gi') ? /^\d+Gi$/.test(m) : /^\d+Mi$/.test(m) && Number(m.slice(0, -2)) % 1024 !== 0))).toBe(true);
    expect(TM_MEMORIES[0]).toBe('1Gi');
    expect(TM_MEMORIES.at(-1)).toBe('64Gi');
  });

  it('starts from a preset with the same readers and the same TaskManagers once packed', () => {
    expect(clusterFromPreset(small)).toEqual({ parallelism: 2, slot: { cpu: '0.5', memory: '768Mi' }, job_manager: { cpu: '0.5', memory: '1Gi' }, shape: 'auto' });
    expect(clusterFromPreset(large)).toEqual({ parallelism: 8, slot: { cpu: '0.5', memory: '768Mi' }, job_manager: { cpu: '1', memory: '1536Mi' }, shape: 'auto' });
    expect(clusterFromPreset(undefined).parallelism).toBe(2);
  });

  it('says why a manual shape can’t hold the readers', () => {
    const base = { parallelism: 6, slot: { cpu: '1', memory: '1Gi' }, job_manager: { cpu: '1', memory: '1Gi' }, shape: 'manual' as const };
    expect(manualError({ ...base, task_managers: { count: 2, slots: 3, cpu: '2', memory: '4Gi' } })).toBeNull();
    expect(manualError({ ...base, task_managers: { count: 2, slots: 2, cpu: '2', memory: '4Gi' } })).toContain('can’t hold 6 readers');
    expect(manualError({ ...base, task_managers: { count: 1, slots: 8, cpu: '2', memory: '2Gi' } })).toContain('512 MiB');
    expect(manualError({ ...base, shape: 'auto' })).toBeNull();
  });

  it('spreads readers over TaskManagers and says so in words', () => {
    const shape = { task_managers: 3, slots: 2, readers: 5, task_manager_cpu: '2', task_manager_memory: '2560Mi', job_manager_cpu: '1', job_manager_memory: '1Gi' };
    expect(readersPerTaskManager(shape)).toEqual([2, 2, 1]);
    expect(clusterSummary(shape)).toBe('A JobManager (1 CPU, 1 GiB) and 3 TaskManagers × 2 slots, 2 CPU and 2.5 GiB each: 5 readers, 1 slot idle.');
    expect(readersPerTaskManager({ ...shape, task_managers: 16, slots: 8, readers: 128 })).toEqual(Array(16).fill(8));
  });

  it('writes memory as people read it and estimates the copy roughly', () => {
    expect(memoryLabel('768Mi')).toBe('768 MiB');
    expect(memoryLabel('6656Mi')).toBe('6.5 GiB');
    expect(memoryLabel('2Gi')).toBe('2 GiB');
    expect(copyMinutes(null, 4)).toBeNull();
    expect(copyMinutes(12_000_000, 1)).toBe(20);
    expect(copyMinutes(12_000_000, 4)).toBe(5);
    expect(copyMinutes(10, 4)).toBe(1);
  });
});
