import { useQuery } from '@tanstack/react-query';
import { client, unwrap, type Schemas } from '@/lib/api';

export type PipelineImage = Schemas['PipelineImageRead'] & { image: string; standing: string; release_notes_url: string | null | undefined };

export const catalogKey = ['debezium-catalog'] as const;
export const imagesKey = (projectId: string) => ['debezium-images', projectId] as const;

/** The install-wide Debezium version catalog (Admins curate it in Settings → Engine versions). */
export function useCatalog() {
  return useQuery({ queryKey: catalogKey, queryFn: () => unwrap(client.GET('/api/v1/debezium-catalog')), staleTime: 60_000 });
}

/** Deployed pipelines of a project grouped by the image they run. */
export function useDebeziumImages(projectId: string) {
  return useQuery({
    queryKey: imagesKey(projectId),
    queryFn: () => unwrap(client.GET('/api/v1/projects/{project_id}/debezium-images', { params: { path: { project_id: projectId } } })),
    refetchInterval: 10_000,
  });
}

/** Each deployed pipeline's image, by pipeline id. */
export function imageByPipeline(data: Schemas['ImagesRead'] | undefined): Map<string, PipelineImage> {
  const out = new Map<string, PipelineImage>();
  for (const group of data?.images ?? []) {
    for (const p of group.pipelines) out.set(p.id, { ...p, image: group.image, standing: group.standing, release_notes_url: group.release_notes_url });
  }
  return out;
}

/** "3.7.0.Final" for official images, "Iceberg sink 1.2.1.Final" for the Iceberg build (digest
 *  dropped), the full reference otherwise. */
export function shortImage(image: string): string {
  const official = 'quay.io/debezium/server:';
  const iceberg = 'ghcr.io/memiiso/debezium-server-iceberg:';
  if (image.startsWith(official)) return image.slice(official.length);
  if (image.startsWith(iceberg)) return `Iceberg sink ${image.slice(iceberg.length).split('@')[0]}`;
  return image;
}

export const SOURCE_LABEL: Record<Schemas['PipelineImageRead']['source'], string> = {
  pinned: 'chosen for this pipeline',
  runtime: 'runtime default',
  default: 'default',
};
