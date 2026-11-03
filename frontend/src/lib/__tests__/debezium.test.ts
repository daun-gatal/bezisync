import { describe, expect, it } from 'vitest';
import { imageByPipeline, shortImage } from '../debezium';

describe('shortImage', () => {
  it('shows only the version of official images', () => {
    expect(shortImage('quay.io/debezium/server:3.7.0.Final')).toBe('3.7.0.Final');
    expect(shortImage('ghcr.io/memiiso/debezium-server-iceberg:1.2.1.Final@sha256:f574')).toBe('Iceberg sink 1.2.1.Final');
    expect(shortImage('registry.internal/dbz:custom')).toBe('registry.internal/dbz:custom');
  });
});

describe('imageByPipeline', () => {
  it('indexes each deployed pipeline with the image group it runs', () => {
    const pipeline = { id: 'p1', name: 'orders', lifecycle: 'streaming', health: 'healthy', runtime: 'k8s', pinned: false, pending_image: null, source: 'runtime' as const };
    const byId = imageByPipeline({
      default_image: 'quay.io/debezium/server:3.7.0.Final',
      default_release_notes_url: null,
      images: [
        { image: 'quay.io/debezium/server:3.6.1.Final', repository: 'quay.io/debezium/server', tag: '3.6.1.Final', official: true, is_default: false, release_notes_url: null, standing: 'deprecated', sink: null, pipelines: [pipeline] },
      ],
    });
    expect(byId.get('p1')).toMatchObject({ image: 'quay.io/debezium/server:3.6.1.Final', standing: 'deprecated', source: 'runtime' });
    expect(byId.get('missing')).toBeUndefined();
  });
});
