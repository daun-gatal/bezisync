import { screen } from '@testing-library/react';
import { renderAt } from '@/test/render';
import { EngineVersionsSection } from '../EngineVersionsSection';

let flinkOn = false;
const entry = (image: string) => ({ id: image, image, label: '', notes: '', is_default: true, deprecated: false, builtin: false, official: false, pipelines: 0, projects: [] });
vi.mock('@/lib/engines', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/engines')>()),
  useEngines: () => ({
    data: [
      { id: 'debezium', title: 'Debezium', available: true, reason: null, default_image: 'quay.io/debezium/server:3.7.0.Final' },
      { id: 'flink-cdc', title: 'Flink CDC', available: flinkOn, reason: flinkOn ? null : 'off', default_image: null },
    ],
  }),
  useEngineCatalog: (engine: string) => ({
    status: 'success',
    isPending: false,
    isError: false,
    data: { default_image: null, entries: engine === 'flink-cdc' ? (flinkOn ? [entry('registry.example/flink-cdc:3.7.0')] : []) : [entry('quay.io/debezium/server:3.7.0.Final')] },
  }),
}));

const path = '/p/$projectSlug/settings/$section';

describe('EngineVersionsSection', () => {
  it('lists Debezium images by default', async () => {
    renderAt(() => <EngineVersionsSection />, { path, at: '/p/default/settings/engines' });
    expect(await screen.findByText('quay.io/debezium/server:3.7.0.Final')).toBeInTheDocument();
    expect(screen.queryByText('Flink CDC is off')).toBeNull();
  });

  it('explains how to turn Flink CDC on while it is off', async () => {
    flinkOn = false;
    renderAt(() => <EngineVersionsSection />, { path, at: '/p/default/settings/engines?engine=flink-cdc' });
    expect(await screen.findByText('Flink CDC is off')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Flink CDC (off)' })).toHaveAttribute('aria-selected', 'true');
    expect(screen.getByText(/repository:tag of your Flink CDC build/)).toBeInTheDocument();
  });

  it('lists the Flink CDC images once one is registered', async () => {
    flinkOn = true;
    renderAt(() => <EngineVersionsSection />, { path, at: '/p/default/settings/engines?engine=flink-cdc' });
    expect(await screen.findByText('registry.example/flink-cdc:3.7.0')).toBeInTheDocument();
    expect(screen.queryByText('Flink CDC is off')).toBeNull();
  });
});
