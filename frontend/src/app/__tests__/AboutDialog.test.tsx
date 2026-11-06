import { screen } from '@testing-library/react';
import { client } from '@/lib/api';
import { renderAt } from '@/test/render';
import { AboutDialog } from '../AboutDialog';

function systemInfo(hit: { status: number; body: unknown }) {
  vi.spyOn(client, 'GET').mockImplementation((() => {
    const response = new Response(JSON.stringify(hit.body), { status: hit.status });
    return Promise.resolve(hit.status < 400 ? { data: hit.body, response } : { error: hit.body, response });
  }) as never);
}

afterEach(() => vi.restoreAllMocks());

describe('About BeziSync', () => {
  it('shows the server version, the license and where every notice is', async () => {
    systemInfo({ status: 200, body: { version: '0.1.0', environment: 'production', default_debezium_image: 'x', telemetry_enabled: false } });
    renderAt(() => <AboutDialog open onOpenChange={() => {}} />);
    expect(await screen.findByText('0.1.0', {}, { timeout: 5000 })).toBeInTheDocument();
    expect(screen.getByText('Apache License 2.0')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'GitHub' })).toHaveAttribute('href', 'https://github.com/daun-gatal/bezisync');
    expect(screen.getByRole('link', { name: 'Web app licenses' })).toHaveAttribute('href', '/licenses.txt');
    expect(screen.getByRole('link', { name: 'Server licenses' })).toHaveAttribute('href', '/api/v1/system/licenses');
  });

  it('says Unknown, never a made-up version, when the server does not answer', async () => {
    systemInfo({ status: 500, body: { code: 'internal', detail: 'Boom' } });
    renderAt(() => <AboutDialog open onOpenChange={() => {}} />);
    expect(await screen.findByText('Unknown', {}, { timeout: 5000 })).toBeInTheDocument();
  });
});
