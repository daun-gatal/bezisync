import { useState } from 'react';
import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { cleanValues, defaultsOf, fieldsOf, isVisible, SchemaForm, type JsonSchema, type JsonSchemaProp } from '../SchemaForm';

const schema: JsonSchema = {
  required: ['host'],
  properties: {
    host: { type: 'string', title: 'Host', ui: { group: 'Connection', placeholder: 'db.internal' } },
    port: { type: 'integer', title: 'Port', default: 5432, ui: { group: 'Connection' } },
    ssl_mode: { enum: ['disable', 'require'], type: 'string', default: 'require', title: 'TLS', ui: { group: 'Security' } },
    sasl: { anyOf: [{ enum: ['PLAIN'], type: 'string' }, { type: 'null' }], default: null, title: 'SASL', ui: { group: 'Security', expert: true } },
    create_topics: { type: 'boolean', default: true, title: 'Create topics' },
  },
};

function Harness({ expert = false }: { expert?: boolean }) {
  const [values, setValues] = useState(defaultsOf(schema));
  return (
    <>
      <SchemaForm idPrefix="t" schema={schema} values={values} onChange={setValues} expert={expert} />
      <output data-testid="out">{JSON.stringify(cleanValues(schema, values))}</output>
    </>
  );
}

describe('SchemaForm', () => {
  it('derives field kinds including nullable enums', () => {
    const kinds = Object.fromEntries(fieldsOf(schema).map((f) => [f.name, [f.kind, f.nullable]]));
    expect(kinds).toEqual({ host: ['string', false], port: ['integer', false], ssl_mode: ['enum', false], sasl: ['enum', true], create_topics: ['boolean', false] });
  });

  it('hides expert fields in guided mode and shows them in expert mode', () => {
    const { unmount } = render(<Harness />);
    expect(screen.queryByLabelText(/SASL/)).toBeNull();
    unmount();
    render(<Harness expert />);
    expect(screen.getByLabelText(/SASL/)).toBeInTheDocument();
  });

  it('emits typed values and drops empty optionals', async () => {
    render(<Harness />);
    await userEvent.type(screen.getByLabelText('Host'), 'db');
    await userEvent.clear(screen.getByLabelText('Port'));
    await userEvent.type(screen.getByLabelText('Port'), '6543');
    expect(JSON.parse(screen.getByTestId('out').textContent ?? '{}')).toEqual({ host: 'db', port: 6543, ssl_mode: 'require', create_topics: true });
  });

  it('secret fields are write-only with a keep-current hint', () => {
    render(<SchemaForm idPrefix="s" schema={{ properties: { password: { type: 'string', title: 'Password' } } }} values={{}} onChange={() => undefined} expert={false} secret setSecrets={['password']} />);
    const input = screen.getByLabelText(/Password/);
    expect(input).toHaveAttribute('type', 'password');
    expect(input).toHaveValue('');
    expect(screen.getByText('Stored. Leave blank to keep the current value.')).toBeInTheDocument();
  });
});

describe('list fields', () => {
  const listSchema = { properties: { pulls: { type: 'array', title: 'Image pull secrets' } } } as JsonSchema;

  it('are edited as chips and sent as arrays (older comma-separated values still parse)', () => {
    expect(fieldsOf(listSchema)[0]?.kind).toBe('list');
    expect(cleanValues(listSchema, { pulls: ' ghcr-pull, ,other ' })).toEqual({ pulls: ['ghcr-pull', 'other'] });
    expect(cleanValues(listSchema, { pulls: ['a', ''] })).toEqual({ pulls: ['a'] });
    expect(cleanValues(listSchema, { pulls: '' })).toEqual({});
  });
});

describe('conditional fields', () => {
  const auth: JsonSchema = {
    properties: {
      auth: { enum: ['none', 'token'], type: 'string', default: 'none', title: 'Sign in', ui: { labels: { none: 'No sign-in', token: 'Bearer token' } } },
      token_header: { type: 'string', title: 'Header', ui: { when: { auth: ['token'] } } },
    },
  };
  it('shows a field only while its condition holds', () => {
    expect(isVisible(auth.properties!.token_header!, {}, auth)).toBe(false);
    expect(isVisible(auth.properties!.token_header!, { auth: 'token' }, auth)).toBe(true);
  });
  it('drops hidden fields when saving', () => {
    expect(cleanValues(auth, { auth: 'none', token_header: 'X-Token' })).toEqual({ auth: 'none' });
    expect(cleanValues(auth, { auth: 'token', token_header: 'X-Token' })).toEqual({ auth: 'token', token_header: 'X-Token' });
  });
  it('labels enum values', async () => {
    render(<SchemaForm idPrefix="t" schema={auth} values={{ auth: 'token' }} onChange={() => {}} expert={false} />);
    expect(screen.getByText('Bearer token')).toBeTruthy();
    expect(screen.getByText('Header')).toBeTruthy();
  });
});

describe('SchemaForm linked connections', () => {
  const linked: JsonSchema = {
    required: ['coordination_kafka'],
    properties: { coordination_kafka: { type: 'string', title: 'Coordination Kafka', ui: { group: 'Coordination', connection: 'sink/kafka' } } },
  };
  function Linked({ links }: { links: Record<string, { value: string; label: string }[]> }) {
    const [values, setValues] = useState({});
    return (
      <>
        <SchemaForm idPrefix="l" schema={linked} values={values} onChange={setValues} expert={false} links={links} />
        <output data-testid="out">{JSON.stringify(cleanValues(linked, values))}</output>
      </>
    );
  }
  it('offers the connections of that kind and type, and stores the chosen id', async () => {
    render(<Linked links={{ 'sink/kafka': [{ value: 'k-1', label: 'events-kafka' }, { value: 'k-2', label: 'coord-kafka' }], 'sink/http': [{ value: 'h', label: 'echo' }] }} />);
    await userEvent.click(screen.getByLabelText('Coordination Kafka'));
    expect(screen.getByRole('option', { name: 'events-kafka' })).toBeInTheDocument();
    expect(screen.queryByRole('option', { name: 'echo' })).toBeNull();
    await userEvent.click(screen.getByRole('option', { name: 'coord-kafka' }));
    expect(screen.getByTestId('out').textContent).toBe('{"coordination_kafka":"k-2"}');
  });
  it('says when the project has none yet', () => {
    render(<Linked links={{}} />);
    expect(screen.getByLabelText('Coordination Kafka')).toHaveTextContent('None in this project yet');
  });
});

describe('a secret form that depends on its config form', () => {
  const config: JsonSchema = {
    properties: { in_cluster: { type: 'boolean', default: true, title: 'Cluster', ui: { labels: { true: 'This cluster', false: 'Another cluster' } } } },
  };
  const kubeconfig: JsonSchemaProp = { anyOf: [{ type: 'string' }, { type: 'null' }], default: null, title: 'Kubeconfig', ui: { multiline: true, when: { in_cluster: [false] } } };
  const secrets: JsonSchema = { properties: { kubeconfig } };
  function Store() {
    const [cfg, setCfg] = useState(defaultsOf(config));
    const [sec, setSec] = useState({});
    return (
      <>
        <SchemaForm idPrefix="c" schema={config} values={cfg} onChange={setCfg} expert={false} />
        <SchemaForm idPrefix="s" schema={secrets} values={sec} onChange={setSec} expert={false} secret context={cfg} />
        <output data-testid="out">{JSON.stringify([cleanValues(config, cfg), cleanValues(secrets, sec, cfg)])}</output>
      </>
    );
  }

  it('shows the kubeconfig only for another cluster, and loads it from a file', async () => {
    render(<Store />);
    expect(screen.getByRole('button', { name: 'This cluster' })).toHaveAttribute('aria-pressed', 'true');
    expect(screen.queryByLabelText(/Kubeconfig/)).toBeNull();
    await userEvent.click(screen.getByRole('button', { name: 'Another cluster' }));
    expect(screen.getByLabelText(/Kubeconfig/)).toBeInTheDocument();
    const file = new File(['apiVersion: v1\n'], 'config', { type: 'text/plain' });
    await userEvent.upload(screen.getByLabelText('Load kubeconfig from a file'), file);
    expect(await screen.findByDisplayValue('apiVersion: v1')).toBeInTheDocument();
    expect(screen.getByTestId('out').textContent).toBe('[{"in_cluster":false},{"kubeconfig":"apiVersion: v1\\n"}]');
    // Back to this cluster: the kubeconfig no longer applies and isn't sent.
    await userEvent.click(screen.getByRole('button', { name: 'This cluster' }));
    expect(screen.getByTestId('out').textContent).toBe('[{"in_cluster":true},{}]');
  });

  it('reads a field the values lack from the context', () => {
    expect(isVisible(kubeconfig, {}, secrets, { in_cluster: false })).toBe(true);
    expect(isVisible(kubeconfig, {}, secrets, { in_cluster: true })).toBe(false);
  });
});
