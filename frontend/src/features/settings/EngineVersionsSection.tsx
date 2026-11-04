import { useState, type FormEvent } from 'react';
import { Link, useNavigate, useSearch } from '@tanstack/react-router';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { ExternalLink } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { InlineError, Pill } from '@/components/ui/feedback';
import { Check, Field, Input, Textarea } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { Confirm } from '@/components/ui/overlays';
import { List, ListRow } from '@/components/ui/table';
import { Tabs } from '@/components/ui/tabs';
import { Ident, Meta, plural } from '@/components/ui/text';
import { client, unwrap, type Schemas } from '@/lib/api';
import { catalogKey } from '@/lib/debezium';
import { ENGINE_TITLE, engineCatalogKey, enginesKey, useEngineCatalog, useEngines, type EngineId } from '@/lib/engines';

type Entry = Schemas['CatalogEntryRead'];

/** The images each engine runs, its default, and where each one runs. Flink CDC is off until an
 *  image is registered here. */
export function EngineVersionsSection() {
  const search = useSearch({ from: '/p/$projectSlug/settings/$section' });
  const navigate = useNavigate({ from: '/p/$projectSlug/settings/$section' });
  const engine: EngineId = search.engine === 'flink-cdc' ? 'flink-cdc' : 'debezium';
  const engines = useEngines();
  const flink = engines.data?.find((e) => e.id === 'flink-cdc');
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-col gap-1">
        <h2 className="m-0 text-xl font-semibold">Engine versions</h2>
        <p className="m-0 text-base text-muted">
          The images pipelines run, per engine. Debezium runs every pipeline unless Flink CDC fits better; Flink CDC is optional and turns on once you register
          an image for it.
        </p>
      </div>
      <Tabs
        idBase="engine-versions"
        label="Engine"
        value={engine}
        onChange={(v) => void navigate({ search: (s) => ({ ...s, engine: v === 'flink-cdc' ? 'flink-cdc' : undefined }), replace: true })}
        tabs={[
          { id: 'debezium', label: 'Debezium' },
          { id: 'flink-cdc', label: flink && !flink.available ? 'Flink CDC (off)' : 'Flink CDC' },
        ]}
      />
      {engine === 'flink-cdc' && flink && !flink.available ? (
        <Card title="Flink CDC is off">
          <p className="m-0 max-w-3xl text-base text-muted">
            BeziSync doesn’t publish a Flink CDC image: build one from <code className="font-mono text-sm">deploy/images/flink-cdc/Dockerfile</code> (add{' '}
            <code className="font-mono text-sm">--build-arg MYSQL_DRIVER=8.4.0</code> for MySQL, MariaDB or StarRocks), push it to a registry your runtimes
            pull from, and add it below. Pipelines can then choose Flink CDC where it fits: StarRocks, Doris, Fluss, and Iceberg from MySQL, MariaDB,
            PostgreSQL or SQL Server.
          </p>
        </Card>
      ) : null}
      <CatalogPanel key={engine} engine={engine} />
    </div>
  );
}

function CatalogPanel({ engine }: { engine: EngineId }) {
  const qc = useQueryClient();
  const catalog = useEngineCatalog(engine);
  const title = ENGINE_TITLE[engine];
  const [image, setImage] = useState('');
  const [label, setLabel] = useState('');
  const [notes, setNotes] = useState('');
  const [makeDefault, setMakeDefault] = useState(false);
  const [removing, setRemoving] = useState<Entry | null>(null);
  /** A default change waiting for confirmation: the entry to make default, or null for the install default. */
  const [defaulting, setDefaulting] = useState<{ entry: Entry; on: boolean } | null>(null);
  const done = (data: Schemas['CatalogRead']) => {
    qc.setQueryData(engineCatalogKey(engine), data);
    if (engine === 'debezium') qc.setQueryData(catalogKey, data);
    void qc.invalidateQueries({ queryKey: enginesKey });
    void qc.invalidateQueries({ queryKey: ['connector-types'] });
    void qc.invalidateQueries({ queryKey: ['debezium-images'] });
    void qc.invalidateQueries({ queryKey: ['runtimes'] });
  };
  const add = useMutation({
    mutationFn: () => unwrap(client.POST('/api/v1/debezium-catalog', { body: { image: image.trim(), label, notes, is_default: makeDefault, engine } })),
    onSuccess: (data) => {
      toast.success(`Added ${image.trim()}`);
      setImage('');
      setLabel('');
      setNotes('');
      setMakeDefault(false);
      done(data);
    },
  });
  const update = useMutation({
    mutationFn: ({ id, body }: { id: string; body: Schemas['CatalogUpdate'] }) =>
      unwrap(client.PATCH('/api/v1/debezium-catalog/{image_id}', { params: { path: { image_id: id } }, body })),
    onSuccess: (data) => {
      setDefaulting(null);
      done(data);
    },
  });
  const remove = useMutation({
    mutationFn: (id: string) => unwrap(client.DELETE('/api/v1/debezium-catalog/{image_id}', { params: { path: { image_id: id } } })),
    onSuccess: (data) => {
      setRemoving(null);
      done(data);
    },
  });

  function submit(e: FormEvent) {
    e.preventDefault();
    add.mutate();
  }

  const current = catalog.data?.entries.find((e) => e.is_default);
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <p className="m-0 max-w-3xl text-base text-muted">
        {engine === 'debezium'
          ? 'The Debezium Server images every project picks from. The default runs pipelines that choose no version (and whose runtime sets none) from their next deploy: changing it never restarts running pipelines. Editors upgrade pipelines from the Pipelines list (select, then Change Debezium version) or from a pipeline’s page.'
          : 'The Flink CDC images pipelines on Flink CDC run. The default runs pipelines that pin none from their next deploy; running pipelines are not restarted.'}
      </p>
      <InlineError error={update.error && !defaulting ? update.error : null} doing="Couldn’t change the image" />
      <Card pad={false}>
        <QueryState query={catalog} doing="Couldn’t load the catalog">
          {(data) => (
            <List label={`${title} images`}>
              {data.entries.map((e) => (
                <ListRow key={e.image} tone={e.deprecated ? 'warn' : null}>
                  <span className="flex min-w-0 flex-[1_1_320px] flex-col gap-1">
                    <Ident className="font-medium">{e.image}</Ident>
                    <Meta>
                      {e.label || (e.official ? `Official ${title === 'Debezium' ? 'Debezium Server' : title}` : 'Custom image')}
                      {e.notes ? ` · ${e.notes}` : ''}
                      {e.pipelines !== null && e.pipelines !== undefined ? ` · ${plural(e.pipelines, 'live pipeline')}` : ''}
                    </Meta>
                    {e.projects?.length ? (
                      <span className="flex flex-wrap items-center gap-x-3 text-xs text-muted">
                        Runs in
                        {e.projects.map((u) => (
                          <Link
                            key={u.project_id}
                            to="/p/$projectSlug/pipelines"
                            params={{ projectSlug: u.slug }}
                            search={{ debezium: e.image }}
                            className="inline-flex min-h-6 items-center text-accent underline"
                          >
                            {u.name} ({u.pipelines})
                          </Link>
                        ))}
                      </span>
                    ) : null}
                    {e.release_notes_url ? (
                      <a
                        href={e.release_notes_url}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex min-h-6 items-center gap-1 self-start text-xs text-accent hover:underline"
                      >
                        Release notes <ExternalLink className="size-3" aria-hidden />
                      </a>
                    ) : null}
                  </span>
                  <span className="flex flex-wrap items-center gap-2">
                    {e.is_default ? <Pill tone="ok">Default</Pill> : null}
                    {e.deprecated ? <Pill tone="warn">Deprecated</Pill> : null}
                    {e.builtin ? <Pill tone="grey">Install default</Pill> : null}
                    {e.id && !e.is_default ? (
                      <Button
                        size="sm"
                        disabled={e.deprecated}
                        why="Undeprecate it first."
                        onClick={() => setDefaulting({ entry: e, on: true })}
                        aria-label={`Make default ${e.image}`}
                      >
                        Make default
                      </Button>
                    ) : null}
                    {e.id && e.is_default ? (
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => setDefaulting({ entry: e, on: false })}
                        aria-label={`Use install default instead of ${e.image}`}
                      >
                        Use install default
                      </Button>
                    ) : null}
                    {e.id && !e.is_default ? (
                      <Button
                        size="sm"
                        variant="ghost"
                        aria-label={`${e.deprecated ? 'Undeprecate' : 'Deprecate'} ${e.image}`}
                        onClick={() => update.mutate({ id: e.id!, body: { deprecated: !e.deprecated } })}
                      >
                        {e.deprecated ? 'Undeprecate' : 'Deprecate'}
                      </Button>
                    ) : null}
                    {e.id && !e.is_default ? (
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => {
                          remove.reset();
                          setRemoving(e);
                        }}
                        aria-label={`Remove ${e.image}`}
                      >
                        Remove
                      </Button>
                    ) : null}
                  </span>
                </ListRow>
              ))}
            </List>
          )}
        </QueryState>
      </Card>
      <Card title="Add an image">
        <form onSubmit={submit} className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,280px),1fr))] gap-4">
          <Field
            label="Image"
            hint={
              engine === 'debezium'
                ? 'repository:tag, e.g. quay.io/debezium/server:3.8.0.Final or your own build with extra drivers.'
                : 'repository:tag of your Flink CDC build, e.g. registry.example.com/bezisync-flink-cdc:3.7.0-2.2.1.'
            }
            className="col-span-full"
          >
            {(id) => <Input id={id} required className="font-mono" spellCheck={false} value={image} onChange={(e) => setImage(e.target.value)} />}
          </Field>
          <Field label="Label" optional>
            {(id) => <Input id={id} value={label} placeholder="3.8 (tested)" onChange={(e) => setLabel(e.target.value)} />}
          </Field>
          <div className="self-end pb-2">
            <Check checked={makeDefault} onChange={setMakeDefault} label="Make it the default" />
          </div>
          <Field label="Notes" optional className="col-span-full">
            {(id) => <Textarea id={id} rows={2} value={notes} onChange={(e) => setNotes(e.target.value)} />}
          </Field>
          <InlineError error={add.error} doing="Couldn’t add the image" className="col-span-full" />
          <div className="col-span-full">
            <Button type="submit" variant="primary" disabled={!image.trim()} why="Enter the image reference." loading={add.isPending}>
              Add image
            </Button>
          </div>
        </form>
      </Card>
      <Confirm
        open={defaulting !== null}
        onOpenChange={(o) => !o && setDefaulting(null)}
        tone="primary"
        title={defaulting?.on ? `Make ${defaulting.entry.image} the default?` : 'Go back to the install default?'}
        effects={[
          engine === 'debezium'
            ? 'Pipelines that choose no version, on runtimes that set none, use it from their next deploy.'
            : 'Flink CDC pipelines that pin no image use it from their next deploy.',
          'Running pipelines are not restarted.',
          current?.pipelines
            ? `Right now ${plural(current.pipelines, 'live pipeline')} run the current default.`
            : 'No live pipeline runs the current default.',
        ]}
        confirmLabel={defaulting?.on ? 'Make default' : 'Use install default'}
        pending={update.isPending}
        error={update.error}
        doing="Couldn’t change the default"
        onConfirm={() => defaulting?.entry.id && update.mutate({ id: defaulting.entry.id, body: { is_default: defaulting.on } })}
      />
      <Confirm
        open={removing !== null}
        onOpenChange={(o) => !o && setRemoving(null)}
        title={`Remove ${removing?.image ?? ''} from the catalog?`}
        effects={[
          'People can no longer pick it.',
          removing?.pipelines
            ? `Its ${plural(removing.pipelines, 'live pipeline')} keep running and are flagged as on an unlisted image.`
            : 'No live pipeline runs it.',
        ]}
        confirmLabel="Remove image"
        pending={remove.isPending}
        error={remove.error}
        doing="Couldn’t remove the image"
        onConfirm={() => removing?.id && remove.mutate(removing.id)}
      />
    </div>
  );
}
