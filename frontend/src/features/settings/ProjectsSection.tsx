import { useState, type FormEvent } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Button, ButtonLink } from '@/components/ui/button';
import { InlineError, Pill } from '@/components/ui/feedback';
import { Field, Input, Textarea } from '@/components/ui/form';
import { Card, QueryState } from '@/components/ui/layout';
import { Confirm, Modal } from '@/components/ui/overlays';
import { List, ListRow } from '@/components/ui/table';
import { Ident, Meta, plural, Title } from '@/components/ui/text';
import { client, unwrap, type Schemas } from '@/lib/api';
import { slugify } from '@/lib/format';
import { meKey, useProject } from '@/lib/session';
import { usersKey } from './PeopleSection';

type Project = Schemas['ProjectRead'];

/** Why a project address can't be used, in words (the server pattern is ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$). */
export function addressError(slug: string, taken: string[]): string | null {
  if (!slug) return 'Use at least one letter or digit.';
  if (slug.length > 63) return 'Keep it to 63 characters.';
  if (!/^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/.test(slug)) return 'Use lowercase letters, digits and dashes, starting and ending with a letter or digit.';
  if (taken.includes(slug)) return `/p/${slug} is already used by another project.`;
  return null;
}

export function ProjectsSection() {
  const { project: current } = useProject();
  const qc = useQueryClient();
  const projects = useQuery({ queryKey: ['projects'], queryFn: () => unwrap(client.GET('/api/v1/projects')) });
  const [name, setName] = useState('');
  const [address, setAddress] = useState<string | null>(null);
  const [editing, setEditing] = useState<Project | null>(null);
  const [deleting, setDeleting] = useState<Project | null>(null);
  const slug = address ?? slugify(name);
  const taken = (projects.data ?? []).map((p) => p.slug);
  const slugErr = name || address !== null ? addressError(slug, taken) : null;
  const refresh = () => {
    void qc.invalidateQueries({ queryKey: ['projects'] });
    void qc.invalidateQueries({ queryKey: meKey });
    void qc.invalidateQueries({ queryKey: usersKey });
  };
  const create = useMutation({
    mutationFn: () => unwrap(client.POST('/api/v1/projects', { body: { name: name.trim(), slug, description: '' } })),
    onSuccess: (p) => {
      toast.success(`Created ${p.name}`);
      setName('');
      setAddress(null);
      refresh();
    },
  });
  const remove = useMutation({
    mutationFn: (p: Project) => unwrap(client.DELETE('/api/v1/projects/{project_id}', { params: { path: { project_id: p.id } } })),
    onSuccess: (_, p) => {
      toast.success(`Deleted ${p.name}`);
      setDeleting(null);
      refresh();
    },
  });

  function submit(e: FormEvent) {
    e.preventDefault();
    if (name.trim() && !slugErr) create.mutate();
  }

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-col gap-1">
        <h2 className="m-0 text-xl font-semibold">Projects</h2>
        <p className="m-0 text-base text-muted">Each team works in its own project. Choose who can open one under People.</p>
      </div>
      <Card pad={false}>
        <QueryState query={projects} doing="Couldn’t load projects">
          {(list) => (
            <List label="Projects">
              {list.map((p) => (
                <ListRow key={p.id}>
                  <span className="flex min-w-0 flex-[1_1_260px] flex-col gap-0.5">
                    <Title as="strong" lines={2} className="text-base font-semibold">
                      {p.name}
                    </Title>
                    <Meta className="wrap-anywhere">
                      /p/{p.slug} · {plural(p.members ?? 0, 'member')} · {plural(p.pipelines ?? 0, 'pipeline')}
                      {p.description ? ` · ${p.description}` : ''}
                    </Meta>
                  </span>
                  <span className="flex flex-wrap items-center gap-2">
                    {p.is_sandbox ? <Pill tone="info">Sandbox</Pill> : null}
                    <ButtonLink
                      size="sm"
                      variant="ghost"
                      to="/p/$projectSlug/settings/$section"
                      params={{ projectSlug: current.slug, section: 'people' }}
                      search={{ in: p.id }}
                      aria-label={`People in ${p.name}`}
                    >
                      People
                    </ButtonLink>
                    <Button size="sm" onClick={() => setEditing(p)} aria-label={`Edit ${p.name}`}>
                      Edit
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={(p.pipelines ?? 0) > 0}
                      why={`Delete its ${plural(p.pipelines ?? 0, 'pipeline')} first, so their connectors and source objects are cleaned up.`}
                      onClick={() => {
                        remove.reset();
                        setDeleting(p);
                      }}
                      aria-label={`Delete ${p.name}`}
                    >
                      Delete
                    </Button>
                  </span>
                </ListRow>
              ))}
            </List>
          )}
        </QueryState>
      </Card>
      <Card title="New project">
        <form onSubmit={submit} className="flex min-w-0 flex-col gap-4">
          <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,280px),1fr))] gap-4">
            <Field label="Name">{(id) => <Input id={id} required maxLength={200} value={name} onChange={(e) => setName(e.target.value)} />}</Field>
            {address !== null ? (
              <Field label="Address" error={slugErr} hint="Used in URLs and YAML. It can’t change later.">
                {(id) => (
                  <Input
                    id={id}
                    className="font-mono"
                    spellCheck={false}
                    value={address}
                    invalid={Boolean(slugErr)}
                    onChange={(e) => setAddress(e.target.value)}
                  />
                )}
              </Field>
            ) : name ? (
              <div className="flex min-w-0 flex-col gap-1.5 self-end pb-1">
                <span className="text-sm wrap-anywhere text-muted">
                  Its address: <Ident>/p/{slug || '…'}</Ident>
                </span>
                {slugErr ? <span className="text-xs text-bad">{slugErr}</span> : null}
                <Button size="sm" variant="ghost" className="self-start" onClick={() => setAddress(slug)}>
                  Change the address
                </Button>
              </div>
            ) : null}
          </div>
          <InlineError error={create.error} doing="Couldn’t create the project" />
          <div>
            <Button
              type="submit"
              variant="primary"
              disabled={!name.trim() || Boolean(slugErr)}
              why={!name.trim() ? 'Name the project.' : slugErr}
              loading={create.isPending}
            >
              Create project
            </Button>
          </div>
        </form>
      </Card>
      {editing ? (
        <EditProjectDialog
          project={editing}
          onClose={() => {
            setEditing(null);
            refresh();
          }}
        />
      ) : null}
      <Confirm
        open={deleting !== null}
        onOpenChange={(o) => !o && setDeleting(null)}
        title={`Delete project ${deleting?.name ?? ''}?`}
        effects={['Its connections, runtimes, templates, policies, alerts, memberships and audit log are removed for good.']}
        confirmLabel="Delete project"
        typeToConfirm={deleting?.slug}
        pending={remove.isPending}
        error={remove.error}
        doing="Couldn’t delete the project"
        onConfirm={() => deleting && remove.mutate(deleting)}
      />
    </div>
  );
}

function EditProjectDialog({ project, onClose }: { project: Project; onClose: () => void }) {
  const [name, setName] = useState(project.name);
  const [description, setDescription] = useState(project.description);
  const save = useMutation({
    mutationFn: () => unwrap(client.PATCH('/api/v1/projects/{project_id}', { params: { path: { project_id: project.id } }, body: { name, description } })),
    onSuccess: (p) => {
      toast.success(`Saved ${p.name}`);
      onClose();
    },
  });
  const dirty = name !== project.name || description !== project.description;
  return (
    <Modal
      open
      onOpenChange={(o) => !o && onClose()}
      dirty={dirty}
      title={`Edit ${project.name}`}
      description={`Its address (/p/${project.slug}) stays the same: it is used in URLs and YAML.`}
      why={name.trim() ? null : 'Name the project.'}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" loading={save.isPending} disabled={!name.trim()} onClick={() => save.mutate()}>
            Save changes
          </Button>
        </>
      }
    >
      <Field label="Name">{(id) => <Input id={id} maxLength={200} value={name} onChange={(e) => setName(e.target.value)} />}</Field>
      <Field label="Description" optional>
        {(id) => <Textarea id={id} rows={3} value={description} onChange={(e) => setDescription(e.target.value)} />}
      </Field>
      <InlineError error={save.error} doing="Couldn’t save the project" />
    </Modal>
  );
}
