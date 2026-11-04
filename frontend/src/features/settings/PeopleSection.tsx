import { useEffect, useState, type FormEvent } from 'react';
import { useNavigate, useSearch } from '@tanstack/react-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { Users } from 'lucide-react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Empty, InlineError, Notice, Pill } from '@/components/ui/feedback';
import { Check, Field, Input, RadioCards, Search, Switch } from '@/components/ui/form';
import { Card, QueryState, Toolbar } from '@/components/ui/layout';
import { Confirm, Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { List, ListRow } from '@/components/ui/table';
import { Meta, Title } from '@/components/ui/text';
import { client, unwrap, type Schemas } from '@/lib/api';
import { ago } from '@/lib/format';
import { meKey, useProject } from '@/lib/session';
import { PagedBox } from '@/components/ui/pager';

type User = Schemas['UserAdminRead'];
type Role = Schemas['Role'];
type Project = Schemas['ProjectRead'];

export const ROLES: { value: Role; label: string; help: string }[] = [
  { value: 'viewer', label: 'Viewer', help: 'Sees pipelines, health and history.' },
  { value: 'operator', label: 'Operator', help: 'Viewer + pause/resume/restart, snapshots, acknowledge alerts, preview data.' },
  { value: 'editor', label: 'Editor', help: 'Operator + create, deploy and upgrade pipelines and connections.' },
];

export const usersKey = ['admin-users'] as const;

function useProjects() {
  return useQuery({ queryKey: ['projects'], queryFn: () => unwrap(client.GET('/api/v1/projects')) });
}

/** What an Admin can do: listed whenever Admin is granted or taken away. */
const ADMIN_POWERS = [
  'Opens every project with every permission, including deploying and approving.',
  'Opens Settings: people, projects, runtimes, secret stores, engine versions, policies and the audit log.',
  'Can make other people Admins.',
];

const roleOptions = ROLES.map((r) => ({ value: r.value, label: r.label, description: r.help }));

/** Saves access changes for one person; optimistic so selects and checkboxes respond at once. */
function useAccess(user: User) {
  const { me } = useProject();
  const qc = useQueryClient();
  const patchUser = (change: (u: User) => User) =>
    qc.setQueriesData<{ items: User[]; next_cursor?: string | null }>({ queryKey: usersKey }, (page) =>
      page ? { ...page, items: page.items.map((u) => (u.id === user.id ? change(u) : u)) } : page,
    );
  const done = () => {
    void qc.invalidateQueries({ queryKey: usersKey });
    void qc.invalidateQueries({ queryKey: ['projects'] });
    if (user.id === me.user.id) void qc.invalidateQueries({ queryKey: meKey });
  };
  const update = useMutation({
    mutationFn: (body: Schemas['UserAdminUpdate']) => unwrap(client.PATCH('/api/v1/users/{user_id}', { params: { path: { user_id: user.id } }, body })),
    onMutate: (body) => patchUser((u) => ({ ...u, is_superuser: body.is_superuser ?? u.is_superuser, is_active: body.is_active ?? u.is_active })),
    onSettled: done,
  });
  const grant = useMutation({
    mutationFn: ({ project, body }: { project: string; body: Schemas['UserAccessUpsert'] }) =>
      unwrap(client.PUT('/api/v1/users/{user_id}/projects/{project_id}', { params: { path: { user_id: user.id, project_id: project } }, body })),
    onMutate: ({ project, body }) =>
      patchUser((u) => ({
        ...u,
        projects: u.projects.map((p) =>
          p.project_id === project ? { ...p, role: body.role, can_approve: body.can_approve ?? false, granted_by_sso: false } : p,
        ),
      })),
    onSettled: done,
  });
  const revoke = useMutation({
    mutationFn: (project: string) =>
      unwrap(client.DELETE('/api/v1/users/{user_id}/projects/{project_id}', { params: { path: { user_id: user.id, project_id: project } } })),
    onSettled: done,
  });
  /** Change a role, then offer to put it back. */
  const changeRole = (project: { project_id: string; name: string; role: Role; can_approve: boolean }, role: Role) => {
    const before = project.role;
    if (before === role) return;
    grant.mutate(
      { project: project.project_id, body: { role, can_approve: project.can_approve } },
      {
        onSuccess: () =>
          toast.success(`${user.email} is now ${role} in ${project.name}`, {
            action: { label: 'Undo', onClick: () => grant.mutate({ project: project.project_id, body: { role: before, can_approve: project.can_approve } }) },
          }),
      },
    );
  };
  const error = update.error ?? grant.error ?? revoke.error;
  return { update, grant, revoke, changeRole, error };
}

/** Everyone with an account. Filter by project to see and change who can open it. */
export function PeopleSection() {
  const { me } = useProject();
  const { q = '', in: inProject, page } = useSearch({ from: '/p/$projectSlug/settings/$section' });
  const navigate = useNavigate({ from: '/p/$projectSlug/settings/$section' });
  const projects = useProjects();
  const scoped = projects.data?.find((p) => p.id === inProject) ?? null;
  const users = useQuery({
    queryKey: [...usersKey, q, inProject ?? ''],
    // Every match, page by page from the server (200 at most per request), then paged here with a
    // count: an organisation's people are hundreds or a few thousand, never millions.
    queryFn: async () => {
      const items: User[] = [];
      let cursor: string | null = null;
      do {
        const got: { items: User[]; next_cursor?: string | null } = await unwrap(
          client.GET('/api/v1/users', { params: { query: { q: q || undefined, project_id: inProject, limit: 200, cursor } } }),
        );
        items.push(...got.items);
        cursor = got.next_cursor ?? null;
      } while (cursor && items.length < 10_000);
      return { items, next_cursor: null };
    },
  });
  const [adding, setAdding] = useState(false);
  const [managing, setManaging] = useState<string | null>(null);
  const managed = users.data?.items.find((u) => u.id === managing) ?? null;

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div className="flex min-w-0 flex-[1_1_320px] flex-col gap-1">
          <h2 className="m-0 text-xl font-semibold">People</h2>
          <p className="m-0 text-base text-muted">
            Admins open every project and Settings. Everyone else gets a role per project: Viewer, Operator or Editor, optionally with the approver right.
          </p>
        </div>
        <Button variant="primary" onClick={() => setAdding(true)}>
          Add person
        </Button>
      </div>
      <Toolbar>
        <Search
          label="Search people"
          placeholder="Search by email or name"
          value={q}
          onChange={(v) => void navigate({ search: (s) => ({ ...s, q: v || undefined, page: undefined }), replace: true })}
        />
        <div className="min-w-0 flex-[0_1_280px]">
          <label htmlFor="people-in" className="sr-only">
            Who can open
          </label>
          <Select
            id="people-in"
            value={inProject ?? ''}
            placeholder="Everyone"
            onChange={(v) => void navigate({ search: (s) => ({ ...s, in: v || undefined, page: undefined }), replace: true })}
            options={[{ value: '', label: 'Everyone' }, ...(projects.data ?? []).map((p) => ({ value: p.id, label: `Can open ${p.name}` }))]}
          />
        </div>
      </Toolbar>
      <Card pad={false}>
        <QueryState query={users} doing="Couldn’t load people">
          {(data) =>
            data.items.length === 0 ? (
              <Empty icon={<Users />} title="Nobody matches">
                Try another search or project.
              </Empty>
            ) : (
              <PagedBox
                rows={data.items}
                label="People"
                noun="people"
                kind="page"
                page={page}
                onPage={(p) => void navigate({ search: (s) => ({ ...s, page: p > 1 ? p : undefined }) })}
              >
                {(shown) => (
                  <List label="People">
                    {shown.map((u) => (
                      <PersonRow key={u.id} user={u} self={u.id === me.user.id} project={scoped} onManage={() => setManaging(u.id)} />
                    ))}
                  </List>
                )}
              </PagedBox>
            )
          }
        </QueryState>
      </Card>
      <AddPersonDialog open={adding} onOpenChange={setAdding} projects={projects.data ?? []} initialProject={inProject} onCreated={() => setAdding(false)} />
      {managed ? <ManagePersonDialog user={managed} projects={projects.data ?? []} onClose={() => setManaging(null)} /> : null}
    </div>
  );
}

function PersonRow({ user: u, self, project, onManage }: { user: User; self: boolean; project: Project | null; onManage: () => void }) {
  const { grant, changeRole, error } = useAccess(u);
  const access = project ? u.projects.find((p) => p.project_id === project.id) : undefined;
  return (
    <ListRow tone={!u.is_active ? 'grey' : null}>
      <span className="flex min-w-0 flex-[1_1_260px] flex-col gap-0.5">
        <Title as="strong" lines={1} className="text-base font-semibold">
          {`${u.email}${self ? ' (you)' : ''}`}
        </Title>
        <Meta className="line-clamp-2 wrap-anywhere">
          {u.name ? `${u.name} · ` : ''}
          {u.last_login_at ? `signed in ${ago(u.last_login_at)}` : 'never signed in'}
          {!project && !u.is_superuser
            ? ` · ${u.projects.length === 0 ? 'no projects' : u.projects.map((p) => `${p.name} (${p.role}${p.can_approve ? ', approver' : ''})`).join(', ')}`
            : ''}
        </Meta>
      </span>
      <span className="flex flex-wrap items-center gap-2">
        {u.is_superuser ? <Pill tone="info">Admin</Pill> : null}
        {u.sso ? <Pill tone="grey">SSO</Pill> : !u.has_password ? <Pill tone="grey">Awaiting SSO sign-in</Pill> : null}
        {!u.is_active ? <Pill tone="bad">Deactivated</Pill> : null}
        {project && access && !u.is_superuser ? (
          <>
            {access.granted_by_sso ? <Pill tone="grey">Managed by SSO</Pill> : null}
            <ApproverToggle
              checked={access.can_approve}
              label={u.email}
              onChange={(checked) => grant.mutate({ project: project.id, body: { role: access.role, can_approve: checked } })}
            />
            <div className="w-36">
              <Select label={`Role for ${u.email}`} value={access.role} onChange={(v) => changeRole(access, v as Role)} options={roleOptions} />
            </div>
          </>
        ) : null}
        <Button size="sm" onClick={onManage} aria-label={`Manage ${u.email}`}>
          Manage
        </Button>
      </span>
      {error ? <InlineError error={error} doing={`Couldn’t change ${u.email}’s access`} className="w-full" /> : null}
    </ListRow>
  );
}

function AddPersonDialog({
  open,
  onOpenChange,
  projects,
  initialProject,
  onCreated,
}: {
  open: boolean;
  onOpenChange: (o: boolean) => void;
  projects: Project[];
  initialProject: string | undefined;
  onCreated: () => void;
}) {
  const { project: current } = useProject();
  const qc = useQueryClient();
  const [email, setEmail] = useState('');
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [access, setAccess] = useState<'project' | 'admin'>('project');
  const [projectId, setProjectId] = useState('');
  const [role, setRole] = useState<Role>('editor');
  const [approver, setApprover] = useState(false);
  const admin = access === 'admin';
  useEffect(() => {
    if (!open) return;
    setEmail('');
    setName('');
    setPassword('');
    setAccess('project');
    setProjectId(initialProject ?? current.id);
    setRole('editor');
    setApprover(false);
    create.reset();
  }, [open, initialProject, current.id]); // eslint-disable-line react-hooks/exhaustive-deps

  const create = useMutation({
    mutationFn: async () => {
      const user = await unwrap(client.POST('/api/v1/users', { body: { email, name, initial_password: password || null, is_superuser: admin } }));
      if (!admin && projectId) {
        await unwrap(
          client.PUT('/api/v1/users/{user_id}/projects/{project_id}', {
            params: { path: { user_id: user.id, project_id: projectId } },
            body: { role, can_approve: approver },
          }),
        );
      }
      return user;
    },
    onSuccess: (u) => {
      const where = projects.find((p) => p.id === projectId)?.name;
      toast.success(admin ? `Added ${u.email} as Admin` : `Added ${u.email}${where ? ` to ${where} as ${role}` : ''}`);
      onCreated();
    },
    onSettled: () => {
      void qc.invalidateQueries({ queryKey: usersKey });
      void qc.invalidateQueries({ queryKey: ['projects'] });
    },
  });
  const tooShort = password.length > 0 && password.length < 12;
  const missing = !email.trim() ? 'Enter their email.' : tooShort ? 'Passwords have at least 12 characters.' : null;
  function submit(e: FormEvent) {
    e.preventDefault();
    if (!missing) create.mutate();
  }
  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      dirty={Boolean(email || name || password)}
      title="Add a person"
      why={missing}
      footer={
        <>
          <Button onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button variant="primary" type="submit" form="add-person" loading={create.isPending} disabled={Boolean(missing)}>
            Add person
          </Button>
        </>
      }
    >
      <form id="add-person" onSubmit={submit} className="flex flex-col gap-4">
        <Field label="Email">{(id) => <Input id={id} type="email" required value={email} onChange={(e) => setEmail(e.target.value)} />}</Field>
        <Field label="Name" optional>
          {(id) => <Input id={id} value={name} onChange={(e) => setName(e.target.value)} />}
        </Field>
        <Field
          label="Initial password"
          optional
          error={tooShort ? 'At least 12 characters.' : null}
          hint="Leave it empty for single sign-on users: the account links to their SSO identity at their first sign-in."
        >
          {(id) => (
            <Input id={id} type="password" autoComplete="new-password" value={password} invalid={tooShort} onChange={(e) => setPassword(e.target.value)} />
          )}
        </Field>
        <Field label="Access" group>
          {() => (
            <RadioCards
              label="Access"
              value={access}
              onChange={setAccess}
              columns={2}
              options={[
                { value: 'project', label: 'A role in one project', description: 'Add more projects afterwards under Manage.' },
                { value: 'admin', label: 'Admin', description: 'Every project, Settings, people and engine versions.' },
              ]}
            />
          )}
        </Field>
        {!admin ? (
          <div className="grid grid-cols-[repeat(auto-fill,minmax(min(100%,200px),1fr))] gap-4">
            <Field label="Project">
              {(id) => <Select id={id} value={projectId} onChange={setProjectId} options={projects.map((p) => ({ value: p.id, label: p.name }))} />}
            </Field>
            <Field label="Role">{(id) => <Select id={id} value={role} onChange={(v) => setRole(v as Role)} options={roleOptions} />}</Field>
            <div className="col-span-full">
              <Check checked={approver} onChange={setApprover} label="Can approve deployments" />
            </div>
          </div>
        ) : (
          <Notice tone="warn" title="An Admin can do everything">
            <ul className="m-0 list-disc pl-5">
              {ADMIN_POWERS.map((x) => (
                <li key={x}>{x}</li>
              ))}
            </ul>
          </Notice>
        )}
        <InlineError error={create.error} doing="Couldn’t add the person" />
      </form>
    </Modal>
  );
}

function ManagePersonDialog({ user, projects, onClose }: { user: User; projects: Project[]; onClose: () => void }) {
  const { me } = useProject();
  const { update, grant, revoke, changeRole, error } = useAccess(user);
  const [projectId, setProjectId] = useState('');
  const [role, setRole] = useState<Role>('editor');
  const [approver, setApprover] = useState(false);
  const [deactivating, setDeactivating] = useState(false);
  const [adminChange, setAdminChange] = useState<boolean | null>(null);
  const [removing, setRemoving] = useState<{ project_id: string; name: string } | null>(null);
  const self = user.id === me.user.id;
  const available = projects.filter((p) => !user.projects.some((a) => a.project_id === p.id));

  return (
    <Modal open onOpenChange={(o) => !o && onClose()} size="lg" title={user.email} description={user.name || undefined}>
      <Switch
        checked={user.is_superuser}
        disabled={self || update.isPending}
        why="You can’t remove your own Admin role."
        onChange={(on) => setAdminChange(on)}
        label="Admin"
        description="Every project, Settings, people and engine versions."
      />
      <InlineError error={error} doing="Couldn’t save the change" />
      {user.is_superuser ? (
        <Meta as="p">Admins open every project with every permission, so they need no project roles.</Meta>
      ) : (
        <section className="flex min-w-0 flex-col gap-2">
          <h3 className="m-0 text-base font-semibold">Projects</h3>
          {user.projects.length === 0 ? (
            <Meta as="p">No projects yet: they can’t open anything.</Meta>
          ) : (
            <div className="rounded-lg border border-line">
              <List label="Projects">
                {user.projects.map((p) => (
                  <ListRow key={p.project_id}>
                    <span className="min-w-0 flex-[1_1_160px] text-base font-medium wrap-anywhere">{p.name}</span>
                    {p.granted_by_sso ? <Pill tone="grey">Managed by SSO</Pill> : null}
                    <ApproverToggle
                      checked={p.can_approve}
                      label={p.name}
                      onChange={(checked) => grant.mutate({ project: p.project_id, body: { role: p.role, can_approve: checked } })}
                    />
                    <div className="w-36">
                      <Select label={`Role in ${p.name}`} value={p.role} onChange={(v) => changeRole(p, v as Role)} options={roleOptions} />
                    </div>
                    <Button size="sm" variant="ghost" onClick={() => setRemoving(p)} aria-label={`Remove from ${p.name}`}>
                      Remove
                    </Button>
                  </ListRow>
                ))}
              </List>
            </div>
          )}
          {available.length > 0 ? (
            <div className="flex flex-wrap items-end gap-3">
              <Field label="Add to project" className="min-w-0 flex-[1_1_200px]">
                {(id) => <Select id={id} value={projectId} onChange={setProjectId} options={available.map((p) => ({ value: p.id, label: p.name }))} />}
              </Field>
              <Field label="Role" className="w-36">
                {(id) => <Select id={id} value={role} onChange={(v) => setRole(v as Role)} options={roleOptions} />}
              </Field>
              <div className="pb-2">
                <Check checked={approver} onChange={setApprover} label="Approver" />
              </div>
              <Button
                variant="primary"
                disabled={!projectId}
                why="Choose a project."
                loading={grant.isPending}
                onClick={() => grant.mutate({ project: projectId, body: { role, can_approve: approver } }, { onSuccess: () => setProjectId('') })}
              >
                Add
              </Button>
            </div>
          ) : null}
          {user.projects.some((p) => p.granted_by_sso) ? (
            <Notice tone="info">Access managed by SSO follows the identity provider’s groups at each sign-in. Changing it here takes it off group sync.</Notice>
          ) : null}
        </section>
      )}

      {!self ? (
        <section className="flex flex-wrap items-center justify-between gap-3 border-t border-line pt-4">
          <span className="min-w-0 flex-[1_1_240px] text-base text-ink-2">
            {user.is_active ? 'Deactivating signs them out everywhere and stops their API tokens.' : 'This account is deactivated.'}
          </span>
          {user.is_active ? (
            <Button variant="danger" onClick={() => setDeactivating(true)}>
              Deactivate
            </Button>
          ) : (
            <Button loading={update.isPending} onClick={() => update.mutate({ is_active: true })}>
              Reactivate
            </Button>
          )}
        </section>
      ) : null}
      <Confirm
        open={deactivating}
        onOpenChange={setDeactivating}
        title={`Deactivate ${user.email}?`}
        effects={[
          'They are signed out everywhere and can’t sign in, with a password or SSO.',
          'Their API tokens stop working.',
          'Their project roles stay, so reactivating restores access.',
        ]}
        confirmLabel="Deactivate"
        pending={update.isPending}
        error={update.error}
        doing="Couldn’t deactivate them"
        onConfirm={() => update.mutate({ is_active: false }, { onSuccess: () => setDeactivating(false) })}
      />
      <Confirm
        open={adminChange !== null}
        onOpenChange={(o) => !o && setAdminChange(null)}
        tone={adminChange ? 'primary' : 'danger'}
        title={adminChange ? `Make ${user.email} an Admin?` : `Remove ${user.email}’s Admin role?`}
        effects={adminChange ? ADMIN_POWERS : ['They lose Settings and every project they have no role in.', 'Projects where they have a role keep that role.']}
        confirmLabel={adminChange ? 'Make Admin' : 'Remove Admin'}
        pending={update.isPending}
        error={update.error}
        doing="Couldn’t change the Admin role"
        onConfirm={() =>
          adminChange !== null &&
          update.mutate(
            { is_superuser: adminChange },
            {
              onSuccess: () => {
                toast.success(adminChange ? `${user.email} is now an Admin` : `${user.email} is no longer an Admin`);
                setAdminChange(null);
              },
            },
          )
        }
      />
      <Confirm
        open={removing !== null}
        onOpenChange={(o) => !o && setRemoving(null)}
        title={`Remove ${user.email} from ${removing?.name ?? ''}?`}
        effects={['They can no longer open the project or its pipelines.', 'Pipelines they own keep running; their owner field stays.']}
        confirmLabel="Remove"
        pending={revoke.isPending}
        error={revoke.error}
        doing="Couldn’t remove them"
        onConfirm={() =>
          removing &&
          revoke.mutate(removing.project_id, {
            onSuccess: () => {
              toast.success(`Removed ${user.email} from ${removing.name}`);
              setRemoving(null);
            },
          })
        }
      />
    </Modal>
  );
}

/** Local state flips in the change handler itself, so the box responds before the save lands. */
export function ApproverToggle({ checked, label, onChange }: { checked: boolean; label: string; onChange: (checked: boolean) => void }) {
  const [value, setValue] = useState(checked);
  useEffect(() => setValue(checked), [checked]);
  return (
    <Check
      checked={value}
      onChange={(on) => {
        setValue(on);
        onChange(on);
      }}
      className="w-auto text-sm"
      label={
        <>
          Approver<span className="sr-only"> ({label})</span>
        </>
      }
    />
  );
}
