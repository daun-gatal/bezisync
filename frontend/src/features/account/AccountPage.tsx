import { useState } from 'react';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { InlineError, Pill } from '@/components/ui/feedback';
import { Field, Input, Segmented } from '@/components/ui/form';
import { Card, Page, PageHeader, Split } from '@/components/ui/layout';
import { Meta } from '@/components/ui/text';
import { NotificationSettings } from '@/app/NotificationSettings';
import { useProject, useUpdateMe } from '@/lib/session';
import { THEMES, useTheme } from '@/lib/theme';
import { AgentsSection } from './AgentsSection';
import { TokensSection } from './TokensSection';

/** Personal settings every member has, whatever their role: profile, theme, mode, notifications, API tokens, AI agents. */
export function AccountPage() {
  const { me, project, expert } = useProject();
  const updateMe = useUpdateMe();
  const [name, setName] = useState(me.user.name);
  const [notifying, setNotifying] = useState(false);
  const [theme, setTheme] = useTheme();
  // An Admin without a project role shows only the Admin badge, not "Admin" twice.
  const role = project.role ?? null;

  return (
    <Page>
      <PageHeader title="Account" description={me.user.email} />
      <Split>
        <Card title="Profile">
          <form
            className="flex flex-wrap items-end gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              updateMe.mutate({ name }, { onSuccess: () => toast.success('Saved your name') });
            }}
          >
            <Field label="Name" className="min-w-0 flex-[1_1_200px]">
              {(id) => <Input id={id} value={name} maxLength={200} onChange={(e) => setName(e.target.value)} />}
            </Field>
            <Button type="submit" loading={updateMe.isPending} disabled={name === me.user.name} why="Nothing changed.">
              Save
            </Button>
          </form>
          <InlineError error={updateMe.error} doing="Couldn’t save your settings" />
          <p className="m-0 flex flex-wrap items-center gap-2 text-base text-ink-2">
            In {project.name}: {role ? <Pill tone="grey">{role}</Pill> : null}
            {me.user.is_superuser ? <Pill tone="info">Admin</Pill> : null}
          </p>
        </Card>
        <Card title="Preferences">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex min-w-0 flex-[1_1_200px] flex-col gap-0.5">
              <strong className="font-semibold">Theme</strong>
              <Meta>{theme === 'system' ? 'Follows your computer’s setting.' : 'Saved in this browser.'}</Meta>
            </span>
            <Segmented label="Theme" value={theme} onChange={setTheme} options={THEMES} />
          </div>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex min-w-0 flex-[1_1_200px] flex-col gap-0.5">
              <strong className="font-semibold">{expert ? 'Expert mode' : 'Guided mode'}</strong>
              <Meta>{expert ? 'Shows raw Debezium properties.' : 'Plain language, Debezium terms as secondary labels.'}</Meta>
            </span>
            <Button size="sm" loading={updateMe.isPending} onClick={() => updateMe.mutate({ ui_mode: expert ? 'guided' : 'expert' })}>
              Switch to {expert ? 'Guided' : 'Expert'}
            </Button>
          </div>
          <div className="flex flex-wrap items-center justify-between gap-3">
            <span className="flex min-w-0 flex-[1_1_200px] flex-col gap-0.5">
              <strong className="font-semibold">Notifications</strong>
              <Meta>What lands in your inbox.</Meta>
            </span>
            <Button size="sm" onClick={() => setNotifying(true)}>
              Notification settings
            </Button>
          </div>
        </Card>
      </Split>
      <TokensSection />
      <AgentsSection />
      <NotificationSettings open={notifying} onOpenChange={setNotifying} />
    </Page>
  );
}
