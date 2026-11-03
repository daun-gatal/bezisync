import { useEffect, useState, type FormEvent } from 'react';
import { useMutation } from '@tanstack/react-query';
import clsx from 'clsx';
import { Check, ShieldAlert } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { InlineError } from '@/components/ui/feedback';
import { Field, Textarea } from '@/components/ui/form';
import { Modal } from '@/components/ui/overlays';
import { client, unwrap } from '@/lib/api';
import { FEEDBACK_KINDS, MAX_FEEDBACK, describeBrowser, feedbackKind, type FeedbackKind } from '@/lib/feedback';
import { useProject, useProviders } from '@/lib/session';

const ICON_TONE = {
  bad: 'bg-bad-soft text-bad',
  warn: 'bg-warn-soft text-warn',
  info: 'bg-accent-soft text-accent-ink',
  grey: 'bg-grey-soft text-grey',
} as const;

/** Feedback from anywhere in the app; only Admins read it (Settings → Feedback). */
/** `page` is the app path the person is on; it is sent along so Admins can reproduce. */
export function FeedbackDialog({
  open,
  onOpenChange,
  page,
  initialKind = 'bug',
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  page: string;
  initialKind?: FeedbackKind;
}) {
  const { me, project } = useProject();
  const lab = useProviders().data?.lab ?? false;
  const [kind, setKind] = useState<FeedbackKind>(initialKind);
  const [message, setMessage] = useState('');
  const send = useMutation({
    mutationFn: () =>
      unwrap(client.POST('/api/v1/projects/{project_id}/feedback', { params: { path: { project_id: project.id } }, body: { kind, message, page } })),
  });
  // Every opening starts fresh; a draft only survives while the dialog stays open.
  useEffect(() => {
    if (!open) return;
    setKind(initialKind);
    setMessage('');
    send.reset();
  }, [open, initialKind]); // eslint-disable-line react-hooks/exhaustive-deps

  const current = feedbackKind(kind);
  const security = kind === 'security';
  const sent = send.isSuccess;
  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (message.trim()) send.mutate();
  };

  if (sent) {
    return (
      <Modal open={open} onOpenChange={onOpenChange} title="Thanks, we got it">
        <div className="flex flex-col items-center gap-3 py-2 text-center">
          <span className="grid size-11 place-items-center rounded-full bg-ok-soft text-ok" aria-hidden>
            <Check className="size-5" />
          </span>
          <p className="m-0 max-w-sm text-base text-ink-2">
            {security
              ? `Only Admins can read security reports.${lab ? ` We will reply to ${me.user.email} within 3 business days.` : ''}`
              : `${lab ? 'The team reads every message.' : 'Your Admins read every message.'} If we need more detail, we will email ${me.user.email}.`}
          </p>
          <div className="mt-2 flex gap-2">
            <Button
              onClick={() => {
                send.reset();
                setKind(initialKind);
                setMessage('');
              }}
            >
              Send another
            </Button>
            <Button variant="primary" onClick={() => onOpenChange(false)}>
              Done
            </Button>
          </div>
        </div>
      </Modal>
    );
  }

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      dirty={Boolean(message.trim())}
      why={message.trim() ? null : 'Write a message to send it.'}
      title="Send feedback"
      description={lab ? 'It goes straight to the BeziSync team. Thanks for trying the lab.' : 'It goes to the Admins of this BeziSync.'}
      footer={
        <>
          <Button onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button variant="primary" type="submit" form="feedback-form" loading={send.isPending} disabled={!message.trim()}>
            {security ? 'Send security report' : 'Send feedback'}
          </Button>
        </>
      }
    >
      <form id="feedback-form" onSubmit={submit} className="flex flex-col gap-4">
        <fieldset className="m-0 flex flex-col gap-2 border-0 p-0">
          <legend className="mb-2 p-0 text-sm font-medium">What is it about?</legend>
          <div className="grid grid-cols-[repeat(auto-fill,minmax(min(100%,220px),1fr))] gap-2">
            {FEEDBACK_KINDS.map((k) => (
              <button
                key={k.kind}
                type="button"
                aria-pressed={kind === k.kind}
                onClick={() => setKind(k.kind)}
                className={clsx(
                  'flex min-h-16 min-w-0 items-start gap-2.5 rounded-lg border px-3 py-2.5 text-left',
                  kind === k.kind ? 'border-accent bg-accent-soft shadow-[inset_0_0_0_1px_var(--bz-accent)]' : 'border-line-strong bg-surface hover:bg-hover',
                )}
              >
                <span className={clsx('grid size-7 shrink-0 place-items-center rounded-lg', ICON_TONE[k.tone])} aria-hidden>
                  <k.icon className="size-4" />
                </span>
                <span className="flex min-w-0 flex-col gap-0.5">
                  <span className="text-base font-semibold text-ink">{k.label}</span>
                  <span className="text-xs text-muted">{k.help}</span>
                </span>
              </button>
            ))}
          </div>
        </fieldset>

        {security ? (
          <div role="status" className="flex items-start gap-2.5 rounded-md bg-warn-soft px-3.5 py-3 text-sm text-warn">
            <ShieldAlert className="mt-px size-4 shrink-0" aria-hidden />
            <span>
              Only Admins see security reports.
              {lab ? (
                <>
                  {' '}
                  Before you test, read the{' '}
                  <a href="/testing-policy" target="_blank" rel="noopener" className="font-semibold text-inherit underline">
                    testing policy
                  </a>
                  : what you may try, and what stays off-limits.
                </>
              ) : null}
            </span>
          </div>
        ) : null}

        <Field
          label="Message"
          hint={
            <span className="flex flex-wrap justify-between gap-x-3">
              <span>Never paste passwords, keys or real customer data.</span>
              <span className="font-mono">
                {message.length.toLocaleString('en-US')} / {MAX_FEEDBACK.toLocaleString('en-US')}
              </span>
            </span>
          }
        >
          {(id) => (
            <Textarea
              id={id}
              rows={5}
              maxLength={MAX_FEEDBACK}
              required
              value={message}
              placeholder={current.placeholder}
              onChange={(e) => setMessage(e.target.value)}
              className="resize-y"
            />
          )}
        </Field>

        <div className="flex flex-col gap-1.5 rounded-lg border border-line bg-subtle px-3 py-2.5 text-xs text-muted">
          <span className="font-semibold text-ink-2">Sent along with your message</span>
          <span className="wrap-anywhere">
            Page <span className="font-mono text-ink-2">{page}</span> · Account <span className="text-ink-2">{me.user.email}</span> · Browser{' '}
            <span className="text-ink-2">{describeBrowser(navigator.userAgent)}</span>
          </span>
        </div>

        <InlineError error={send.error} doing="Couldn’t send your feedback" />
      </form>
    </Modal>
  );
}
