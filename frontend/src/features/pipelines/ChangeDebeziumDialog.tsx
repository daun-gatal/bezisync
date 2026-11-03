import { useEffect, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { InlineError, Notice, Pill } from '@/components/ui/feedback';
import { Check, Field, Input, RadioCards } from '@/components/ui/form';
import { Modal } from '@/components/ui/overlays';
import { Select } from '@/components/ui/select';
import { client, unwrap, type Schemas } from '@/lib/api';
import { imagesKey, useCatalog } from '@/lib/debezium';
import { useProject } from '@/lib/session';

const OTHER = '__other__';
type Result = Schemas['ImageChangeResult'];

/** Move one or more pipelines to another Debezium Server image: each gets a new version you can roll back. */
export function ChangeDebeziumDialog({
  pipelines,
  open,
  onOpenChange,
  onDone,
}: {
  pipelines: { id: string; name: string }[];
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onDone?: () => void;
}) {
  const { project, can } = useProject();
  const qc = useQueryClient();
  const catalog = useCatalog();
  const offered = (catalog.data?.entries ?? []).filter((e) => !e.deprecated);
  const [target, setTarget] = useState<'default' | 'custom'>('custom');
  const [pick, setPick] = useState(OTHER);
  const [custom, setCustom] = useState('');
  const [deployNow, setDeployNow] = useState(true);
  const [results, setResults] = useState<Result[] | null>(null);
  const canDeploy = can('pipeline.deploy');
  const chosen = pick === OTHER ? custom.trim() : pick;
  const count = pipelines.length;
  const noun = `pipeline${count === 1 ? '' : 's'}`;

  useEffect(() => {
    if (!open) return;
    setResults(null);
    setTarget('custom');
    setCustom('');
    setPick(offered.find((e) => !e.is_default)?.image ?? offered[0]?.image ?? OTHER);
    setDeployNow(true);
    apply.reset();
    // Reset only when the dialog opens; the catalog arriving later must not clear a choice.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const apply = useMutation({
    mutationFn: () =>
      unwrap(
        client.POST('/api/v1/projects/{project_id}/debezium-images:apply', {
          params: { path: { project_id: project.id } },
          body: { pipeline_ids: pipelines.map((p) => p.id), image: target === 'default' ? null : chosen, deploy: deployNow && canDeploy },
        }),
      ),
    onSuccess: (r) => {
      setResults(r);
      const failed = r.filter((x) => !x.ok).length;
      if (!failed) toast.success(`Changed ${r.length} ${noun}`);
      void qc.invalidateQueries({ queryKey: imagesKey(project.id) });
      void qc.invalidateQueries({ queryKey: ['pipelines', project.id] });
      onDone?.();
    },
  });

  return (
    <Modal
      open={open}
      onOpenChange={onOpenChange}
      size="lg"
      dirty={!results && (pick === OTHER ? Boolean(custom.trim()) : false)}
      title={results ? 'Result' : count === 1 ? `Change Debezium version of ${pipelines[0]?.name ?? ''}` : `Change Debezium version of ${count} pipelines`}
      description={results ? undefined : 'Each pipeline gets a new version with the chosen image. Roll back from its History tab if needed.'}
      why={!results && target === 'custom' && !chosen ? 'Enter the image reference.' : null}
      footer={
        results ? (
          <Button variant="primary" onClick={() => onOpenChange(false)}>
            Done
          </Button>
        ) : (
          <>
            <Button onClick={() => onOpenChange(false)}>Cancel</Button>
            <Button variant="primary" loading={apply.isPending} disabled={target === 'custom' && !chosen} onClick={() => apply.mutate()}>
              {deployNow && canDeploy ? 'Change and deploy' : 'Save new versions'}
            </Button>
          </>
        )
      }
    >
      {results ? (
        <ul className="m-0 flex list-none flex-col gap-2 p-0">
          {results.map((r) => (
            <li key={r.pipeline_id} className="flex min-w-0 flex-wrap items-start gap-2 text-base">
              <Pill tone={r.ok ? 'ok' : 'bad'}>{r.ok ? 'Changed' : 'Not changed'}</Pill>
              <span className="min-w-0 flex-[1_1_240px] wrap-anywhere">
                <strong>{r.name}</strong>
                {r.version ? ` → v${r.version}` : ''}: {r.message}
              </span>
            </li>
          ))}
        </ul>
      ) : (
        <>
          {count > 1 ? <Notice tone="info">Safer: change a single pipeline first (a canary), watch it reach Healthy, then change the rest.</Notice> : null}
          <Field label="Debezium version" group>
            {() => (
              <RadioCards
                label="Debezium version"
                value={target}
                onChange={setTarget}
                columns={2}
                options={[
                  {
                    value: 'custom',
                    label: 'A specific version',
                    description: 'One your Admins offer, or another image (extra converters, SMTs or JDBC drivers).',
                  },
                  {
                    value: 'default',
                    label: 'Follow the default',
                    description: `Use the runtime’s image, or the default ${catalog.data?.default_image ?? ''}. Pipelines already following it are redeployed when the default changed.`,
                  },
                ]}
              />
            )}
          </Field>
          {target === 'custom' ? (
            <Field label="Version">
              {(id) => (
                <Select
                  id={id}
                  value={pick}
                  onChange={setPick}
                  options={[
                    ...offered.map((e) => ({
                      value: e.image,
                      label: <span className="font-mono">{e.image}</span>,
                      text: e.image,
                      description: [e.label, e.is_default ? 'the default' : ''].filter(Boolean).join(' · ') || undefined,
                    })),
                    { value: OTHER, label: 'Another image…', description: 'Type any image reference' },
                  ]}
                />
              )}
            </Field>
          ) : null}
          {target === 'custom' && pick === OTHER ? (
            <Field
              label="Image reference"
              hint="registry/repository:tag or @sha256 digest. The runtime must be able to pull it. Images not offered by your Admins get a warning in checks."
            >
              {(id) => (
                <Input
                  id={id}
                  className="font-mono"
                  value={custom}
                  placeholder="quay.io/debezium/server:3.7.0.Final"
                  onChange={(e) => setCustom(e.target.value)}
                />
              )}
            </Field>
          ) : null}
          {canDeploy ? (
            <Check
              checked={deployNow}
              onChange={setDeployNow}
              label="Deploy now"
              description="Each connector restarts on the new image and resumes from its stored offsets. Pipelines with other undeployed edits are skipped."
            />
          ) : (
            <Notice tone="info">You can save the new versions; someone with deploy permission rolls them out.</Notice>
          )}
          <InlineError error={apply.error} doing="Couldn’t change the Debezium version" />
        </>
      )}
    </Modal>
  );
}
