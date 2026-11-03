import { useState } from 'react';
import { LayoutTemplate } from 'lucide-react';
import { toast } from 'sonner';
import { Button, ButtonLink } from '@/components/ui/button';
import { Empty } from '@/components/ui/feedback';
import { Card, QueryState } from '@/components/ui/layout';
import { Confirm } from '@/components/ui/overlays';
import { Meta } from '@/components/ui/text';
import { useProject } from '@/lib/session';
import { templateSummary, useDeleteTemplate, useTemplates, type Template } from '@/lib/templates';

export function TemplatesSection() {
  const { project, can } = useProject();
  const templates = useTemplates(project.id);
  const remove = useDeleteTemplate(project.id);
  const [deleting, setDeleting] = useState<Template | null>(null);
  return (
    <div className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-col gap-1">
        <h2 className="m-0 text-xl font-semibold">Templates</h2>
        <p className="m-0 text-base text-muted">
          Agreed starting points: people pick a template, choose their tables and deploy. To publish one, open a pipeline that is set up the way you want, then
          History → Save as template.
        </p>
      </div>
      <QueryState query={templates} doing="Couldn’t load templates">
        {(list) =>
          list.length === 0 ? (
            <Card>
              <Empty icon={<LayoutTemplate />} title="No templates yet">
                Publish one from a pipeline’s History tab.
              </Empty>
            </Card>
          ) : (
            <div className="grid min-w-0 grid-cols-[repeat(auto-fill,minmax(min(100%,340px),1fr))] gap-4">
              {list.map((t) => (
                <Card
                  key={t.id}
                  title={t.title}
                  footer={
                    can('pipeline.edit') || can('project.manage') ? (
                      <>
                        {can('project.manage') ? (
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={() => {
                              remove.reset();
                              setDeleting(t);
                            }}
                            aria-label={`Delete ${t.title}`}
                          >
                            Delete
                          </Button>
                        ) : null}
                        {can('pipeline.edit') ? (
                          <ButtonLink size="sm" to="/p/$projectSlug/pipelines/new" params={{ projectSlug: project.slug }} search={{ template: t.id }}>
                            Use template
                          </ButtonLink>
                        ) : null}
                      </>
                    ) : null
                  }
                >
                  {t.description ? <p className="m-0 text-base text-ink-2 wrap-break-word">{t.description}</p> : null}
                  <Meta as="p" className="wrap-anywhere">
                    {templateSummary(t)}
                  </Meta>
                </Card>
              ))}
            </div>
          )
        }
      </QueryState>
      <Confirm
        open={deleting !== null}
        onOpenChange={(o) => !o && setDeleting(null)}
        title={`Delete template “${deleting?.title ?? ''}”?`}
        effects={['People can no longer start from it.', 'Pipelines created from it keep their settings.']}
        confirmLabel="Delete"
        pending={remove.isPending}
        error={remove.error}
        doing="Couldn’t delete the template"
        onConfirm={() =>
          deleting &&
          remove.mutate(deleting.id, {
            onSuccess: () => {
              toast.success('Template deleted');
              setDeleting(null);
            },
          })
        }
      />
    </div>
  );
}
