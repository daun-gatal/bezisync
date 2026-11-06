-- +goose Up
-- Raw Debezium properties now need the project policy allow_advanced_properties. Projects with a
-- pipeline that already uses them (in its working or deployed version) get it, so nothing that
-- deploys today is blocked by the upgrade; each such project gets an audit event.
CREATE TEMPORARY TABLE bz_advanced_projects ON COMMIT DROP AS
SELECT DISTINCT p.project_id
FROM public.pipelines p
JOIN public.pipeline_versions v ON v.id IN (p.current_version_id, p.deployed_version_id)
WHERE p.deleted_at IS NULL
  AND jsonb_typeof(v.spec -> 'advanced') = 'object'
  AND v.spec -> 'advanced' <> '{}'::jsonb;

UPDATE public.projects
SET policies = COALESCE(policies, '{}'::jsonb) || '{"allow_advanced_properties": true}'::jsonb
WHERE id IN (SELECT project_id FROM bz_advanced_projects);

INSERT INTO public.events (id, occurred_at, project_id, actor_id, actor_label, kind, resource_type, resource_id, summary, details, audit, request_id)
SELECT gen_random_uuid(), now(), project_id, NULL, 'BeziSync upgrade', 'project.policies_updated', 'project', project_id,
       'Changed project policies: allow_advanced_properties (pipelines here already use advanced Debezium properties)',
       '{"allow_advanced_properties": true}'::jsonb, TRUE, NULL
FROM bz_advanced_projects;

-- +goose Down
-- Older releases refuse unknown policy keys.
UPDATE public.projects SET policies = policies - 'allow_advanced_properties' WHERE policies ? 'allow_advanced_properties';
