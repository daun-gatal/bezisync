-- +goose Up
-- A version is sealed once something depends on it: it was deployed or sent for approval. Until
-- then it is the pipeline's working draft, and its author's further saves update it in place, so
-- one change is one version however many saves it took.
ALTER TABLE public.pipeline_versions ADD COLUMN sealed_at timestamp with time zone;

-- Existing versions stay as they are: only the current version of a pipeline that was never
-- deployed and never sent for approval stays a draft.
UPDATE public.pipeline_versions v SET sealed_at = v.created_at
WHERE NOT EXISTS (
    SELECT 1 FROM public.pipelines p
    WHERE p.current_version_id = v.id AND p.deployed_at IS NULL AND p.deployed_version_id IS NULL
) OR EXISTS (SELECT 1 FROM public.change_requests c WHERE c.version_id = v.id);

-- +goose Down
ALTER TABLE public.pipeline_versions DROP COLUMN sealed_at;
