-- +goose Up
-- Placement profiles of a Kubernetes runtime (Admins): named node selectors, tolerations, a
-- priority class and labels a pipeline picks by name. Other runtimes have none.
ALTER TABLE public.runtimes ADD COLUMN placement_profiles jsonb NOT NULL DEFAULT '[]'::jsonb;

-- +goose Down
ALTER TABLE public.runtimes DROP COLUMN placement_profiles;
