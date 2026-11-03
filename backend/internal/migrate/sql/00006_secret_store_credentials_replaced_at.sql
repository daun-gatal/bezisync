-- +goose Up
-- When a secret store's credentials were last set or replaced (shown on its card; never the values).
ALTER TABLE secret_stores ADD COLUMN secrets_updated_at timestamp with time zone;
UPDATE secret_stores SET secrets_updated_at = updated_at WHERE secrets_enc IS NOT NULL;

-- +goose Down
ALTER TABLE secret_stores DROP COLUMN secrets_updated_at;
