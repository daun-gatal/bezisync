-- +goose Up
-- A pipeline runs on one engine: Debezium Server (every pipeline before engines existed) or Flink
-- CDC. It is chosen with the destination and fixed once the pipeline is deployed.
ALTER TABLE public.pipelines ADD COLUMN engine character varying(20) NOT NULL DEFAULT 'debezium';
ALTER TABLE public.pipelines ADD CONSTRAINT pipelines_engine_check CHECK (engine IN ('debezium', 'flink-cdc'));

-- A Flink CDC pipeline's own secrets (the key its saved positions are encrypted with, the TLS
-- keystore its JobManager and TaskManagers share), generated once, sealed like every secret.
ALTER TABLE public.pipelines ADD COLUMN engine_secrets_enc text;

-- The image catalog lists images of both engines; each engine has its own default.
ALTER TABLE public.debezium_images ADD COLUMN engine character varying(20) NOT NULL DEFAULT 'debezium';
ALTER TABLE public.debezium_images ADD CONSTRAINT debezium_images_engine_check CHECK (engine IN ('debezium', 'flink-cdc'));

-- +goose Down
ALTER TABLE public.debezium_images DROP CONSTRAINT debezium_images_engine_check;
ALTER TABLE public.debezium_images DROP COLUMN engine;
ALTER TABLE public.pipelines DROP COLUMN engine_secrets_enc;
ALTER TABLE public.pipelines DROP CONSTRAINT pipelines_engine_check;
ALTER TABLE public.pipelines DROP COLUMN engine;
