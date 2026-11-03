-- +goose Up
-- Single sign-on providers, managed in Settings → Sign-in (they replace the BEZISYNC_OIDC_* settings).
CREATE TABLE public.sso_providers (
    id uuid NOT NULL,
    label character varying(60) NOT NULL,
    issuer character varying(500) NOT NULL,
    client_id character varying(500) NOT NULL,
    client_secret_enc text NOT NULL,
    scopes jsonb NOT NULL,
    groups_claim character varying(100) NOT NULL,
    allow_unverified_email boolean NOT NULL,
    role_rules jsonb NOT NULL,
    default_role character varying(16),
    default_projects jsonb NOT NULL,
    enabled boolean NOT NULL,
    last_test jsonb,
    version integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT pk_sso_providers PRIMARY KEY (id),
    CONSTRAINT uq_sso_providers_label UNIQUE (label)
);

-- +goose Down
DROP TABLE public.sso_providers;
