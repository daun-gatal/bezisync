-- +goose Up
-- Sign-in providers beyond OpenID Connect: plain OAuth2 with a userinfo endpoint (GitHub, …) and
-- SAML 2.0. kind says which; config holds what that kind needs (endpoints, claim names, the
-- identity provider's SAML metadata). SAML providers have no client secret.
ALTER TABLE public.sso_providers ADD COLUMN kind character varying(16) DEFAULT 'oidc'::character varying NOT NULL;
ALTER TABLE public.sso_providers ADD COLUMN config jsonb DEFAULT '{}'::jsonb NOT NULL;
ALTER TABLE public.sso_providers ALTER COLUMN client_secret_enc DROP NOT NULL;

-- SAML sign-ins in progress: the AuthnRequest ID the response must answer, with a random relay
-- token the browser carries back. Each is used once.
CREATE TABLE public.sso_requests (
    id character varying(64) NOT NULL,
    provider_id uuid NOT NULL,
    relay_hash character varying(64) NOT NULL,
    next_path character varying(1000) NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    CONSTRAINT pk_sso_requests PRIMARY KEY (id),
    CONSTRAINT uq_sso_requests_relay_hash UNIQUE (relay_hash),
    CONSTRAINT fk_sso_requests_provider_id_sso_providers FOREIGN KEY (provider_id) REFERENCES public.sso_providers(id) ON DELETE CASCADE
);

-- SAML assertions already used, kept until they expire, so none is accepted twice.
CREATE TABLE public.sso_replays (
    id character varying(300) NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    CONSTRAINT pk_sso_replays PRIMARY KEY (id)
);

CREATE INDEX ix_sso_replays_expires_at ON public.sso_replays USING btree (expires_at);

-- +goose Down
DROP TABLE public.sso_replays;
DROP TABLE public.sso_requests;
ALTER TABLE public.sso_providers ALTER COLUMN client_secret_enc SET NOT NULL;
ALTER TABLE public.sso_providers DROP COLUMN config;
ALTER TABLE public.sso_providers DROP COLUMN kind;
