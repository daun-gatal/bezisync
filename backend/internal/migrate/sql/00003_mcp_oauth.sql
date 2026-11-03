-- +goose Up
-- MCP (Settings → MCP): agents that sign in with OAuth (BeziSync is the authorization server).
-- Codes and tokens are stored hashed; a grant is one agent's access for one person.
CREATE TABLE public.oauth_clients (
    id character varying(64) NOT NULL,
    name character varying(200) NOT NULL,
    redirect_uris jsonb NOT NULL,
    client_uri character varying(500),
    status character varying(16) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    last_used_at timestamp with time zone,
    decided_by_id uuid,
    decided_at timestamp with time zone,
    CONSTRAINT pk_oauth_clients PRIMARY KEY (id),
    CONSTRAINT fk_oauth_clients_decided_by_id_users FOREIGN KEY (decided_by_id) REFERENCES public.users(id) ON DELETE SET NULL
);

CREATE TABLE public.oauth_requests (
    id uuid NOT NULL,
    client_id character varying(64) NOT NULL,
    redirect_uri text NOT NULL,
    state character varying(500),
    code_challenge character varying(128) NOT NULL,
    resource character varying(500),
    status character varying(16) NOT NULL,
    user_id uuid,
    project_id uuid,
    read_only boolean NOT NULL,
    grant_days integer NOT NULL,
    code_hash character varying(64),
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT pk_oauth_requests PRIMARY KEY (id),
    CONSTRAINT uq_oauth_requests_code_hash UNIQUE (code_hash),
    CONSTRAINT fk_oauth_requests_client_id_oauth_clients FOREIGN KEY (client_id) REFERENCES public.oauth_clients(id) ON DELETE CASCADE,
    CONSTRAINT fk_oauth_requests_user_id_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE,
    CONSTRAINT fk_oauth_requests_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE
);

CREATE INDEX ix_oauth_requests_expires_at ON public.oauth_requests USING btree (expires_at);

CREATE TABLE public.oauth_grants (
    id uuid NOT NULL,
    client_id character varying(64) NOT NULL,
    user_id uuid NOT NULL,
    project_id uuid,
    read_only boolean NOT NULL,
    access_hash character varying(64) NOT NULL,
    access_expires_at timestamp with time zone NOT NULL,
    refresh_hash character varying(64) NOT NULL,
    previous_refresh_hash character varying(64),
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL,
    last_used_at timestamp with time zone,
    revoked_at timestamp with time zone,
    CONSTRAINT pk_oauth_grants PRIMARY KEY (id),
    CONSTRAINT uq_oauth_grants_access_hash UNIQUE (access_hash),
    CONSTRAINT uq_oauth_grants_refresh_hash UNIQUE (refresh_hash),
    CONSTRAINT fk_oauth_grants_client_id_oauth_clients FOREIGN KEY (client_id) REFERENCES public.oauth_clients(id) ON DELETE CASCADE,
    CONSTRAINT fk_oauth_grants_user_id_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE,
    CONSTRAINT fk_oauth_grants_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE
);

CREATE INDEX ix_oauth_grants_user_id ON public.oauth_grants USING btree (user_id);

-- +goose Down
DROP TABLE public.oauth_grants;
DROP TABLE public.oauth_requests;
DROP TABLE public.oauth_clients;
