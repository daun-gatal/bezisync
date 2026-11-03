-- +goose Up
-- The schema exactly as Alembic revision 0018 left it (the last Python migration).
-- Databases already at 0018 are stamped with this version without running it.
CREATE TABLE public.agent_requests (
    id uuid NOT NULL,
    runtime_id uuid NOT NULL,
    op character varying(32) NOT NULL,
    args jsonb NOT NULL,
    state character varying(16) NOT NULL,
    result jsonb,
    created_at timestamp with time zone NOT NULL
);

CREATE TABLE public.agent_workloads (
    id uuid NOT NULL,
    runtime_id uuid NOT NULL,
    short_id character varying(32) NOT NULL,
    config_hash character varying(32) NOT NULL,
    spec_enc text NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.alert_channels (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    name character varying(100) NOT NULL,
    kind character varying(16) NOT NULL,
    config jsonb NOT NULL,
    secrets_enc text,
    enabled boolean NOT NULL,
    last_delivery_at timestamp with time zone,
    last_error text,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.alert_rules (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    name character varying(200) NOT NULL,
    kind character varying(32) NOT NULL,
    severity character varying(16) NOT NULL,
    for_seconds integer NOT NULL,
    params jsonb NOT NULL,
    match_labels jsonb NOT NULL,
    channel_ids jsonb NOT NULL,
    notify_owner boolean NOT NULL,
    enabled boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.alerts (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    rule_id uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    state character varying(16) NOT NULL,
    severity character varying(16) NOT NULL,
    title text NOT NULL,
    message text NOT NULL,
    fix text NOT NULL,
    started_at timestamp with time zone NOT NULL,
    fired_at timestamp with time zone,
    last_seen_at timestamp with time zone NOT NULL,
    resolved_at timestamp with time zone,
    acknowledged_at timestamp with time zone,
    acknowledged_by character varying(320),
    deliveries jsonb NOT NULL
);

CREATE TABLE public.api_tokens (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    name character varying(200) NOT NULL,
    token_hash character varying(64) NOT NULL,
    prefix character varying(16) NOT NULL,
    project_id uuid,
    read_only boolean NOT NULL,
    expires_at timestamp with time zone,
    last_used_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.change_requests (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    version_id uuid NOT NULL,
    state character varying(16) NOT NULL,
    requested_by_id uuid,
    force_reason text,
    note text NOT NULL,
    preflight jsonb,
    decided_by_id uuid,
    decided_at timestamp with time zone,
    comment text NOT NULL,
    created_at timestamp with time zone NOT NULL
);

CREATE TABLE public.connections (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    kind character varying(8) NOT NULL,
    type character varying(32) NOT NULL,
    name character varying(63) NOT NULL,
    description text NOT NULL,
    provider character varying(32) NOT NULL,
    config jsonb NOT NULL,
    secrets_enc text,
    managed boolean NOT NULL,
    last_check jsonb,
    last_check_ok boolean,
    last_checked_at timestamp with time zone,
    version integer NOT NULL,
    created_by_id uuid,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.debezium_images (
    id uuid NOT NULL,
    image character varying(300) NOT NULL,
    label character varying(100) NOT NULL,
    notes text NOT NULL,
    is_default boolean NOT NULL,
    deprecated boolean NOT NULL,
    created_by_id uuid,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.events (
    id uuid NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    project_id uuid,
    actor_id uuid,
    actor_label character varying(320) NOT NULL,
    kind character varying(64) NOT NULL,
    resource_type character varying(32) NOT NULL,
    resource_id uuid,
    summary text NOT NULL,
    details jsonb NOT NULL,
    audit boolean NOT NULL,
    request_id character varying(64)
);

CREATE TABLE public.feedback (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    user_id uuid,
    email character varying(320) NOT NULL,
    kind character varying(16) NOT NULL,
    message text NOT NULL,
    page character varying(500) NOT NULL,
    user_agent character varying(300) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    resolved_at timestamp with time zone,
    resolved_by character varying(320)
);

CREATE TABLE public.instance_state (
    key character varying(64) NOT NULL,
    value jsonb NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.jobs (
    id uuid NOT NULL,
    kind character varying(64) NOT NULL,
    project_id uuid,
    resource_id uuid,
    payload jsonb NOT NULL,
    state character varying(16) NOT NULL,
    attempts integer NOT NULL,
    max_attempts integer NOT NULL,
    run_after timestamp with time zone NOT NULL,
    locked_by character varying(100),
    locked_at timestamp with time zone,
    result jsonb,
    error text,
    created_at timestamp with time zone NOT NULL,
    finished_at timestamp with time zone
);

CREATE TABLE public.memberships (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    user_id uuid NOT NULL,
    role character varying(16) NOT NULL,
    can_approve boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    granted_by_sso boolean DEFAULT false NOT NULL
);

CREATE TABLE public.metric_samples (
    id integer NOT NULL,
    pipeline_id uuid NOT NULL,
    at timestamp with time zone NOT NULL,
    resolution integer NOT NULL,
    events_per_second double precision,
    lag_ms double precision,
    events_total double precision,
    queue_used_ratio double precision,
    source_retained_bytes double precision,
    healthy boolean
);

CREATE SEQUENCE public.metric_samples_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.metric_samples_id_seq OWNED BY public.metric_samples.id;

CREATE TABLE public.notifications (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    project_id uuid NOT NULL,
    kind character varying(32) NOT NULL,
    title text NOT NULL,
    body text NOT NULL,
    link character varying(500) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    read_at timestamp with time zone
);

CREATE TABLE public.pipeline_templates (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    name character varying(63) NOT NULL,
    title character varying(200) NOT NULL,
    description text NOT NULL,
    spec jsonb NOT NULL,
    sink_id uuid,
    runtime_id uuid,
    created_by_id uuid,
    version integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.pipeline_versions (
    id uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    number integer NOT NULL,
    spec jsonb NOT NULL,
    spec_hash character varying(64) NOT NULL,
    note text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    created_by_id uuid,
    created_by_label character varying(320) NOT NULL
);

CREATE TABLE public.pipelines (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    name character varying(63) NOT NULL,
    description text NOT NULL,
    owner character varying(200) NOT NULL,
    labels jsonb NOT NULL,
    source_id uuid NOT NULL,
    sink_id uuid,
    runtime_id uuid,
    desired_state character varying(16) NOT NULL,
    current_version_id uuid,
    deployed_version_id uuid,
    deployed_at timestamp with time zone,
    status jsonb NOT NULL,
    lifecycle character varying(16) NOT NULL,
    health character varying(16) NOT NULL,
    artifacts jsonb NOT NULL,
    deleted_at timestamp with time zone,
    version integer NOT NULL,
    created_by_id uuid,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    deployed_image character varying(300)
);

CREATE TABLE public.projects (
    id uuid NOT NULL,
    slug character varying(63) NOT NULL,
    name character varying(200) NOT NULL,
    description text NOT NULL,
    is_sandbox boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    policies jsonb DEFAULT '{}'::jsonb NOT NULL
);

CREATE TABLE public.runtimes (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    name character varying(63) NOT NULL,
    kind character varying(16) NOT NULL,
    mode character varying(16) NOT NULL,
    config jsonb NOT NULL,
    secrets_enc text,
    default_image character varying(300),
    last_test jsonb,
    last_seen_at timestamp with time zone,
    capacity jsonb,
    version integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    agent_token_hash character varying(64),
    agent_version character varying(40),
    agent_state jsonb,
    enabled boolean DEFAULT true NOT NULL
);

CREATE TABLE public.schema_changes (
    id uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    "table" character varying(300) NOT NULL,
    kind character varying(32) NOT NULL,
    "column" character varying(300),
    before text,
    after text,
    breaking boolean NOT NULL,
    detected_at timestamp with time zone NOT NULL,
    reviewed_at timestamp with time zone
);

CREATE TABLE public.secret_stores (
    id uuid NOT NULL,
    project_id uuid NOT NULL,
    name character varying(63) NOT NULL,
    kind character varying(16) NOT NULL,
    config jsonb NOT NULL,
    secrets_enc text,
    last_test jsonb,
    version integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.user_sessions (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    token_hash character varying(64) NOT NULL,
    csrf_token character varying(64) NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    user_agent character varying(400) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);

CREATE TABLE public.users (
    id uuid NOT NULL,
    email character varying(320) NOT NULL,
    name character varying(200) NOT NULL,
    password_hash text,
    is_superuser boolean NOT NULL,
    is_active boolean NOT NULL,
    ui_mode character varying(16) NOT NULL,
    sso_subject character varying(400),
    last_login_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    muted_notifications jsonb DEFAULT '[]'::jsonb NOT NULL
);

ALTER TABLE ONLY public.metric_samples ALTER COLUMN id SET DEFAULT nextval('public.metric_samples_id_seq'::regclass);

ALTER TABLE ONLY public.agent_requests
    ADD CONSTRAINT pk_agent_requests PRIMARY KEY (id);

ALTER TABLE ONLY public.agent_workloads
    ADD CONSTRAINT pk_agent_workloads PRIMARY KEY (id);

ALTER TABLE ONLY public.alert_channels
    ADD CONSTRAINT pk_alert_channels PRIMARY KEY (id);

ALTER TABLE ONLY public.alert_rules
    ADD CONSTRAINT pk_alert_rules PRIMARY KEY (id);

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT pk_alerts PRIMARY KEY (id);

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT pk_api_tokens PRIMARY KEY (id);

ALTER TABLE ONLY public.change_requests
    ADD CONSTRAINT pk_change_requests PRIMARY KEY (id);

ALTER TABLE ONLY public.connections
    ADD CONSTRAINT pk_connections PRIMARY KEY (id);

ALTER TABLE ONLY public.debezium_images
    ADD CONSTRAINT pk_debezium_images PRIMARY KEY (id);

ALTER TABLE ONLY public.events
    ADD CONSTRAINT pk_events PRIMARY KEY (id);

ALTER TABLE ONLY public.feedback
    ADD CONSTRAINT pk_feedback PRIMARY KEY (id);

ALTER TABLE ONLY public.instance_state
    ADD CONSTRAINT pk_instance_state PRIMARY KEY (key);

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT pk_jobs PRIMARY KEY (id);

ALTER TABLE ONLY public.memberships
    ADD CONSTRAINT pk_memberships PRIMARY KEY (id);

ALTER TABLE ONLY public.metric_samples
    ADD CONSTRAINT pk_metric_samples PRIMARY KEY (id);

ALTER TABLE ONLY public.notifications
    ADD CONSTRAINT pk_notifications PRIMARY KEY (id);

ALTER TABLE ONLY public.pipeline_templates
    ADD CONSTRAINT pk_pipeline_templates PRIMARY KEY (id);

ALTER TABLE ONLY public.pipeline_versions
    ADD CONSTRAINT pk_pipeline_versions PRIMARY KEY (id);

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT pk_pipelines PRIMARY KEY (id);

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT pk_projects PRIMARY KEY (id);

ALTER TABLE ONLY public.runtimes
    ADD CONSTRAINT pk_runtimes PRIMARY KEY (id);

ALTER TABLE ONLY public.schema_changes
    ADD CONSTRAINT pk_schema_changes PRIMARY KEY (id);

ALTER TABLE ONLY public.secret_stores
    ADD CONSTRAINT pk_secret_stores PRIMARY KEY (id);

ALTER TABLE ONLY public.user_sessions
    ADD CONSTRAINT pk_user_sessions PRIMARY KEY (id);

ALTER TABLE ONLY public.users
    ADD CONSTRAINT pk_users PRIMARY KEY (id);

ALTER TABLE ONLY public.agent_workloads
    ADD CONSTRAINT uq_agent_workloads_runtime_id_short_id UNIQUE (runtime_id, short_id);

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT uq_api_tokens_token_hash UNIQUE (token_hash);

ALTER TABLE ONLY public.connections
    ADD CONSTRAINT uq_connections_project_id_name UNIQUE (project_id, name);

ALTER TABLE ONLY public.debezium_images
    ADD CONSTRAINT uq_debezium_images_image UNIQUE (image);

ALTER TABLE ONLY public.memberships
    ADD CONSTRAINT uq_memberships_project_id_user_id UNIQUE (project_id, user_id);

ALTER TABLE ONLY public.pipeline_templates
    ADD CONSTRAINT uq_pipeline_templates_project_id_name UNIQUE (project_id, name);

ALTER TABLE ONLY public.pipeline_versions
    ADD CONSTRAINT uq_pipeline_versions_pipeline_id_number UNIQUE (pipeline_id, number);

ALTER TABLE ONLY public.projects
    ADD CONSTRAINT uq_projects_slug UNIQUE (slug);

ALTER TABLE ONLY public.runtimes
    ADD CONSTRAINT uq_runtimes_agent_token_hash UNIQUE (agent_token_hash);

ALTER TABLE ONLY public.runtimes
    ADD CONSTRAINT uq_runtimes_project_id_name UNIQUE (project_id, name);

ALTER TABLE ONLY public.secret_stores
    ADD CONSTRAINT uq_secret_stores_project_id_name UNIQUE (project_id, name);

ALTER TABLE ONLY public.user_sessions
    ADD CONSTRAINT uq_user_sessions_token_hash UNIQUE (token_hash);

ALTER TABLE ONLY public.users
    ADD CONSTRAINT uq_users_email UNIQUE (email);

ALTER TABLE ONLY public.users
    ADD CONSTRAINT uq_users_sso_subject UNIQUE (sso_subject);

CREATE INDEX ix_agent_requests_pending ON public.agent_requests USING btree (runtime_id, state);

CREATE INDEX ix_agent_workloads_runtime_id ON public.agent_workloads USING btree (runtime_id);

CREATE INDEX ix_alert_channels_project_id ON public.alert_channels USING btree (project_id);

CREATE INDEX ix_alert_rules_project_id ON public.alert_rules USING btree (project_id);

CREATE INDEX ix_alerts_pipeline_id ON public.alerts USING btree (pipeline_id);

CREATE INDEX ix_alerts_project_state ON public.alerts USING btree (project_id, state);

CREATE INDEX ix_api_tokens_user_id ON public.api_tokens USING btree (user_id);

CREATE INDEX ix_change_requests_pipeline_id ON public.change_requests USING btree (pipeline_id);

CREATE INDEX ix_change_requests_project_state ON public.change_requests USING btree (project_id, state);

CREATE INDEX ix_connections_project_id ON public.connections USING btree (project_id);

CREATE INDEX ix_events_project_time ON public.events USING btree (project_id, occurred_at);

CREATE INDEX ix_events_resource ON public.events USING btree (resource_type, resource_id);

CREATE INDEX ix_feedback_resolved_created ON public.feedback USING btree (resolved_at, created_at);

CREATE INDEX ix_jobs_ready ON public.jobs USING btree (state, run_after);

CREATE INDEX ix_jobs_resource_id ON public.jobs USING btree (resource_id);

CREATE INDEX ix_memberships_project_id ON public.memberships USING btree (project_id);

CREATE INDEX ix_memberships_user_id ON public.memberships USING btree (user_id);

CREATE INDEX ix_metric_samples_pipeline_at ON public.metric_samples USING btree (pipeline_id, at);

CREATE INDEX ix_notifications_user_project ON public.notifications USING btree (user_id, project_id, created_at);

CREATE INDEX ix_pipeline_templates_project_id ON public.pipeline_templates USING btree (project_id);

CREATE INDEX ix_pipeline_versions_pipeline_id ON public.pipeline_versions USING btree (pipeline_id);

CREATE INDEX ix_pipelines_lifecycle ON public.pipelines USING btree (lifecycle);

CREATE INDEX ix_pipelines_project_id ON public.pipelines USING btree (project_id);

CREATE INDEX ix_pipelines_runtime_id ON public.pipelines USING btree (runtime_id);

CREATE INDEX ix_pipelines_sink_id ON public.pipelines USING btree (sink_id);

CREATE INDEX ix_pipelines_source_id ON public.pipelines USING btree (source_id);

CREATE INDEX ix_runtimes_project_id ON public.runtimes USING btree (project_id);

CREATE INDEX ix_schema_changes_pipeline_detected ON public.schema_changes USING btree (pipeline_id, detected_at);

CREATE INDEX ix_secret_stores_project_id ON public.secret_stores USING btree (project_id);

CREATE INDEX ix_user_sessions_user_id ON public.user_sessions USING btree (user_id);

CREATE UNIQUE INDEX uq_alerts_open ON public.alerts USING btree (rule_id, pipeline_id) WHERE ((state)::text <> 'resolved'::text);

CREATE UNIQUE INDEX uq_pipelines_project_name_live ON public.pipelines USING btree (project_id, name) WHERE (deleted_at IS NULL);

ALTER TABLE ONLY public.agent_requests
    ADD CONSTRAINT fk_agent_requests_runtime_id_runtimes FOREIGN KEY (runtime_id) REFERENCES public.runtimes(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.agent_workloads
    ADD CONSTRAINT fk_agent_workloads_runtime_id_runtimes FOREIGN KEY (runtime_id) REFERENCES public.runtimes(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.alert_channels
    ADD CONSTRAINT fk_alert_channels_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.alert_rules
    ADD CONSTRAINT fk_alert_rules_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT fk_alerts_pipeline_id_pipelines FOREIGN KEY (pipeline_id) REFERENCES public.pipelines(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT fk_alerts_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT fk_alerts_rule_id_alert_rules FOREIGN KEY (rule_id) REFERENCES public.alert_rules(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT fk_api_tokens_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.api_tokens
    ADD CONSTRAINT fk_api_tokens_user_id_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.change_requests
    ADD CONSTRAINT fk_change_requests_decided_by_id_users FOREIGN KEY (decided_by_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.change_requests
    ADD CONSTRAINT fk_change_requests_pipeline_id_pipelines FOREIGN KEY (pipeline_id) REFERENCES public.pipelines(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.change_requests
    ADD CONSTRAINT fk_change_requests_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.change_requests
    ADD CONSTRAINT fk_change_requests_requested_by_id_users FOREIGN KEY (requested_by_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.change_requests
    ADD CONSTRAINT fk_change_requests_version_id_pipeline_versions FOREIGN KEY (version_id) REFERENCES public.pipeline_versions(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.connections
    ADD CONSTRAINT fk_connections_created_by_id_users FOREIGN KEY (created_by_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.connections
    ADD CONSTRAINT fk_connections_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.debezium_images
    ADD CONSTRAINT fk_debezium_images_created_by_id_users FOREIGN KEY (created_by_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.events
    ADD CONSTRAINT fk_events_actor_id_users FOREIGN KEY (actor_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.events
    ADD CONSTRAINT fk_events_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.feedback
    ADD CONSTRAINT fk_feedback_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.feedback
    ADD CONSTRAINT fk_feedback_user_id_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.jobs
    ADD CONSTRAINT fk_jobs_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.memberships
    ADD CONSTRAINT fk_memberships_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.memberships
    ADD CONSTRAINT fk_memberships_user_id_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.metric_samples
    ADD CONSTRAINT fk_metric_samples_pipeline_id_pipelines FOREIGN KEY (pipeline_id) REFERENCES public.pipelines(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.notifications
    ADD CONSTRAINT fk_notifications_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.notifications
    ADD CONSTRAINT fk_notifications_user_id_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pipeline_templates
    ADD CONSTRAINT fk_pipeline_templates_created_by_id_users FOREIGN KEY (created_by_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.pipeline_templates
    ADD CONSTRAINT fk_pipeline_templates_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pipeline_templates
    ADD CONSTRAINT fk_pipeline_templates_runtime_id_runtimes FOREIGN KEY (runtime_id) REFERENCES public.runtimes(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.pipeline_templates
    ADD CONSTRAINT fk_pipeline_templates_sink_id_connections FOREIGN KEY (sink_id) REFERENCES public.connections(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.pipeline_versions
    ADD CONSTRAINT fk_pipeline_versions_created_by_id_users FOREIGN KEY (created_by_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.pipeline_versions
    ADD CONSTRAINT fk_pipeline_versions_pipeline_id_pipelines FOREIGN KEY (pipeline_id) REFERENCES public.pipelines(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT fk_pipelines_created_by_id_users FOREIGN KEY (created_by_id) REFERENCES public.users(id) ON DELETE SET NULL;

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT fk_pipelines_current_version FOREIGN KEY (current_version_id) REFERENCES public.pipeline_versions(id);

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT fk_pipelines_deployed_version FOREIGN KEY (deployed_version_id) REFERENCES public.pipeline_versions(id);

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT fk_pipelines_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT fk_pipelines_runtime_id_runtimes FOREIGN KEY (runtime_id) REFERENCES public.runtimes(id);

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT fk_pipelines_sink_id_connections FOREIGN KEY (sink_id) REFERENCES public.connections(id);

ALTER TABLE ONLY public.pipelines
    ADD CONSTRAINT fk_pipelines_source_id_connections FOREIGN KEY (source_id) REFERENCES public.connections(id);

ALTER TABLE ONLY public.runtimes
    ADD CONSTRAINT fk_runtimes_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.schema_changes
    ADD CONSTRAINT fk_schema_changes_pipeline_id_pipelines FOREIGN KEY (pipeline_id) REFERENCES public.pipelines(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.secret_stores
    ADD CONSTRAINT fk_secret_stores_project_id_projects FOREIGN KEY (project_id) REFERENCES public.projects(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.user_sessions
    ADD CONSTRAINT fk_user_sessions_user_id_users FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;

-- +goose Down
-- The baseline cannot be reverted.
