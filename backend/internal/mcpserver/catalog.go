package mcpserver

import (
	"net/http"
	"sync"

	"github.com/daun-gatal/bezisync/backend/internal/auth"
)

// Every BeziSync operation is a tool except the ones that take or hand out a credential: API
// and agent tokens, runtime and secret-store credentials, alert channels (their URLs and keys
// are secrets), sign-in providers, passwords, and the MCP settings themselves (an agent must
// not widen its own access). People do those in the UI.

var (
	catalogOnce sync.Once
	catalog     []*Tool
)

// Catalog lists every tool in a stable order.
func Catalog() []*Tool {
	catalogOnce.Do(func() { catalog = buildCatalog() })
	return catalog
}

// Find returns a tool by name.
func Find(name string) *Tool {
	for _, t := range Catalog() {
		if t.Name == name {
			return t
		}
	}
	return nil
}

func get(path func(*call) (string, error)) func(*call) (any, error) {
	return func(cl *call) (any, error) {
		p, err := path(cl)
		if err != nil {
			return nil, err
		}
		return cl.do(http.MethodGet, p, nil)
	}
}

func send(method string, path func(*call) (string, error), fields ...string) func(*call) (any, error) {
	return func(cl *call) (any, error) {
		p, err := path(cl)
		if err != nil {
			return nil, err
		}
		var body any
		if method != http.MethodDelete {
			body = cl.body(fields...)
		}
		return cl.do(method, p, body)
	}
}

func at(suffix string) func(*call) (string, error) {
	return func(cl *call) (string, error) {
		b, err := cl.base()
		return b + suffix, err
	}
}

func onPipeline(suffix string) func(*call) (string, error) {
	return func(cl *call) (string, error) {
		p, err := cl.pipeline()
		return p + suffix, err
	}
}

func onConnection(suffix string) func(*call) (string, error) {
	return func(cl *call) (string, error) {
		id, err := cl.connection("connection")
		if err != nil {
			return "", err
		}
		b, _ := cl.base()
		return b + "/connections/" + id + suffix, nil
	}
}

func onRuntime(suffix string) func(*call) (string, error) {
	return func(cl *call) (string, error) {
		id, err := cl.runtime("runtime")
		if err != nil {
			return "", err
		}
		b, _ := cl.base()
		return b + "/runtimes/" + id + suffix, nil
	}
}

func fixed(path string) func(*call) (string, error) {
	return func(*call) (string, error) { return path, nil }
}

func onSub(arg, collection, suffix string) func(*call) (string, error) {
	return func(cl *call) (string, error) {
		p, err := cl.sub(arg, collection)
		return p + suffix, err
	}
}

var (
	pipelineArg   = req("pipeline", str("The pipeline's id or name."))
	connectionArg = req("connection", str("The connection's id or name."))
	runtimeArg    = req("runtime", str("The runtime's id or name."))
	roleArg       = oneOf("Project role.", "viewer", "operator", "editor")
)

func buildCatalog() []*Tool {
	pipelineSpec := obj("The pipeline spec (the same shape as the YAML's spec: source.tables.include, source.columns " +
		"{table: {mask, exclude, keep}}, sink.naming, snapshot, transforms, policies, …). Only the fields you send change.")
	return []*Tool{
		// --- General ---
		{Name: "list_projects", Title: "List projects", Kind: Read,
			Description: "The projects you can open, with your role and permissions in each. Call this first.",
			Call:        func(cl *call) (any, error) { return cl.c.me.Projects, nil }},
		{Name: "get_system_info", Title: "BeziSync version and features", Kind: Read,
			Description: "BeziSync's version, the default Debezium image and which features are on.",
			Call:        get(fixed("/api/v1/system/info"))},
		{Name: "list_connector_types", Title: "Connector types", Kind: Read, Project: true, Perm: auth.ConnectionView,
			Description: "Sources and destinations BeziSync supports, with their configuration schemas and capabilities. Pass type to read one.",
			Params:      []Param{opt("kind", oneOf("Only sources or only destinations.", "source", "sink")), opt("type", str("One connector type, e.g. postgresql or kafka."))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				all, err := cl.do(http.MethodGet, b+"/connector-types", nil)
				if err != nil {
					return nil, err
				}
				var out []any
				for _, t := range asList(all) {
					m, _ := t.(map[string]any)
					if (cl.s("kind") == "" || m["kind"] == cl.s("kind")) && (cl.s("type") == "" || m["type"] == cl.s("type")) {
						if cl.s("type") == "" { // a list stays short: schemas only for one type
							delete(m, "config_schema")
							delete(m, "secret_schema")
						}
						out = append(out, m)
					}
				}
				return out, nil
			}},
		{Name: "list_notifications", Title: "My notifications", Kind: Read, Project: true, Perm: auth.ProjectView,
			Description: "Your in-app notifications in a project (approvals, alerts, …).",
			Params:      []Param{opt("limit", num("At most this many (default 50)."))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, b+"/notifications"+query("limit", cl.intArg("limit")), nil)
			}},
		{Name: "mark_notifications_read", Title: "Mark notifications read", Kind: Write, Project: true, Perm: auth.ProjectView,
			Description: "Marks all your notifications in the project as read.",
			Call:        send(http.MethodPost, at("/notifications/read-all"))},
		{Name: "list_audit_events", Title: "Audit log", Kind: Read, Project: true, Perm: auth.AuditView,
			Description: "Who changed what in the project, newest first.",
			Params:      []Param{opt("resource_id", str("Only events about this resource (a pipeline id, …).")), opt("cursor", str("The next cursor from a previous page.")), opt("limit", num("Page size, at most 200."))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, b+"/events"+query("audit_only", "true", "resource_id", cl.s("resource_id"), "cursor", cl.s("cursor"), "limit", cl.intArg("limit")), nil)
			}},

		// --- Pipelines ---
		{Name: "list_pipelines", Title: "List pipelines", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "Pipelines in the project with their lifecycle, health and status.",
			Params:      []Param{opt("search", str("Only pipelines whose name contains this.")), opt("include_deleted", flag("Also list deleted pipelines."))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, b+"/pipelines"+query("q", cl.s("search"), "include_deleted", cl.boolArg("include_deleted")), nil)
			}},
		{Name: "get_pipeline", Title: "Get a pipeline", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "A pipeline's current spec, versions, health, status and any pending approval.",
			Params:      []Param{pipelineArg}, Call: get(onPipeline(""))},
		{Name: "get_pipeline_metrics", Title: "Pipeline metrics", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "Throughput, lag and errors over time.",
			Params:      []Param{pipelineArg, opt("range", oneOf("Time range.", "1h", "6h", "24h", "7d"))},
			Call: func(cl *call) (any, error) {
				p, err := cl.pipeline()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, p+"/metrics"+query("range", cl.s("range")), nil)
			}},
		{Name: "get_pipeline_logs", Title: "Connector logs", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "The latest log lines of the pipeline's connector (a Flink CDC pipeline: of one component).",
			Params: []Param{pipelineArg, opt("tail", num("How many lines (default 200).")),
				opt("component", str("Flink CDC only: jobmanager (default), taskmanager-<n> or tunnel."))},
			Call: func(cl *call) (any, error) {
				p, err := cl.pipeline()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, p+"/logs"+query("tail", cl.intArg("tail"), "component", cl.s("component")), nil)
			}},
		{Name: "read_pipeline_events", Title: "Read delivered events", Kind: Read, Project: true, Perm: auth.PipelinePreviewData,
			Description: "Recent events as the destination received them. Values that look like personal data are always masked here.",
			Params:      []Param{pipelineArg, req("table", str("A captured table, e.g. public.orders.")), opt("limit", num("How many events (default 10)."))},
			Call: func(cl *call) (any, error) {
				p, err := cl.pipeline()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, p+"/events"+query("table", cl.s("table"), "limit", cl.intArg("limit"), "mask", "true"), nil)
			}},
		{Name: "get_pipeline_schema", Title: "Captured schema and changes", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "The captured tables' columns and the schema changes detected since, including new columns that look like personal data.",
			Params:      []Param{pipelineArg}, Call: get(onPipeline("/schema"))},
		{Name: "get_pipeline_versions", Title: "Pipeline versions", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "Every saved version with its note and author.", Params: []Param{pipelineArg}, Call: get(onPipeline("/versions"))},
		{Name: "diff_pipeline_versions", Title: "Compare versions", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "A unified diff between two versions and what the change does.",
			Params:      []Param{pipelineArg, req("from", num("Older version number.")), req("to", num("Newer version number."))},
			Call: func(cl *call) (any, error) {
				p, err := cl.pipeline()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, p+"/diff"+query("from", cl.intArg("from"), "to", cl.intArg("to")), nil)
			}},
		{Name: "get_pipeline_config", Title: "Generated configuration", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "The configuration BeziSync generates: application.properties on Debezium, pipeline.yaml on Flink CDC (secrets appear only as ${ENV} placeholders).",
			Params:      []Param{pipelineArg, opt("version", num("A version number; the current one when left out."))},
			Call: func(cl *call) (any, error) {
				p, err := cl.pipeline()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, p+"/config"+query("version", cl.intArg("version")), nil)
			}},
		{Name: "get_pipeline_offsets", Title: "Stored position", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "Where the connector will resume reading the source.", Params: []Param{pipelineArg}, Call: get(onPipeline("/offsets"))},
		{Name: "export_pipeline", Title: "Export as YAML", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "The pipeline as a config-as-code manifest (connections and runtime by name).", Params: []Param{pipelineArg}, Call: get(onPipeline("/manifest"))},
		{Name: "export_pipelines", Title: "Export all pipelines", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "Every pipeline of the project as one YAML document stream.", Call: get(at("/pipelines:export"))},
		{Name: "create_pipeline", Title: "Create a pipeline", Kind: Write, Project: true, Perm: auth.PipelineEdit,
			Description: "Creates a draft pipeline (version 1). Nothing runs until deploy_pipeline. Columns that look like personal data should be masked, removed or kept on purpose in spec.source.columns.",
			Params: []Param{req("name", str("A unique name in the project.")), req("source", str("The source connection's id or name.")),
				opt("destination", str("The destination connection's id or name.")), opt("runtime", str("The runtime's id or name.")),
				opt("spec", pipelineSpec), opt("owner", str("Owning team.")), opt("description", str("What it is for.")),
				opt("labels", obj("Labels (string values).")), opt("template_id", str("Start from this template.")),
				opt("engine", oneOf("The engine: debezium (events, the default where it fits) or flink-cdc (tables kept in sync; StarRocks, Doris, Fluss and Iceberg).", "debezium", "flink-cdc"))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				body := cl.body("name", "spec", "owner", "description", "labels", "template_id", "engine")
				if body["source_id"], err = cl.connection("source"); err != nil {
					return nil, err
				}
				if cl.has("destination") {
					if body["sink_id"], err = cl.connection("destination"); err != nil {
						return nil, err
					}
				}
				if cl.has("runtime") {
					if body["runtime_id"], err = cl.runtime("runtime"); err != nil {
						return nil, err
					}
				}
				return cl.do(http.MethodPost, b+"/pipelines", body)
			}},
		{Name: "update_pipeline", Title: "Change a pipeline", Kind: Write, Project: true, Perm: auth.PipelineEdit,
			Description: "Saves the change as a version: your undeployed draft is updated in place, otherwise a new version starts (deployed pipelines keep running the deployed version until deploy_pipeline).",
			Params: []Param{pipelineArg, opt("spec", pipelineSpec), opt("name", str("New name.")), opt("description", str("")), opt("owner", str("")),
				opt("labels", obj("")), opt("destination", str("Another destination connection (id or name).")), opt("runtime", str("Another runtime (id or name).")),
				opt("note", str("Why you changed it (shown in the version history).")), opt("engine", oneOf("The engine: debezium (events, the default where it fits) or flink-cdc (tables kept in sync; StarRocks, Doris, Fluss and Iceberg).", "debezium", "flink-cdc")), expectedVersion()},
			Call: func(cl *call) (any, error) {
				p, err := cl.pipeline()
				if err != nil {
					return nil, err
				}
				body := cl.body("spec", "name", "description", "owner", "labels", "note", "engine")
				if cl.has("destination") {
					if body["sink_id"], err = cl.connection("destination"); err != nil {
						return nil, err
					}
				}
				if cl.has("runtime") {
					if body["runtime_id"], err = cl.runtime("runtime"); err != nil {
						return nil, err
					}
				}
				return cl.withVersion(p, body)
			}},
		{Name: "check_pipeline", Title: "Run the final checks", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "Preflight: every check on the source, destination, runtime, configuration (including personal data) and project policies, with fixes.",
			Params:      []Param{pipelineArg},
			Call: func(cl *call) (any, error) {
				p, err := cl.pipeline()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodPost, p+"/preflight", map[string]any{})
			}},
		{Name: "deploy_pipeline", Title: "Deploy a pipeline", Kind: Write, Project: true, Perm: auth.PipelineDeploy,
			Description: "Runs the final checks and deploys a version (the latest when left out). With approvals on, this asks for approval instead. force_reason deploys past failed checks where the project allows it; policy failures can never be forced.",
			Params:      []Param{pipelineArg, opt("version", num("Version number to deploy (also rolls back).")), opt("force_reason", str("Why it is safe to deploy despite failed checks (recorded)."))},
			Call:        send(http.MethodPost, onPipeline("/deploy"), "version", "force_reason")},
		{Name: "pause_pipeline", Title: "Pause", Kind: Write, Project: true, Perm: auth.PipelineOperate,
			Description: "Stops the connector; the position is kept and the source keeps its log for it.", Params: []Param{pipelineArg}, Call: send(http.MethodPost, onPipeline("/pause"))},
		{Name: "resume_pipeline", Title: "Resume", Kind: Write, Project: true, Perm: auth.PipelineOperate,
			Description: "Starts a paused pipeline from where it stopped.", Params: []Param{pipelineArg}, Call: send(http.MethodPost, onPipeline("/resume"))},
		{Name: "restart_pipeline", Title: "Restart", Kind: Write, Project: true, Perm: auth.PipelineOperate,
			Description: "Restarts the connector.", Params: []Param{pipelineArg}, Call: send(http.MethodPost, onPipeline("/restart"))},
		{Name: "snapshot_pipeline", Title: "Re-copy tables", Kind: Write, Project: true, Perm: auth.PipelineOperate,
			Description: "Starts an incremental snapshot of some tables while streaming continues.",
			Params:      []Param{pipelineArg, req("tables", strs("Captured tables to copy again.")), opt("ignore_window", flag("Start now even outside the snapshot window."))},
			Call:        send(http.MethodPost, onPipeline("/snapshots"), "tables", "ignore_window")},
		{Name: "review_pipeline_schema_changes", Title: "Mark schema changes reviewed", Kind: Write, Project: true, Perm: auth.PipelineOperate,
			Description: "Marks the pipeline's breaking schema changes as reviewed (clears the alert).", Params: []Param{pipelineArg},
			Call: send(http.MethodPost, onPipeline("/schema/review"))},
		{Name: "preview_pipeline_transforms", Title: "Preview transforms", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "What an event of a table looks like after the spec's transforms (pure: nothing is read or written).",
			Params: []Param{pipelineArg, req("table", str("A captured table.")), opt("spec", pipelineSpec), opt("operation", oneOf("The change to preview.", "insert", "update", "delete", "snapshot")),
				opt("columns", map[string]any{"type": "array", "items": map[string]any{"type": "object"}, "description": "Columns {name, type, nullable, pii}; discovered when left out."})},
			Call: send(http.MethodPost, onPipeline("/transforms:preview"), "table", "spec", "operation", "columns")},
		{Name: "promote_pipeline", Title: "Promote to another project", Kind: Write, Project: true, Perm: auth.PipelineView,
			Description: "Applies the pipeline's latest version in another project (that project's permissions, policies and approvals apply).",
			Params: []Param{pipelineArg, req("target_project", str("The other project's id.")), req("source_id", str("Source connection id in the other project.")),
				opt("sink_id", str("Destination connection id there.")), opt("runtime_id", str("Runtime id there.")), opt("name", str("Name there.")),
				opt("dry_run", flag("Only show what would change.")), opt("deploy", flag("Deploy after applying."))},
			Call: func(cl *call) (any, error) {
				p, err := cl.pipeline()
				if err != nil {
					return nil, err
				}
				body := cl.body("source_id", "sink_id", "runtime_id", "name", "dry_run", "deploy")
				body["target_project_id"] = cl.s("target_project")
				return cl.do(http.MethodPost, p+"/promote", body)
			}},
		{Name: "apply_pipeline_manifests", Title: "Apply YAML", Kind: Write, Project: true, Perm: auth.PipelineEdit,
			Description: "Applies kind: Pipeline manifests, all or nothing. Defaults to a dry run that returns the diff; pass dry_run false to save.",
			Params:      []Param{req("manifest", str("One or more YAML documents.")), opt("dry_run", flag("Only show the diff (default true).")), opt("deploy", flag("Deploy after saving."))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				body := cl.body("manifest", "deploy")
				body["dry_run"] = true
				if v, ok := cl.args["dry_run"].(bool); ok {
					body["dry_run"] = v
				}
				return cl.do(http.MethodPost, b+"/pipelines:apply", body)
			}},
		{Name: "adopt_pipeline_properties", Title: "Read an application.properties", Kind: Read, Project: true, Perm: auth.PipelineEdit,
			Description: "Maps an existing Debezium Server application.properties to a BeziSync spec without saving anything (plain passwords are never kept).",
			Params:      []Param{req("properties", str("The file's text."))}, Call: send(http.MethodPost, at("/pipelines:adopt"), "properties")},
		{Name: "reset_pipeline_offsets", Title: "Reset the stored position", Kind: Destructive, Project: true, Perm: auth.OffsetsReset,
			Description: "Starts a paused pipeline over: re-copies everything (resnapshot), skips to now (changes in between are lost) or, on Flink CDC from MySQL or SQL Server, reads changes since from_time. Type the pipeline's name in confirm.",
			Params: []Param{pipelineArg, req("mode", oneOf("What to do.", "resnapshot", "skip_to_now", "from_time")), req("confirm", str("The pipeline's name, to confirm.")),
				opt("from_time", str("With from_time: an RFC 3339 time in the past."))},
			Call: send(http.MethodPost, onPipeline("/offsets/reset"), "mode", "confirm", "from_time")},
		{Name: "list_saved_positions", Title: "Saved positions", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "A Flink CDC pipeline's saved positions, newest first; it resumes from the newest current one.",
			Params:      []Param{pipelineArg}, Call: get(onPipeline("/positions"))},
		{Name: "restore_saved_position", Title: "Restore a saved position", Kind: Destructive, Project: true, Perm: auth.OffsetsReset,
			Description: "Makes a paused Flink CDC pipeline resume from an earlier saved position (changes after it are read again). Type the pipeline's name in confirm.",
			Params:      []Param{pipelineArg, req("position_id", num("The saved position's id (list_saved_positions).")), req("confirm", str("The pipeline's name, to confirm."))},
			Call: func(cl *call) (any, error) {
				p, err := cl.pipeline()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodPost, p+"/positions/"+cl.intArg("position_id")+"/restore", cl.body("confirm"))
			}},
		{Name: "delete_pipeline", Title: "Delete a pipeline", Kind: Destructive, Project: true, Perm: auth.PipelineEdit,
			Description: "Stops the connector and removes the replication slot and its artifacts.",
			Params:      []Param{pipelineArg, opt("drop_destinations", flag("Also delete the topics/tables it wrote to."))},
			Call: func(cl *call) (any, error) {
				p, err := cl.pipeline()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodDelete, p+query("drop_destinations", cl.boolArg("drop_destinations")), nil)
			}},

		// --- Debezium versions ---
		{Name: "list_debezium_versions", Title: "Debezium versions in use", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "Pipelines grouped by the Debezium Server image they run, and how each image stands in the catalog.",
			Call:        get(at("/debezium-images"))},
		{Name: "get_debezium_catalog", Title: "Engine image catalog", Kind: Read,
			Description: "The images Admins offer for an engine (Debezium by default), the default and deprecated ones.",
			Params:      []Param{opt("engine", oneOf("Which engine's images.", "debezium", "flink-cdc"))},
			Call: func(cl *call) (any, error) {
				return cl.do(http.MethodGet, "/api/v1/debezium-catalog"+query("engine", cl.s("engine")), nil)
			}},
		{Name: "list_engine_settings", Title: "A pipeline's engine settings", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "The settings this pipeline can tune (for its engine, source and destination), each with its bounds, default, what changing it does, " +
				"its current value and this project's limit, and whether raw properties are allowed. Change them with update_pipeline (spec.tuning; " +
				"a Flink setting in spec.flink_cdc.flink_conf): the same checks, limits and approvals apply.",
			Params: []Param{pipelineArg}, Call: get(onPipeline("/settings"))},
		{Name: "list_engines", Title: "Engines", Kind: Read,
			Description: "The engines, whether each can run pipelines now (Flink CDC once an Admin registers an image), what each reads and writes, and Flink CDC's settings and sizes.",
			Call:        get(fixed("/api/v1/engines"))},
		{Name: "move_pipelines_to_debezium_version", Title: "Move pipelines to a Debezium version", Kind: Write, Project: true, Perm: auth.PipelineDeploy,
			Description: "Saves a new version of each pipeline with another image; deploy true ships them (approvals apply).",
			Params:      []Param{req("pipeline_ids", strs("Pipeline ids.")), opt("image", str("The image; the default when left out.")), opt("deploy", flag("Deploy each after saving."))},
			Call:        send(http.MethodPost, at("/debezium-images:apply"), "pipeline_ids", "image", "deploy")},
		{Name: "add_debezium_catalog_image", Title: "Add an engine image", Kind: Write, Admin: true,
			Description: "Offers another image to every project: a Debezium Server image, or a Flink CDC image (the first one turns Flink CDC on).",
			Params: []Param{req("image", str("Image reference.")), opt("label", str("")), opt("notes", str("")), opt("is_default", flag("Make it the default.")),
				opt("engine", oneOf("The engine the image runs (default debezium).", "debezium", "flink-cdc"))},
			Call: send(http.MethodPost, fixed("/api/v1/debezium-catalog"), "image", "label", "notes", "is_default", "engine")},
		{Name: "update_debezium_catalog_image", Title: "Change a Debezium image", Kind: Write, Admin: true,
			Description: "Labels, deprecates or makes an image the default.",
			Params:      []Param{req("image_id", str("Catalog entry id.")), opt("label", str("")), opt("notes", str("")), opt("deprecated", flag("")), opt("is_default", flag(""))},
			Call: func(cl *call) (any, error) {
				return cl.do(http.MethodPatch, "/api/v1/debezium-catalog/"+cl.s("image_id"), cl.body("label", "notes", "deprecated", "is_default"))
			}},
		{Name: "remove_debezium_catalog_image", Title: "Remove a Debezium image", Kind: Destructive, Admin: true,
			Description: "Removes an image from the catalog (pipelines running it keep running).", Params: []Param{req("image_id", str("Catalog entry id."))},
			Call: func(cl *call) (any, error) {
				return cl.do(http.MethodDelete, "/api/v1/debezium-catalog/"+cl.s("image_id"), nil)
			}},

		// --- Connections ---
		{Name: "list_connections", Title: "List connections", Kind: Read, Project: true, Perm: auth.ConnectionView,
			Description: "Sources and destinations with their last check. Secrets are never shown, only which fields are set.",
			Params:      []Param{opt("kind", oneOf("Only sources or destinations.", "source", "sink"))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, b+"/connections"+query("kind", cl.s("kind")), nil)
			}},
		{Name: "get_connection", Title: "Get a connection", Kind: Read, Project: true, Perm: auth.ConnectionView,
			Description: "A connection's configuration and last check (secrets never shown).", Params: []Param{connectionArg}, Call: get(onConnection(""))},
		{Name: "get_connection_usage", Title: "Pipelines using a connection", Kind: Read, Project: true, Perm: auth.ConnectionView,
			Description: "Which pipelines read from or write to it.", Params: []Param{connectionArg}, Call: get(onConnection("/usage"))},
		{Name: "check_connection", Title: "Check a connection", Kind: Read, Project: true, Perm: auth.ConnectionView,
			Description: "Runs the readiness checks (network, sign-in, CDC setup, permissions, retention…) with exact fixes for the provider.",
			Params:      []Param{connectionArg, opt("tables", strs("Also check these tables."))},
			Call:        send(http.MethodPost, onConnection("/check"), "tables")},
		{Name: "discover_connection_tables", Title: "List a source's tables", Kind: Read, Project: true, Perm: auth.ConnectionView,
			Description: "Tables and columns BeziSync can capture, with keys, size estimates and columns that look like personal data.",
			Params:      []Param{connectionArg}, Call: send(http.MethodPost, onConnection("/discover"))},
		{Name: "create_connection", Title: "Add a connection", Kind: Write, Project: true, Perm: auth.ConnectionManage,
			Description: "Adds a source or destination. Secret fields (passwords, keys) can only be secret-store references; " +
				"otherwise ask a person to enter them in the BeziSync UI. list_connector_types with type shows the config schema.",
			Params: []Param{req("kind", oneOf("Source or destination.", "source", "sink")), req("type", str("Connector type, e.g. postgresql.")),
				req("name", str("A unique name (lowercase, digits and dashes).")), req("config", obj("Non-secret settings per the type's config schema.")),
				opt("provider", str("Provider preset, e.g. rds or cloud_sql.")), opt("description", str("")), secretRefs()},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				body := cl.body("kind", "type", "name", "config", "provider", "description")
				if err := cl.refs(body); err != nil {
					return nil, err
				}
				return cl.do(http.MethodPost, b+"/connections", body)
			}},
		{Name: "update_connection", Title: "Change a connection", Kind: Write, Project: true, Perm: auth.ConnectionManage,
			Description: "Changes non-secret settings, or points secret fields at a secret store. Stored secrets stay as they are.",
			Params:      []Param{connectionArg, opt("config", obj("Non-secret settings (the whole config).")), opt("provider", str("")), opt("description", str("")), secretRefs(), expectedVersion()},
			Call: func(cl *call) (any, error) {
				p, err := onConnection("")(cl)
				if err != nil {
					return nil, err
				}
				body := cl.body("config", "provider", "description")
				if err := cl.refs(body); err != nil {
					return nil, err
				}
				return cl.withVersion(p, body)
			}},
		{Name: "delete_connection", Title: "Delete a connection", Kind: Destructive, Project: true, Perm: auth.ConnectionManage,
			Description: "Deletes a connection no pipeline uses (its secrets are destroyed).", Params: []Param{connectionArg}, Call: send(http.MethodDelete, onConnection(""))},

		// --- Runtimes ---
		{Name: "list_runtimes", Title: "List runtimes", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "Where connectors run (Kubernetes, Docker, agents), with reachability and capacity.", Call: get(at("/runtimes"))},
		{Name: "get_runtime", Title: "Get a runtime", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "A runtime's details and last test (credentials never shown).", Params: []Param{runtimeArg}, Call: get(onRuntime(""))},
		{Name: "test_runtime", Title: "Test a runtime", Kind: Read, Project: true, Perm: auth.RuntimeManage,
			Description: "Checks that BeziSync can reach the runtime and run connectors there.", Params: []Param{runtimeArg}, Call: send(http.MethodPost, onRuntime("/test"))},
		{Name: "update_runtime", Title: "Rename or cordon a runtime", Kind: Write, Project: true, Perm: auth.RuntimeManage,
			Description: "Renames a runtime, turns it off for new pipelines (running ones keep running) or sets its default Debezium image. Credentials are changed in the UI.",
			Params:      []Param{runtimeArg, opt("name", str("")), opt("enabled", flag("false: no new pipelines here.")), opt("default_image", str("Default Debezium image."))},
			Call: func(cl *call) (any, error) {
				p, err := onRuntime("")(cl)
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodPatch, p, cl.body("name", "enabled", "default_image"))
			}},
		{Name: "delete_runtime", Title: "Delete a runtime", Kind: Destructive, Project: true, Perm: auth.RuntimeManage,
			Description: "Deletes a runtime no live pipeline uses.", Params: []Param{runtimeArg}, Call: send(http.MethodDelete, onRuntime(""))},

		// --- Secret stores (read only: their credentials are entered in the UI) ---
		{Name: "list_secret_stores", Title: "Secret stores", Kind: Read, Project: true, Perm: auth.ConnectionView,
			Description: "Vault, AWS, Google or Kubernetes stores connections can reference (ref+<store>://<path>#<key>).", Call: get(at("/secret-stores"))},
		{Name: "check_secret_reference", Title: "Check a secret reference", Kind: Read, Project: true, Perm: auth.ConnectionManage,
			Description: "Confirms a reference resolves, without returning the value.",
			Params:      []Param{req("store_id", str("The store's id.")), req("path", str("Path in the store.")), opt("key", str("Key in the secret."))},
			Call: func(cl *call) (any, error) {
				p, err := cl.sub("store_id", "secret-stores")
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodPost, p+"/check-ref", cl.body("path", "key"))
			}},
		{Name: "list_secret_store_secrets", Title: "Secrets in a store", Kind: Read, Project: true, Perm: auth.ConnectionManage,
			Description: "The paths of a store's secrets (at most 1,000), to build a reference ref+<store>://<path>#<key>. Never reads a value.",
			Params:      []Param{req("store_id", str("The store's id."))},
			Call: func(cl *call) (any, error) {
				p, err := cl.sub("store_id", "secret-stores")
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, p+"/secrets", nil)
			}},
		{Name: "list_secret_keys", Title: "Keys of a secret", Kind: Read, Project: true, Perm: auth.ConnectionManage,
			Description: "The key names of one secret in a store; whole_value means it has none and a reference uses all of it. Never returns a value.",
			Params:      []Param{req("store_id", str("The store's id.")), req("path", str("Path in the store."))},
			Call: func(cl *call) (any, error) {
				p, err := cl.sub("store_id", "secret-stores")
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodPost, p+"/keys", cl.body("path"))
			}},

		// --- Alerts ---
		{Name: "list_alerts", Title: "Alerts", Kind: Read, Project: true, Perm: auth.AlertView,
			Description: "Pending, firing and resolved alerts.",
			Params:      []Param{opt("state", oneOf("Only this state.", "pending", "firing", "resolved")), opt("pipeline_id", str("Only this pipeline.")), opt("limit", num(""))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, b+"/alerts"+query("state", cl.s("state"), "pipeline_id", cl.s("pipeline_id"), "limit", cl.intArg("limit")), nil)
			}},
		{Name: "acknowledge_alert", Title: "Acknowledge an alert", Kind: Write, Project: true, Perm: auth.AlertAck,
			Description: "Says someone is on it.", Params: []Param{req("alert_id", str(""))}, Call: send(http.MethodPost, onSub("alert_id", "alerts", "/acknowledge"))},
		{Name: "list_alert_rules", Title: "Alert rules", Kind: Read, Project: true, Perm: auth.AlertView,
			Description: "When alerts fire and where they go.", Call: get(at("/alert-rules"))},
		{Name: "list_alert_kinds", Title: "Alert kinds", Kind: Read, Project: true, Perm: auth.AlertView,
			Description: "Conditions a rule can watch, with their parameters.", Call: get(at("/alert-kinds"))},
		{Name: "list_alert_channels", Title: "Alert channels", Kind: Read, Project: true, Perm: auth.AlertView,
			Description: "Where alerts are delivered (names and kinds; their URLs and keys are never shown). Channels are added in the UI.", Call: get(at("/alert-channels"))},
		{Name: "create_alert_rule", Title: "Add an alert rule", Kind: Write, Project: true, Perm: auth.AlertManage,
			Description: "Watches a condition (list_alert_kinds) and notifies channels.",
			Params: []Param{req("name", str("")), req("kind", str("A kind from list_alert_kinds.")), opt("params", obj("The kind's parameters.")),
				opt("severity", oneOf("", "info", "warning", "critical")), opt("for_seconds", num("How long the condition must hold.")),
				opt("channel_ids", strs("Channel ids.")), opt("match_labels", obj("Only pipelines with these labels.")), opt("notify_owner", flag("")), opt("enabled", flag(""))},
			Call: send(http.MethodPost, at("/alert-rules"), "name", "kind", "params", "severity", "for_seconds", "channel_ids", "match_labels", "notify_owner", "enabled")},
		{Name: "update_alert_rule", Title: "Change an alert rule", Kind: Write, Project: true, Perm: auth.AlertManage,
			Description: "Changes a rule's parameters, channels or turns it off.",
			Params: []Param{req("rule_id", str("")), opt("name", str("")), opt("params", obj("")), opt("severity", oneOf("", "info", "warning", "critical")),
				opt("for_seconds", num("")), opt("channel_ids", strs("")), opt("match_labels", obj("")), opt("notify_owner", flag("")), opt("enabled", flag(""))},
			Call: send(http.MethodPatch, onSub("rule_id", "alert-rules", ""), "name", "params", "severity", "for_seconds", "channel_ids", "match_labels", "notify_owner", "enabled")},
		{Name: "delete_alert_rule", Title: "Delete an alert rule", Kind: Destructive, Project: true, Perm: auth.AlertManage,
			Description: "Deletes a rule.", Params: []Param{req("rule_id", str(""))}, Call: send(http.MethodDelete, onSub("rule_id", "alert-rules", ""))},

		// --- Approvals ---
		{Name: "list_change_requests", Title: "Approval requests", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "Deploys waiting for approval, and past decisions.",
			Params:      []Param{opt("state", oneOf("", "pending", "approved", "rejected", "cancelled"))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodGet, b+"/change-requests"+query("state", cl.s("state")), nil)
			}},
		{Name: "approve_change_request", Title: "Approve and deploy", Kind: Write, Project: true, Perm: auth.PipelineApprove,
			Description: "Approves someone else's deploy: preflight runs again, then that exact version deploys.",
			Params:      []Param{req("request_id", str("")), opt("comment", str(""))}, Call: send(http.MethodPost, onSub("request_id", "change-requests", "/approve"), "comment")},
		{Name: "reject_change_request", Title: "Reject a deploy", Kind: Write, Project: true, Perm: auth.PipelineApprove,
			Description: "Rejects a pending deploy with a reason.", Params: []Param{req("request_id", str("")), opt("comment", str(""))},
			Call: send(http.MethodPost, onSub("request_id", "change-requests", "/reject"), "comment")},
		{Name: "cancel_change_request", Title: "Withdraw a request", Kind: Write, Project: true, Perm: auth.PipelineDeploy,
			Description: "Withdraws your own pending deploy.", Params: []Param{req("request_id", str(""))}, Call: send(http.MethodPost, onSub("request_id", "change-requests", "/cancel"))},

		// --- Templates ---
		{Name: "list_templates", Title: "Pipeline templates", Kind: Read, Project: true, Perm: auth.PipelineView,
			Description: "Starting points for new pipelines.", Call: get(at("/templates"))},
		{Name: "create_template", Title: "Save a template", Kind: Write, Project: true, Perm: auth.PipelineEdit,
			Description: "Saves a pipeline (or a spec) as a template, without its tables.",
			Params: []Param{req("name", str("Slug.")), req("title", str("")), opt("description", str("")), opt("from_pipeline_id", str("Copy this pipeline.")),
				opt("spec", pipelineSpec), opt("sink_id", str("")), opt("runtime_id", str(""))},
			Call: send(http.MethodPost, at("/templates"), "name", "title", "description", "from_pipeline_id", "spec", "sink_id", "runtime_id")},
		{Name: "update_template", Title: "Change a template", Kind: Write, Project: true, Perm: auth.PipelineEdit,
			Description: "Changes a template's title or description.", Params: []Param{req("template_id", str("")), opt("title", str("")), opt("description", str(""))},
			Call: send(http.MethodPatch, onSub("template_id", "templates", ""), "title", "description")},
		{Name: "delete_template", Title: "Delete a template", Kind: Destructive, Project: true, Perm: auth.PipelineEdit,
			Description: "Deletes a template.", Params: []Param{req("template_id", str(""))}, Call: send(http.MethodDelete, onSub("template_id", "templates", ""))},

		// --- Projects, policies and people ---
		{Name: "get_project_policies", Title: "Project policies", Kind: Read, Project: true, Perm: auth.ProjectView,
			Description: "What the project allows (runtimes, types, snapshot modes, sizes, approvals, transforms).", Call: get(at("/policies"))},
		{Name: "update_project_policies", Title: "Change project policies", Kind: Write, Project: true, Admin: true,
			Description: "Replaces the project's policies (send the whole object from get_project_policies with your changes).",
			Params:      []Param{req("policies", obj("The policies."))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodPut, b+"/policies", cl.args["policies"])
			}},
		{Name: "create_project", Title: "Create a project", Kind: Write, Admin: true,
			Description: "Creates an empty project.", Params: []Param{req("slug", str("Lowercase, digits and dashes.")), req("name", str("")), opt("description", str(""))},
			Call: send(http.MethodPost, fixed("/api/v1/projects"), "slug", "name", "description")},
		{Name: "update_project", Title: "Rename a project", Kind: Write, Project: true, Admin: true,
			Description: "Changes a project's name or description.", Params: []Param{opt("name", str("")), opt("description", str(""))},
			Call: func(cl *call) (any, error) {
				b, err := cl.base()
				if err != nil {
					return nil, err
				}
				return cl.do(http.MethodPatch, b, cl.body("name", "description"))
			}},
		{Name: "delete_project", Title: "Delete a project", Kind: Destructive, Project: true, Admin: true,
			Description: "Deletes a project without live pipelines.", Call: send(http.MethodDelete, at(""))},
		{Name: "list_project_members", Title: "Project members", Kind: Read, Project: true, Perm: auth.MemberManage,
			Description: "Who has which role in the project (and every Admin).", Call: get(at("/members"))},
		{Name: "set_project_member", Title: "Give someone a role", Kind: Write, Project: true, Perm: auth.MemberManage,
			Description: "Adds a person (by email) to the project or changes their role. People without an account sign in with single sign-on; passwords are set in the UI.",
			Params:      []Param{req("email", str("")), req("role", roleArg), opt("can_approve", flag("May approve deploys."))},
			Call:        send(http.MethodPut, at("/members"), "email", "role", "can_approve")},
		{Name: "remove_project_member", Title: "Remove someone from a project", Kind: Destructive, Project: true, Perm: auth.MemberManage,
			Description: "Removes a person's role in the project.", Params: []Param{req("user_id", str(""))}, Call: send(http.MethodDelete, onSub("user_id", "members", ""))},
		{Name: "list_users", Title: "People", Kind: Read, Admin: true,
			Description: "Everyone with an account, their projects and whether they are Admins.",
			Params:      []Param{opt("search", str("Name or email contains.")), opt("cursor", str("")), opt("limit", num(""))},
			Call: func(cl *call) (any, error) {
				return cl.do(http.MethodGet, "/api/v1/users"+query("q", cl.s("search"), "cursor", cl.s("cursor"), "limit", cl.intArg("limit")), nil)
			}},
		{Name: "update_user", Title: "Change a person", Kind: Write, Admin: true,
			Description: "Renames, makes Admin (or not), or deactivates (signs them out everywhere) a person.",
			Params:      []Param{req("user_id", str("")), opt("name", str("")), opt("is_superuser", flag("Admin.")), opt("is_active", flag("false: deactivate."))},
			Call: func(cl *call) (any, error) {
				return cl.do(http.MethodPatch, "/api/v1/users/"+cl.s("user_id"), cl.body("name", "is_superuser", "is_active"))
			}},

		// --- Feedback (Admins) ---
		{Name: "list_feedback", Title: "Feedback", Kind: Read, Admin: true,
			Description: "What people reported from the app.", Params: []Param{opt("status", oneOf("", "open", "resolved")), opt("cursor", str("")), opt("limit", num(""))},
			Call: func(cl *call) (any, error) {
				return cl.do(http.MethodGet, "/api/v1/feedback"+query("status", cl.s("status"), "cursor", cl.s("cursor"), "limit", cl.intArg("limit")), nil)
			}},
		{Name: "resolve_feedback", Title: "Resolve feedback", Kind: Write, Admin: true,
			Description: "Marks a report resolved (or open again).", Params: []Param{req("feedback_id", str("")), req("resolved", flag(""))},
			Call: func(cl *call) (any, error) {
				return cl.do(http.MethodPatch, "/api/v1/feedback/"+cl.s("feedback_id"), cl.body("resolved"))
			}},
	}
}
