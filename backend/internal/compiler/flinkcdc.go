package compiler

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/flinkhelper"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

// Flink CDC: a pipeline compiles to three files the helper reads from /opt/bezisync/config
// (pipeline.yaml, flink-conf.yaml, settings.json), secret files (the keystore and its password)
// and the JobManager's environment (every secret value; pipeline.yaml only names them).

// FlinkInput is what a Flink CDC compile needs besides the spec and the connections.
type FlinkInput struct {
	// Cluster is a custom cluster's resolved shape (services.FlinkSizeOf); nil for the presets.
	Cluster *engine.FlinkSize
	// RuntimeKind ("docker" or "kubernetes") decides the JobManager's address.
	RuntimeKind string
	// Columns are each spec table's columns as they were at deploy (nil when unknown). A table
	// that holds new columns back, leaves columns out or renames them is written column by column.
	Columns map[string][]string
	// Defaults are each table's columns that have a default in the source (nil when unknown).
	Defaults map[string][]string
	// Startup is artifacts.flink_startup (Start over), RestoreAfter artifacts.flink_restore_after.
	Startup      map[string]any
	RestoreAfter int64
	// Layout is artifacts.flink_layout (ADR 0012): per destination, the columns a start without a
	// saved position writes, in order. nil compiles every table as the spec says.
	Layout map[string][]domain.LayoutColumn
	// FreshStart: the compile planned a start without a saved position (settings fresh_start).
	FreshStart bool
	// Generation is artifacts.flink_layout.generation (ADR 0013): settings generation, so a saved
	// position from an older one restores without the schema coordinator's state.
	Generation int
	// StateKey, Keystore and KeystorePassword are the pipeline's own secrets.
	StateKey, Keystore, KeystorePassword string
}

// FlinkCompiled is a Flink CDC pipeline ready for a runtime.
type FlinkCompiled struct {
	Pipeline     string
	FlinkConf    string
	Settings     string
	Env          map[string]string
	Files        map[string]string
	Image        string
	Size         engine.FlinkSize
	Destinations map[string]string
	Tunnel       map[string]any
}

// FlinkStopGrace is how long a JobManager may take to stop (its helper saves the position in
// FlinkStopTimeout of it).
const (
	FlinkStopGrace   = 90
	FlinkStopTimeout = 60
)

// ConfigHash identifies what the cluster runs: any change recreates it.
func (c *FlinkCompiled) ConfigHash() string {
	raw, err := json.Marshal(map[string]any{"p": c.Pipeline, "c": c.FlinkConf, "s": c.Settings, "e": c.Env, "f": c.Files, "i": c.Image,
		"z": c.Size, "t": c.Tunnel})
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:16]
}

// Config are the files of /opt/bezisync/config.
func (c *FlinkCompiled) Config() map[string]string {
	return map[string]string{"pipeline.yaml": c.Pipeline, "flink-conf.yaml": c.FlinkConf, "settings.json": c.Settings}
}

// Topology is the cluster's shape for the runtime driver.
func (c *FlinkCompiled) Topology(helperImage string) *runtime.FlinkTopology {
	return &runtime.FlinkTopology{TaskManagers: c.Size.TaskManagers, JobManagerCPU: c.Size.JobManagerCPU, JobManagerMemory: c.Size.JobManagerMemory,
		TaskManagerCPU: c.Size.TaskManagerCPU, TaskManagerMemory: c.Size.TaskManagerMemory, HelperImage: helperImage, Config: c.Config(),
		StateSize: "512Mi"}
}

// flinkStartupModes maps the spec's snapshot mode to Flink CDC's scan.startup.mode.
var flinkStartupModes = map[string]string{"initial": "initial", "when_needed": "initial", "no_data": "latest-offset", "initial_only": "snapshot"}

// CompileFlink compiles a Flink CDC pipeline; image is the Flink CDC image to run.
func CompileFlink(c connectors.CompileContext, source, sink connectors.Resolved, image string, in FlinkInput) (*FlinkCompiled, error) {
	if err := noConfigExpressions(source, sink); err != nil {
		return nil, err
	}
	spec := c.Spec
	if image == "" {
		return nil, problem.New(422, "engine_unavailable", "No Flink CDC image is registered: an Admin adds one in Settings → Engine versions.")
	}
	src, err := connectors.GetSource(source.Type)
	if err != nil {
		return nil, err
	}
	snk, err := connectors.GetSink(sink.Type)
	if err != nil {
		return nil, err
	}
	fsrc, ok := src.(connectors.FlinkSource)
	fsnk, ok2 := snk.(connectors.FlinkSink)
	if !ok || !ok2 { // engine.Supports decides at creation; this is what is implemented
		return nil, problem.New(422, "engine_unsupported", fmt.Sprintf("Flink CDC can't run %s to %s.", source.Type, sink.Type))
	}
	if d, err := connectors.Describe("sink", sink.Type); err == nil && d != nil {
		if err := d.Sink.CheckNaming(d.Title, spec.Sink.Naming); err != nil {
			return nil, err
		}
	}
	tables := spec.Source.Tables.Include
	if len(tables) == 0 {
		return nil, problem.New(422, "no_tables", "Select at least one table.")
	}
	if err := connectors.FlinkTableNames(tables); err != nil {
		return nil, err
	}
	if !spec.Values.Empty() {
		return nil, problem.New(422, "engine_settings", "Value formats are Debezium options; Flink CDC writes each column with its own type.")
	}
	if spec.Snapshot.DelaySeconds != 0 || spec.Snapshot.Window != nil {
		return nil, problem.New(422, "engine_settings", "A copy delay or window is a Debezium option; Flink CDC copies as soon as it starts.")
	}
	fc := spec.FlinkCDC.Effective()
	if bad := engine.CheckFlinkSettings(fc.FlinkConf); len(bad) > 0 {
		var errs []map[string]any
		for _, k := range engine.SortedKeys(bad) {
			errs = append(errs, map[string]any{"loc": []string{"flink_cdc", "flink_conf", k}, "message": bad[k]})
		}
		return nil, problem.New(422, "flink_conf", "Some Flink settings can't be used.").WithErrors(errs)
	}
	size, ok := engine.FlinkSizes[spec.Policies.Resources]
	if !ok {
		size = engine.FlinkSizes["small"]
	}
	if spec.Policies.Resources == "custom" {
		if in.Cluster == nil {
			return nil, problem.New(422, "flink_cluster", "A custom Flink CDC cluster needs its shape resolved for the runtime.")
		}
		size = *in.Cluster
	}
	parallelism := size.Parallelism()

	env := map[string]string{}
	at := connectors.FlinkEndpoint{Host: connectors.PyStr(source.Config["host"]), Port: connectors.Int(source.Config, "port")}
	var tunnel map[string]any
	sshCfg, key, err := connectors.SSHOf(source.Config, source.Secrets)
	if err != nil {
		return nil, problem.New(422, "ssh_incomplete", err.Error())
	}
	if sshCfg != nil {
		// Every JobManager and TaskManager reaches the source through the tunnel as bz-tunnel.
		at = connectors.FlinkEndpoint{Host: "bz-tunnel", Port: connectors.TunnelPort}
		env["BZ_SSH_KEY"] = key
		tunnel = map[string]any{"host": connectors.PyStr(source.Config["host"]), "port": connectors.Int(source.Config, "port"),
			"ssh_host": sshCfg.Host, "ssh_port": sshCfg.Port, "ssh_user": sshCfg.User, "ssh_host_key": sshCfg.HostKey,
			"listen_port": connectors.TunnelPort, "image": config.Get().AgentImage}
	}
	srcBlock, err := fsrc.FlinkSource(source, c, at, parallelism)
	if err != nil {
		return nil, err
	}
	snkBlock, err := fsnk.FlinkSink(sink, c)
	if err != nil {
		return nil, err
	}
	for k, v := range srcBlock.Env {
		env[k] = v
	}
	for k, v := range snkBlock.Env {
		if _, taken := env[k]; taken {
			return nil, fmt.Errorf("compiler: source and destination both set %s", k)
		}
		env[k] = v
	}
	if err := startupOptions(&srcBlock.Options, spec, in.Startup, source.Type); err != nil {
		return nil, err
	}
	tuned, err := tuningSettings(engine.FlinkCDC, source.Type, sink.Type, spec.Tuning)
	if err != nil {
		return nil, err
	}
	pipelineTuning := connectors.FlinkOptions{}
	for _, s := range tuned {
		block, option := flinkTuningBlock(s.Key)
		target := map[string]*connectors.FlinkOptions{"source": &srcBlock.Options, "sink": &snkBlock.Options, "pipeline": &pipelineTuning}[block]
		if target == nil {
			return nil, fmt.Errorf("compiler: setting %s has no block", s.Key)
		}
		if err := ownedByBeziSync(s, target.Get(option) != ""); err != nil {
			return nil, err
		}
		target.Set(option, spec.Tuning[s.Key])
	}
	// Raw (expert) options, through the pass-through prefixes only; never over BeziSync's own or a
	// setting's. The YAML encoder quotes every value (F1).
	var refused []map[string]any
	for _, k := range advancedKeys(spec.Advanced) {
		block, option, _ := strings.Cut(k, ".")
		target := map[string]*connectors.FlinkOptions{"source": &srcBlock.Options, "sink": &snkBlock.Options}[block]
		managed := map[string]bool{}
		if target != nil && target.Get(option) != "" {
			managed[k] = true
		}
		if v := ClassifyFlinkAdvanced(k, spec.Advanced[k], managed); v.Refused() {
			refused = append(refused, map[string]any{"loc": []string{"advanced", k}, "message": v.Message})
			continue
		}
		if why := connectors.FlinkSourceIgnores(source.Type, option); block == "source" && why != "" {
			refused = append(refused, map[string]any{"loc": []string{"advanced", k}, "message": why})
			continue
		}
		target.Set(option, spec.Advanced[k])
	}
	if len(refused) > 0 {
		return nil, problem.New(422, "managed_property", "Some advanced properties cannot be set.").WithErrors(refused)
	}

	destinations, routes, err := flinkRoutes(c, fsrc, fsnk, source, sink)
	if err != nil {
		return nil, err
	}
	transforms, err := flinkTransforms(c, fsrc, source, in.Columns, in.Defaults, fc.NewColumns, snkBlock, destinations, in.Layout, nil)
	if err != nil {
		return nil, err
	}
	behavior := map[string]string{"apply": "lenient", "ignore": "ignore", "stop": "exception"}[fc.SchemaChanges]
	doc := yamlMap{
		{"source", optionsMap(srcBlock.Options)},
		{"sink", optionsMap(snkBlock.Options)},
		{"route", routes},
	}
	if len(transforms) > 0 {
		doc = append(doc, yamlKV{"transform", transforms})
	}
	pipeline := yamlMap{
		{"name", "bezisync " + c.Name},
		{"parallelism", strconv.Itoa(parallelism)},
		{"operator.uid.prefix", "bz-" + c.ShortID},
		{"schema.change.behavior", behavior},
		{"route-mode", "FIRST_MATCH"},
	}
	for _, o := range pipelineTuning {
		for _, kv := range pipeline {
			if kv.k == o[0] {
				return nil, problem.New(422, "tuning", "pipeline."+o[0]+" is set by BeziSync and can't be tuned.")
			}
		}
		pipeline = append(pipeline, yamlKV{o[0], o[1]})
	}
	doc = append(doc, yamlKV{"pipeline", pipeline})

	conf, err := flinkConf(c, in.RuntimeKind, size, fc)
	if err != nil {
		return nil, err
	}
	drivers := append(slices.Clone(srcBlock.Drivers), snkBlock.Drivers...)
	slices.Sort(drivers)
	settings, err := json.MarshalIndent(flinkhelper.Settings{PipelineID: c.PipelineID, ShortID: c.ShortID,
		Connectors: append(flinkConnectors(srcBlock.Connector, snkBlock.Connector), snkBlock.Extra...), Drivers: slices.Compact(drivers), TaskManagers: size.TaskManagers,
		Restore: "latest", RestoreAfter: in.RestoreAfter, FreshStart: in.FreshStart, Generation: in.Generation, StateDir: runtime.FlinkStateDir, KeepCheckpoints: 5, HealthPort: HTTPPort,
		StopTimeoutSeconds: FlinkStopTimeout}, "", "  ")
	if err != nil {
		return nil, err
	}
	stateEnv, err := flinkStateEnv()
	if err != nil {
		return nil, err
	}
	for k, v := range stateEnv {
		env[k] = v
	}
	env["BZ_STATE_KEY"] = in.StateKey
	files := map[string]string{
		"keystore.p12.b64": in.Keystore,
		flinkhelper.SecretSettingsFile: "security.ssl.internal.keystore-password: " + in.KeystorePassword + "\n" +
			"security.ssl.internal.key-password: " + in.KeystorePassword + "\n" +
			"security.ssl.internal.truststore-password: " + in.KeystorePassword + "\n",
	}
	return &FlinkCompiled{Pipeline: renderYAML(doc), FlinkConf: conf, Settings: string(settings) + "\n", Env: env, Files: files, Image: image,
		Size: size, Destinations: destinations, Tunnel: tunnel}, nil
}

// flinkRoutes sends each spec table to its destination, first match only.
func flinkRoutes(c connectors.CompileContext, fsrc connectors.FlinkSource, fsnk connectors.FlinkSink, source, sink connectors.Resolved) (map[string]string, []yamlMap, error) {
	spec := c.Spec
	destinations := map[string]string{}
	owner := map[string]string{}
	routed := domain.RouteTargets(spec)
	var routes []yamlMap
	for _, t := range spec.Source.Tables.Include {
		schema, table, _ := strings.Cut(t, ".")
		name := c.Destination(schema, table)
		to, merged := routed[t]
		if merged {
			name = to
		}
		dest := fsnk.FlinkDestination(sink, c, name)
		// Two tables meet in one destination only when a route sends them there.
		if other, taken := owner[dest]; taken && !(merged && routed[other] == to) {
			return nil, nil, problem.New(422, "destination_collision", fmt.Sprintf("%s and %s would both be written to %s. Change the naming pattern.", other, t, dest))
		}
		owner[dest], destinations[t] = t, dest
		routes = append(routes, yamlMap{{"source-table", fsrc.FlinkTableID(source, t)}, {"sink-table", dest}})
	}
	return destinations, routes, nil
}

// FlinkProduced is, for each spec table, the columns it writes in the order the spec writes them
// and its destination table (ADR 0012: what the layout planner compares with the destination).
// columns and defaults are the tables' source columns (as for the compile).
func FlinkProduced(c connectors.CompileContext, source, sink connectors.Resolved, columns, defaults map[string][]string) (map[string][]domain.FlinkColumn, map[string]string, error) {
	src, err := connectors.GetSource(source.Type)
	if err != nil {
		return nil, nil, err
	}
	snk, err := connectors.GetSink(sink.Type)
	if err != nil {
		return nil, nil, err
	}
	fsrc, ok := src.(connectors.FlinkSource)
	fsnk, ok2 := snk.(connectors.FlinkSink)
	if !ok || !ok2 {
		return nil, nil, problem.New(422, "engine_unsupported", fmt.Sprintf("Flink CDC can't run %s to %s.", source.Type, sink.Type))
	}
	destinations, _, err := flinkRoutes(c, fsrc, fsnk, source, sink)
	if err != nil {
		return nil, nil, err
	}
	snkBlock, err := fsnk.FlinkSink(sink, c)
	if err != nil {
		return nil, nil, err
	}
	produced := map[string][]domain.FlinkColumn{}
	if _, err := flinkTransforms(c, fsrc, source, columns, defaults, c.Spec.FlinkCDC.Effective().NewColumns, snkBlock, destinations, nil, produced); err != nil {
		return nil, nil, err
	}
	return produced, destinations, nil
}

// flinkConnectors are the connector JARs: the source's, the destination's, and what they need.
func flinkConnectors(source, sink string) []string {
	out := []string{source, sink}
	if sink == "iceberg" {
		out = append(out, "hadoop-client-api", "hadoop-client-runtime", "commons-logging")
	}
	return out
}

// startupOptions sets how a pipeline without a saved position starts: Start over's choice
// (until the next deploy), otherwise the spec's snapshot mode.
func startupOptions(o *connectors.FlinkOptions, spec domain.PipelineSpec, startup map[string]any, sourceType string) error {
	mode, ok := flinkStartupModes[spec.Snapshot.Mode]
	if !ok {
		return problem.New(422, "engine_settings", "Copying every table on each start is a Debezium option: a Flink CDC pipeline resumes from its saved position.")
	}
	if m, _ := startup["mode"].(string); m != "" {
		mode = m
	}
	o.Set("scan.startup.mode", mode)
	if mode == "timestamp" {
		if sourceType != "mysql" && sourceType != "sqlserver" {
			return problem.New(422, "engine_unsupported", "Only MySQL and SQL Server sources can start from a point in time.")
		}
		ms := startupMillis(startup["timestamp_ms"])
		if ms <= 0 {
			return problem.New(422, "engine_settings", "Starting from a point in time needs the time.")
		}
		o.Set("scan.startup.timestamp-millis", strconv.FormatInt(ms, 10))
	}
	if spec.Snapshot.ChunkSize != domain.DefaultSpec().Snapshot.ChunkSize {
		o.Set("scan.incremental.snapshot.chunk.size", strconv.Itoa(spec.Snapshot.ChunkSize))
	}
	o.Set("scan.snapshot.fetch.size", strconv.Itoa(spec.Snapshot.FetchSize))
	return nil
}

// startupMillis reads the stored start time, however the artifact was decoded.
func startupMillis(v any) int64 {
	switch x := v.(type) {
	case json.Number:
		n, err := x.Int64()
		if err != nil {
			f, _ := x.Float64()
			return int64(f)
		}
		return n
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

// flinkConf is flink-conf.yaml: BeziSync's settings (never overridable) and the pipeline's
// chosen ones.
func flinkConf(c connectors.CompileContext, runtimeKind string, size engine.FlinkSize, fc domain.FlinkCDCSpec) (string, error) {
	lines := [][2]string{
		{"jobmanager.rpc.address", runtime.FlinkJobManagerHost(runtimeKind, c.ShortID)},
		{"jobmanager.rpc.port", strconv.Itoa(runtime.FlinkRPCPort)},
		{"blob.server.port", strconv.Itoa(runtime.FlinkBlobPort)},
		{"jobmanager.bind-host", "0.0.0.0"},
		{"taskmanager.bind-host", "0.0.0.0"},
		// The REST API (job submission, savepoints) answers only the helper beside it.
		{"rest.address", "127.0.0.1"},
		{"rest.bind-address", "127.0.0.1"},
		{"jobmanager.memory.process.size", memSize(size.JobManagerMemory)},
		{"taskmanager.memory.process.size", memSize(size.TaskManagerMemory)},
		{"taskmanager.numberOfTaskSlots", strconv.Itoa(size.SlotsPerTaskManager())},
		{"parallelism.default", strconv.Itoa(size.Parallelism())},
		{"pipeline.max-parallelism", "128"},
		{"execution.checkpointing.interval", strconv.Itoa(fc.CommitSeconds) + " s"},
		{"execution.checkpointing.storage", "filesystem"},
		{"execution.checkpointing.dir", "file://" + runtime.FlinkStateDir + "/cp"},
		{"execution.checkpointing.savepoint-dir", "file://" + runtime.FlinkStateDir + "/sp"},
		{"execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION"},
		{"execution.checkpointing.num-retained", "3"},
		{"state.backend.type", "hashmap"},
		{"classloader.resolve-order", "parent-first"},
		// Traffic between the JobManager and TaskManagers: mutual TLS with the pipeline's own
		// keystore (passwords in the secret settings).
		{"security.ssl.internal.enabled", "true"},
		{"security.ssl.internal.keystore", "/tmp/bezisync/keystore.p12"},
		{"security.ssl.internal.truststore", "/tmp/bezisync/keystore.p12"},
		{"security.ssl.internal.keystore-type", "PKCS12"},
		{"security.ssl.internal.truststore-type", "PKCS12"},
		// The key is ECDSA P-256; Flink's default suite is RSA only.
		{"security.ssl.protocol", "TLSv1.2"},
		{"security.ssl.algorithms", "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"},
	}
	chosen := map[string]string{}
	for k, v := range engine.FlinkDefaults {
		chosen[k] = v
	}
	for k, v := range fc.FlinkConf {
		chosen[k] = v
	}
	for _, k := range engine.SortedKeys(chosen) {
		lines = append(lines, [2]string{k, chosen[k]})
	}
	var b strings.Builder
	b.WriteString("# Generated by BeziSync. Do not edit: changes are overwritten on deploy.\n")
	for _, kv := range lines {
		if strings.ContainsAny(kv[1], "\n\r") {
			return "", fmt.Errorf("compiler: Flink setting %s has a line break", kv[0])
		}
		b.WriteString(kv[0] + ": " + kv[1] + "\n")
	}
	return b.String(), nil
}

// memSize is a Kubernetes quantity ("2Gi", "1536Mi") as Flink writes memory ("2048m").
func memSize(q string) string {
	return strconv.FormatInt(runtime.QuantityMem(q)>>20, 10) + "m"
}

// flinkStateEnv is where the helper stores saved positions: the state database without its
// credentials (BZ_STATE_DSN) and those apart.
func flinkStateEnv() (map[string]string, error) {
	u, err := url.Parse(db.NormalizeURL(config.Get().ConnectorStateDatabaseURL()))
	if err != nil {
		return nil, err
	}
	user := u.User.Username()
	password, _ := u.User.Password()
	u.User = nil
	return map[string]string{"BZ_STATE_DSN": u.String(), "BZ_STATE_USER": user, "BZ_STATE_PASSWORD": password}, nil
}
