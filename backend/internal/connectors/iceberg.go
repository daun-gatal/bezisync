package connectors

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud/s3" // S3 file IO for checks and the Events tab
	"github.com/apache/iceberg-go/table"
	icebergutils "github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// Apache Iceberg sink: one Iceberg table per source table, through a REST catalog or AWS Glue,
// written by the Debezium Server Iceberg build (github.com/memiiso/debezium-server-iceberg).
// The connector receives Kafka Connect structs (format "connect"), flattened by the
// row-after-change step with __op and __source_ts_ms, creates each table on its first change
// (row identity = the record key) and adds new columns as they appear. BeziSync checks and
// reads the catalog with iceberg-go.

type icebergSink struct{ BaseSink }

func init() { Register(&icebergSink{BaseSink{Title: "Apache Iceberg"}}) }

func (*icebergSink) Kind() string { return "sink" }
func (*icebergSink) Type() string { return "iceberg" }

type icebergConfig struct {
	CatalogType, URI, Warehouse, Namespace string
	Auth, ClientID, Scope, OAuthURI        string
	SigningName, Region, Storage           string
	S3Endpoint, AccessKey                  string
	PathStyle                              bool
	ClientSecret, Token, SecretKey         string
}

func icebergCfg(conn Resolved) (icebergConfig, error) {
	d, _ := Describe("sink", "iceberg")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return icebergConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return icebergConfig{}, err
	}
	c := icebergConfig{CatalogType: Str(cfg, "catalog_type"), URI: Str(cfg, "uri"), Warehouse: Str(cfg, "warehouse"), Namespace: Str(cfg, "namespace"),
		Auth: Str(cfg, "auth"), ClientID: Str(cfg, "client_id"), Scope: Str(cfg, "scope"), OAuthURI: Str(cfg, "oauth2_server_uri"),
		SigningName: Str(cfg, "signing_name"), Region: Str(cfg, "region"), Storage: Str(cfg, "storage"), S3Endpoint: Str(cfg, "s3_endpoint"),
		AccessKey: Str(cfg, "access_key_id"), PathStyle: Bool(cfg, "path_style"),
		ClientSecret: Str(sec, "client_secret"), Token: Str(sec, "token"), SecretKey: Str(sec, "secret_access_key")}
	if c.CatalogType == "glue" {
		c.Auth = "none"
	}
	return c, c.validate()
}

// validate checks what the schema cannot: fields each choice needs, and values that would
// break application.properties.
func (c icebergConfig) validate() error {
	fail := func(field, msg string) error {
		return problem.New(422, "iceberg_config", msg).WithErrors([]map[string]any{{"loc": []string{"config", field}, "message": msg}})
	}
	for field, v := range map[string]string{"uri": c.URI, "warehouse": c.Warehouse, "client_id": c.ClientID, "scope": c.Scope,
		"oauth2_server_uri": c.OAuthURI, "region": c.Region, "s3_endpoint": c.S3Endpoint, "access_key_id": c.AccessKey} {
		if strings.Contains(v, "${") || strings.ContainsAny(v, "\r\n") {
			return fail(field, "Remove '${' and line breaks from this value.")
		}
	}
	switch {
	case c.CatalogType == "rest" && c.URI == "":
		return fail("uri", "A REST catalog needs its URL.")
	case c.CatalogType == "rest" && !strings.HasPrefix(c.URI, "http://") && !strings.HasPrefix(c.URI, "https://"):
		return fail("uri", "The catalog URL starts with http:// or https://.")
	case c.Auth == "oauth2" && (c.ClientID == "" || c.ClientSecret == ""):
		return fail("client_id", "OAuth2 sign-in needs a client ID and secret.")
	case c.Auth == "token" && c.Token == "":
		return fail("auth", "Bearer-token sign-in needs the token.")
	case (c.Auth == "sigv4" || c.CatalogType == "glue") && c.region() == "":
		return fail("region", "AWS request signing needs the region.")
	case c.AccessKey != "" && c.SecretKey == "":
		return fail("access_key_id", "An access key ID needs its secret access key.")
	case c.CatalogType == "glue" && !strings.HasPrefix(c.Warehouse, "s3://") && !strings.HasPrefix(c.Warehouse, "s3a://"):
		return fail("warehouse", "A Glue warehouse is an s3:// location.")
	}
	return nil
}

func (c icebergConfig) region() string {
	if c.Region != "" {
		return c.Region
	}
	if c.S3Endpoint != "" {
		return "us-east-1"
	}
	return ""
}

func (c icebergConfig) namespace() table.Identifier { return strings.Split(c.Namespace, ".") }

// --- Debezium Server configuration ---

func (s *icebergSink) CompileSink(conn Resolved, c CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := icebergCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	opts, err := SinkOptions("iceberg", c.Spec)
	if err != nil {
		return nil, nil, err
	}
	partitions, err := icebergPartitions(c, opts)
	if err != nil {
		return nil, nil, err
	}
	const p = "debezium.sink.iceberg."
	props := map[string]string{
		"debezium.sink.type":                    "iceberg",
		p + "catalog-name":                      "bezisync",
		p + "type":                              cfg.CatalogType,
		p + "warehouse":                         cfg.Warehouse,
		p + "table-namespace":                   cfg.Namespace,
		p + "destination-lowercase-table-names": "true",
		p + "write.format.default":              "parquet",
		p + "format-version":                    Str(opts, "format_version"),
		p + "upsert":                            fmt.Sprint(Str(opts, "write_mode") != "append"),
		p + "upsert-keep-deletes":               fmt.Sprint(Str(opts, "write_mode") == "upsert_keep_deletes"),
	}
	env := map[string]string{}
	if cfg.CatalogType == "rest" {
		props[p+"uri"] = cfg.URI
		switch cfg.Auth {
		case "oauth2":
			props[p+"credential"] = "${BZ_ICEBERG_CREDENTIAL}"
			env["BZ_ICEBERG_CREDENTIAL"] = cfg.ClientID + ":" + cfg.ClientSecret
			if cfg.Scope != "" {
				props[p+"scope"] = cfg.Scope
			}
			if cfg.OAuthURI != "" {
				props[p+"oauth2-server-uri"] = cfg.OAuthURI
			}
		case "token":
			props[p+"token"] = "${BZ_ICEBERG_TOKEN}"
			env["BZ_ICEBERG_TOKEN"] = cfg.Token
		case "sigv4":
			props[p+"rest.sigv4-enabled"] = "true"
			props[p+"rest.signing-name"] = cfg.SigningName
			props[p+"rest.signing-region"] = cfg.region()
		}
	}
	if cfg.Storage == "vended" && cfg.CatalogType == "rest" {
		props[p+"header.X-Iceberg-Access-Delegation"] = "vended-credentials"
	} else if cfg.S3Endpoint != "" {
		props[p+"s3.endpoint"] = cfg.S3Endpoint
		props[p+"s3.path-style-access"] = fmt.Sprint(cfg.PathStyle)
	}
	if r := cfg.region(); r != "" {
		props[p+"client.region"] = r
		env["AWS_REGION"] = r
	}
	// The AWS default chain serves S3, Glue and request signing alike; empty keys leave it to
	// the runtime's IAM role.
	if cfg.AccessKey != "" {
		env["AWS_ACCESS_KEY_ID"], env["AWS_SECRET_ACCESS_KEY"] = cfg.AccessKey, cfg.SecretKey
	}
	switch Str(opts, "commit_interval") {
	case "now":
		props["debezium.sink.batch.batch-size-wait"] = "NoBatchSizeWait"
	default:
		wait := icebergCommitInterval(Str(opts, "commit_interval"))
		props["debezium.sink.batch.batch-size-wait"] = "MaxBatchSizeWait"
		props["debezium.sink.batch.batch-size-wait.max-wait-ms"] = fmt.Sprint(wait.Milliseconds())
		props["debezium.sink.batch.batch-size-wait.wait-interval-ms"] = fmt.Sprint(min(wait/6, 10*time.Second).Milliseconds())
	}
	for topic, expr := range partitions {
		props[p+"partition-by."+topic] = expr
	}
	return props, env, nil
}

func icebergCommitInterval(option string) time.Duration {
	switch option {
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "now":
		return 0
	}
	return time.Minute
}

// DeliveryDelay: changes wait up to the commit interval, so health does not call that lagging.
func (s *icebergSink) DeliveryDelay(options map[string]any) time.Duration {
	return icebergCommitInterval(Str(options, "commit_interval"))
}

var icebergColumnRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// icebergPartitions is the build's per-table partition-by (ADR 0005 R1, R2), keyed by the event's
// destination after routes (debeziumDestinationFields). The build reads the value as a list: the
// comma inside bucket(…) and truncate(…) is escaped.
func icebergPartitions(c CompileContext, opts map[string]any) (map[string]string, error) {
	byTopic, _, err := debeziumDestinationFields(c, opts)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(byTopic))
	for topic, fields := range byTopic {
		out[topic] = debeziumPartitionBy(fields)
	}
	return out, nil
}

// debeziumDestinationFields is how Debezium partitions each destination (the event's topic after
// routes), and which topic each captured table goes to. A destination a table Partition by lists
// goes to gets its fields (tables routed together must list the same: 422 otherwise); any other
// gets the automatic default of the tables sent there (debeziumAutomatic) when they agree, and
// stays unpartitioned when they don't.
func debeziumDestinationFields(c CompileContext, opts map[string]any) (map[string][]PartitionField, map[string]string, error) {
	listed, err := IcebergPartitionBy(c.Spec, opts)
	if err != nil {
		return nil, nil, err
	}
	routed := domain.RouteTargets(c.Spec)
	topicOf := map[string]string{}
	listedBy, autoBy := map[string]string{}, map[string][]string{}
	out := map[string][]PartitionField{}
	auto := map[string][]PartitionField{}
	for _, t := range c.Spec.Source.Tables.Include {
		schema, name, _ := strings.Cut(t, ".")
		topic := c.Destination(schema, name)
		if to, ok := routed[t]; ok {
			topic = to
		}
		topicOf[t] = topic
		if fields, ok := listed[t]; ok {
			if prev, seen := listedBy[topic]; seen && debeziumPartitionBy(out[topic]) != debeziumPartitionBy(fields) {
				return nil, nil, problem.New(422, "engine_settings", fmt.Sprintf("%s and %s are routed to %s with different partitioning: give them the same.", prev, t, topic))
			}
			out[topic], listedBy[topic] = fields, t
			continue
		}
		fields := debeziumAutomatic(opts, t)
		if _, seen := autoBy[topic]; !seen {
			auto[topic] = fields
		} else if debeziumPartitionBy(auto[topic]) != debeziumPartitionBy(fields) {
			auto[topic] = nil // tables that disagree: none of them applies
		}
		autoBy[topic] = append(autoBy[topic], t)
	}
	for topic, fields := range auto {
		if _, taken := listedBy[topic]; !taken && len(fields) > 0 {
			out[topic] = fields
		}
	}
	return out, topicOf, nil
}

// debeziumPartitionBy writes fields as the build's partition-by list: a column, year(c), month(c),
// day(c), hour(c), bucket(c\,N), truncate(c\,N) (column first: IcebergUtil.transformArgPair).
func debeziumPartitionBy(fields []PartitionField) string {
	parts := make([]string, len(fields))
	for i, f := range fields {
		switch f.Transform {
		case "identity":
			parts[i] = f.Column
		case "bucket", "truncate":
			parts[i] = fmt.Sprintf(`%s(%s\,%d)`, f.Transform, f.Column, f.N)
		default:
			parts[i] = f.Transform + "(" + f.Column + ")"
		}
	}
	return strings.Join(parts, ",")
}

// Destination is the table the build writes: the namespace, then the routed name lower-cased
// with dots as underscores.
func (s *icebergSink) Destination(conn Resolved, _ CompileContext, routed string) string {
	ns := Str(conn.Config, "namespace")
	if ns == "" {
		ns = "cdc"
	}
	return ns + "." + strings.ToLower(strings.ReplaceAll(routed, ".", "_"))
}

func icebergTableIdent(cfg icebergConfig, destination string) table.Identifier {
	name := destination[strings.LastIndex(destination, ".")+1:]
	return append(append(table.Identifier{}, cfg.namespace()...), name)
}

// --- catalog access (checks, prepare, cleanup, Events tab) ---

func (c icebergConfig) awsConfig(ctx context.Context) (aws.Config, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if r := c.region(); r != "" {
		opts = append(opts, awsconfig.WithRegion(r))
	}
	if c.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")))
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	return GuardAWS(ac, 20*time.Second), err
}

// ioProps are the file IO properties for data files when the catalog does not vend credentials.
func (c icebergConfig) ioProps() iceberg.Properties {
	props := iceberg.Properties{}
	if c.S3Endpoint != "" {
		props[icebergio.S3EndpointURL] = c.S3Endpoint
		props[icebergio.S3ForceVirtualAddressing] = fmt.Sprint(!c.PathStyle)
	}
	if r := c.region(); r != "" {
		props[icebergio.S3Region] = r
	}
	if c.AccessKey != "" {
		props[icebergio.S3AccessKeyID], props[icebergio.S3SecretAccessKey] = c.AccessKey, c.SecretKey
	}
	return props
}

// open connects to the catalog through the SSRF guard. The returned context carries the AWS
// configuration file IO uses.
func (c icebergConfig) open(ctx context.Context) (catalog.Catalog, context.Context, error) {
	awsCfg, err := c.awsConfig(ctx)
	if err != nil {
		return nil, ctx, err
	}
	ctx = icebergutils.WithAwsConfig(ctx, &awsCfg)
	if c.CatalogType == "glue" {
		cat, err := glue.NewCatalog(glue.WithAwsConfig(awsCfg))
		return cat, ctx, err
	}
	opts := []rest.Option{rest.WithWarehouseLocation(c.Warehouse), rest.WithCustomTransport(ssrf.HTTPClient(0).Transport),
		rest.WithAdditionalProps(c.ioProps())}
	switch c.Auth {
	case "oauth2":
		opts = append(opts, rest.WithCredential(c.ClientID+":"+c.ClientSecret))
		if c.Scope != "" {
			opts = append(opts, rest.WithScope(c.Scope))
		}
		if c.OAuthURI != "" {
			u, err := url.Parse(c.OAuthURI)
			if err != nil {
				return nil, ctx, err
			}
			opts = append(opts, rest.WithAuthURI(u))
		}
	case "token":
		opts = append(opts, rest.WithOAuthToken(c.Token))
	case "sigv4":
		opts = append(opts, rest.WithSigV4RegionSvc(c.region(), c.SigningName), rest.WithAwsConfig(awsCfg))
	}
	if c.Storage == "vended" {
		opts = append(opts, rest.WithHeaders(map[string]string{"X-Iceberg-Access-Delegation": "vended-credentials"}))
	}
	cat, err := rest.NewCatalog(ctx, "bezisync", c.URI, opts...)
	return cat, ctx, err
}

// icebergProblem classifies a catalog error for check messages.
func icebergProblem(err error) (code, title, category, message string) {
	text := strings.ToLower(err.Error())
	var blocked *ssrf.BlockedAddress
	switch {
	case errors.As(err, &blocked):
		return "iceberg_catalog_blocked", "Reach the catalog", checks.CatNetwork, blocked.Error()
	case strings.Contains(text, "401") || strings.Contains(text, "403") || strings.Contains(text, "unauthorized") ||
		strings.Contains(text, "forbidden") || strings.Contains(text, "not authorized") || strings.Contains(text, "accessdenied") ||
		strings.Contains(text, "invalid_client") || strings.Contains(text, "signature"):
		return "iceberg_auth", "Sign in", checks.CatAuth, "The catalog refused the credentials."
	case strings.Contains(text, "no such host") || strings.Contains(text, "connection refused") || strings.Contains(text, "timeout") ||
		strings.Contains(text, "deadline exceeded"):
		return "iceberg_catalog_unreachable", "Reach the catalog", checks.CatNetwork, "Could not reach the catalog."
	}
	return "iceberg_catalog", "Catalog", checks.CatDestination, "The catalog returned an error."
}

func (s *icebergSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	run := &checks.Run{}
	cfg, err := icebergCfg(conn)
	if err != nil {
		var p *problem.Problem
		if errors.As(err, &p) {
			run.Add(checks.Check{Code: "iceberg_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
				Message: p.Detail})
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	cat, ctx, err := cfg.open(ctx)
	if err == nil {
		_, err = cat.ListNamespaces(ctx, nil)
	}
	if err != nil {
		code, title, category, msg := icebergProblem(err)
		c := checks.Check{Code: code, Title: title, Status: checks.Fail, Category: category, Blocking: true, Message: msg,
			Technical: firstRunes(err.Error(), 300)}
		if code == "iceberg_auth" {
			c.Fix = &checks.Fix{Kind: "text", Who: "catalog admin", Code: icebergAuthFix(conn.Provider)}
		}
		run.Add(c)
		return checks.Summarize(run.Checks, started), nil
	}
	run.Passed("iceberg_catalog", "Catalog", checks.CatDestination, "Signed in to the catalog.", "")
	ns := cfg.namespace()
	exists, err := cat.CheckNamespaceExists(ctx, ns)
	switch {
	case err != nil:
		run.Add(checks.Check{Code: "iceberg_namespace", Title: "Namespace", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: fmt.Sprintf("Could not look up namespace %s.", cfg.Namespace), Technical: firstRunes(err.Error(), 300)})
		return checks.Summarize(run.Checks, started), nil
	case exists:
		run.Passed("iceberg_namespace", "Namespace", checks.CatDestination, fmt.Sprintf("Namespace %s exists.", cfg.Namespace), "")
	default:
		run.Passed("iceberg_namespace", "Namespace", checks.CatDestination, fmt.Sprintf("Namespace %s does not exist yet; BeziSync creates it on deploy.", cfg.Namespace), "")
	}
	if exists {
		icebergWriteCheck(ctx, run, cat, cfg)
	}
	return checks.Summarize(run.Checks, started), nil
}

func icebergAuthFix(provider string) string {
	switch provider {
	case "s3_tables":
		return "Allow s3tables:GetTableBucket, s3tables:*Namespace*, s3tables:*Table*, s3tables:PutTableData and s3tables:GetTableData on the table bucket for the BeziSync and runtime principals."
	case "glue":
		return "Allow glue:GetDatabase(s), glue:CreateDatabase, glue:GetTable(s), glue:CreateTable, glue:UpdateTable and s3:GetObject/PutObject/DeleteObject/ListBucket on the warehouse."
	case "open_catalog":
		return "Grant the service connection's principal role CATALOG_MANAGE_CONTENT (or TABLE_WRITE_DATA and NAMESPACE_CREATE) on the catalog."
	case "unity":
		return "Grant the token's principal USE CATALOG, USE SCHEMA, CREATE SCHEMA, CREATE TABLE and MODIFY on the catalog; enable external data access on the metastore."
	}
	return "Give the credentials permission to list and create namespaces, and to create, load and commit tables."
}

// icebergWriteCheck proves the connector's write path: it creates a one-row table in the
// namespace, reads it back and drops it.
func icebergWriteCheck(ctx context.Context, run *checks.Run, cat catalog.Catalog, cfg icebergConfig) {
	ident := append(append(table.Identifier{}, cfg.namespace()...), fmt.Sprintf("bezisync_check_%d", time.Now().UnixNano()%1e9))
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "checked_at", Type: iceberg.PrimitiveTypes.String, Required: false})
	fail := func(msg string, err error) {
		run.Add(checks.Check{Code: "iceberg_write", Title: "Write a test table", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: msg, Technical: firstRunes(err.Error(), 300),
			Why: "The connector creates tables and writes Parquet files with these credentials."})
	}
	var opts []catalog.CreateTableOpt
	if cfg.CatalogType == "glue" {
		opts = append(opts, catalog.WithLocation(strings.TrimRight(cfg.Warehouse, "/")+"/"+strings.Join(ident, "/")))
	}
	tbl, err := cat.CreateTable(ctx, ident, schema, opts...)
	if err != nil {
		fail("Could not create a table in the namespace.", err)
		return
	}
	defer func() { _, _ = icebergDrop(context.WithoutCancel(ctx), cat, ident) }()
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "checked_at", Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	b := array.NewStringBuilder(memory.DefaultAllocator)
	b.Append(pytime.ISOFormat(time.Now()))
	col := b.NewArray()
	defer col.Release()
	at := array.NewTableFromSlice(arrowSchema, [][]arrow.Array{{col}})
	defer at.Release()
	tx := tbl.NewTransaction()
	if err := tx.AppendTable(ctx, at, 1, nil); err != nil {
		fail("Could not write a data file to the warehouse.", err)
		return
	}
	if tbl, err = tx.Commit(ctx); err != nil {
		fail("Could not commit to the table.", err)
		return
	}
	got, err := tbl.Scan().ToArrowTable(ctx)
	if err != nil {
		fail("Could not read the test table back.", err)
		return
	}
	rows := got.NumRows()
	got.Release()
	if rows != 1 {
		fail("The test table did not return the row written.", fmt.Errorf("%d rows", rows))
		return
	}
	run.Passed("iceberg_write", "Write a test table", checks.CatDestination, "Created a table, wrote and read a row, and dropped it.", "")
}

// icebergDrop drops a table, purging its files where the catalog allows it.
func icebergDrop(ctx context.Context, cat catalog.Catalog, ident table.Identifier) (purged bool, err error) {
	if r, ok := cat.(*rest.Catalog); ok {
		if r.PurgeTable(ctx, ident) == nil {
			return true, nil
		}
	}
	return false, cat.DropTable(ctx, ident)
}

func (s *icebergSink) PrepareSink(ctx context.Context, conn Resolved, _ CompileContext, _ map[string]string) ([]ArtifactResult, error) {
	cfg, err := icebergCfg(conn)
	if err != nil {
		return nil, err
	}
	cat, ctx, err := cfg.open(ctx)
	if err != nil {
		return []ArtifactResult{{Kind: "namespace", Name: cfg.Namespace, Outcome: "failed", Detail: firstRunes(err.Error(), 200)}}, nil
	}
	exists, err := cat.CheckNamespaceExists(ctx, cfg.namespace())
	if err != nil {
		return []ArtifactResult{{Kind: "namespace", Name: cfg.Namespace, Outcome: "failed", Detail: firstRunes(err.Error(), 200)}}, nil
	}
	if exists {
		return []ArtifactResult{{Kind: "namespace", Name: cfg.Namespace, Outcome: "exists", Detail: "Tables are created by their first change."}}, nil
	}
	if err := cat.CreateNamespace(ctx, cfg.namespace(), iceberg.Properties{}); err != nil && !errors.Is(err, catalog.ErrNamespaceAlreadyExists) {
		return []ArtifactResult{{Kind: "namespace", Name: cfg.Namespace, Outcome: "failed", Detail: firstRunes(err.Error(), 200)}}, nil
	}
	return []ArtifactResult{{Kind: "namespace", Name: cfg.Namespace, Outcome: "created", Detail: "Tables are created by their first change."}}, nil
}

func (s *icebergSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := icebergCfg(conn)
	if err != nil {
		return nil, err
	}
	cat, ctx, err := cfg.open(ctx)
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	for _, dest := range sortedUnique(destinations) {
		ident := icebergTableIdent(cfg, dest)
		purged, err := icebergDrop(ctx, cat, ident)
		switch {
		case errors.Is(err, catalog.ErrNoSuchTable):
			results = append(results, ArtifactResult{Kind: "table", Name: dest, Outcome: "not_found"})
		case err != nil:
			results = append(results, ArtifactResult{Kind: "table", Name: dest, Outcome: "failed", Detail: firstRunes(err.Error(), 200)})
		case purged:
			results = append(results, ArtifactResult{Kind: "table", Name: dest, Outcome: "removed"})
		default:
			results = append(results, ArtifactResult{Kind: "table", Name: dest, Outcome: "removed", Detail: "The catalog kept the data files."})
		}
	}
	return results, nil
}

// Tail reads the newest rows: those whose change happened in the last day (__source_ts_ms
// prunes files by their statistics), newest first.
func (s *icebergSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := icebergCfg(conn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, max(timeout, 20*time.Second))
	defer cancel()
	cat, ctx, err := cfg.open(ctx)
	if err != nil {
		return nil, err
	}
	tbl, err := cat.LoadTable(ctx, icebergTableIdent(cfg, destination))
	if errors.Is(err, catalog.ErrNoSuchTable) {
		return []TailRecord{}, nil
	}
	if err != nil {
		return nil, err
	}
	var opts []table.ScanOption
	if _, ok := tbl.Schema().FindFieldByNameCaseInsensitive("__source_ts_ms"); ok {
		since := iceberg.Timestamp(time.Now().Add(-24 * time.Hour).UnixMicro())
		opts = append(opts, table.WithRowFilter(iceberg.GreaterThanEqual(iceberg.Reference("__source_ts_ms"), since)))
	}
	opts = append(opts, table.WithLimit(int64(limit*50)))
	_, batches, err := tbl.Scan(opts...).ToArrowRecords(ctx)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	for rec, err := range batches {
		if err != nil {
			return nil, err
		}
		var buf bytes.Buffer
		err = array.RecordToJSON(rec, &buf)
		rec.Release()
		if err != nil {
			return nil, err
		}
		for _, line := range bytes.Split(buf.Bytes(), []byte("\n")) {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			if v, err := decodeJSON(line); err == nil {
				if m, ok := v.(map[string]any); ok {
					rows = append(rows, m)
				}
			}
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		return fmt.Sprint(rows[i]["__source_ts_ms"]) > fmt.Sprint(rows[j]["__source_ts_ms"])
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]TailRecord, 0, len(rows))
	for i := len(rows) - 1; i >= 0; i-- {
		var ts *string
		if v, ok := rows[i]["__source_ts_ms"].(string); ok {
			ts = &v
		}
		out = append(out, TailRecord{Destination: destination, Timestamp: ts, Value: rows[i]})
	}
	return out, nil
}

// CheckPipeline: keys for upserts, partitioning the wizard recorded, and tables that already
// exist.
func (s *icebergSink) CheckPipeline(ctx context.Context, conn Resolved, c CompileContext, facts PipelineFacts) []checks.Check {
	var out []checks.Check
	upsert := Str(facts.Options, "write_mode") != "append"
	if upsert && conn.Provider == "rustfs" {
		out = append(out, checks.Check{Code: "iceberg_rustfs_upsert", Title: "Rows", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: "RustFS S3 Tables (preview) refuses the first upsert commit of a new table (a row-level commit needs an earlier snapshot).",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Choose \"Every change appended\" in the Destination step."}})
	}
	keys, _ := facts.Options["partitions"].(map[string]any)
	// On Flink CDC a table without a key fails preflight (flink_primary_key), and tables are not
	// partitioned.
	if facts.Tables != nil && upsert && !facts.FlinkCDC {
		// Partition by replaces the recorded key for the tables it lists.
		listedPartitions, _ := facts.Options["partition_by"].(map[string]any)
		var keyless, stale []string
		for _, t := range facts.Tables {
			key := t.PrimaryKey
			if chosen := c.Spec.Keys[t.Identifier()]; len(chosen) > 0 {
				key = chosen
			}
			if len(key) == 0 {
				keyless = append(keyless, t.Identifier())
			}
			if _, listed := listedPartitions[t.Identifier()]; !listed && Str(facts.Options, "partitioning") != "none" {
				want := ""
				if len(key) > 0 {
					want = key[0]
				}
				if got, listed := keys[t.Identifier()].(string); !listed || got != want {
					stale = append(stale, t.Identifier())
				}
			}
		}
		if len(keyless) > 0 {
			out = append(out, checks.Check{Code: "iceberg_no_key", Title: "Row identity", Status: checks.Warn, Category: checks.CatDestination,
				Message: fmt.Sprintf("%s %s no primary key: %s get every change appended instead of upserted.", strings.Join(keyless, ", "),
					pluralVerb(len(keyless), "has", "have"), pluralVerb(len(keyless), "it", "they")),
				Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Choose key columns for them in the Transform step (Message key)."}})
		} else {
			out = append(out, checks.Check{Code: "iceberg_no_key", Title: "Row identity", Status: checks.Pass, Category: checks.CatDestination,
				Message: "Every table has a key: rows are upserted by it."})
		}
		out = append(out, icebergKeyColumnChecks(c.Spec, facts)...)
		if len(stale) > 0 {
			out = append(out, checks.Check{Code: "iceberg_partitioning", Title: "Partitioning", Status: checks.Warn, Category: checks.CatDestination,
				Message: fmt.Sprintf("The recorded partition keys of %s do not match their keys: new tables would be unpartitioned or bucketed by the wrong column.",
					strings.Join(stale, ", ")),
				Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Open the Destination step and continue: BeziSync records each table's key again."}})
		}
	}
	cfg, err := icebergCfg(conn)
	if err != nil {
		return out
	}
	cat, ctx, err := cfg.open(ctx)
	if err != nil {
		out = append(out, partitionChecks(ctx, unreachableCatalog{err}, cfg, c, facts)...)
		return append(out, partitionKeyChecks(ctx, unreachableCatalog{err}, cfg, c, facts)...)
	}
	out = append(out, partitionChecks(ctx, cat, cfg, c, facts)...)
	out = append(out, partitionKeyChecks(ctx, cat, cfg, c, facts)...)
	var existing, noIdentity []string
	for _, dest := range sortedUnique(facts.Destinations) {
		tbl, err := cat.LoadTable(ctx, icebergTableIdent(cfg, dest))
		if err != nil {
			continue
		}
		existing = append(existing, dest)
		if upsert && len(tbl.Schema().IdentifierFieldIDs) == 0 {
			noIdentity = append(noIdentity, dest)
		}
	}
	if len(existing) > 0 {
		c := checks.Check{Code: "iceberg_existing_tables", Title: "Existing tables", Status: checks.Pass, Category: checks.CatDestination,
			Message: fmt.Sprintf("%s already %s; the pipeline writes into %s, adding new columns, with %s partitioning.", strings.Join(existing, ", "),
				pluralVerb(len(existing), "exists", "exist"), pluralVerb(len(existing), "it", "them"), pluralVerb(len(existing), "its", "their"))}
		if len(noIdentity) > 0 {
			c.Status = checks.Warn
			c.Message = fmt.Sprintf("%s %s no row identity: changes are appended to %s instead of upserted.", strings.Join(noIdentity, ", "),
				pluralVerb(len(noIdentity), "has", "have"), pluralVerb(len(noIdentity), "it", "them"))
			c.Fix = &checks.Fix{Kind: "text", Who: "data engineer", Code: "ALTER TABLE … SET IDENTIFIER FIELDS <key columns> (Spark), or drop the table so the pipeline creates it."}
		}
		out = append(out, c)
	}
	return out
}

func pluralVerb(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}
