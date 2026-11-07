package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// Apache Paimon: one primary-key table per source table in a filesystem catalog on S3 (or an
// S3-compatible store), created and written by Flink CDC's Paimon sink. Only Flink CDC writes it.
// Storage credentials travel as catalog properties of the job (filled in by the helper), as for
// Iceberg on Flink CDC. BeziSync checks the bucket with a test object; it has no Paimon reader, so
// a pipeline's rows can't be read back (Events stay empty, health is Unknown), and its tables are
// left in place on delete (the statement to drop them is shown).

type paimonSink struct{ BaseSink }

func init() { Register(&paimonSink{BaseSink{Title: "Apache Paimon"}}) }

func (*paimonSink) Kind() string { return "sink" }
func (*paimonSink) Type() string { return "paimon" }

type paimonConfig struct {
	Warehouse, Bucket, Prefix string
	Endpoint, Region          string
	PathStyle                 bool
	Database                  string
	AccessKey, SecretKey      string
}

func paimonCfg(conn Resolved) (paimonConfig, error) {
	d, _ := Describe("sink", "paimon")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return paimonConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return paimonConfig{}, err
	}
	c := paimonConfig{Warehouse: strings.TrimRight(Str(cfg, "warehouse"), "/"), Endpoint: Str(cfg, "s3_endpoint"), Region: Str(cfg, "region"),
		PathStyle: Bool(cfg, "path_style"), Database: Str(cfg, "database"), AccessKey: Str(cfg, "access_key_id"), SecretKey: Str(sec, "secret_access_key")}
	fail := func(field, msg string) error {
		return problem.New(422, "paimon_config", msg).WithErrors([]map[string]any{{"loc": []string{"config", field}, "message": msg}})
	}
	u, err := url.Parse(c.Warehouse)
	switch {
	case err != nil || u.Scheme != "s3" || u.Host == "":
		return c, fail("warehouse", "The warehouse is an S3 location: s3://bucket/path.")
	case !flinkName.MatchString(c.Database):
		return c, fail("database", "The database name may only use letters, digits and underscores.")
	case c.AccessKey != "" && c.SecretKey == "":
		return c, fail("access_key_id", "An access key needs its secret key.")
	}
	c.Bucket, c.Prefix = u.Host, strings.Trim(u.Path, "/")
	return c, nil
}

func (s *paimonSink) CompileSink(Resolved, CompileContext) (map[string]string, map[string]string, error) {
	return nil, nil, problem.New(422, "engine_unsupported", "Apache Paimon is written by Flink CDC: run this pipeline on Flink CDC.")
}

func (s *paimonSink) FlinkSink(conn Resolved, _ CompileContext) (*FlinkBlock, error) {
	cfg, err := paimonCfg(conn)
	if err != nil {
		return nil, err
	}
	const p = "catalog.properties."
	o := FlinkOptions{{"type", "paimon"}, {p + "metastore", "filesystem"}, {p + "warehouse", cfg.Warehouse}}
	env := map[string]string{}
	if cfg.Endpoint != "" {
		o.Set(p+"s3.endpoint", cfg.Endpoint)
		o.Set(p+"s3.path.style.access", fmt.Sprint(cfg.PathStyle))
	}
	if cfg.Region != "" {
		o.Set(p+"s3.region", cfg.Region)
	}
	if cfg.AccessKey != "" {
		o.Set(p+"s3.access-key", "${BZ_S3_ACCESS_KEY}")
		o.Set(p+"s3.secret-key", "${BZ_S3_SECRET_KEY}")
		env["BZ_S3_ACCESS_KEY"], env["BZ_S3_SECRET_KEY"] = cfg.AccessKey, cfg.SecretKey
	}
	// Paimon's S3 file IO, and the Hadoop classes its writer loads (the image carries them for Iceberg).
	return &FlinkBlock{Options: o, Env: env, Connector: "paimon", Extra: []string{"paimon-s3", "hadoop-client-api", "hadoop-client-runtime", "commons-logging"}}, nil
}

// FlinkDestination: the routed name, lower-cased, in the connection's database.
func (s *paimonSink) FlinkDestination(conn Resolved, _ CompileContext, routed string) string {
	return databaseDestination(conn, routed)
}

// Destination is the same name for every caller that shows one.
func (s *paimonSink) Destination(conn Resolved, _ CompileContext, routed string) string {
	return databaseDestination(conn, routed)
}

// client signs with the connection's key, or else the runtime's own credentials (an IAM role,
// as the form offers), and dials only what the SSRF guard allows.
func (c paimonConfig) client(ctx context.Context) (*s3.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	region := c.Region
	if region == "" {
		region = "us-east-1"
	}
	opts = append(opts, awsconfig.WithRegion(region))
	if c.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")))
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	ac = GuardAWS(ac, 15*time.Second)
	return s3.NewFromConfig(ac, func(o *s3.Options) {
		o.UsePathStyle = c.PathStyle
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	}), nil
}

func (s *paimonSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	run := &checks.Run{}
	cfg, err := paimonCfg(conn)
	if err != nil {
		var p *problem.Problem
		if errors.As(err, &p) {
			run.Add(checks.Check{Code: "paimon_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true, Message: p.Detail})
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	// Paimon writes data, manifests and snapshots under the warehouse: prove a write and a delete.
	key := strings.TrimPrefix(cfg.Prefix+"/bezisync-check-"+fmt.Sprint(time.Now().UnixNano()), "/")
	c, err := cfg.client(ctx)
	if err == nil {
		_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String(key), Body: strings.NewReader("bezisync")})
	}
	if err == nil {
		_, err = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String(key)})
	}
	var blocked *ssrf.BlockedAddress
	switch {
	case errors.As(err, &blocked):
		run.Add(checks.Check{Code: "paimon_storage", Title: "Storage", Status: checks.Fail, Category: checks.CatNetwork, Blocking: true,
			Message: "BeziSync may not reach this storage address (" + blocked.Error() + ").", Fix: &checks.Fix{Kind: "text", Who: "Admin", Code: "Allow the address in BEZISYNC_SSRF_ALLOW_CIDRS, or use another endpoint."}})
	case err != nil:
		run.Add(checks.Check{Code: "paimon_storage", Title: "Storage", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
			Message: "Writing and deleting a test object under " + cfg.Warehouse + " failed.", Technical: firstRunes(err.Error(), 300),
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Check the bucket, the endpoint (and path-style access for S3-compatible stores) and that the key, or the runtime's role when no key is set, may put, get, list and delete objects under the warehouse (the check tries put and delete)."}})
	default:
		run.Passed("paimon_storage", "Storage", checks.CatPermissions, "Wrote and deleted a test object under "+cfg.Warehouse+".", "")
	}
	return checks.Summarize(run.Checks, started), nil
}

// CleanupSink: BeziSync has no Paimon client; it says which tables stay and how to drop them.
func (s *paimonSink) CleanupSink(_ context.Context, _ Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	var out []ArtifactResult
	for _, dest := range sortedUnique(destinations) {
		out = append(out, ArtifactResult{Kind: "table", Name: dest, Outcome: "retained",
			Detail: "BeziSync can't drop Paimon tables: drop it with Flink SQL or Spark on the same catalog (DROP TABLE " + dest + ")."})
	}
	return out, nil
}

// databaseDestination is the routed name, lower-cased, in the connection's database ("cdc" when
// the stored settings predate a default).
func databaseDestination(conn Resolved, routed string) string {
	db := Str(conn.Config, "database")
	if db == "" {
		db = "cdc"
	}
	return db + "." + strings.ToLower(strings.ReplaceAll(routed, ".", "_"))
}

// paimonObjects is what the keyless-table check reads from the warehouse.
type paimonObjects interface {
	list(ctx context.Context, prefix string) ([]string, error)
	get(ctx context.Context, key string) ([]byte, error)
}

type paimonS3 struct {
	c      *s3.Client
	bucket string
}

func (p paimonS3) list(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	pg := s3.NewListObjectsV2Paginator(p.c, &s3.ListObjectsV2Input{Bucket: aws.String(p.bucket), Prefix: aws.String(prefix)})
	for pg.HasMorePages() {
		out, err := pg.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, o := range out.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	return keys, nil
}

func (p paimonS3) get(ctx context.Context, key string) ([]byte, error) {
	out, err := p.c.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(p.bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer func() { _ = out.Body.Close() }()
	return io.ReadAll(io.LimitReader(out.Body, 4<<20))
}

// CheckPipeline is paimon_keyless_table: a table that already exists without a primary key is
// an append table, which can't take the updates and deletes Flink CDC writes (the job fails on
// the first one, or the changes are kept as rows). Tables the pipeline creates get the key.
func (s *paimonSink) CheckPipeline(ctx context.Context, conn Resolved, _ CompileContext, facts PipelineFacts) []checks.Check {
	cfg, err := paimonCfg(conn)
	if err != nil {
		return nil
	}
	c, err := cfg.client(ctx)
	if err != nil {
		return nil
	}
	return paimonKeylessChecks(ctx, paimonS3{c: c, bucket: cfg.Bucket}, cfg.Prefix, facts.Destinations)
}

func paimonKeylessChecks(ctx context.Context, store paimonObjects, prefix string, destinations map[string]string) []checks.Check {
	var keyless, unread []string
	checked := false
	for _, dest := range sortedUnique(destinations) {
		db, table, _ := strings.Cut(dest, ".")
		// A table's schema files: <warehouse>/<db>.db/<table>/schema/schema-<n> (the highest is current).
		dir := strings.TrimPrefix(prefix+"/"+db+".db/"+table+"/schema/schema-", "/")
		keys, err := store.list(ctx, dir)
		if err != nil {
			unread = append(unread, dest)
			continue
		}
		latest, n := "", -1
		for _, k := range keys {
			if v, err := strconv.Atoi(strings.TrimPrefix(k, dir)); err == nil && v > n {
				latest, n = k, v
			}
		}
		checked = true
		if latest == "" {
			continue // the pipeline creates it, with the key
		}
		body, err := store.get(ctx, latest)
		var schema struct {
			PrimaryKeys []string `json:"primaryKeys"`
		}
		if err != nil || json.Unmarshal(body, &schema) != nil {
			unread = append(unread, dest)
			continue
		}
		if len(schema.PrimaryKeys) == 0 {
			keyless = append(keyless, dest)
		}
	}
	var out []checks.Check
	if len(keyless) > 0 {
		out = append(out, checks.Check{Code: "paimon_keyless_table", Title: "Table without a key", Status: checks.Fail, Blocking: true, Category: checks.CatDestination,
			Message: "These Paimon tables exist without a primary key, so they can't take the updates and deletes Flink CDC writes: " + strings.Join(keyless, ", ") + ".",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Drop them (with Flink SQL or Spark on the same catalog) and let the pipeline create them with the table's key, or name other destinations in the Destination step."}})
	}
	if len(unread) > 0 {
		out = append(out, checks.Check{Code: "paimon_keyless_table", Title: "Table without a key", Status: checks.Warn, Category: checks.CatDestination,
			Message: "Couldn't read whether these tables exist, or their schema: " + strings.Join(unread, ", ") + ".",
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Check that the connection may list and read objects under the warehouse, then run preflight again."}})
	}
	if len(keyless)+len(unread) == 0 && checked {
		out = append(out, checks.Check{Code: "paimon_keyless_table", Title: "Table without a key", Status: checks.Pass, Category: checks.CatDestination,
			Message: "No destination exists without a primary key."})
	}
	return out
}
