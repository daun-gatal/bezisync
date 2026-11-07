package connectors

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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

func (c paimonConfig) client() *s3.Client {
	opts := s3.Options{Region: c.Region, HTTPClient: ssrf.HTTPClient(15 * time.Second), UsePathStyle: c.PathStyle}
	if opts.Region == "" {
		opts.Region = "us-east-1"
	}
	if c.Endpoint != "" {
		opts.BaseEndpoint = aws.String(c.Endpoint)
	}
	if c.AccessKey != "" {
		opts.Credentials = credentials.NewStaticCredentialsProvider(c.AccessKey, c.SecretKey, "")
	} else {
		opts.Credentials = aws.AnonymousCredentials{}
	}
	return s3.New(opts)
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
	c := cfg.client()
	_, err = c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String(key), Body: strings.NewReader("bezisync")})
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
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Check the bucket, the endpoint (and path-style access for S3-compatible stores) and that the key may put, get, list and delete objects under the warehouse."}})
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
