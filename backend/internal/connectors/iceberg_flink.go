package connectors

import (
	"fmt"

	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Apache Iceberg on Flink CDC: the same REST or Glue catalog connections, written by Flink CDC's
// Iceberg sink (Iceberg 1.10, upserts by primary key with equality deletes, schema changes
// applied). Data files are written by the TaskManagers, which get no environment secrets: storage
// and signing credentials travel as catalog properties of the pipeline (the helper fills them in).
// A Glue catalog signs in with the runtime's AWS role (Iceberg has no static keys for Glue).

func (s *icebergSink) FlinkSink(conn Resolved, c CompileContext) (*FlinkBlock, error) {
	cfg, err := icebergCfg(conn)
	if err != nil {
		return nil, err
	}
	opts, err := SinkOptions("iceberg", c.Spec)
	if err != nil {
		return nil, err
	}
	refuse := func(msg string) (*FlinkBlock, error) {
		return nil, problem.New(422, "engine_settings", msg)
	}
	if !flinkName.MatchString(icebergNamespace(conn)) {
		return refuse("On Flink CDC the Iceberg namespace is one level of letters, digits and underscores.")
	}
	switch {
	case Str(opts, "write_mode") == "append":
		return refuse("Flink CDC writes Iceberg tables by primary key: choose upsert, or run this pipeline on Debezium to append every change.")
	case Str(opts, "partitioning") != "none" && len(icebergListedPartitions(opts)) > 0:
		return refuse("Bucket partitioning is a Debezium option: on Flink CDC, Iceberg tables are written unpartitioned.")
	case cfg.CatalogType == "glue" && cfg.AccessKey != "":
		return refuse("On Flink CDC a Glue catalog signs in with the runtime's AWS role: remove the access key from the connection.")
	}
	const p = "catalog.properties."
	o := FlinkOptions{{"type", "iceberg"}, {p + "type", cfg.CatalogType}, {p + "warehouse", cfg.Warehouse}}
	env := map[string]string{}
	if cfg.CatalogType == "rest" {
		o.Set(p+"uri", cfg.URI)
		switch cfg.Auth {
		case "oauth2":
			o.Set(p+"credential", "${BZ_ICEBERG_CREDENTIAL}")
			env["BZ_ICEBERG_CREDENTIAL"] = cfg.ClientID + ":" + cfg.ClientSecret
			if cfg.Scope != "" {
				o.Set(p+"scope", cfg.Scope)
			}
			if cfg.OAuthURI != "" {
				o.Set(p+"oauth2-server-uri", cfg.OAuthURI)
			}
		case "token":
			o.Set(p+"token", "${BZ_ICEBERG_TOKEN}")
			env["BZ_ICEBERG_TOKEN"] = cfg.Token
		case "sigv4":
			o.Set(p+"rest.sigv4-enabled", "true")
			o.Set(p+"rest.signing-name", cfg.SigningName)
			o.Set(p+"rest.signing-region", cfg.region())
			if cfg.AccessKey != "" {
				o.Set(p+"rest.access-key-id", "${BZ_AWS_ACCESS_KEY_ID}")
				o.Set(p+"rest.secret-access-key", "${BZ_AWS_SECRET_ACCESS_KEY}")
			}
		}
	}
	o.Set(p+"io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
	if cfg.Storage == "vended" && cfg.CatalogType == "rest" {
		o.Set(p+"header.X-Iceberg-Access-Delegation", "vended-credentials")
	} else {
		if cfg.S3Endpoint != "" {
			o.Set(p+"s3.endpoint", cfg.S3Endpoint)
			o.Set(p+"s3.path-style-access", fmt.Sprint(cfg.PathStyle))
		}
		if cfg.AccessKey != "" {
			o.Set(p+"s3.access-key-id", "${BZ_AWS_ACCESS_KEY_ID}")
			o.Set(p+"s3.secret-access-key", "${BZ_AWS_SECRET_ACCESS_KEY}")
		}
	}
	if cfg.AccessKey != "" {
		env["BZ_AWS_ACCESS_KEY_ID"], env["BZ_AWS_SECRET_ACCESS_KEY"] = cfg.AccessKey, cfg.SecretKey
	}
	if r := cfg.region(); r != "" {
		o.Set(p+"client.region", r)
	}
	o.Set("table.properties.format-version", Str(opts, "format_version"))
	o.Set("table.properties.write.format.default", "parquet")
	b := &FlinkBlock{Options: o, Env: env, Connector: "iceberg", Extra: []string{"iceberg-aws"}}
	if Str(opts, "write_mode") == "upsert_keep_deletes" {
		// A deleted row stays, marked: __op is Flink CDC's change type (-D for a delete).
		b.Projection, b.Converter = []string{"__data_event_type__ AS `__op`"}, "SOFT_DELETE"
	}
	return b, nil
}

// FlinkDestination is the table Debezium's build would write: the namespace, then the routed name
// lower-cased with dots as underscores.
func (s *icebergSink) FlinkDestination(conn Resolved, c CompileContext, routed string) string {
	return s.Destination(conn, c, routed)
}

func icebergNamespace(conn Resolved) string {
	if ns := Str(conn.Config, "namespace"); ns != "" {
		return ns
	}
	return "cdc"
}

// icebergListedPartitions are the tables with a chosen partition column.
func icebergListedPartitions(opts map[string]any) []string {
	keys, _ := opts["partitions"].(map[string]any)
	var out []string
	for t := range keys {
		out = append(out, t)
	}
	return out
}
