package services

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Read and reset a pipeline's stored position (Debezium's JDBC offset store).

func init() { connectors.OffsetsReader = ReadOffsets }

func stateConn(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, db.NormalizeURL(config.Get().EffectiveStateDatabaseURL()))
}

// PgLSN renders a numeric LSN as X/Y.
func PgLSN(v uint64) string {
	return fmt.Sprintf("%X/%X", v>>32, v&0xFFFFFFFF)
}

func truthyAny(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		f, _ := x.Float64()
		return f != 0
	case float64:
		return x != 0
	case map[string]any:
		return len(x) > 0
	case []any:
		return len(x) > 0
	}
	return true
}

func pyFormat(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case bool:
		if x {
			return "True"
		}
		return "False"
	case json.Number:
		if s, err := pyjson.Dumps(x); err == nil {
			return s
		}
		return string(x)
	case string:
		return x
	}
	s, err := pyjson.Dumps(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return s
}

// DescribeOffset is a human-readable view of a connector-specific offset.
func DescribeOffset(position map[string]any) map[string]any {
	out := map[string]any{}
	switch {
	case has(position, "lsn"):
		n, _ := strconv.ParseUint(pyFormat(position["lsn"]), 10, 64)
		out["position"] = "LSN " + PgLSN(n)
	case has(position, "file"):
		pos := "?"
		if v, ok := position["pos"]; ok {
			pos = pyFormat(v)
		}
		out["position"] = fmt.Sprintf("binlog %s:%s", pyFormat(position["file"]), pos)
		if truthyAny(position["gtids"]) {
			out["gtids"] = position["gtids"]
		}
	case has(position, "change_lsn") || has(position, "commit_lsn"):
		v := position["commit_lsn"]
		if !truthyAny(v) {
			v = position["change_lsn"]
		}
		out["position"] = "LSN " + pyFormat(v)
	case has(position, "resume_token") || has(position, "sec"):
		sec := "?"
		if v, ok := position["sec"]; ok {
			sec = pyFormat(v)
		}
		out["position"] = "resume token at " + sec
	case has(position, "scn") || has(position, "commit_scn"): // Oracle
		v := position["scn"]
		if !truthyAny(v) {
			v = position["commit_scn"]
		}
		out["position"] = "SCN " + pyFormat(v)
	case has(position, "vgtid"): // Vitess: one GTID set per shard
		var shards []any
		if s, ok := position["vgtid"].(string); ok {
			if g, err := pyjson.DecodeGeneric([]byte(s)); err == nil {
				shards, _ = g.([]any)
			}
		}
		out["position"] = fmt.Sprintf("VGTID across %d shard(s)", len(shards))
	case has(position, "offset.cursor"): // CockroachDB changefeed cursor (HLC timestamp)
		out["position"] = "changefeed cursor " + pyFormat(position["offset.cursor"])
		if ms, ok := position["offset.timestamp"]; ok {
			out["source_time"] = pytime.ISOFormat(time.UnixMilli(int64(toFloat(ms))))
		}
	case has(position, "partitionToken") || (has(position, "offset") && has(position, "queryStartedAt")): // Spanner, per partition
		at := pyFormat(position["offset"])
		out["position"] = "commit timestamp " + at
		if t, err := time.Parse(time.RFC3339Nano, at); err == nil {
			out["source_time"] = pytime.ISOFormat(t)
		}
	default:
		out["position"] = "stored"
	}
	factors := []struct {
		key    string
		factor float64
	}{{"ts_usec", 1e6}, {"ts_ms", 1e3}, {"ts_sec", 1}, {"sec", 1}}
	for _, f := range factors {
		if v, ok := position[f.key]; ok {
			secs := toFloat(v) / f.factor
			us := math.RoundToEven(secs * 1e6)
			out["source_time"] = pytime.ISOFormat(time.UnixMicro(int64(us)))
			break
		}
	}
	if truthyAny(position["snapshot"]) || position["last_snapshot_record"] == false {
		out["snapshot_in_progress"] = true
	}
	return out
}

func has(m map[string]any, k string) bool {
	_, ok := m[k]
	return ok
}

// ReadOffsets lists the latest stored offset per key.
func ReadOffsets(ctx context.Context, shortID string) ([]map[string]any, error) {
	c, err := stateConn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = c.Close(ctx) }()
	var exists *string
	if err := c.QueryRow(ctx, "SELECT to_regclass($1)::text", compiler.StateSchema+".offsets_"+shortID).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == nil {
		return []map[string]any{}, nil
	}
	rows, err := c.Query(ctx, fmt.Sprintf(`SELECT offset_key, offset_val, record_insert_ts FROM %s."offsets_%s" ORDER BY record_insert_ts DESC, record_insert_seq DESC`,
		compiler.StateSchema, shortID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	seen := map[string]bool{}
	for rows.Next() {
		var key, value *string
		var at *time.Time
		if err := rows.Scan(&key, &value, &at); err != nil {
			return nil, err
		}
		k := ""
		if key != nil {
			k = *key
		}
		if seen[k] {
			continue
		}
		seen[k] = true
		var position map[string]any
		if value != nil {
			if g, err := pyjson.DecodeGeneric([]byte(*value)); err == nil {
				position, _ = g.(map[string]any)
			}
		}
		if position == nil {
			var raw any
			if value != nil {
				raw = *value
			}
			position = map[string]any{"raw": raw}
		}
		entry := map[string]any{"key": key, "raw": position, "stored_at": nil}
		if at != nil {
			entry["stored_at"] = pytime.ISOFormat(*at)
		}
		for dk, dv := range DescribeOffset(position) {
			entry[dk] = dv
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

// ClearOffsets drops stored offsets and schema history; the next start begins anew.
func ClearOffsets(ctx context.Context, shortID string) error {
	c, err := stateConn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()
	return pgx.BeginFunc(ctx, c, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s."offsets_%s"`, compiler.StateSchema, shortID)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s."history_%s"`, compiler.StateSchema, shortID))
		return err
	})
}
