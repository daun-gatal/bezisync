package services_test

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/services"
)

// Positions as Debezium Server 3.7 stores them for the Oracle, Vitess, CockroachDB and Spanner
// connectors (captured from a run against each database).
func TestDescribeOffsetOfNewerSources(t *testing.T) {
	cases := []struct {
		name, raw, position, sourceTime string
	}{
		{"oracle", `{"commit_scn":"2299592:1:0a001f0052020000","snapshot_scn":"2298510","scn":"2299592"}`, "SCN 2299592", ""},
		{"vitess", `{"vgtid":"[{\"keyspace\":\"shop\",\"shard\":\"-80\",\"gtid\":\"MySQL56/ee86:1-32\",\"table_p_ks\":[]},{\"keyspace\":\"shop\",\"shard\":\"80-\",\"gtid\":\"MySQL56/ee86:1-32\",\"table_p_ks\":[]}]"}`,
			"VGTID across 2 shard(s)", ""},
		{"cockroachdb", `{"offset.cursor":"1793521900489638109.0000000000","offset.timestamp":1793521900489,"snapshot_completed":"true"}`,
			"changefeed cursor 1793521900489638109.0000000000", "2026-11-01T08:31:40.489000+00:00"},
		{"spanner", `{"queryStartedAt":"2026-11-01T08:27:46.946000000Z","offset":"2026-11-01T08:24:41.206020000Z","startTime":"2026-11-01T08:24:24.951569000Z"}`,
			"commit timestamp 2026-11-01T08:24:41.206020000Z", "2026-11-01T08:24:41.206020+00:00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g, err := pyjson.DecodeGeneric([]byte(c.raw))
			if err != nil {
				t.Fatal(err)
			}
			got := services.DescribeOffset(g.(map[string]any))
			if got["position"] != c.position {
				t.Fatalf("position %v", got["position"])
			}
			if st, _ := got["source_time"].(string); st != c.sourceTime {
				t.Fatalf("source_time %q", st)
			}
			if got["snapshot_in_progress"] != nil {
				t.Fatal("not a snapshot")
			}
		})
	}
}
