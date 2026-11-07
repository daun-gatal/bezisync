package services

import (
	"slices"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// E10 (ADR 0006): source.jdbc.properties.* do nothing on Flink CDC's Oracle reader and are refused
// when saved; other sources and Debezium keys are unaffected.
func TestDriverPropertyRefusalFlinkOracle(t *testing.T) {
	if DriverPropertyRefusal("oracle", "source.jdbc.properties.oracle.net.CONNECT_TIMEOUT") == "" {
		t.Error("oracle jdbc.properties allowed")
	}
	for _, c := range [][2]string{{"mysql", "source.jdbc.properties.connectTimeout"}, {"oracle", "source.debezium.log.mining.batch.size.default"},
		{"oracle", "debezium.source.log.mining.batch.size.default"}} {
		if why := DriverPropertyRefusal(c[0], c[1]); why != "" {
			t.Errorf("%v refused: %s", c, why)
		}
	}
}

// E17: added tables block while a saved position would be restored (or nobody can tell), and pass
// once nothing is left; sources that copy added tables get no check.
func TestFlinkTablesAddedCheck(t *testing.T) {
	why := engine.CapsFor(engine.FlinkCDC, "oracle").Why["add_tables"]
	added := []string{"SHOP.ITEMS"}
	if c := tablesAddedCheck(added, true, true, why); c.Status != checks.Fail || !c.Blocking || !strings.Contains(c.Message, "SHOP.ITEMS") ||
		!strings.Contains(c.Fix.Code, "Start over") || !strings.Contains(c.Fix.Code, "second pipeline") {
		t.Fatalf("%+v", c)
	}
	if c := tablesAddedCheck(added, false, false, why); c.Status != checks.Fail || !c.Blocking || !strings.Contains(c.Message, "couldn't read") {
		t.Fatalf("%+v", c)
	}
	if c := tablesAddedCheck(added, false, true, why); c.Status != checks.Pass || c.Blocking {
		t.Fatalf("%+v", c)
	}
	for _, src := range []string{"mysql", "postgresql", "sqlserver"} {
		if !engine.CapsFor(engine.FlinkCDC, src).AddTables {
			t.Errorf("%s would get the check", src)
		}
	}
	if !slices.Contains(flinkConfigChecks, "flink_oracle_version") {
		t.Fatal("the version check must be one a forced deploy can't skip")
	}
}

// E18: a position at or below the floor recorded when tables were added is refused; later ones and
// pipelines without a floor are not.
func TestRestoreBelowTablesFloor(t *testing.T) {
	p := &models.Pipeline{Artifacts: map[string]any{ArtifactTablesFloor: float64(12)}}
	for id, refused := range map[int64]bool{11: true, 12: true, 13: false} {
		err := restoreBelowFloor(p, id)
		if (err != nil) != refused {
			t.Errorf("#%d: %v", id, err)
		}
		if err != nil && !strings.Contains(err.Error(), "position_before_tables") && !strings.Contains(err.Error(), "added tables") {
			t.Errorf("#%d: %v", id, err)
		}
	}
	if err := restoreBelowFloor(&models.Pipeline{Artifacts: map[string]any{}}, 1); err != nil {
		t.Fatal(err)
	}
}

// E23: the delivery canary runs only on Debezium, where its row can reach the destination.
func TestCanaryRunsOnlyOnDebezium(t *testing.T) {
	src, _ := connectors.Describe("source", "postgresql")
	snk, _ := connectors.Describe("sink", "iceberg")
	if !CanaryRuns(engine.Debezium, src, snk) || CanaryRuns(engine.FlinkCDC, src, snk) {
		t.Fatal("canary engine rule")
	}
	ms, _ := connectors.Describe("source", "sqlserver")
	if CanaryRuns(engine.Debezium, ms, snk) || CanaryRuns(engine.Debezium, nil, snk) {
		t.Fatal("canary source rule")
	}
}
