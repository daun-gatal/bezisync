package services

import "testing"

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
