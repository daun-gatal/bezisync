package connectors

import (
	"sort"

	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Kafka on Flink CDC: the same Kafka connections, written by Flink CDC's Kafka sink. Each table
// goes to the topic its route names (one per table, created at deploy as on Debezium), records are
// keyed by the primary key and spread by it (never all to partition 0), and values are Flink CDC's
// debezium-json: before/after/op with a short source block (no schema, no ts_ms). Client
// credentials travel as producer properties filled in by the helper.

func (s *kafkaSink) FlinkSink(conn Resolved, c CompileContext) (*FlinkBlock, error) {
	if f := c.Spec.Sink.Format; f.Type != "json" || f.IncludeSchema {
		return nil, problem.New(422, "engine_settings", "On Flink CDC, Kafka records are Debezium-style JSON without a schema: choose JSON without the schema, or run this pipeline on Debezium.")
	}
	props, env, err := KafkaClientProps(conn, "properties", "BZ_SINK")
	if err != nil {
		return nil, err
	}
	o := FlinkOptions{{"type", "kafka"}, {"partition.strategy", "hash-by-key"}, {"key.format", "json"}, {"value.format", "debezium-json"}}
	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		o.Set(k, props[k])
	}
	return &FlinkBlock{Options: o, Env: env, Connector: "kafka"}, nil
}

// FlinkDestination: the topic is the routed name, as on Debezium.
func (s *kafkaSink) FlinkDestination(_ Resolved, _ CompileContext, routed string) string {
	return routed
}
