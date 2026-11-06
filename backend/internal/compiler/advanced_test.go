package compiler_test

import (
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
)

func TestClassifyAdvanced(t *testing.T) {
	managed := map[string]bool{"debezium.source.max.queue.size": true, "debezium.sink.http.url": true, "debezium.source.decimal.handling.mode": true}
	for _, c := range []struct {
		key, value string
		want       compiler.AdvancedClass
		fix        string
	}{
		// Passed through.
		{"debezium.source.poll.interval.ms", "250", compiler.AdvancedAllowed, ""},
		{"debezium.source.driver.loginTimeout", "30", compiler.AdvancedAllowed, ""},
		{"debezium.source.database.tcpKeepAlive", "true", compiler.AdvancedAllowed, ""},
		{"debezium.source.tombstones.on.delete", "false", compiler.AdvancedAllowed, ""},
		{"debezium.source.offset.flush.interval.ms", "5000", compiler.AdvancedAllowed, ""},
		{"debezium.sink.kafka.producer.compression.type", "zstd", compiler.AdvancedAllowed, ""},
		{"debezium.sink.kafka.producer.linger.ms", "20", compiler.AdvancedAllowed, ""},
		{"debezium.source.poll.interval.ms", "", compiler.AdvancedAllowed, ""},
		// A1, A2: what BeziSync compiled for this pipeline.
		{"debezium.source.max.queue.size", "65536", compiler.AdvancedOwned, ""},
		{"debezium.sink.http.url", "https://collector.example.net", compiler.AdvancedModelled, "sink"},
		{"debezium.sink.type", "http", compiler.AdvancedOwned, ""},
		{"debezium.source.offset.storage.jdbc.url", "jdbc:x", compiler.AdvancedModelled, "review"},
		// A3, A4, A5: keys and values BeziSync can't pass on.
		{"Debezium.source.poll.interval.ms", "1", compiler.AdvancedInvalid, ""},
		{" debezium.source.poll.interval.ms", "1", compiler.AdvancedInvalid, ""},
		{"debezium.source.poll.interval.ms ", "1", compiler.AdvancedInvalid, ""},
		{"debezium.source.poll.", "1", compiler.AdvancedInvalid, ""},
		{"debezium.source..poll", "1", compiler.AdvancedInvalid, ""},
		{"debezium", "1", compiler.AdvancedInvalid, ""},
		{"debezium.source.pоll.interval.ms", "1", compiler.AdvancedInvalid, ""}, // Cyrillic о
		{"debezium.source.poll\u200b.interval.ms", "1", compiler.AdvancedInvalid, ""},
		{"debezium.source." + strings.Repeat("a", 250), "1", compiler.AdvancedInvalid, ""},
		{"debezium.source.poll.interval.ms", strings.Repeat("9", 4097), compiler.AdvancedInvalid, ""},
		{"other.prefix.key", "1", compiler.AdvancedInvalid, ""},
		// A6: expansion and injection.
		{"debezium.sink.http.headers.x-token", "1", compiler.AdvancedCredential, "connection"},
		{"debezium.sink.http.headers.x-trace", "${BZ_STATE_PASSWORD}", compiler.AdvancedInvalid, ""},
		{"debezium.sink.http.headers.x-trace", "$${BZ_STATE_PASSWORD}", compiler.AdvancedInvalid, ""},
		{"debezium.source.poll.interval.ms", "250\ndebezium.sink.http.url=https://x", compiler.AdvancedInvalid, ""},
		{"debezium.source.poll.interval.ms", "250\r", compiler.AdvancedInvalid, ""},
		{"debezium.source.poll.interval.ms", "250\x00", compiler.AdvancedInvalid, ""},
		{"debezium.source.poll.interval.ms", "2\t50", compiler.AdvancedInvalid, ""},
		// A7: credentials, whatever their spelling.
		{"debezium.source.database.sslpassword", "x", compiler.AdvancedCredential, "connection"},
		{"debezium.source.database.Password", "x", compiler.AdvancedCredential, "connection"},
		{"debezium.sink.kafka.producer.sasl.jaas.config", "x", compiler.AdvancedCredential, "connection"},
		{"debezium.sink.kafka.producer.ssl.keystore.location", "/x", compiler.AdvancedCredential, "connection"},
		{"debezium.sink.pubsub.credentials.json", "{}", compiler.AdvancedCredential, "connection"},
		{"debezium.sink.http.authentication.oauth2.client.secret", "x", compiler.AdvancedCredential, "connection"},
		{"debezium.sink.kinesis.access.key", "x", compiler.AdvancedCredential, "connection"},
		// A8: unsafe.
		{"debezium.source.slot.drop.on.stop", "true", compiler.AdvancedForbidden, ""},
		{"debezium.source.database.initial.statements", "DELETE FROM t", compiler.AdvancedForbidden, ""},
		{"debezium.source.heartbeat.action.query", "INSERT 1", compiler.AdvancedForbidden, ""},
		{"debezium.source.replica.identity.autoset.values", "public.t:FULL", compiler.AdvancedForbidden, ""},
		{"debezium.source.offset.mismatch.strategy", "trust_slot", compiler.AdvancedForbidden, ""},
		{"debezium.source.lsn.flush.mode", "manual", compiler.AdvancedForbidden, ""},
		{"debezium.source.database.sslfactory", "org.postgresql.ssl.NonValidatingFactory", compiler.AdvancedForbidden, ""},
		{"debezium.source.converters", "geo", compiler.AdvancedForbidden, ""},
		{"debezium.source.snapshot.mode.custom.name", "mine", compiler.AdvancedForbidden, ""},
		{"debezium.source.snapshot.locking.mode.custom.name", "mine", compiler.AdvancedForbidden, ""},
		{"debezium.source.topic.naming.strategy", "x.Y", compiler.AdvancedForbidden, ""},
		{"debezium.source.custom.sanitize.pattern", ".*nothing.*", compiler.AdvancedForbidden, ""},
		{"debezium.source.snapshot.select.statement.overrides", "public.users", compiler.AdvancedForbidden, ""},
		{"debezium.source.snapshot.select.statement.overrides.public.users", "SELECT * FROM public.users", compiler.AdvancedForbidden, ""},
		{"debezium.source.internal.slot.seek.to.known.offset.on.start", "true", compiler.AdvancedForbidden, ""},
		{"debezium.source.event.processing.failure.handling.mode", "skip", compiler.AdvancedForbidden, ""},
		{"debezium.source.include.unknown.datatypes", "true", compiler.AdvancedForbidden, ""},
		// A9.
		{"quarkus.log.level", "DEBUG", compiler.AdvancedForbidden, ""},
		{"quarkus.http.host", "0.0.0.0", compiler.AdvancedForbidden, ""},
		// A13–A15: JDBC driver options that read files, load code, call out or weaken TLS.
		{"debezium.source.driver.allowLoadLocalInfile", "true", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.database.allowLoadLocalInfile", "true", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.allowUrlInLocalInfile", "true", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.autoDeserialize", "true", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.queryInterceptors", "x.Y", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.socketFactory", "x.Y", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.database.socketFactory", "x.Y", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.loggerFile", "/tmp/x", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.authenticationPluginClassName", "x.Y", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.xmlFactoryFactory", "x.Y", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.trustServerCertificate", "true", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.encrypt", "false", compiler.AdvancedAllowed, ""},
		{"debezium.source.driver.hostNameInCertificate", "*", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.oracle.net.wallet_location", "/w", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.accessTokenCallbackClass", "x.Y", compiler.AdvancedCredential, "connection"},
		{"debezium.source.driver.allowPublicKeyRetrieval", "true", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.options", "-c log_statement=all", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.driver.sessionVariables", "x=1", compiler.AdvancedForbidden, "connection"},
		{"debezium.source.database.autosave", "conservative", compiler.AdvancedAllowed, ""},
		{"debezium.source.driver.connectTimeout", "10", compiler.AdvancedAllowed, ""},
		{"debezium.source.driver.prepareThreshold", "0", compiler.AdvancedAllowed, ""},
		// Set by a choice in the wizard.
		{"debezium.source.table.include.list", "public.secrets", compiler.AdvancedModelled, "tables"},
		{"debezium.source.column.exclude.list", "public.users.ssn", compiler.AdvancedModelled, "tables"},
		{"debezium.source.column.mask.hash.v2.SHA-256.with.salt.x", "public.users.email", compiler.AdvancedModelled, "tables"},
		{"debezium.source.column.truncate.to.10.chars", "public.users.note", compiler.AdvancedModelled, "transform"}, // the Truncate step
		{"debezium.source.message.key.columns", "public.t:id", compiler.AdvancedModelled, "tables"},
		// Value formats the wizard doesn't offer stay possible, unless the spec chose one itself.
		{"debezium.source.time.precision.mode", "adaptive_time_microseconds", compiler.AdvancedAllowed, ""},
		{"debezium.source.decimal.handling.mode", "string", compiler.AdvancedOwned, ""},
		{"debezium.source.skipped.operations", "d", compiler.AdvancedModelled, "transform"},
		{"debezium.source.snapshot.mode", "never", compiler.AdvancedModelled, "review"},
		{"debezium.source.heartbeat.interval.ms", "1", compiler.AdvancedModelled, "review"},
		{"debezium.source.notification.enabled.channels", "sink", compiler.AdvancedModelled, "review"},
		{"debezium.source.signal.enabled.channels", "kafka", compiler.AdvancedModelled, "review"},
		{"debezium.source.errors.max.retries", "0", compiler.AdvancedModelled, "review"},
		{"debezium.source.database.hostname", "evil", compiler.AdvancedModelled, "connection"},
		{"debezium.source.database.sslmode", "disable", compiler.AdvancedModelled, "connection"},
		{"debezium.source.topic.prefix", "x", compiler.AdvancedModelled, "review"},
		{"debezium.source.slot.name", "x", compiler.AdvancedModelled, "review"},
		{"debezium.format.value", "avro", compiler.AdvancedModelled, "sink"},
		{"debezium.transforms", "x", compiler.AdvancedModelled, "transform"},
		{"debezium.transforms.x.type", "a.B", compiler.AdvancedModelled, "transform"},
		{"debezium.predicates.p.type", "a.B", compiler.AdvancedModelled, "transform"},
		// Where events go.
		{"debezium.sink.kafka.producer.bootstrap.servers", "evil:9092", compiler.AdvancedModelled, "sink"},
		{"debezium.sink.pulsar.client.serviceUrl", "pulsar://evil", compiler.AdvancedModelled, "sink"},
		{"debezium.sink.kinesis.endpoint", "http://evil", compiler.AdvancedModelled, "sink"},
		{"debezium.sink.kinesis.region", "us-east-1", compiler.AdvancedModelled, "sink"},
		{"debezium.sink.redis.address", "evil:6379", compiler.AdvancedModelled, "sink"},
		{"debezium.sink.sqs.queue.url", "https://evil", compiler.AdvancedModelled, "sink"},
		{"debezium.sink.kafka.producer.security.protocol", "PLAINTEXT", compiler.AdvancedModelled, "connection"},
		{"debezium.sink.kafka.producer.ssl.endpoint.identification.algorithm", "", compiler.AdvancedModelled, "connection"},
		{"debezium.sink.kafka.producer.ssl.enabled.protocols", "TLSv1", compiler.AdvancedModelled, "connection"},
	} {
		got := compiler.ClassifyAdvanced(c.key, c.value, managed)
		if got.Class != c.want || got.Fix != c.fix {
			t.Errorf("%q=%q: %s (fix %q), want %s (fix %q): %s", c.key, c.value, got.Class, got.Fix, c.want, c.fix, got.Message)
		}
		if got.Refused() != (c.want != compiler.AdvancedAllowed) {
			t.Errorf("%q: Refused() = %v", c.key, got.Refused())
		}
		if got.Refused() && got.Message == "" {
			t.Errorf("%q: refused without saying why", c.key)
		}
		// I5: a refusal never repeats the value (it may be a credential).
		if c.value != "" && len(c.value) > 3 && strings.Contains(got.Message, c.value) {
			t.Errorf("%q: message echoes the value", c.key)
		}
	}
}

// I4: whatever a spec holds, no advanced property changes a value BeziSync compiled.
func TestAdvancedCannotOverrideCompiledKeys(t *testing.T) {
	flinkConfig(t)
	checked := 0
	for _, src := range connectors.Descriptors() {
		if src.Kind != "source" {
			continue
		}
		for _, snk := range connectors.Descriptors() {
			if snk.Kind != "sink" || flinkOnly[snk.Type] || snk.Type == "flinktest" {
				continue
			}
			raw := debeziumMatrixSpecs["rich"]
			if src.Type == "mongodb" || src.Type == "spanner" { // they can't mask
				raw = debeziumMatrixSpecs["basic"]
			}
			if snk.Type == "sns" {
				raw = withNaming(raw, "{schema}_{table}")
			}
			spec, err := matrixSpec(t, raw)
			if err != nil {
				t.Fatal(err)
			}
			ctx := connectors.CompileContext{PipelineID: "3f2c1d4e-5b6a-4789-8abc-0123456789ab", ShortID: "3f2c1d4e5b6a", Name: "orders-cdc", Spec: spec}
			c, err := compiler.Compile(ctx, matrixConn(t, src), matrixConn(t, snk), "")
			if err != nil {
				t.Fatalf("%s → %s: %v", src.Type, snk.Type, err)
			}
			for key := range c.Properties {
				if _, fromSpec := spec.Advanced[key]; fromSpec {
					continue
				}
				if v := compiler.ClassifyAdvanced(key, "x", c.Managed); !v.Refused() {
					t.Errorf("%s → %s: advanced %s would override a compiled value", src.Type, snk.Type, key)
				}
				checked++
			}
		}
	}
	if checked < 1000 {
		t.Fatalf("only %d compiled keys checked", checked)
	}
}

func TestAdvancedRefusalsSorted(t *testing.T) {
	got := compiler.AdvancedRefusals(map[string]string{
		"debezium.source.slot.drop.on.stop": "true", "debezium.source.poll.interval.ms": "1", "debezium.source.converters": "x",
	}, nil)
	if len(got) != 2 || got[0].Key != "debezium.source.converters" || got[1].Key != "debezium.source.slot.drop.on.stop" {
		t.Fatalf("got %+v", got)
	}
}
