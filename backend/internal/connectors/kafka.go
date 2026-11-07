package connectors

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Apache Kafka sink (any Kafka-compatible broker: MSK, Confluent Cloud, Redpanda, …).

const kafkaProbeTopic = "bezisync-permission-probe"

type kafkaSink struct{ BaseSink }

func init() { Register(&kafkaSink{BaseSink{Title: "Kafka"}}) }

func (*kafkaSink) Kind() string { return "sink" }
func (*kafkaSink) Type() string { return "kafka" }

type kafkaConfig struct {
	Bootstrap, Protocol string
	Mechanism, Username *string
	CreateTopics        bool
	Partitions, RF      int
	Password, CAPEM     *string
}

func kafkaCfg(conn Resolved) (kafkaConfig, error) {
	d, _ := Describe("sink", "kafka")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return kafkaConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return kafkaConfig{}, err
	}
	return kafkaConfig{Bootstrap: Str(cfg, "bootstrap_servers"), Protocol: Str(cfg, "security_protocol"),
		Mechanism: OptStr(cfg, "sasl_mechanism"), Username: OptStr(cfg, "username"), CreateTopics: Bool(cfg, "create_topics"),
		Partitions: Int(cfg, "partitions"), RF: Int(cfg, "replication_factor"),
		Password: OptStr(sec, "password"), CAPEM: OptStr(sec, "ssl_ca_pem")}, nil
}

func orEmpty(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// ParseBootstrap splits "host:port,host:port".
func ParseBootstrap(servers string) ([][2]string, error) {
	var out [][2]string
	for _, item := range strings.Split(servers, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		i := strings.LastIndex(item, ":")
		host, port := "", item
		if i >= 0 {
			host, port = item[:i], item[i+1:]
		}
		if host == "" || !isDigits(port) {
			return nil, fmt.Errorf("'%s' is not host:port", item)
		}
		out = append(out, [2]string{strings.Trim(host, "[]"), port})
	}
	return out, nil
}

// kafkaClient builds a client with the settings shared by checks, previews and probes.
func kafkaClient(cfg kafkaConfig, timeout time.Duration, extra ...kgo.Opt) (*kgo.Client, error) {
	var seeds []string
	for _, s := range strings.Split(cfg.Bootstrap, ",") {
		if s = strings.TrimSpace(s); s != "" {
			seeds = append(seeds, s)
		}
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.ClientID("bezisync"),
		kgo.DialTimeout(timeout),
		kgo.RequestTimeoutOverhead(timeout),
		kgo.RetryTimeout(timeout),
	}
	// Every broker the metadata names is dialled through the SSRF guard (ADR 0004). franz-go
	// can't combine a dialer with DialTLSConfig, so the dialer speaks TLS itself.
	var tc *tls.Config
	if strings.HasSuffix(cfg.Protocol, "SSL") {
		tc = &tls.Config{MinVersion: tls.VersionTLS12}
		if ca := orEmpty(cfg.CAPEM); ca != "" {
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM([]byte(ca))
			tc.RootCAs = pool
		}
	}
	opts = append(opts, kgo.Dialer(tlsDialer(tc)))
	if strings.HasPrefix(cfg.Protocol, "SASL") {
		mech := orEmpty(cfg.Mechanism)
		if mech == "" {
			mech = "PLAIN"
		}
		user, pass := orEmpty(cfg.Username), orEmpty(cfg.Password)
		switch mech {
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()))
		default:
			opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()))
		}
	}
	return kgo.NewClient(append(opts, extra...)...)
}

func kerrName(err error) string {
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return ke.Message
	}
	return ""
}

func (s *kafkaSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	cfg, err := kafkaCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	brokers, err := ParseBootstrap(cfg.Bootstrap)
	if err != nil {
		run.Add(checks.Check{Code: "kafka_bootstrap", Title: "Broker list", Status: checks.Fail, Category: checks.CatNetwork, Message: err.Error(), Blocking: true})
		return checks.Summarize(run.Checks, started), nil
	}
	probe := &checks.Run{}
	reachable := 0
	for _, b := range brokers {
		port, _ := strconv.Atoi(b[1])
		if NetworkCheck(ctx, probe, b[0], port, "kafka_network", fmt.Sprintf("%s:%d", b[0], port), 0) {
			reachable++
		}
	}
	if reachable == 0 {
		for _, c := range probe.Checks {
			if c.Status == checks.Fail {
				run.Add(c)
			}
		}
		return checks.Summarize(run.Checks, started), nil
	}
	run.Passed("kafka_network", "Reach brokers", checks.CatNetwork, fmt.Sprintf("%d of %d bootstrap servers reachable.", reachable, len(brokers)), "")
	client, err := kafkaClient(cfg, 8*time.Second)
	if err != nil {
		return checks.Report{}, err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	mctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	meta, err := admin.BrokerMetadata(mctx)
	cancel()
	if err != nil {
		text := err.Error()
		lower := strings.ToLower(text)
		auth := strings.Contains(lower, "auth") || strings.Contains(lower, "sasl")
		c := checks.Check{Code: "kafka_metadata", Title: "Talk to Kafka", Status: checks.Fail, Category: checks.CatNetwork, Blocking: true,
			Message: "The port is open but Kafka did not answer.", Technical: text,
			Why: "An open port is not enough: the connector must authenticate and fetch metadata.",
			Fix: &checks.Fix{Kind: "text", Who: "DBA", Code: "Check security protocol (PLAINTEXT vs SASL_SSL) and that advertised.listeners resolve from the runtime."}}
		if auth {
			c.Code, c.Title, c.Category, c.Message, c.Fix = "kafka_auth", "Sign in", checks.CatAuth, "Kafka rejected the credentials.", nil
		}
		run.Add(c)
		return checks.Summarize(run.Checks, started), nil
	}
	brokerCount := len(meta.Brokers)
	run.Passed("kafka_auth", "Sign in", checks.CatAuth, fmt.Sprintf("Connected to a cluster with %d broker%s.", brokerCount, pick(brokerCount != 1, "s", "")), "")
	if cfg.RF > brokerCount {
		run.Add(checks.Check{Code: "kafka_replication", Title: "Replication factor", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: fmt.Sprintf("Replication factor %d needs at least that many brokers (cluster has %d).", cfg.RF, brokerCount)})
	}
	if cfg.CreateTopics {
		tctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		resp, err := admin.ValidateCreateTopics(tctx, 1, int16(min(cfg.RF, brokerCount)), nil, kafkaProbeTopic)
		cancel()
		if err == nil {
			if r, ok := resp[kafkaProbeTopic]; ok && r.Err != nil {
				err = r.Err
			}
		}
		name := kerrName(err)
		switch {
		case err == nil || name == "TOPIC_ALREADY_EXISTS":
			run.Passed("kafka_create_topics", "Create topics", checks.CatPermissions, "Allowed to create topics.", "")
		case strings.Contains(name, "AUTHORIZATION"):
			user := orEmpty(cfg.Username)
			if user == "" {
				user = "<user>"
			}
			run.Add(checks.Check{Code: "kafka_create_topics", Title: "Create topics", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
				Message: "This user may not create topics.", Technical: err.Error(),
				Fix: &checks.Fix{Kind: "shell", Who: "Kafka admin", Code: "kafka-acls --bootstrap-server <broker> --add --allow-principal User:" + user +
					" --operation Create --operation Write --operation Describe --topic '*' --resource-pattern-type literal"},
				Why: "Turn off 'Create topics automatically' if topics are provisioned another way."})
		default:
			run.Add(checks.Check{Code: "kafka_create_topics", Title: "Create topics", Status: checks.Warn, Category: checks.CatPermissions,
				Message: "Could not confirm topic-creation permission.", Technical: err.Error()})
		}
	}
	return checks.Summarize(run.Checks, started), nil
}

func (s *kafkaSink) CompileSink(conn Resolved, c CompileContext) (map[string]string, map[string]string, error) {
	prefix := "debezium.sink.kafka.producer"
	serializer := "org.apache.kafka.common.serialization.StringSerializer"
	if t := c.Spec.Sink.Format.Type; t == "avro" || t == "protobuf" {
		serializer = "org.apache.kafka.common.serialization.ByteArraySerializer"
	}
	props := map[string]string{
		"debezium.sink.type":           "kafka",
		prefix + ".key.serializer":     serializer,
		prefix + ".value.serializer":   serializer,
		prefix + ".enable.idempotence": "true",
		prefix + ".acks":               "all",
		prefix + ".compression.type":   "zstd",
	}
	client, env, err := KafkaClientProps(conn, prefix, "BZ_SINK")
	if err != nil {
		return nil, nil, err
	}
	for k, v := range client {
		props[k] = v
	}
	return props, env, nil
}

// KafkaClientProps are a Kafka connection's client properties under prefix: bootstrap servers,
// security protocol, SASL (the JAAS config in env <envKey>_JAAS) and the CA (<envKey>_CA).
func KafkaClientProps(conn Resolved, prefix, envKey string) (map[string]string, map[string]string, error) {
	cfg, err := kafkaCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	props := map[string]string{
		prefix + ".bootstrap.servers": cfg.Bootstrap,
		prefix + ".security.protocol": cfg.Protocol,
	}
	env := map[string]string{}
	if strings.HasPrefix(cfg.Protocol, "SASL") {
		mech := orEmpty(cfg.Mechanism)
		if mech == "" {
			mech = "PLAIN"
		}
		module := "org.apache.kafka.common.security.scram.ScramLoginModule"
		if mech == "PLAIN" {
			module = "org.apache.kafka.common.security.plain.PlainLoginModule"
		}
		props[prefix+".sasl.mechanism"] = mech
		props[prefix+".sasl.jaas.config"] = "${" + envKey + "_JAAS}"
		user := strings.ReplaceAll(orEmpty(cfg.Username), `"`, `\"`)
		password := strings.ReplaceAll(orEmpty(cfg.Password), `"`, `\"`)
		env[envKey+"_JAAS"] = fmt.Sprintf(`%s required username="%s" password="%s";`, module, user, password)
	}
	if ca := orEmpty(cfg.CAPEM); ca != "" && strings.HasSuffix(cfg.Protocol, "SSL") {
		props[prefix+".ssl.truststore.type"] = "PEM"
		props[prefix+".ssl.truststore.certificates"] = "${" + envKey + "_CA}"
		env[envKey+"_CA"] = ca
	}
	return props, env, nil
}

func sortedUnique(m map[string]string) []string {
	set := map[string]bool{}
	for _, v := range m {
		set[v] = true
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func (s *kafkaSink) PrepareSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := kafkaCfg(conn)
	if err != nil {
		return nil, err
	}
	if !cfg.CreateTopics {
		var out []ArtifactResult
		for _, t := range destinationValues(destinations) {
			out = append(out, ArtifactResult{Kind: "topic", Name: t, Outcome: "retained", Detail: "Topic creation is disabled."})
		}
		return out, nil
	}
	client, err := kafkaClient(cfg, 8*time.Second)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	mctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	meta, err := admin.BrokerMetadata(mctx)
	cancel()
	if err != nil {
		return nil, err
	}
	rf := min(cfg.RF, max(len(meta.Brokers), 1))
	topics := sortedUnique(destinations)
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	resp, err := admin.CreateTopics(cctx, int32(cfg.Partitions), int16(rf), nil, topics...)
	cancel()
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	for _, t := range topics {
		r, ok := resp[t]
		switch {
		case !ok:
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "failed", Detail: "no response"})
		case r.Err == nil:
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "created"})
		case kerrName(r.Err) == "TOPIC_ALREADY_EXISTS":
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "exists"})
		default:
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "failed", Detail: r.Err.Error()})
		}
	}
	return results, nil
}

// destinationValues lists destination names in table order (dict.values()).
func destinationValues(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]string, len(keys))
	for i, k := range keys {
		out[i] = m[k]
	}
	return out
}

func (s *kafkaSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := kafkaCfg(conn)
	if err != nil {
		return nil, err
	}
	client, err := kafkaClient(cfg, 8*time.Second)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	topics := sortedUnique(destinations)
	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	resp, err := admin.DeleteTopics(dctx, topics...)
	cancel()
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	for _, t := range topics {
		r, ok := resp[t]
		switch {
		case !ok:
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "failed", Detail: "no response"})
		case r.Err == nil:
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "removed"})
		default:
			results = append(results, ArtifactResult{Kind: "topic", Name: t, Outcome: "failed", Detail: r.Err.Error()})
		}
	}
	return results, nil
}

// decodePayload is bytes.decode(errors="replace") then json.loads, falling back to text.
func decodePayload(raw []byte) any {
	if raw == nil {
		return nil
	}
	text := string(raw)
	if !utf8.ValidString(text) {
		text = strings.ToValidUTF8(text, "�")
	}
	if v, err := pyjson.DecodeGeneric([]byte(text)); err == nil {
		return v
	}
	return text
}

func (s *kafkaSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := kafkaCfg(conn)
	if err != nil {
		return nil, err
	}
	client, err := kafkaClient(cfg, 8*time.Second)
	if err != nil {
		return nil, err
	}
	admin := kadm.NewClient(client)
	tctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	starts, err := admin.ListStartOffsets(tctx, destination)
	if err != nil {
		client.Close()
		return nil, err
	}
	ends, err := admin.ListEndOffsets(tctx, destination)
	if err != nil {
		client.Close()
		return nil, err
	}
	client.Close()
	assign := map[int32]kgo.Offset{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil || o.Topic != destination {
			return
		}
		low := int64(0)
		if so, ok := starts.Lookup(destination, o.Partition); ok && so.Err == nil {
			low = so.Offset
		}
		if o.Offset > low {
			assign[o.Partition] = kgo.NewOffset().At(max(low, o.Offset-int64(limit)))
		}
	})
	if len(assign) == 0 {
		return []TailRecord{}, nil
	}
	consumer, err := kafkaClient(cfg, 8*time.Second, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{destination: assign}))
	if err != nil {
		return nil, err
	}
	defer consumer.Close()
	wanted := min(limit*len(assign), 10*limit)
	var records []TailRecord
	deadline := time.Now().Add(timeout)
	for len(records) < wanted && time.Now().Before(deadline) {
		pctx, pcancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fetches := consumer.PollFetches(pctx)
		pcancel()
		fetches.EachRecord(func(r *kgo.Record) {
			var ts *string
			if !r.Timestamp.IsZero() && r.Timestamp.UnixMilli() > 0 {
				t := pytime.ISOFormat(time.UnixMilli(r.Timestamp.UnixMilli()))
				ts = &t
			}
			p := int(r.Partition)
			off := r.Offset
			records = append(records, TailRecord{Destination: destination, Partition: &p, Offset: &off, Timestamp: ts,
				Key: decodePayload(r.Key), Value: decodePayload(r.Value)})
		})
	}
	sort.SliceStable(records, func(i, j int) bool {
		a, b := orEmpty(records[i].Timestamp), orEmpty(records[j].Timestamp)
		if a != b {
			return a < b
		}
		return derefI(records[i].Offset) < derefI(records[j].Offset)
	})
	if len(records) > limit {
		records = records[len(records)-limit:]
	}
	if records == nil {
		records = []TailRecord{}
	}
	return records, nil
}

func derefI(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}
