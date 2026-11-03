package connectors

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Azure Event Hubs sink: every table goes to one event hub (Debezium's sink ignores the
// destination), partitioned by the record key so each row keeps its order. It signs in with a
// connection string (shared access key) without EntityPath. Sending needs Send rights; the
// Events tab and the delivery canary read with Listen rights, from a second connection string
// when the first cannot listen.

type eventHubsSink struct{ BaseSink }

func init() { Register(&eventHubsSink{BaseSink{Title: "Azure Event Hubs"}}) }

func (*eventHubsSink) Kind() string { return "sink" }
func (*eventHubsSink) Type() string { return "eventhubs" }

type eventHubsConfig struct {
	Hub, Connection, ListenConnection string
}

func eventHubsCfg(conn Resolved) (eventHubsConfig, error) {
	d, _ := Describe("sink", "eventhubs")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return eventHubsConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return eventHubsConfig{}, err
	}
	c := eventHubsConfig{Hub: Str(cfg, "hub_name"), Connection: Str(sec, "connection_string"), ListenConnection: Str(sec, "listen_connection_string")}
	for field, v := range map[string]string{"connection_string": c.Connection, "listen_connection_string": c.ListenConnection} {
		if v == "" {
			continue
		}
		// The sink appends ";EntityPath=<hub>"; a second one breaks the connection string.
		if strings.Contains(strings.ToLower(v), "entitypath=") {
			msg := "Use the namespace's connection string, without EntityPath (the hub is set separately)."
			return c, problem.New(422, "eventhubs_config", msg).WithErrors([]map[string]any{{"loc": []string{"secrets", field}, "message": msg}})
		}
	}
	if c.Connection == "" {
		msg := "Enter the connection string."
		return c, problem.New(422, "eventhubs_config", msg).WithErrors([]map[string]any{{"loc": []string{"secrets", "connection_string"}, "message": msg}})
	}
	return c, nil
}

// endpoint is the namespace host and AMQP port of a connection string.
func eventHubsEndpoint(connection string) (string, int) {
	emulator := false
	host := ""
	for _, part := range strings.Split(connection, ";") {
		k, v, _ := strings.Cut(part, "=")
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "endpoint":
			if u, err := url.Parse(strings.TrimSpace(v)); err == nil {
				host = u.Hostname()
			}
		case "usedevelopmentemulator":
			emulator = strings.EqualFold(strings.TrimSpace(v), "true")
		}
	}
	if emulator {
		return host, 5672
	}
	return host, 5671
}

func (s *eventHubsSink) CompileSink(conn Resolved, _ CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := eventHubsCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	props := map[string]string{
		"debezium.sink.type":                       "eventhubs",
		"debezium.sink.eventhubs.connectionstring": "${BZ_EVENTHUBS_CONNECTION}",
		"debezium.sink.eventhubs.hubname":          cfg.Hub,
	}
	return props, map[string]string{"BZ_EVENTHUBS_CONNECTION": cfg.Connection}, nil
}

// Destination: every table goes to the hub.
func (s *eventHubsSink) Destination(conn Resolved, _ CompileContext, _ string) string {
	return Str(conn.Config, "hub_name")
}

func eventHubsFailure(run *checks.Run, err error, code, title string, blocking bool, msg string) {
	status := checks.Fail
	if !blocking {
		status = checks.Warn
	}
	c := checks.Check{Code: code, Title: title, Status: status, Category: checks.CatAuth, Blocking: blocking, Message: msg,
		Technical: firstRunes(err.Error(), 300)}
	var ehErr *azeventhubs.Error
	if errors.As(err, &ehErr) && ehErr.Code == azeventhubs.ErrorCodeUnauthorizedAccess {
		c.Fix = &checks.Fix{Kind: "text", Who: "Azure admin", Code: "Create a shared access policy on the namespace with Send (and Listen for the Events tab), and use its connection string."}
	}
	run.Add(c)
}

func (s *eventHubsSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	run := &checks.Run{}
	cfg, err := eventHubsCfg(conn)
	if err != nil {
		var p *problem.Problem
		if errors.As(err, &p) {
			run.Add(checks.Check{Code: "eventhubs_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true, Message: p.Detail})
			return checks.Summarize(run.Checks, started), nil
		}
		return checks.Report{}, err
	}
	host, port := eventHubsEndpoint(cfg.Connection)
	if host == "" {
		run.Add(checks.Check{Code: "eventhubs_config", Title: "Settings", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: "The connection string has no Endpoint."})
		return checks.Summarize(run.Checks, started), nil
	}
	if !NetworkCheck(ctx, run, host, port, "eventhubs_network", "Event Hubs", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	producer, err := azeventhubs.NewProducerClientFromConnectionString(cfg.Connection, cfg.Hub, nil)
	var props azeventhubs.EventHubProperties
	if err == nil {
		defer func() { _ = producer.Close(context.WithoutCancel(ctx)) }()
		props, err = producer.GetEventHubProperties(ctx, nil)
	}
	if err != nil {
		eventHubsFailure(run, err, "eventhubs_auth", "Sign in", true, fmt.Sprintf("Event Hubs refused the connection string, or hub %s does not exist.", cfg.Hub))
		return checks.Summarize(run.Checks, started), nil
	}
	run.Passed("eventhubs_auth", "Sign in", checks.CatAuth, fmt.Sprintf("Hub %s has %d partitions.", cfg.Hub, len(props.PartitionIDs)), "")
	if _, err := eventHubsLastSequences(ctx, cfg); err != nil {
		eventHubsFailure(run, err, "eventhubs_listen", "Events tab", false,
			"These credentials cannot read the hub (Listen), so the Events tab and the delivery canary stay empty. Add a connection string with Listen rights.")
	} else {
		run.Passed("eventhubs_listen", "Events tab", checks.CatAuth, "BeziSync can read the hub for the Events tab.", "")
	}
	return checks.Summarize(run.Checks, started), nil
}

func (c eventHubsConfig) listen() string {
	if c.ListenConnection != "" {
		return c.ListenConnection
	}
	return c.Connection
}

// eventHubsLastSequences reads each partition's last sequence number (needs Listen).
func eventHubsLastSequences(ctx context.Context, cfg eventHubsConfig) (map[string]int64, error) {
	consumer, err := azeventhubs.NewConsumerClientFromConnectionString(cfg.listen(), cfg.Hub, azeventhubs.DefaultConsumerGroup, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = consumer.Close(context.WithoutCancel(ctx)) }()
	props, err := consumer.GetEventHubProperties(ctx, nil)
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	for _, id := range props.PartitionIDs {
		p, err := consumer.GetPartitionProperties(ctx, id, nil)
		if err != nil {
			return nil, err
		}
		if !p.IsEmpty {
			out[id] = p.LastEnqueuedSequenceNumber
		}
	}
	return out, nil
}

// Tail reads the last events of every partition (a reader, not a checkpointing consumer), newest
// last.
func (s *eventHubsSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := eventHubsCfg(conn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, max(timeout, 10*time.Second))
	defer cancel()
	last, err := eventHubsLastSequences(ctx, cfg)
	if err != nil {
		return nil, err
	}
	consumer, err := azeventhubs.NewConsumerClientFromConnectionString(cfg.listen(), cfg.Hub, azeventhubs.DefaultConsumerGroup, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = consumer.Close(context.WithoutCancel(ctx)) }()
	var out []TailRecord
	for id, seq := range last {
		start := max(seq-int64(limit)+1, 0)
		pc, err := consumer.NewPartitionClient(id, &azeventhubs.PartitionClientOptions{
			StartPosition: azeventhubs.StartPosition{SequenceNumber: &start, Inclusive: true}})
		if err != nil {
			return nil, err
		}
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		events, err := pc.ReceiveEvents(rctx, int(seq-start+1), nil)
		rcancel()
		_ = pc.Close(context.WithoutCancel(ctx))
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		for _, e := range events {
			var value any = string(e.Body)
			if v, err := decodeJSON(e.Body); err == nil {
				value = v
			}
			var ts *string
			if e.EnqueuedTime != nil {
				t := pytime.ISOFormat(*e.EnqueuedTime)
				ts = &t
			}
			var key any
			if e.PartitionKey != nil {
				key = *e.PartitionKey
			}
			partition, _ := strconv.Atoi(id)
			offset := e.SequenceNumber
			out = append(out, TailRecord{Destination: destination, Partition: &partition, Offset: &offset, Timestamp: ts, Key: key, Value: value})
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return orEmpty(out[i].Timestamp) < orEmpty(out[j].Timestamp) })
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	if out == nil {
		out = []TailRecord{}
	}
	return out, nil
}
