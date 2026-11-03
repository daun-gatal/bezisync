package connectors

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// NATS JetStream sink: one subject per table in a persistent stream.

type natsSink struct{ BaseSink }

func init() { Register(&natsSink{BaseSink{Title: "NATS JetStream"}}) }

func (*natsSink) Kind() string { return "sink" }
func (*natsSink) Type() string { return "nats" }

type natsConfig struct {
	URL, Stream    string
	User, Password *string
}

func natsCfg(conn Resolved) (natsConfig, error) {
	d, _ := Describe("sink", "nats")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return natsConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return natsConfig{}, err
	}
	return natsConfig{URL: Str(cfg, "url"), Stream: Str(cfg, "stream"), User: OptStr(cfg, "user"), Password: OptStr(sec, "password")}, nil
}

func natsConnect(cfg natsConfig) (*nats.Conn, error) {
	opts := []nats.Option{nats.Timeout(5 * time.Second), nats.NoReconnect(), nats.MaxReconnects(0)}
	if cfg.User != nil || cfg.Password != nil {
		opts = append(opts, nats.UserInfo(orEmpty(cfg.User), orEmpty(cfg.Password)))
	}
	return nats.Connect(cfg.URL, opts...)
}

func (s *natsSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	cfg, err := natsCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	hostPort := strings.SplitN(strings.SplitN(cfg.URL, "://", 2)[1], "/", 2)[0]
	var host, port string
	if i := strings.LastIndex(hostPort, ":"); i >= 0 {
		host, port = hostPort[:i], hostPort[i+1:]
	}
	p := 4222
	if isDigits(port) {
		p, _ = strconv.Atoi(port)
	}
	if host == "" {
		host = hostPort
	}
	if !NetworkCheck(ctx, run, host, p, "nats_network", "NATS", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	err = func() error {
		nc, err := natsConnect(cfg)
		if err != nil {
			return err
		}
		defer nc.Close()
		js, err := nc.JetStream()
		if err != nil {
			return err
		}
		_, err = js.AccountInfo()
		return err
	}()
	if err == nil {
		run.Passed("nats_jetstream", "JetStream available", checks.CatDestination, "Connected; JetStream is enabled.", "")
	} else {
		text := err.Error()
		auth := strings.Contains(strings.ToLower(text), "authorization") || errors.Is(err, nats.ErrAuthorization) ||
			errors.Is(err, nats.ErrAuthExpired) || errors.Is(err, nats.ErrAuthRevoked)
		c := checks.Check{Code: "nats_jetstream", Title: "JetStream available", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: "JetStream is not enabled on this server.", Technical: firstRunes(text, 300)}
		if auth {
			c.Code, c.Title, c.Category, c.Message = "nats_auth", "Sign in", checks.CatAuth, "NATS rejected the credentials."
		}
		run.Add(c)
	}
	return checks.Summarize(run.Checks, started), nil
}

func (s *natsSink) CompileSink(conn Resolved, _ CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := natsCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	props := map[string]string{
		"debezium.sink.type":                         "nats-jetstream",
		"debezium.sink.nats-jetstream.url":           cfg.URL,
		"debezium.sink.nats-jetstream.create-stream": "false",
	}
	env := map[string]string{}
	if u := orEmpty(cfg.User); u != "" {
		props["debezium.sink.nats-jetstream.auth.user"] = u
	}
	if pw := orEmpty(cfg.Password); pw != "" {
		props["debezium.sink.nats-jetstream.auth.password"] = "${BZ_SINK_PASSWORD}"
		env["BZ_SINK_PASSWORD"] = pw
	}
	return props, env, nil
}

func (s *natsSink) PrepareSink(_ context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := natsCfg(conn)
	if err != nil {
		return nil, err
	}
	subjects := sortedUnique(destinations)
	nc, err := natsConnect(cfg)
	if err != nil {
		return nil, err
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	outcome := "exists"
	info, err := js.StreamInfo(cfg.Stream)
	switch {
	case errors.Is(err, nats.ErrStreamNotFound):
		if _, err := js.AddStream(&nats.StreamConfig{Name: cfg.Stream, Subjects: subjects}); err != nil {
			return nil, err
		}
		outcome = "created"
	case err != nil:
		return nil, err
	default:
		current := map[string]bool{}
		for _, s := range info.Config.Subjects {
			current[s] = true
		}
		missing := false
		for _, s := range subjects {
			if !current[s] {
				missing = true
				current[s] = true
			}
		}
		if missing {
			merged := make([]string, 0, len(current))
			for s := range current {
				merged = append(merged, s)
			}
			sort.Strings(merged)
			info.Config.Subjects = merged
			if _, err := js.UpdateStream(&info.Config); err != nil {
				return nil, err
			}
		}
	}
	return []ArtifactResult{{Kind: "stream", Name: cfg.Stream, Outcome: outcome, Detail: "Subjects: " + strings.Join(subjects, ", ")}}, nil
}

func (s *natsSink) Tail(_ context.Context, conn Resolved, destination string, limit int, _ time.Duration) ([]TailRecord, error) {
	cfg, err := natsCfg(conn)
	if err != nil {
		return nil, err
	}
	nc, err := natsConnect(cfg)
	if err != nil {
		return nil, err
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	info, err := js.StreamInfo(cfg.Stream)
	if err != nil {
		return nil, err
	}
	var out []TailRecord
	last := int64(info.State.LastSeq)
	firstSeq := max(int64(1), int64(info.State.FirstSeq))
	for seq := last; seq >= firstSeq && len(out) < limit && seq > last-int64(limit*20); seq-- {
		msg, err := js.GetMsg(cfg.Stream, uint64(seq))
		if err != nil {
			continue
		}
		if msg.Subject != destination {
			continue
		}
		data := msg.Data
		if data == nil {
			data = []byte("null")
		}
		var value any
		if v, err := decodeJSON(data); err == nil {
			value = v
		} else {
			value = decodeText(msg.Data)
		}
		off := seq
		out = append(out, TailRecord{Destination: destination, Offset: &off, Value: value})
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	if out == nil {
		out = []TailRecord{}
	}
	return out, nil
}
