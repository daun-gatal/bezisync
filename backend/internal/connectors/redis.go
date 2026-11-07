package connectors

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Redis Streams sink: XADD events, one stream per table.

type redisSink struct{ BaseSink }

func init() { Register(&redisSink{BaseSink{Title: "Redis Streams"}}) }

func (*redisSink) Kind() string { return "sink" }
func (*redisSink) Type() string { return "redis" }

type redisConfig struct {
	Host           string
	Port           int
	User, Password *string
	TLS            bool
	MaxLen         *int
}

func redisCfg(conn Resolved) (redisConfig, error) {
	d, _ := Describe("sink", "redis")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return redisConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return redisConfig{}, err
	}
	out := redisConfig{Host: Str(cfg, "host"), Port: Int(cfg, "port"), User: OptStr(cfg, "user"), Password: OptStr(sec, "password"), TLS: Bool(cfg, "tls")}
	if v, ok := cfg["max_stream_length"]; ok && v != nil {
		n := Int(cfg, "max_stream_length")
		out.MaxLen = &n
	}
	return out, nil
}

// redisClient mirrors redis-py: TLS requires a valid certificate chain but does not check
// the hostname (ssl_check_hostname defaults to False).
func redisClient(cfg redisConfig, timeout time.Duration) *redis.Client {
	opts := &redis.Options{
		Addr: net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)), Username: orEmpty(cfg.User), Password: orEmpty(cfg.Password),
		DialTimeout: timeout, ReadTimeout: timeout, WriteTimeout: timeout, MaxRetries: -1, Protocol: 2,
		DisableIdentity: true,
	}
	if cfg.TLS {
		opts.TLSConfig = &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true, //nolint:gosec // chain verified below, hostname deliberately not (redis-py default)
			VerifyConnection: func(cs tls.ConnectionState) error {
				certs := cs.PeerCertificates
				if len(certs) == 0 {
					return errors.New("no server certificate")
				}
				inter := x509.NewCertPool()
				for _, c := range certs[1:] {
					inter.AddCert(c)
				}
				_, err := certs[0].Verify(x509.VerifyOptions{Intermediates: inter})
				return err
			},
		}
	}
	// The SSRF guard (ADR 0004). A custom dialer replaces go-redis's, TLS included.
	opts.Dialer = tlsDialer(opts.TLSConfig)
	return redis.NewClient(opts)
}

func isRedisAuth(err error) bool {
	s := err.Error()
	return strings.HasPrefix(s, "WRONGPASS") || strings.HasPrefix(s, "NOAUTH") || strings.Contains(s, "invalid username-password") ||
		strings.Contains(s, "AUTH failed") || strings.Contains(s, "invalid password")
}

func isRedisResponse(err error) bool {
	var re redis.Error
	return errors.As(err, &re) && !isRedisAuth(err)
}

func (s *redisSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	cfg, err := redisCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	if !NetworkCheck(ctx, run, cfg.Host, cfg.Port, "redis_network", "Redis", 0) {
		return checks.Summarize(run.Checks, started), nil
	}
	r := redisClient(cfg, 5*time.Second)
	defer func() { _ = r.Close() }()
	err = func() error {
		if err := r.Ping(ctx).Err(); err != nil {
			return err
		}
		run.Passed("redis_auth", "Sign in", checks.CatAuth, "Redis answered PING.", "")
		probe := "bezisync:permission-probe"
		if err := r.XAdd(ctx, &redis.XAddArgs{Stream: probe, MaxLen: 1, Values: []any{"probe", "1"}}).Err(); err != nil {
			return err
		}
		if err := r.Del(ctx, probe).Err(); err != nil {
			return err
		}
		run.Passed("redis_write", "Write streams", checks.CatPermissions, "Can append to streams.", "")
		info, err := r.InfoMap(ctx, "memory").Result()
		if err != nil {
			return err
		}
		mem := info["Memory"]
		used, _ := strconv.ParseInt(mem["used_memory"], 10, 64)
		limit, _ := strconv.ParseInt(mem["maxmemory"], 10, 64)
		if limit != 0 && float64(used) > float64(limit)*0.8 {
			run.Add(checks.Check{Code: "redis_memory", Title: "Memory headroom", Status: checks.Warn, Category: checks.CatCapacity,
				Message: fmt.Sprintf("Redis uses %d%% of maxmemory; streams grow with every change.", used*100/limit),
				Why:     "Set a stream length limit or add memory."})
		}
		return nil
	}()
	if err != nil {
		text := firstRunes(err.Error(), 300)
		switch {
		case isRedisAuth(err):
			run.Add(checks.Check{Code: "redis_auth", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
				Message: "Redis rejected the credentials.", Technical: text})
		case isRedisResponse(err):
			run.Add(checks.Check{Code: "redis_write", Title: "Write streams", Status: checks.Fail, Category: checks.CatPermissions, Blocking: true,
				Message: "This user may not write streams.", Technical: text})
		default:
			run.Add(checks.Check{Code: "redis_connect", Title: "Talk to Redis", Status: checks.Fail, Category: checks.CatNetwork, Blocking: true,
				Message: "The port is open but Redis did not answer.", Technical: text})
		}
	}
	return checks.Summarize(run.Checks, started), nil
}

func (s *redisSink) CompileSink(conn Resolved, _ CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := redisCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	props := map[string]string{
		"debezium.sink.type":                 "redis",
		"debezium.sink.redis.address":        fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		"debezium.sink.redis.ssl.enabled":    boolStr(cfg.TLS),
		"debezium.sink.redis.message.format": "compact",
	}
	env := map[string]string{}
	if u := orEmpty(cfg.User); u != "" {
		props["debezium.sink.redis.user"] = u
	}
	if pw := orEmpty(cfg.Password); pw != "" {
		props["debezium.sink.redis.password"] = "${BZ_SINK_PASSWORD}"
		env["BZ_SINK_PASSWORD"] = pw
	}
	if cfg.MaxLen != nil && *cfg.MaxLen != 0 {
		props["debezium.sink.redis.stream.max.length"] = strconv.Itoa(*cfg.MaxLen)
	}
	return props, env, nil
}

func (s *redisSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := redisCfg(conn)
	if err != nil {
		return nil, err
	}
	r := redisClient(cfg, 5*time.Second)
	defer func() { _ = r.Close() }()
	var results []ArtifactResult
	for _, stream := range sortedUnique(destinations) {
		n, err := r.Del(ctx, stream).Result()
		if err != nil {
			return nil, err
		}
		results = append(results, ArtifactResult{Kind: "stream", Name: stream, Outcome: pick(n > 0, "removed", "not_found")})
	}
	return results, nil
}

func redisDecode(raw any) any {
	s, ok := raw.(string)
	if !ok {
		return raw
	}
	if v, err := decodeJSON([]byte(s)); err == nil {
		return v
	}
	return s
}

func (s *redisSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := redisCfg(conn)
	if err != nil {
		return nil, err
	}
	r := redisClient(cfg, timeout)
	defer func() { _ = r.Close() }()
	// Raw XREVRANGE keeps each entry's field order (the first field is key, its value the event).
	res, err := r.Do(ctx, "XREVRANGE", destination, "+", "-", "COUNT", limit).Slice()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return []TailRecord{}, nil
		}
		return nil, err
	}
	records := []TailRecord{}
	for i := len(res) - 1; i >= 0; i-- {
		entry, ok := res[i].([]any)
		if !ok || len(entry) < 2 {
			continue
		}
		id, _ := entry[0].(string)
		msStr, _, _ := strings.Cut(id, "-")
		ms, _ := strconv.ParseInt(msStr, 10, 64)
		var key, value any
		if fields, ok := entry[1].([]any); ok && len(fields) >= 2 {
			key, value = fields[0], fields[1]
		}
		ts := pytime.ISOFormat(time.UnixMilli(ms))
		records = append(records, TailRecord{Destination: destination, Key: redisDecode(key), Value: redisDecode(value), Timestamp: &ts})
	}
	return records, nil
}
