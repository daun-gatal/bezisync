package connectors

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// trap is a TCP listener on loopback that counts the connections it accepts.
func trap(t *testing.T) (string, int, *atomic.Int64) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Close() })
	var n atomic.Int64
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			_ = c.Close()
		}
	}()
	return "127.0.0.1", l.Addr().(*net.TCPAddr).Port, &n
}

// guardOn loads the default settings: loopback is refused (no BEZISYNC_SSRF_ALLOW_CIDRS).
func guardOn(t *testing.T) {
	t.Helper()
	s, err := config.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
}

// Every driver dials through the SSRF guard (ADR 0004, Known issue 1): pointed at a loopback
// listener, each is refused with BlockedAddress and the listener never sees a connection. This
// covers every path that opens the driver (Discover, Prepare, Cleanup, Signal, Probe, Tail), not
// only connection checks, which probe first.
func TestEveryDriverDialsThroughTheGuard(t *testing.T) {
	guardOn(t)
	host, port, accepted := trap(t)
	addr := net.JoinHostPort(host, itoa(port))
	var ctx context.Context // each case gets its own deadline below
	db := func(typ string, extra map[string]any) Resolved {
		cfg := map[string]any{"host": host, "port": port, "database": "d", "user": "u"}
		for k, v := range extra {
			cfg[k] = v
		}
		return Resolved{Type: typ, Provider: "self_hosted", Config: cfg, Secrets: map[string]any{"password": "p"}}
	}
	for name, open := range map[string]func() error{
		"postgresql": func() error { _, _, err := pgConnect(ctx, db("postgresql", nil), 3*time.Second); return err },
		"cockroachdb": func() error {
			_, _, err := crdbConnect(ctx, db("cockroachdb", nil), 3*time.Second)
			return err
		},
		"mysql": func() error {
			_, _, err := myConnect(ctx, db("mysql", map[string]any{"server_id": 5400}), 3*time.Second)
			return err
		},
		"vitess": func() error {
			_, _, err := vtConnect(ctx, db("vitess", map[string]any{"keyspace": "k"}), 3*time.Second)
			return err
		},
		"sqlserver": func() error { _, _, err := msConnect(ctx, db("sqlserver", nil), 3*time.Second); return err },
		"oracle": func() error {
			_, _, err := oraConnect(ctx, db("oracle", map[string]any{"service_name": "FREE"}), "FREE", 3*time.Second)
			return err
		},
		"mongodb": func() error {
			c, done, err := mongoConnect(ctx, Resolved{Type: "mongodb", Provider: "self_hosted",
				Config: map[string]any{"hosts": addr, "user": "u", "auth_source": "admin"}, Secrets: map[string]any{"password": "p"}}, 2*time.Second)
			if err == nil {
				defer done()
				err = c.Ping(ctx, nil)
			}
			return err
		},
		"kafka": func() error {
			c, err := kafkaClient(kafkaConfig{Bootstrap: addr, Protocol: "PLAINTEXT"}, 2*time.Second)
			if err == nil {
				defer c.Close()
				err = c.Ping(ctx)
			}
			return err
		},
		"kafka over TLS": func() error {
			c, err := kafkaClient(kafkaConfig{Bootstrap: addr, Protocol: "SSL"}, 2*time.Second)
			if err == nil {
				defer c.Close()
				err = c.Ping(ctx)
			}
			return err
		},
		"redis": func() error {
			c := redisClient(redisConfig{Host: host, Port: port}, 2*time.Second)
			defer func() { _ = c.Close() }()
			return c.Ping(ctx).Err()
		},
		"redis over TLS": func() error {
			c := redisClient(redisConfig{Host: host, Port: port, TLS: true}, 2*time.Second)
			defer func() { _ = c.Close() }()
			return c.Ping(ctx).Err()
		},
		"nats": func() error { _, err := natsConnect(natsConfig{URL: "nats://" + addr}); return err },
		"jdbc postgres": func() error {
			_, err := jdbcConfig{DBType: "postgresql", Host: host, Port: port, Database: "d", User: "u", TLS: "disable"}.open(ctx, 2*time.Second)
			return err
		},
		"jdbc mysql": func() error {
			_, err := jdbcConfig{DBType: "mysql", Host: host, Port: port, Database: "d", User: "u", TLS: "disable"}.open(ctx, 2*time.Second)
			return err
		},
		"jdbc sqlserver": func() error {
			_, err := jdbcConfig{DBType: "sqlserver", Host: host, Port: port, Database: "d", User: "u", TLS: "disable"}.open(ctx, 2*time.Second)
			return err
		},
		"starrocks/doris": func() error {
			_, _, err := olapConfig{Host: host, QueryPort: port, User: "u"}.open(ctx, 2*time.Second)
			return err
		},
		"pubsub emulator": func() error {
			pub, err := pubsubPublisher(ctx, pubsubConfig{ProjectID: "p", Emulator: &addr})
			if err == nil {
				defer func() { _ = pub.Close() }()
				_, err = listTopics(ctx, pub, "p")
			}
			return err
		},
		"spanner emulator": func() error {
			c, err := spClient(ctx, spConfig{Project: "p", Instance: "i", Database: "d", Emulator: addr})
			if err == nil {
				defer c.Close()
				_, err = c.Single().ReadRow(ctx, "t", nil, []string{"c"})
			}
			return err
		},
		"kinesis endpoint": func() error {
			ak, sk, ep := "a", "s", "http://"+addr
			c, err := kinesisClient(ctx, kinesisConfig{Region: "us-east-1", AccessKey: &ak, SecretKey: &sk, Endpoint: &ep})
			if err == nil {
				_, err = c.ListStreams(ctx, &kinesis.ListStreamsInput{})
			}
			return err
		},
		"sqs endpoint": func() error {
			c, err := sqsConfig{Region: "us-east-1", AccessKey: "a", SecretKey: "s", Endpoint: "http://" + addr, QueueURL: "http://" + addr + "/1/q"}.client(ctx)
			if err == nil {
				_, err = c.ListQueues(ctx, &sqs.ListQueuesInput{})
			}
			return err
		},
		"event hubs": func() error {
			return eventHubsGuard(ctx, "Endpoint=sb://"+addr+"/;SharedAccessKeyName=k;SharedAccessKey=v")
		},
	} {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		before := accepted.Load()
		err := open()
		cancel()
		var blocked *ssrf.BlockedAddress
		switch {
		case accepted.Load() != before:
			t.Errorf("%s: reached the loopback listener", name)
		case err == nil:
			t.Errorf("%s: no error", name)
		case strings.HasSuffix(name, "emulator"):
			// gRPC retries a refused dial until the deadline; that it never connected is the proof.
		case !errors.As(err, &blocked) && !strings.Contains(err.Error(), "BeziSync is not allowed to contact"):
			t.Errorf("%s: want the guard's refusal, got %v", name, err)
		}
	}
	if n := accepted.Load(); n != 0 {
		t.Fatalf("the loopback listener accepted %d connection(s)", n)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }

// A tunnelled connection may reach the local end of its own forward and nothing else on
// loopback: not another connection's tunnel, and no other connection may reach this one's
// (ADR 0004).
func TestTunnelEndIsOnlyForItsOwnConnection(t *testing.T) {
	guardOn(t)
	host, port, accepted := trap(t)
	_, otherPort, otherAccepted := trap(t)
	own := net.JoinHostPort(host, strconv.Itoa(port))
	other := net.JoinHostPort(host, strconv.Itoa(otherPort))
	ctx := context.Background()
	dial := tunnelDialer(own)
	c, err := dial(ctx, "tcp", own)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if _, err := dial(ctx, "tcp", other); err == nil {
		t.Fatal("a tunnel's dialer reached another loopback listener")
	}
	if _, err := DialContext(ctx, "tcp", own); err == nil {
		t.Fatal("another connection reached this tunnel's listener")
	}
	deadline := time.Now().Add(2 * time.Second)
	for accepted.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if accepted.Load() != 1 || otherAccepted.Load() != 0 {
		t.Fatalf("own %d, other %d", accepted.Load(), otherAccepted.Load())
	}
	// Without SSH, Tunnel hands back the plain guard.
	if _, _, d, done, err := Tunnel(ctx, map[string]any{}, map[string]any{}, host, port); err != nil {
		t.Fatal(err)
	} else if done(); d == nil {
		t.Fatal("no dialer")
	} else if _, err := d(ctx, "tcp", own); err == nil {
		t.Fatal("an untunnelled connection reached loopback")
	}
}

// tlsDialer speaks TLS over the guarded connection with the driver's configuration and the
// server's name (SNI) when the configuration has none (Kafka and Redis over TLS).
func TestTLSDialerHandshakes(t *testing.T) {
	s, err := config.Load(func(k string) (string, bool) {
		if k == "BEZISYNC_SSRF_ALLOW_CIDRS" {
			return `["127.0.0.0/8"]`, true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	addr := strings.TrimPrefix(srv.URL, "https://") // 127.0.0.1:port, a name the certificate holds
	c, err := tlsDialer(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatal(err) // verification uses the dialled host: it fails if ServerName isn't filled in
	}
	defer func() { _ = c.Close() }()
	tc, ok := c.(*tls.Conn)
	if !ok || !tc.ConnectionState().HandshakeComplete || len(tc.ConnectionState().VerifiedChains) == 0 {
		t.Fatalf("tls %v %+v", ok, tc.ConnectionState())
	}
	if raw, err := tlsDialer(nil)(context.Background(), "tcp", addr); err != nil {
		t.Fatal(err)
	} else if _, isTLS := raw.(*tls.Conn); isTLS {
		t.Fatal("no TLS configuration must be plain TCP")
	} else {
		_ = raw.Close()
	}
}
