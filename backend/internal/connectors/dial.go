package connectors

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// Every socket a connector opens to an address a user typed, or one a server it reached
// advertised (Kafka brokers, MongoDB members, NATS servers, Redis nodes), goes through
// DialContext (ADR 0004). Probing first is never enough: a driver resolves DNS again and follows
// what the server tells it.

// DialFunc opens a connection (the shape pgx, the MySQL driver and franz-go take).
type DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error)

// DialContext opens a connection for a connector through the SSRF guard.
func DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	return ssrf.DialContext(ctx, network, addr)
}

// tunnelDialer is the dialer of one tunnelled connection: the local end of its own SSH forward
// (BeziSync's listener on loopback, which the guard would refuse) is dialled directly; any
// other address, another tunnel's included, goes through the guard.
func tunnelDialer(local string) DialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr == local {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		}
		return DialContext(ctx, network, addr)
	}
}

// guardedDialer is a DialFunc (DialContext when nil) as the dialer interface drivers take (SQL
// Server, Oracle, MongoDB, NATS).
type guardedDialer struct {
	dial    DialFunc
	timeout time.Duration
}

func (g guardedDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if g.dial != nil {
		return g.dial(ctx, network, addr)
	}
	return DialContext(ctx, network, addr)
}

// Dial is for drivers without a context (NATS): the dialer's own timeout bounds it.
func (g guardedDialer) Dial(network, addr string) (net.Conn, error) {
	timeout := g.timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return g.DialContext(ctx, network, addr)
}

// tlsDialer dials through the guard, then speaks TLS (for drivers that skip their own TLS
// settings when given a dialer: franz-go, go-redis). A nil config is plain TCP.
func tlsDialer(cfg *tls.Config) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		raw, err := DialContext(ctx, network, addr)
		if err != nil || cfg == nil {
			return raw, err
		}
		c := cfg.Clone()
		if c.ServerName == "" {
			if host, _, err := net.SplitHostPort(addr); err == nil {
				c.ServerName = host
			}
		}
		conn := tls.Client(raw, c)
		if err := conn.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			return nil, err
		}
		return conn, nil
	}
}

// grpcDial is the SSRF guard as a gRPC dial option, for user-set emulator hosts (Pub/Sub,
// Spanner); Google's own endpoints are fixed and public.
func grpcDial(ctx context.Context, addr string) (net.Conn, error) {
	return DialContext(ctx, "tcp", addr)
}

// GuardAWS sends an AWS configuration's service calls (to a user-set endpoint or AWS's own)
// through the SSRF guard, after the configuration loaded: the credential chain resolved during
// LoadDefaultConfig keeps the SDK's client, so an IAM role from instance or pod metadata (a
// link-local address the guard refuses) keeps working (ADR 0004).
func GuardAWS(ac aws.Config, timeout time.Duration) aws.Config {
	ac.HTTPClient = ssrf.HTTPClient(timeout)
	return ac
}
