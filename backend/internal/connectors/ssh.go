package connectors

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/logging"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// SSH tunnels to databases behind a bastion host. The control plane opens a short-lived
// local forward per connection (Tunnel); connector workloads run `bezisync tunnel` as a
// sidecar that keeps one open (Serve). The bastion's host key can be pinned; without it the
// key is accepted and the readiness check warns.

// TunnelPort is where Debezium reaches the sidecar.
const TunnelPort = 15432

// SSHConfig is the bastion part of a source config.
type SSHConfig struct {
	Host    string
	Port    int
	User    string
	HostKey string
}

// TunnelError means the tunnel could not be opened; the message is safe to show.
type TunnelError struct{ Msg string }

func (e *TunnelError) Error() string { return e.Msg }

// SSHOf returns the bastion settings and key, or nil when the source connects directly.
func SSHOf(config, secrets map[string]any) (*SSHConfig, string, error) {
	cfg := &SSHConfig{Host: Str(config, "ssh_host"), Port: 22, User: Str(config, "ssh_user"), HostKey: Str(config, "ssh_host_key")}
	if _, ok := config["ssh_port"]; ok && config["ssh_port"] != nil {
		cfg.Port = Int(config, "ssh_port")
	}
	if cfg.Host == "" {
		return nil, "", nil
	}
	key := ""
	if v, ok := secrets["ssh_private_key"]; ok && v != nil {
		key = PyStr(v)
	}
	if cfg.User == "" || key == "" {
		return nil, "", &TunnelError{"An SSH tunnel needs the SSH user and private key."}
	}
	return cfg, key, nil
}

func dialSSH(ctx context.Context, cfg *SSHConfig, key string, dial func(ctx context.Context, network, addr string) (net.Conn, error)) (*ssh.Client, error) {
	signer, err := ssh.ParsePrivateKey([]byte(key))
	if err != nil {
		return nil, &TunnelError{fmt.Sprintf("Cannot open SSH to %s:%d: invalid private key (%v)", cfg.Host, cfg.Port, err)}
	}
	hostKeyCallback := ssh.InsecureIgnoreHostKey() //nolint:gosec // unpinned: accepted, the readiness check warns
	if cfg.HostKey != "" {
		pinned, _, _, _, err := ssh.ParseAuthorizedKey([]byte(cfg.HostKey))
		if err != nil {
			return nil, &TunnelError{"The bastion's host key does not match the pinned key."}
		}
		hostKeyCallback = ssh.FixedHostKey(pinned)
	}
	conf := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: hostKeyCallback,
		Timeout:         10 * time.Second,
	}
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	raw, err := dial(dctx, "tcp", addr)
	if err != nil {
		return nil, &TunnelError{fmt.Sprintf("Cannot open SSH to %s:%d: %v", cfg.Host, cfg.Port, err)}
	}
	_ = raw.SetDeadline(time.Now().Add(10 * time.Second))
	c, chans, reqs, err := ssh.NewClientConn(raw, addr, conf)
	if err != nil {
		_ = raw.Close()
		switch {
		case isHostKeyError(err):
			return nil, &TunnelError{"The bastion's host key does not match the pinned key."}
		case isAuthError(err):
			return nil, &TunnelError{fmt.Sprintf("The bastion refused the SSH key for user %s.", cfg.User)}
		}
		return nil, &TunnelError{fmt.Sprintf("Cannot open SSH to %s:%d: %v", cfg.Host, cfg.Port, err)}
	}
	_ = raw.SetDeadline(time.Time{})
	client := ssh.NewClient(c, chans, reqs)
	go keepalive(client)
	return client, nil
}

func isHostKeyError(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "host key mismatch") || strings.Contains(err.Error(), "ssh: host key"))
}

func isAuthError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "unable to authenticate")
}

func keepalive(c *ssh.Client) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for range t.C {
		if _, _, err := c.SendRequest("keepalive@openssh.com", true, nil); err != nil {
			return
		}
	}
}

// forward serves l by forwarding each accepted connection to host:port through client.
func forward(l net.Listener, client *ssh.Client, host string, port int) {
	target := net.JoinHostPort(host, strconv.Itoa(port))
	for {
		local, err := l.Accept()
		if err != nil {
			return
		}
		go func() {
			defer func() { _ = local.Close() }()
			remote, err := client.Dial("tcp", target)
			if err != nil {
				return
			}
			defer func() { _ = remote.Close() }()
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); _, _ = io.Copy(remote, local); _ = remote.Close() }()
			go func() { defer wg.Done(); _, _ = io.Copy(local, remote); _ = local.Close() }()
			wg.Wait()
		}()
	}
}

// Tunnel returns the (host, port) to connect to (the database itself, or the local end of an
// SSH forward), the dialer this connection's driver must use (the SSRF guard, which lets this
// connection, and only it, reach its own forward: ADR 0004), and close, which releases it.
func Tunnel(ctx context.Context, config, secrets map[string]any, host string, port int) (string, int, DialFunc, func(), error) {
	cfg, key, err := SSHOf(config, secrets)
	if err != nil {
		return "", 0, nil, nil, err
	}
	if cfg == nil {
		return host, port, DialContext, func() {}, nil
	}
	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	// The control plane reaches the bastion through the SSRF guard. The forward's target is
	// resolved by the bastion ("localhost" there is a normal setup), so it can't be checked here.
	client, err := dialSSH(dctx, cfg, key, ssrf.DialContext)
	if err != nil {
		return "", 0, nil, nil, err
	}
	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		_ = client.Close()
		return "", 0, nil, nil, &TunnelError{fmt.Sprintf("Cannot open SSH to %s:%d: %v", cfg.Host, cfg.Port, err)}
	}
	go forward(l, client, host, port)
	closer := func() {
		_ = l.Close()
		_ = client.Close()
	}
	return "127.0.0.1", l.Addr().(*net.TCPAddr).Port, tunnelDialer(l.Addr().String()), closer, nil
}

// Serve keeps a forward open forever, reconnecting with backoff (the connector sidecar).
func Serve(ctx context.Context, cfg *SSHConfig, key, host string, port, listenPort int) error {
	return ServeOn(ctx, cfg, key, host, port, "127.0.0.1", listenPort)
}

// ServeOn is Serve listening on listenHost: a Flink CDC pipeline's tunnel on Docker listens on the
// pipeline's private network so its JobManager and TaskManagers all reach it.
func ServeOn(ctx context.Context, cfg *SSHConfig, key, host string, port int, listenHost string, listenPort int) error {
	log := logging.Logger("bezisync.tunnel")
	delay := time.Second
	for ctx.Err() == nil {
		// The sidecar runs next to the connector, which reaches the same hosts unguarded: BeziSync's
		// SSRF guard is the control plane's (ADR 0004), and the bastion passed it at check time.
		client, err := dialSSH(ctx, cfg, key, (&net.Dialer{}).DialContext)
		if err == nil {
			var l net.Listener
			var lc net.ListenConfig
			l, err = lc.Listen(ctx, "tcp", net.JoinHostPort(listenHost, strconv.Itoa(listenPort)))
			if err != nil {
				_ = client.Close()
				err = &TunnelError{fmt.Sprintf("Cannot open SSH to %s:%d: %v", cfg.Host, cfg.Port, err)}
			} else {
				log.Info(fmt.Sprintf("tunnel %s:%d → %s:%d via %s", listenHost, listenPort, host, port, cfg.Host))
				delay = time.Second
				go forward(l, client, host, port)
				_ = client.Wait()
				_ = l.Close()
				log.Warn("SSH connection closed; reconnecting")
			}
		}
		var te *TunnelError
		if errors.As(err, &te) {
			log.Warn(fmt.Sprintf("%s (retrying in %.0fs)", te.Msg, delay.Seconds()))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		delay = min(delay*2, 30*time.Second)
	}
	return nil
}

// SSHChecks reaches the bastion, then opens the tunnel to the database. Returns false
// (with failing checks recorded) when the database cannot be reached.
func SSHChecks(ctx context.Context, run *checks.Run, config, secrets map[string]any, host string, port int, code string) bool {
	cfg, _, err := SSHOf(config, secrets)
	if err != nil {
		run.Add(checks.Check{Code: code + "_ssh", Title: "SSH tunnel", Status: checks.Fail, Category: checks.CatNetwork,
			Blocking: true, Message: err.Error()})
		return false
	}
	if !NetworkCheck(ctx, run, cfg.Host, cfg.Port, code+"_ssh", "the SSH bastion", 0) {
		return false
	}
	_, _, _, closer, err := Tunnel(ctx, config, secrets, host, port)
	if err != nil {
		var te *TunnelError
		msg := err.Error()
		if errors.As(err, &te) {
			msg = te.Msg
		}
		run.Add(checks.Check{Code: code + "_ssh_tunnel", Title: "SSH tunnel", Status: checks.Fail, Category: checks.CatNetwork,
			Blocking: true, Message: msg, Why: "Connectors reach the database through this bastion, exactly like this check."})
		return false
	}
	closer()
	status, msg, why := checks.Pass, fmt.Sprintf("Tunnel to %s:%d through %s works.", host, port, cfg.Host), ""
	if cfg.HostKey == "" {
		status = checks.Warn
		msg += " The bastion's host key is not pinned, so it is not verified."
		why = "Pin the host key so nobody can impersonate the bastion."
	}
	run.Add(checks.Check{Code: code + "_ssh_tunnel", Title: "SSH tunnel", Status: status, Category: checks.CatNetwork, Message: msg, Why: why})
	return true
}
