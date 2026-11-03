// Package ssrf guards connections BeziSync opens on a user's behalf.
//
// Connection checks let users make the server open sockets to arbitrary hosts. Private
// networks must stay reachable (that is where databases live), but loopback, link-local
// and cloud metadata endpoints are denied unless explicitly allowed.
package ssrf

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/config"
)

// DefaultDeny are the networks never contacted unless BEZISYNC_SSRF_ALLOW_CIDRS allows them.
var DefaultDeny = []string{
	"0.0.0.0/8",
	"127.0.0.0/8",
	"169.254.0.0/16",     // link-local incl. 169.254.169.254 metadata
	"100.100.100.200/32", // Alibaba metadata
	"224.0.0.0/4",
	"::1/128",
	"fe80::/10",
	"fd00:ec2::254/128", // AWS IMDS over IPv6
	"ff00::/8",
}

// BlockedAddress means the destination resolves to a blocked network.
type BlockedAddress struct{ Msg string }

func (e *BlockedAddress) Error() string { return e.Msg }

func prefixes(values []string) []netip.Prefix {
	var out []netip.Prefix
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		p, err := netip.ParsePrefix(v)
		if err != nil {
			if a, err2 := netip.ParseAddr(v); err2 == nil {
				p = netip.PrefixFrom(a, a.BitLen())
			} else {
				continue
			}
		}
		out = append(out, p.Masked())
	}
	return out
}

func contains(nets []netip.Prefix, a netip.Addr) bool {
	for _, n := range nets {
		if n.Addr().Is4() != a.Is4() {
			continue
		}
		if n.Contains(a) {
			return true
		}
	}
	return false
}

// IsAllowed reports whether an IP may be contacted.
func IsAllowed(ip string) bool {
	a, err := netip.ParseAddr(ip)
	if err != nil {
		return false
	}
	a = a.Unmap()
	s := config.Get()
	if contains(prefixes(s.SSRFAllowCIDRs), a) {
		return true
	}
	deny := prefixes(append(append([]string{}, DefaultDeny...), s.SSRFExtraDenyCIDRs...))
	return !contains(deny, a)
}

// Resolver is overridable in tests.
var Resolver = net.DefaultResolver

// ResolveChecked resolves host and returns its addresses, refusing if any address is
// blocked (a DNS answer mixing a public and a metadata address is refused).
func ResolveChecked(ctx context.Context, host string, port int) ([]string, error) {
	var ips []string
	if a, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		ips = []string{a.Unmap().String()}
	} else {
		addrs, err := Resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("cannot resolve host '%s'", host)
		}
		seen := map[string]bool{}
		for _, a := range addrs {
			s := a.IP.String()
			if a.Zone != "" {
				s += "%" + a.Zone
			}
			if !seen[s] {
				seen[s] = true
				ips = append(ips, s)
			}
		}
		sort.Strings(ips)
	}
	var blocked []string
	for _, ip := range ips {
		if !IsAllowed(strings.SplitN(ip, "%", 2)[0]) {
			blocked = append(blocked, ip)
		}
	}
	if len(blocked) > 0 {
		return nil, &BlockedAddress{Msg: fmt.Sprintf("'%s' resolves to %s, which BeziSync is not allowed to contact. "+
			"An administrator can allow it with BEZISYNC_SSRF_ALLOW_CIDRS.", host, strings.Join(blocked, ", "))}
	}
	return ips, nil
}

// TCPProbe opens (and closes) a TCP connection and returns the latency in ms.
func TCPProbe(ctx context.Context, host string, port int, timeout time.Duration) (float64, error) {
	ips, err := ResolveChecked(ctx, host, port)
	if err != nil {
		return 0, err
	}
	var last error
	for _, ip := range ips {
		started := time.Now()
		d := net.Dialer{Timeout: timeout}
		c, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
		if err == nil {
			_ = c.Close()
			return float64(time.Since(started).Microseconds()) / 1000, nil
		}
		last = err
	}
	if last == nil {
		last = fmt.Errorf("no address")
	}
	return 0, last
}

// DialContext dials only addresses that pass the SSRF guard (used for webhooks, secret
// stores and HTTP sinks, which Python sent through network_check-style guards).
func DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	port, _ := strconv.Atoi(portStr)
	ips, err := ResolveChecked(ctx, host, port)
	if err != nil {
		return nil, err
	}
	var d net.Dialer
	var last error
	for _, ip := range ips {
		c, err := d.DialContext(ctx, network, net.JoinHostPort(ip, portStr))
		if err == nil {
			return c, nil
		}
		last = err
	}
	return nil, last
}

// HTTPClient is an HTTP client whose every dial (redirects and proxies included) passes the
// guard: connector checks and previews use it for user-supplied endpoints (catalogs, object
// stores, cloud APIs with an endpoint override).
func HTTPClient(timeout time.Duration) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	t.DialContext = DialContext
	return &http.Client{Timeout: timeout, Transport: t}
}
