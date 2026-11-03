package ssrf_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

func TestGuardBlocksMetadataAndLoopback(t *testing.T) {
	s, err := config.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	for ip, want := range map[string]bool{"169.254.169.254": false, "127.0.0.1": false, "::1": false, "10.1.2.3": true, "not-an-ip": false} {
		if got := ssrf.IsAllowed(ip); got != want {
			t.Errorf("IsAllowed(%s) = %v", ip, got)
		}
	}
	_, err = ssrf.ResolveChecked(context.Background(), "localhost", 5432)
	var blocked *ssrf.BlockedAddress
	if !errors.As(err, &blocked) {
		t.Fatalf("localhost resolved: %v", err)
	}
	allowed := *s
	allowed.SSRFAllowCIDRs = []string{"127.0.0.0/8"}
	config.Set(&allowed)
	if !ssrf.IsAllowed("127.0.0.1") {
		t.Fatal("allow list ignored")
	}
}

func TestHTTPClientRefusesLoopback(t *testing.T) {
	s, err := config.Load(func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	_, err = ssrf.HTTPClient(2 * time.Second).Get("http://127.0.0.1:9/")
	var blocked *ssrf.BlockedAddress
	if !errors.As(err, &blocked) {
		t.Fatalf("got %v, want a blocked address", err)
	}
}
