package services_test

import (
	"errors"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/services"
)

// A setting holding "${" is refused when a connection is saved; secrets may hold anything.
func TestValidateConnectionRefusesConfigExpressions(t *testing.T) {
	cfg := map[string]any{"bootstrap_servers": "kafka:9092", "security_protocol": "SASL_SSL", "sasl_mechanism": "PLAIN", "username": "${BZ_STATE_PASSWORD}"}
	_, _, err := services.ValidateConnection("sink", "kafka", cfg, map[string]any{"password": "pa${ss"})
	var p *problem.Problem
	if !errors.As(err, &p) || p.Status != 422 || len(p.Errors) != 1 {
		t.Fatalf("got %v", err)
	}
	if loc := p.Errors[0]["loc"].([]string); len(loc) != 2 || loc[0] != "config" || loc[1] != "username" {
		t.Fatalf("loc %v", p.Errors[0]["loc"])
	}
	cfg["username"] = "svc-$user"
	if _, sec, err := services.ValidateConnection("sink", "kafka", cfg, map[string]any{"password": "pa${ss"}); err != nil || sec["password"] != "pa${ss" {
		t.Fatalf("a $ alone, or ${ in a secret, must pass: %v %v", err, sec)
	}
}
