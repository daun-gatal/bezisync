package services_test

import (
	"context"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/schemas"
	"github.com/daun-gatal/bezisync/backend/internal/schemaval"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// A runtime saved without image pull secrets must build again from its stored config
// (Pydantic fills default_factory=list fields with []; null would fail re-validation).
func TestRuntimeConfigRoundTripsThroughValidation(t *testing.T) {
	k, _ := schemas.Runtime("kubernetes")
	v := &val.V{}
	cfg, ok := schemaval.Model(v, nil, k.ConfigSchema, map[string]any{"namespace": "cdc", "in_cluster": false})
	if !ok {
		t.Fatal(v.Errors)
	}
	if l, isList := cfg["image_pull_secrets"].([]any); !isList || len(l) != 0 {
		t.Fatalf("image_pull_secrets = %#v, want []", cfg["image_pull_secrets"])
	}
	again := &val.V{}
	if _, ok := schemaval.Model(again, nil, k.ConfigSchema, cfg); !ok {
		t.Fatal(again.Errors)
	}
	// The real driver builds from it (a kubeconfig is required outside a cluster).
	kubeconfig := "apiVersion: v1\nkind: Config\nclusters: [{name: c, cluster: {server: 'https://10.0.0.1:6443'}}]\n" +
		"contexts: [{name: c, context: {cluster: c, user: u}}]\ncurrent-context: c\nusers: [{name: u, user: {token: t}}]\n"
	if _, err := services.BuildDriver(context.Background(), "kubernetes", cfg, map[string]any{"kubeconfig": kubeconfig}); err != nil {
		t.Fatal(err)
	}
}
