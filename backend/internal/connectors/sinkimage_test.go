package connectors

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/config"
)

func withSinkImage(t *testing.T, override string) {
	t.Helper()
	s, err := config.Load(func(k string) (string, bool) {
		if k == "BEZISYNC_ICEBERG_IMAGE" && override != "" {
			return override, true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	config.Set(s)
	t.Cleanup(config.Reset)
	d := &Descriptor{Kind: "sink", Type: "iceberg", Title: "Apache Iceberg",
		Sink: SinkCaps{Image: &SinkImage{Repository: "ghcr.io/memiiso/debezium-server-iceberg", Tag: "1.2.1.Final", Digest: "sha256:abc", Platforms: []string{"amd64"}}}}
	old, had := descriptors["sink/iceberg"]
	descriptors["sink/iceberg"] = d
	t.Cleanup(func() {
		if had {
			descriptors["sink/iceberg"] = old
		} else {
			delete(descriptors, "sink/iceberg")
		}
	})
}

func TestSinkImageFamily(t *testing.T) {
	withSinkImage(t, "")
	if got := SinkImageFor("iceberg"); got != "ghcr.io/memiiso/debezium-server-iceberg:1.2.1.Final@sha256:abc" {
		t.Fatalf("pinned image: %q", got)
	}
	if SinkImageFor("kafka") != "" {
		t.Fatal("official sinks have no image of their own")
	}
	for image, want := range map[string]bool{
		"ghcr.io/memiiso/debezium-server-iceberg:1.3.0.Final": true,
		"ghcr.io/memiiso/debezium-server-iceberg@sha256:def":  true,
		"quay.io/debezium/server:3.7.0.Final":                 false,
		"registry.internal/mirror/iceberg:1.2.1.Final":        false,
	} {
		if got := InSinkImageFamily("iceberg", image); got != want {
			t.Errorf("InSinkImageFamily(%s) = %v", image, got)
		}
	}
}

func TestSinkImageMirror(t *testing.T) {
	withSinkImage(t, "registry.internal/mirror/iceberg:1.2.1.Final")
	if got := SinkImageFor("iceberg"); got != "registry.internal/mirror/iceberg:1.2.1.Final" {
		t.Fatalf("mirror: %q", got)
	}
	if !InSinkImageFamily("iceberg", "registry.internal/mirror/iceberg:1.3.0.Final") {
		t.Fatal("pins to the mirror stay in the family")
	}
}
