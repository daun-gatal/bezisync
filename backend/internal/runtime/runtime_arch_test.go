package runtime

import (
	"reflect"
	"testing"
)

func TestArchitectures(t *testing.T) {
	if got := Architectures("x86_64", "amd64", "aarch64", ""); !reflect.DeepEqual(got, []string{"amd64", "arm64"}) {
		t.Fatalf("got %v", got)
	}
	if got := Architectures(); got == nil || len(got) != 0 {
		t.Fatalf("empty: %v", got)
	}
}
