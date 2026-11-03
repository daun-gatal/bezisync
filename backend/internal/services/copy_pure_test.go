package services

import (
	"reflect"
	"strings"
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
)

func TestFreeName(t *testing.T) {
	taken := map[string]bool{"orders-2": true}
	if got := freeName("orders", func(n string) bool { return taken[n] }); got != "orders-3" {
		t.Fatal(got)
	}
	long := strings.Repeat("a", 62) + "b"
	if got := freeName(long, func(string) bool { return false }); len(got) > 63 || !strings.HasSuffix(got, "-2") {
		t.Fatal(got)
	}
}

func TestMissingSecretsFollowTheSchema(t *testing.T) {
	d, err := connectors.Describe("source", "postgresql")
	if err != nil {
		t.Fatal(err)
	}
	if got := MissingSecrets(d.SecretSchema, map[string]any{}); !reflect.DeepEqual(got, []string{"password"}) {
		t.Fatal(got)
	}
	if got := MissingSecrets(d.SecretSchema, map[string]any{"password": "ref+vault://x#y"}); len(got) != 0 {
		t.Fatal(got)
	}
	open := WithoutRequired(d.SecretSchema)
	if len(RequiredSecrets(open)) != 0 || len(RequiredSecrets(d.SecretSchema)) == 0 {
		t.Fatal("WithoutRequired must not change the descriptor")
	}
}

func TestConfigChangesListOnlyDifferences(t *testing.T) {
	got := configChanges(map[string]any{"host": "a", "port": 5432}, map[string]any{"host": "b", "port": 5432, "ssl": true})
	if len(got) != 2 || got[0].Field != "host" || got[1].Field != "ssl" {
		t.Fatal(got)
	}
}
