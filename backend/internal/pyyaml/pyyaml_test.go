package pyyaml_test

import (
	"testing"

	"github.com/daun-gatal/bezisync/backend/internal/contract"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/pyyaml"
)

type yamlFixture struct {
	Dumps []struct {
		DocJSON  string `json:"doc_json"`
		SafeDump string `json:"safe_dump"`
		Unicode  string `json:"unicode"`
	} `json:"dumps"`
	Loads []struct {
		Text string `json:"text"`
		JSON string `json:"json"`
	} `json:"loads"`
}

func TestDumpMatchesPyYAML(t *testing.T) {
	var fx yamlFixture
	contract.Load(t, "yaml", &fx)
	for _, c := range fx.Dumps {
		doc, err := om.DecodeOrdered([]byte(c.DocJSON))
		if err != nil {
			t.Fatal(err)
		}
		got, err := pyyaml.Dump(doc, false)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.SafeDump {
			t.Errorf("safe_dump(%s)\n--- got\n%s\n--- want\n%s", c.DocJSON, got, c.SafeDump)
		}
		second := om.New().Set("second", doc)
		got, err = pyyaml.DumpAll([]any{doc, second}, true)
		if err != nil {
			t.Fatal(err)
		}
		if got != c.Unicode {
			t.Errorf("safe_dump_all(allow_unicode)(%s)\n--- got\n%s\n--- want\n%s", c.DocJSON, got, c.Unicode)
		}
	}
}

func TestLoadMatchesPyYAML(t *testing.T) {
	var fx yamlFixture
	contract.Load(t, "yaml", &fx)
	for _, c := range fx.Loads {
		docs, err := pyyaml.LoadAll(c.Text)
		if err != nil {
			t.Fatalf("%q: %v", c.Text, err)
		}
		if docs == nil {
			docs = []any{}
		}
		got, err := pyjson.Dumps(docs)
		if err != nil {
			t.Fatal(err)
		}
		if want := c.JSON; got != want {
			t.Errorf("safe_load_all(%q)\n got %s\nwant %s", c.Text, got, want)
		}
	}
}
