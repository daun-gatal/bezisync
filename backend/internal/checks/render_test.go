package checks

import (
	"slices"
	"strings"
	"testing"
)

// section returns the rendered text of one "## name" section of the reference.
func section(md, name string) string {
	start := strings.Index(md, "\n## "+name+"\n")
	if start < 0 {
		return ""
	}
	rest := md[start+1:]
	if end := strings.Index(rest[3:], "\n## "); end >= 0 {
		return rest[:end+3]
	}
	return rest
}

// Each source and destination section says which engines read or write it, and a source Flink
// CDC reads links the Flink CDC checks that run for it.
func TestRenderEnginesAndFlinkLinks(t *testing.T) {
	md := Render(LoadCatalog())
	for name, want := range map[string]string{
		"PostgreSQL":           "*Read by Debezium and by Flink CDC.*",
		"Oracle":               "*Read by Debezium and by Flink CDC.*",
		"MongoDB":              "*Read by Debezium only.*",
		"Kafka":                "*Written by Debezium (the default) and by Flink CDC.*",
		"Apache Iceberg":       "*Written by Debezium (the default) and by Flink CDC.*",
		"StarRocks":            "*Written by Flink CDC only.*",
		"Elasticsearch":        "*Written by Flink CDC only.*",
		"Amazon SQS":           "*Written by Debezium only.*",
		"Google Cloud Spanner": "*Read by Debezium only.*",
	} {
		if got := section(md, name); !strings.Contains(got, want) {
			t.Errorf("section %q lacks %q:\n%s", name, want, got)
		}
	}
	pg := section(md, "PostgreSQL")
	for _, link := range []string{"(#flink-replica-identity)", "(#flink-replication-slots)", "(#flink-primary-key)"} {
		if !strings.Contains(pg, link) {
			t.Errorf("PostgreSQL section does not link %s", link)
		}
	}
	if strings.Contains(section(md, "MongoDB"), "On Flink CDC") {
		t.Error("MongoDB links Flink CDC checks, but Flink CDC doesn't read it")
	}
	if strings.Contains(section(md, "Deploy"), "*Read by") || strings.Contains(section(md, "Deploy"), "*Written by") {
		t.Error("Deploy is not a connection, so it names no engines")
	}
}

// The catalog's extra structure points only at sections it renders.
func TestCatalogAlsoAndConnectorsNameSections(t *testing.T) {
	c := LoadCatalog()
	for _, e := range c.Entries {
		for _, s := range e.Also {
			if !slices.Contains(c.Sections, s) || s == e.Section {
				t.Errorf("%s: also %q is not another section", e.Code, s)
			}
		}
	}
	for s, conn := range c.Connectors {
		if !slices.Contains(c.Sections, s) {
			t.Errorf("connectors: %q is not a section", s)
		}
		if conn.Kind != "source" && conn.Kind != "sink" {
			t.Errorf("connectors: %q has kind %q", s, conn.Kind)
		}
	}
}
