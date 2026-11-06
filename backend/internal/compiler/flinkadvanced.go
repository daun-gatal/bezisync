package compiler

import (
	"sort"
	"strings"
)

// Raw (expert) properties of a Flink CDC pipeline: a key names its YAML block and option
// ("source.jdbc.properties.connectTimeout", "sink.table.properties.write.parquet.compression-codec").
// Only the pass-through prefixes a connector hands on are open; every other option is a typed
// setting (the registry), set by BeziSync, or refused. Values go into the YAML through its encoder.

// Pass-through prefixes per block.
var (
	flinkSinkPrefixes = []string{"sink.properties.", "table.create.properties.", "table.properties."}
	// Destination options that carry credentials or point it elsewhere, never raw.
	flinkSinkForbidden = []string{"catalog.properties.", "hadoop.conf."}
	// Iceberg table properties that would move or rewrite a table (F5).
	icebergMoving = map[string]bool{"format-version": true, "location": true, "write.data.path": true, "write.metadata.path": true,
		"write.folder-storage.path": true, "write.object-storage.path": true}
)

// ClassifyFlinkAdvanced is what BeziSync makes of one raw key of a Flink CDC pipeline, and its
// value: allowed through one of the pass-through prefixes, or refused with why. Keys BeziSync sets
// (managed: "block.option") are owned.
func ClassifyFlinkAdvanced(key, value string, managed map[string]bool) AdvancedVerdict {
	v := AdvancedVerdict{Key: key, Class: AdvancedAllowed}
	refuse := func(class AdvancedClass, msg, fix string) AdvancedVerdict {
		v.Class, v.Message, v.Fix = class, msg, fix
		return v
	}
	if len(key) > maxAdvancedKey || !advancedKeyRe.MatchString(key) {
		return refuse(AdvancedInvalid, "Not a property name.", "")
	}
	if len(value) > maxAdvancedValue || strings.ContainsAny(value, "\r\n\x00") {
		return refuse(AdvancedInvalid, "A value is at most 4096 characters, on one line.", "")
	}
	if strings.Contains(value, "${") {
		return refuse(AdvancedInvalid, "${…} is BeziSync's own; secrets live on connections.", "connection")
	}
	block, option, _ := strings.Cut(key, ".")
	if managed[key] {
		return refuse(AdvancedOwned, "BeziSync sets this for the pipeline.", "")
	}
	switch block {
	case "source":
		switch {
		case strings.HasPrefix(option, "jdbc.properties."):
			name := strings.TrimPrefix(option, "jdbc.properties.")
			if jdbcUnsafeRe.MatchString(name) || credentialRe.MatchString(name) {
				return refuse(AdvancedForbidden, "This driver option can load files or code, change TLS or carry credentials. TLS is set on the connection.", "connection")
			}
			return v
		case strings.HasPrefix(option, "debezium."):
			// The embedded Debezium engine: Debezium's own rules (F6).
			if d := ClassifyAdvanced("debezium.source."+strings.TrimPrefix(option, "debezium."), value, nil); d.Refused() {
				return refuse(d.Class, d.Message, d.Fix)
			}
			return v
		}
		return refuse(AdvancedInvalid, "Only jdbc.properties.* and debezium.* source options pass through; the rest are typed settings.", "review")
	case "sink":
		for _, p := range flinkSinkForbidden {
			if strings.HasPrefix(option, p) {
				return refuse(AdvancedForbidden, "Catalog and Hadoop options come from the destination connection: they carry credentials and say where data goes.", "connection")
			}
		}
		if t, ok := strings.CutPrefix(option, "table.properties."); ok && icebergMoving[t] {
			return refuse(AdvancedForbidden, "It would move or rewrite the destination's tables.", "")
		}
		for _, p := range flinkSinkPrefixes {
			if strings.HasPrefix(option, p) {
				if credentialRe.MatchString(option) {
					return refuse(AdvancedCredential, "Credentials live on the destination connection, encrypted.", "connection")
				}
				return v
			}
		}
		return refuse(AdvancedInvalid, "Only sink.properties.*, table.create.properties.* and table.properties.* destination options pass through; the rest are typed settings.", "review")
	case "pipeline":
		return refuse(AdvancedInvalid, "Pipeline options are typed settings or set by BeziSync.", "review")
	}
	// F8: a bare Flink key (taskmanager.*, state.*, …) belongs to the cluster settings.
	return refuse(AdvancedInvalid, "A Flink setting goes in Engine settings (from the catalogue), not here; raw keys start with source. or sink.", "review")
}

// FlinkAdvancedRefusals are the refused raw keys of a Flink CDC pipeline, sorted.
func FlinkAdvancedRefusals(advanced map[string]string, managed map[string]bool) []AdvancedVerdict {
	var out []AdvancedVerdict
	for _, k := range advancedKeys(advanced) {
		if v := ClassifyFlinkAdvanced(k, advanced[k], managed); v.Refused() {
			out = append(out, v)
		}
	}
	return out
}

func advancedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
