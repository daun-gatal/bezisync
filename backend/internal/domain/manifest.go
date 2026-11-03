package domain

import (
	"github.com/daun-gatal/bezisync/backend/internal/difflib"
	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
	"github.com/daun-gatal/bezisync/backend/internal/pyyaml"
	"github.com/daun-gatal/bezisync/backend/internal/val"
)

// The pipeline manifest: config-as-code for pipelines (YAML, `kind: Pipeline`).
// Connections and the runtime are referenced by name; everything else is a PipelineSpec.

// APIVersion of pipeline manifests.
const APIVersion = "bezisync.io/v1"

// ManifestError means the document is not a valid manifest; Errors lists the problems.
type ManifestError struct {
	Message string
	Errors  []map[string]any
}

func (e *ManifestError) Error() string { return e.Message }

// ParsedPipeline is one manifest document, resolved to product terms.
type ParsedPipeline struct {
	Name             string
	Description      string
	Owner            string
	Labels           map[string]string
	SourceConnection string
	SinkConnection   *string
	Runtime          *string
	Spec             PipelineSpec
}

// ParseDocuments splits YAML text into manifest documents (mappings).
func ParseDocuments(text string) ([]map[string]any, error) {
	all, err := pyyaml.LoadAll(text)
	if err != nil {
		return nil, &ManifestError{Message: "Not valid YAML: " + err.Error()}
	}
	var docs []map[string]any
	for _, d := range all {
		if d == nil {
			continue
		}
		m, ok := d.(map[string]any)
		if !ok {
			return nil, &ManifestError{Message: "Each document must be a mapping with apiVersion, kind, metadata and spec."}
		}
		docs = append(docs, m)
	}
	if len(docs) == 0 {
		return nil, &ManifestError{Message: "The document is empty."}
	}
	return docs, nil
}

func manifestErrors(errs []problem.FieldError, prefix ...string) []map[string]any {
	out := make([]map[string]any, len(errs))
	for i, e := range errs {
		loc := append(append([]string{}, prefix...), e.Loc...)
		out[i] = map[string]any{"loc": loc, "message": e.Message}
	}
	return out
}

// FromDocument validates one manifest document.
func FromDocument(doc map[string]any) (*ParsedPipeline, error) {
	v := &val.V{}
	o := v.Model(nil, doc, "Manifest")
	var apiVersion, kind string
	versionKey := "apiVersion"
	if !o.Has("apiVersion") && o.Has("api_version") {
		versionKey = "api_version" // populate_by_name
	}
	if f := o.Get(versionKey); f.Required() {
		if s, ok := v.Str(f.Loc, f.Raw); ok {
			if s != APIVersion {
				v.ValueError(f.Loc, "apiVersion must be "+APIVersion)
			}
			apiVersion = s
		}
	}
	if f := o.Get("kind"); f.Required() {
		if s, ok := v.Str(f.Loc, f.Raw); ok {
			if s != "Pipeline" {
				v.ValueError(f.Loc, "kind must be Pipeline")
			}
			kind = s
		}
	}
	_, _ = apiVersion, kind
	meta := struct {
		name, description, owner string
		labels                   map[string]string
	}{labels: map[string]string{}}
	if f := o.Get("metadata"); f.Required() {
		m := v.Model(f.Loc, f.Raw, "Metadata")
		if !m.Failed {
			if g := m.Get("name"); g.Required() {
				if s, ok := v.Constrained(g.Loc, g.Raw, val.StrRules{Pattern: `^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`}); ok {
					meta.name = s
				}
			}
			if g := m.Get("description"); g.Present {
				if s, ok := v.Str(g.Loc, g.Raw); ok {
					meta.description = s
				}
			}
			if g := m.Get("owner"); g.Present {
				if s, ok := v.Str(g.Loc, g.Raw); ok {
					meta.owner = s
				}
			}
			if g := m.Get("labels"); g.Present {
				if l, ok := v.StrDict(g.Loc, g.Raw); ok {
					meta.labels = l
				}
			}
			if g := m.Get("project"); g.Present && !g.IsNull() {
				v.Str(g.Loc, g.Raw)
			}
			m.Forbid()
		}
	}
	var spec map[string]any
	if f := o.Get("spec"); f.Required() {
		if m, ok := v.Dict(f.Loc, f.Raw); ok {
			spec = m
		}
	}
	o.Forbid()
	if !v.OK() {
		return nil, &ManifestError{Message: "Not a valid pipeline manifest.", Errors: manifestErrors(v.Errors)}
	}
	body := make(map[string]any, len(spec))
	for k, x := range spec {
		body[k] = x
	}
	source := copyMap(body["source"])
	sink := copyMap(body["sink"])
	sourceConn, hasSource := source["connection"]
	delete(source, "connection")
	sinkConn := sink["connection"]
	delete(sink, "connection")
	runtime, hasRuntime := body["runtime"]
	delete(body, "runtime")
	sc, ok := sourceConn.(string)
	if !hasSource || !ok || sc == "" {
		return nil, &ManifestError{Message: "spec.source.connection (the source connection's name) is required."}
	}
	var runtimeName *string
	if hasRuntime && runtime != nil {
		r, ok := runtime.(string)
		if !ok {
			return nil, &ManifestError{Message: "spec.runtime must be a runtime name."}
		}
		runtimeName = &r
	}
	body["source"], body["sink"] = source, sink
	if img, ok := body["debeziumImage"]; ok {
		delete(body, "debeziumImage")
		body["debezium_image"] = img
	}
	sv := &val.V{}
	parsed, ok := ParseSpec(sv, nil, body)
	if !ok {
		return nil, &ManifestError{Message: "The pipeline spec is invalid.", Errors: manifestErrors(sv.Errors, "spec")}
	}
	var sinkName *string
	if s, ok := sinkConn.(string); ok {
		sinkName = &s
	}
	return &ParsedPipeline{
		Name: meta.name, Description: meta.description, Owner: meta.owner, Labels: meta.labels,
		SourceConnection: sc, SinkConnection: sinkName, Runtime: runtimeName, Spec: parsed,
	}, nil
}

// copyMap is dict(x or {}): a shallow copy of a mapping (non-mappings become empty, as
// Python's dict() would have raised for most of them anyway).
func copyMap(x any) map[string]any {
	out := map[string]any{}
	if m, ok := x.(map[string]any); ok {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

// ToDocument renders a pipeline as a manifest document (ordered like the Python dict).
func ToDocument(name, project, description, owner string, labels map[string]string,
	sourceConnection string, sinkConnection, runtime *string, spec PipelineSpec) *om.Map {
	body := spec.Dump()
	image, _ := body.Pop("debezium_image")
	src, _ := body.Get("source")
	newSrc := om.New().Set("connection", sourceConnection)
	for _, k := range src.(*om.Map).Keys() {
		x, _ := src.(*om.Map).Get(k)
		newSrc.Set(k, x)
	}
	body.Set("source", newSrc)
	snk, _ := body.Get("sink")
	newSink := om.New()
	if sinkConnection != nil && *sinkConnection != "" {
		newSink.Set("connection", *sinkConnection)
	}
	for _, k := range snk.(*om.Map).Keys() {
		x, _ := snk.(*om.Map).Get(k)
		newSink.Set(k, x)
	}
	body.Set("sink", newSink)
	if runtime != nil && *runtime != "" {
		body.SetFirst("runtime", *runtime)
	}
	if s, ok := image.(string); ok && s != "" {
		body.Set("debeziumImage", s)
	}
	metadata := om.New().Set("name", name).Set("project", project)
	if description != "" {
		metadata.Set("description", description)
	}
	if owner != "" {
		metadata.Set("owner", owner)
	}
	if len(labels) > 0 {
		l := om.New()
		keys := make([]string, 0, len(labels))
		for k := range labels {
			keys = append(keys, k)
		}
		for _, k := range om.JSONBKeys(keys) {
			l.Set(k, labels[k])
		}
		metadata.Set("labels", l)
	}
	return om.New().Set("apiVersion", APIVersion).Set("kind", "Pipeline").Set("metadata", metadata).Set("spec", body)
}

// ToYAML is yaml.safe_dump_all(docs, sort_keys=False, allow_unicode=True).
func ToYAML(docs []any) string {
	out, err := pyyaml.DumpAll(docs, true)
	if err != nil {
		panic(err)
	}
	return out
}

// SpecDiff is a unified diff of two specs' YAML (n=3, no line terminators).
func SpecDiff(old *PipelineSpec, new PipelineSpec, a, b string) []string {
	dump := func(s *PipelineSpec) []string {
		if s == nil {
			return []string{}
		}
		text, err := pyyaml.Dump(s.Dump(), false)
		if err != nil {
			panic(err)
		}
		return SplitLines(text)
	}
	return difflib.UnifiedDiff(dump(old), dump(&new), a, b, 3)
}
