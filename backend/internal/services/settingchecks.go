package services

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/compiler"
	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/engine"
	"github.com/daun-gatal/bezisync/backend/internal/models"
)

// Checks and recommendations on a Debezium pipeline's settings, from evidence preflight has just
// gathered (the image's release, the source's connections, the tables' sizes). Missing evidence is
// Unknown, never a pass; a recommendation is offered only with evidence from this run.

// Recommendation is a setting value preflight suggests, with why and when its evidence was read.
type Recommendation struct {
	Key        string    `json:"key"`
	Value      string    `json:"value"`
	Why        string    `json:"why"`
	EvidenceAt time.Time `json:"evidence_at"`
}

// RecommendationFreshness is how long a recommendation's evidence holds: older ones are hidden.
const RecommendationFreshness = 10 * time.Minute

// release compares a.b[.c] with a parsed release.
func atLeast(r [3]int, version string) bool {
	var want [3]int
	for i, part := range strings.SplitN(version, ".", 3) {
		want[i], _ = strconv.Atoi(part)
	}
	for i := range 3 {
		if r[i] != want[i] {
			return r[i] > want[i]
		}
	}
	return true
}

// settingVersionCheck: each tuned setting must exist in the image's Debezium release (C15, C16).
func settingVersionCheck(spec domain.PipelineSpec, image string) *checks.Check {
	var bounded []engine.Setting
	for _, k := range tuningKeys(spec.Tuning) {
		if s := engine.SettingOf(engine.Debezium, k); s != nil && (s.Since != "" || s.Until != "") {
			bounded = append(bounded, *s)
		}
	}
	if len(bounded) == 0 {
		return nil
	}
	title := "Settings in this Debezium version"
	r, _, ok := DebeziumRelease(image)
	if !ok {
		keys := make([]string, len(bounded))
		for i, s := range bounded {
			keys[i] = s.Key
		}
		return &checks.Check{Code: "setting_version", Title: title, Category: checks.CatDestination, Status: checks.Unknown,
			Message: fmt.Sprintf("BeziSync can't tell which Debezium release %s is, so it can't check %s exist%s in it.", image, strings.Join(keys, ", "),
				map[bool]string{true: "s", false: ""}[len(keys) == 1])}
	}
	var missing []string
	for _, s := range bounded {
		switch {
		case s.Since != "" && !atLeast(r, s.Since):
			missing = append(missing, fmt.Sprintf("%s (from Debezium %s)", s.Key, s.Since))
		case s.Until != "" && atLeast(r, s.Until) && s.Renamed != "":
			missing = append(missing, fmt.Sprintf("%s (renamed %s in Debezium %s)", s.Key, s.Renamed, s.Until))
		case s.Until != "" && atLeast(r, s.Until):
			missing = append(missing, fmt.Sprintf("%s (removed in Debezium %s)", s.Key, s.Until))
		}
	}
	if len(missing) == 0 {
		return &checks.Check{Code: "setting_version", Title: title, Category: checks.CatDestination, Status: checks.Pass,
			Message: fmt.Sprintf("Debezium %d.%d.%d has every tuned setting.", r[0], r[1], r[2])}
	}
	return &checks.Check{Code: "setting_version", Title: title, Category: checks.CatDestination, Status: checks.Fail, Blocking: true,
		Message: fmt.Sprintf("Debezium %d.%d.%d doesn't have %s.", r[0], r[1], r[2], strings.Join(missing, ", ")),
		Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Remove the setting in Review → Engine settings, or move the pipeline to a Debezium version that has it."}}
}

// connectionsOf reads the source's connection headroom, when BeziSync can reach it itself (an
// agent's source is checked by the agent, which doesn't report connections).
func connectionsOf(ctx context.Context, source connectors.Resolved, sourceType string, rt *models.Runtime) (limit, used int, ok bool) {
	if rt == nil || rt.Mode == "agent" {
		return 0, 0, false
	}
	src, err := connectors.GetSource(sourceType)
	if err != nil {
		return 0, 0, false
	}
	counter, can := src.(connectors.ConnectionCounter)
	if !can {
		return 0, 0, false
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	l, u, err := counter.Connections(ctx, source)
	return l, u, err == nil && l > 0
}

// copies reports whether the pipeline's next start copies existing rows.
func copies(p *models.Pipeline, spec domain.PipelineSpec) bool {
	return p.DeployedVersionID == nil && spec.Snapshot.Mode != "no_data"
}

// threadsCheck: snapshot threads and the stream each hold a source connection while copying (C14).
func threadsCheck(p *models.Pipeline, spec domain.PipelineSpec, limit, used int, known bool) *checks.Check {
	threads := 1
	if v, ok := spec.Tuning["debezium.source.snapshot.max.threads"]; ok {
		threads, _ = strconv.Atoi(v)
	}
	if threads <= 1 || !copies(p, spec) {
		return nil
	}
	title := "Connections for the copy"
	if !known {
		return &checks.Check{Code: "setting_threads", Title: title, Category: checks.CatCapacity, Status: checks.Unknown,
			Message: fmt.Sprintf("%d snapshot threads open %d connections to the source while copying; BeziSync couldn't read how many the source allows.", threads, threads+1)}
	}
	free := limit - used
	if threads+1 > free {
		return &checks.Check{Code: "setting_threads", Title: title, Category: checks.CatCapacity, Status: checks.Warn,
			Message: fmt.Sprintf("%d snapshot threads open %d connections while copying; the source allows %d and %d are open, so %d are free.", threads, threads+1, limit, used, max(free, 0)),
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: fmt.Sprintf("Use at most %d snapshot threads, or raise the source's connection limit.", max(free-1, 1))}}
	}
	return &checks.Check{Code: "setting_threads", Title: title, Category: checks.CatCapacity, Status: checks.Pass,
		Message: fmt.Sprintf("%d snapshot threads need %d connections; %d of %d are free.", threads, threads+1, free, limit)}
}

// averageRow is the selected tables' average row size from their size estimates (0: unknown).
func averageRow(found connectors.Discovery, tables []string) (bytes int64, rows int64) {
	for _, t := range found.Tables {
		if !slices.Contains(tables, t.Schema+"."+t.Name) || t.RowsEstimate == nil || t.BytesEstimate == nil {
			continue
		}
		rows += *t.RowsEstimate
		bytes += *t.BytesEstimate
	}
	if rows <= 0 {
		return 0, rows
	}
	return bytes / rows, rows
}

// eventBytes is about what one change takes in memory: the row before and after, and the
// envelope around them.
func eventBytes(avgRow int64) int64 { return 2*avgRow + 512 }

// memoryChecks: the queue and a fetch must fit the heap the size gives (C17). Estimated from the
// tables' sizes (no events have been seen before a deploy), and said so.
func memoryChecks(spec domain.PipelineSpec, compiled *compiler.Compiled, avgRow int64) []checks.Check {
	if compiled == nil || compiled.Flink != nil {
		return nil
	}
	heap := heapBytes(compiler.ResourceFor(spec.Policies).Heap)
	queue, _ := strconv.Atoi(compiled.Properties["debezium.source.max.queue.size"])
	limit, _ := strconv.ParseInt(compiled.Properties["debezium.source.max.queue.size.in.bytes"], 10, 64)
	title := "Queue memory"
	_, tunedQueue := spec.Tuning["debezium.source.max.queue.size"]
	_, tunedBytes := spec.Tuning["debezium.source.max.queue.size.in.bytes"]
	if (avgRow <= 0 || heap <= 0) && !tunedQueue && !tunedBytes {
		// BeziSync's own queue for the size is made to fit it: nothing to say without estimates.
		return nil
	}
	if avgRow <= 0 || heap <= 0 {
		return []checks.Check{{Code: "setting_queue_memory", Title: title, Category: checks.CatCapacity, Status: checks.Unknown,
			Message: "BeziSync has no size estimate for these tables, so it can't tell how much of the heap the queue takes."}}
	}
	need := int64(queue) * eventBytes(avgRow)
	if limit > 0 && limit < need {
		need = limit
	}
	var out []checks.Check
	mib := func(b int64) int64 { return b >> 20 }
	if need > heap/2 {
		out = append(out, checks.Check{Code: "setting_queue_memory", Title: title, Category: checks.CatCapacity, Status: checks.Warn,
			Message: fmt.Sprintf("A full queue of %d changes takes about %d MiB (estimated from the tables' average row of %d bytes): more than half the %d MiB heap.",
				queue, mib(need), avgRow, mib(heap)),
			Fix: &checks.Fix{Kind: "text", Who: "you", Code: "Lower Queue size, set a Queue memory limit, or choose a bigger size."}})
	} else {
		out = append(out, checks.Check{Code: "setting_queue_memory", Title: title, Category: checks.CatCapacity, Status: checks.Pass,
			Message: fmt.Sprintf("A full queue takes about %d MiB of the %d MiB heap (estimated from the tables' sizes).", mib(need), mib(heap))})
	}
	if fetch := int64(spec.Snapshot.FetchSize) * avgRow; spec.Snapshot.Mode != "no_data" && fetch > heap/4 {
		out = append(out, checks.Check{Code: "setting_fetch_memory", Title: "Copy fetch size", Category: checks.CatCapacity, Status: checks.Warn,
			Message: fmt.Sprintf("Each fetch of %d rows takes about %d MiB (estimated): more than a quarter of the %d MiB heap.", spec.Snapshot.FetchSize, mib(fetch), mib(heap)),
			Fix:     &checks.Fix{Kind: "text", Who: "you", Code: "Lower the fetch size in the Snapshot step, or choose a bigger size."}})
	}
	return out
}

// recommendations are setting values worth offering for this pipeline now, from this run's
// evidence (C13, C18).
func recommendations(p *models.Pipeline, spec domain.PipelineSpec, sourceType string, rows int64, tables int, limit, used int, known bool, now time.Time) []Recommendation {
	var out []Recommendation
	if _, set := spec.Tuning["debezium.source.snapshot.max.threads"]; !set && copies(p, spec) && known && rows > 1_000_000 && tables > 1 &&
		slices.Contains(engine.SettingOf(engine.Debezium, "debezium.source.snapshot.max.threads").Connectors, sourceType) {
		threads := min(4, tables, limit-used-2)
		if threads >= 2 {
			out = append(out, Recommendation{Key: "debezium.source.snapshot.max.threads", Value: strconv.Itoa(threads), EvidenceAt: now,
				Why: fmt.Sprintf("%s rows across %d tables to copy, and the source has %d free connections: %d threads copy them about %d× as fast.",
					humanCount(rows), tables, limit-used, threads, threads)})
		}
	}
	excluded := false
	for _, c := range spec.Source.Columns {
		excluded = excluded || len(c.Exclude) > 0
	}
	if _, set := spec.Tuning["debezium.source.skip.messages.without.change"]; !set && excluded {
		out = append(out, Recommendation{Key: "debezium.source.skip.messages.without.change", Value: "true", EvidenceAt: now,
			Why: "Columns are left out of this pipeline: an update that changed only those would arrive as a change with nothing changed."})
	}
	return out
}

func humanCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return strconv.FormatFloat(float64(n)/1e9, 'f', 1, 64) + " billion"
	case n >= 1_000_000:
		return strconv.FormatFloat(float64(n)/1e6, 'f', 1, 64) + " million"
	}
	return strconv.FormatInt(n, 10)
}

func tuningKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// heapBytes reads a Java heap ("512m", "1g").
func heapBytes(h string) int64 {
	n, err := strconv.ParseInt(strings.TrimRight(h, "mg"), 10, 64)
	if err != nil {
		return 0
	}
	if strings.HasSuffix(h, "g") {
		return n << 30
	}
	return n << 20
}

// isText reports whether a column type holds text a truncation can cut (JSON excluded: cut, it
// would no longer parse).
func isText(typ string) bool {
	t := strings.ToLower(typ)
	if strings.Contains(t, "json") {
		return false
	}
	for _, s := range []string{"char", "text", "string", "clob"} {
		if strings.Contains(t, s) {
			return true
		}
	}
	return false
}

// truncateChecks: a truncation cuts text only (T5). On Debezium another type is left as it is
// (a warning); on Flink CDC SUBSTR would fail the job, so it blocks.
func truncateChecks(found connectors.Discovery, spec domain.PipelineSpec, flink bool) []checks.Check {
	types := map[string]string{}
	for _, t := range found.Tables {
		for _, c := range t.Columns {
			types[t.Identifier()+"."+c.Name] = c.Type
		}
	}
	var notText, unknown []string
	for _, t := range spec.Transforms {
		for _, c := range t.Truncate {
			typ, known := types[c.Table+"."+c.Column]
			switch {
			case !known:
				unknown = append(unknown, c.Table+"."+c.Column)
			case !isText(typ):
				notText = append(notText, fmt.Sprintf("%s.%s (%s)", c.Table, c.Column, typ))
			}
		}
	}
	var out []checks.Check
	if len(notText) > 0 {
		c := checks.Check{Code: "transform_truncate_type", Title: "Truncated columns", Category: checks.CatTables, Status: checks.Warn,
			Message: "Only text can be cut: " + strings.Join(notText, ", ") + " stay as they are."}
		if flink {
			c.Status, c.Blocking = checks.Fail, true
			c.Message = "Flink CDC can only cut text: " + strings.Join(notText, ", ") + " would fail the job."
			c.Fix = &checks.Fix{Kind: "text", Who: "you", Code: "Remove those columns from the Truncate step."}
		}
		out = append(out, c)
	}
	if len(unknown) > 0 {
		out = append(out, checks.Check{Code: "transform_truncate_type", Title: "Truncated columns", Category: checks.CatTables, Status: checks.Unknown,
			Message: "BeziSync couldn't read these columns' types: " + strings.Join(unknown, ", ") + "."})
	}
	return out
}
