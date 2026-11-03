package compiler

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/domain"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// SnapshotProps are the snapshot properties of a pipeline, in the source's own terms: a source
// that cannot copy existing rows (Spanner) gets none, and a mode it lacks is refused.
func SnapshotProps(desc *connectors.Descriptor, spec domain.PipelineSpec) (map[string]string, error) {
	caps := desc.Capture
	if len(caps.SnapshotModes) == 0 {
		return map[string]string{}, nil
	}
	mode := caps.DebeziumMode(spec.Snapshot.Mode)
	if mode == "" {
		return nil, problem.New(422, "snapshot_mode_unsupported", fmt.Sprintf("%s cannot use the snapshot mode '%s'. Available: %s.",
			desc.Title, spec.Snapshot.Mode, strings.Join(caps.SnapshotModes, ", ")))
	}
	props := map[string]string{
		"debezium.source.snapshot.mode":       mode,
		"debezium.source.snapshot.fetch.size": strconv.Itoa(spec.Snapshot.FetchSize),
		"debezium.source.snapshot.delay.ms":   strconv.Itoa(spec.Snapshot.DelaySeconds * 1000),
	}
	if caps.Incremental {
		props["debezium.source.incremental.snapshot.chunk.size"] = strconv.Itoa(spec.Snapshot.ChunkSize)
	}
	return props, nil
}
