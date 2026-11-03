package services

import (
	"context"
	"sort"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/connectors"
	"github.com/daun-gatal/bezisync/backend/internal/db"
)

// Mask is what masked preview values show.
const Mask = "•••••"

// MaskValue recursively replaces values of PII-looking or explicitly listed keys.
func MaskValue(value any, extra, masked map[string]bool) any {
	switch x := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, item := range x {
			_, isMap := item.(map[string]any)
			_, isList := item.([]any)
			if (extra[k] || connectors.LooksLikePII(k)) && !isMap && !isList {
				masked[k] = true
				if item == nil {
					out[k] = nil
				} else {
					out[k] = Mask
				}
			} else {
				out[k] = MaskValue(item, extra, masked)
			}
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = MaskValue(v, extra, masked)
		}
		return out
	}
	return value
}

// MaskRecords masks a preview and lists the keys it masked.
func MaskRecords(records []connectors.TailRecord, extra map[string]bool) ([]connectors.TailRecord, []string) {
	masked := map[string]bool{}
	out := make([]connectors.TailRecord, len(records))
	for i, r := range records {
		r.Key = MaskValue(r.Key, extra, masked)
		r.Value = MaskValue(r.Value, extra, masked)
		out[i] = r
	}
	names := make([]string, 0, len(masked))
	for k := range masked {
		names = append(names, k)
	}
	sort.Strings(names)
	return out, names
}

// ConnectionUsage lists live pipelines that use a connection as source or sink.
func ConnectionUsage(ctx context.Context, q db.Querier, connectionID uuid.UUID) ([]string, error) {
	rows, err := db.Select[struct {
		Name string `db:"name"`
	}](ctx, q, `SELECT name FROM pipelines WHERE (source_id = $1 OR sink_id = $1) AND deleted_at IS NULL ORDER BY name`, connectionID)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Name
	}
	return out, nil
}
