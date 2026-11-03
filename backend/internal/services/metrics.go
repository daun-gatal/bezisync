package services

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Metric history: queries for charts, rollups and retention.

// MetricRanges are the chart ranges.
var MetricRanges = map[string]time.Duration{
	"1h": time.Hour, "6h": 6 * time.Hour, "24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour, "30d": 30 * 24 * time.Hour,
}

const (
	rawKeep   = 24 * time.Hour
	maxPoints = 300
)

// MetricPoint is one chart point.
type MetricPoint struct {
	At              pytime.Time `json:"at"`
	EventsPerSecond *float64    `json:"events_per_second"`
	LagMS           *float64    `json:"lag_ms"`
	RetainedBytes   *float64    `json:"retained_bytes"`
	QueueUsedRatio  *float64    `json:"queue_used_ratio"`
	Healthy         *bool       `json:"healthy"`
}

// MetricSeries returns downsampled points over a range.
func MetricSeries(ctx context.Context, q db.Querier, pipelineID uuid.UUID, rangeKey string) ([]MetricPoint, error) {
	span := MetricRanges[rangeKey]
	since := db.Now().Add(-span)
	bucket := max(15, int(span.Seconds()/maxPoints))
	filter := ""
	if span > rawKeep {
		filter = "AND resolution = 60"
	}
	query := func(sql string, args ...any) ([]MetricPoint, error) {
		rows, err := q.Query(ctx, sql, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out []MetricPoint
		for rows.Next() {
			var p MetricPoint
			var t time.Time
			if err := rows.Scan(&t, &p.EventsPerSecond, &p.LagMS, &p.RetainedBytes, &p.QueueUsedRatio, &p.Healthy); err != nil {
				return nil, err
			}
			p.At = pytime.T(t)
			out = append(out, p)
		}
		return out, rows.Err()
	}
	points, err := query(fmt.Sprintf(`
            SELECT to_timestamp(floor(extract(epoch FROM at) / $1) * $1) AS t,
                   avg(events_per_second), avg(lag_ms), max(source_retained_bytes), avg(queue_used_ratio),
                   bool_and(healthy)
              FROM metric_samples
             WHERE pipeline_id = $2 AND at >= $3 %s
             GROUP BY 1 ORDER BY 1`, filter), bucket, pipelineID, since)
	if err != nil {
		return nil, err
	}
	if span > rawKeep {
		// The newest day is still raw: merge it in so long ranges reach "now".
		raw, err := query(`
                SELECT to_timestamp(floor(extract(epoch FROM at) / $1) * $1),
                       avg(events_per_second), avg(lag_ms), max(source_retained_bytes), avg(queue_used_ratio), bool_and(healthy)
                  FROM metric_samples WHERE pipeline_id = $2 AND resolution = 0 AND at >= $3
                 GROUP BY 1 ORDER BY 1`, bucket, pipelineID, db.Now().Add(-rawKeep))
		if err != nil {
			return nil, err
		}
		points = append(points, raw...)
	}
	if points == nil {
		points = []MetricPoint{}
	}
	return points, nil
}

// EnforceMetricRetention rolls raw samples older than a day into one-minute averages and
// drops data past retention.
func EnforceMetricRetention(ctx context.Context, q db.Querier, now time.Time) (map[string]int64, error) {
	cutoff := now.Add(-rawKeep)
	rolled, err := q.Exec(ctx, `
            INSERT INTO metric_samples (pipeline_id, at, resolution, events_per_second, lag_ms, events_total,
                                        queue_used_ratio, source_retained_bytes, healthy)
            SELECT pipeline_id, date_trunc('minute', at), 60, avg(events_per_second), avg(lag_ms), max(events_total),
                   avg(queue_used_ratio), max(source_retained_bytes), bool_and(healthy)
              FROM metric_samples WHERE resolution = 0 AND at < $1
             GROUP BY pipeline_id, date_trunc('minute', at)`, cutoff)
	if err != nil {
		return nil, err
	}
	raw, err := q.Exec(ctx, `DELETE FROM metric_samples WHERE resolution = 0 AND at < $1`, cutoff)
	if err != nil {
		return nil, err
	}
	keep := now.Add(-time.Duration(config.Get().MetricsRetentionDays) * 24 * time.Hour)
	old, err := q.Exec(ctx, `DELETE FROM metric_samples WHERE at < $1`, keep)
	if err != nil {
		return nil, err
	}
	return map[string]int64{"rolled": rolled.RowsAffected(), "deleted_raw": raw.RowsAffected(), "deleted_old": old.RowsAffected()}, nil
}
