package api_test

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/config"
	"github.com/daun-gatal/bezisync/backend/internal/db"
	"github.com/daun-gatal/bezisync/backend/internal/models"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
	"github.com/daun-gatal/bezisync/backend/internal/services"
	"github.com/daun-gatal/bezisync/backend/internal/testdb"
)

func TestControllerRecordsSamplesWithRatesAndServesSeries(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	u := "/api/v1/projects/" + e.pid + "/pipelines/" + pipe.Str("id")
	e.admin.post(u+"/deploy", map[string]any{}).expect(200)
	e.driver.Metrics += `debezium_metrics_TotalNumberOfEventsSeen{context="streaming",name="x",plugin="postgres"} 100.0` + "\n"
	e.tick()
	at20 := pytime.ISOFormat(time.Now().UTC().Add(-20 * time.Second))
	exec(t, fmt.Sprintf(`UPDATE pipelines SET status = status || jsonb_build_object('counters', jsonb_build_object('at', '%s', 'total', 60)) WHERE id = '%s'`, at20, pipe.Str("id")))
	e.tick()
	samples, err := db.Select[models.MetricSample](context.Background(), testdb.Pool(t), `SELECT * FROM metric_samples WHERE pipeline_id = $1 ORDER BY at`, pipe.Str("id"))
	if err != nil || len(samples) != 2 {
		t.Fatal(samples, err)
	}
	last := samples[1]
	if last.EventsPerSecond == nil || *last.EventsPerSecond < 1.5 || *last.EventsPerSecond > 2.5 || last.LagMS == nil || *last.LagMS != 500 {
		t.Fatalf("%+v", last)
	}
	summary := e.admin.get(u).Str("status", "source_impact", "summary")
	if !strings.HasPrefix(summary, "Slot") && !strings.HasPrefix(summary, "The replication") {
		t.Fatal(summary)
	}
	series := e.admin.get(u + "/metrics?range=1h").expect(200)
	points := series.List("points")
	if len(points) == 0 || at(t, points[len(points)-1], "lag_ms") != float64(500) {
		t.Fatal(string(series.Raw))
	}
	e.admin.get(u + "/metrics?range=2y").expect(422)
}

func TestRetentionRollsUpAndExpires(t *testing.T) {
	e := newPipelineEnv(t)
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	now := time.Now().UTC().Truncate(time.Minute).Add(30 * time.Second)
	ctx := context.Background()
	q := testdb.Pool(t)
	for _, m := range []float64{60 * 25, 60*25 - 0.1, 60 * 24 * 40} {
		if _, err := q.Exec(ctx, `INSERT INTO metric_samples (pipeline_id, at, resolution, lag_ms, healthy) VALUES ($1, $2, 0, 100, TRUE)`,
			pipe.Str("id"), now.Add(-time.Duration(m*float64(time.Minute)))); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := q.Exec(ctx, `INSERT INTO metric_samples (pipeline_id, at, resolution, lag_ms, healthy) VALUES ($1, $2, 0, 7, TRUE)`, pipe.Str("id"), now.Add(-5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	result, err := services.EnforceMetricRetention(ctx, q, now)
	if err != nil || result["deleted_raw"] != 3 || result["deleted_old"] < 1 {
		t.Fatal(result, err)
	}
	rows, err := db.Select[models.MetricSample](ctx, q, `SELECT * FROM metric_samples WHERE pipeline_id = $1 ORDER BY resolution`, pipe.Str("id"))
	if err != nil || len(rows) != 2 || rows[0].Resolution != 0 || rows[1].Resolution != 60 {
		t.Fatal(rows, err)
	}
}

func TestPrometheusEndpointRequiresToken(t *testing.T) {
	e := newPipelineEnv(t)
	createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	anon := newClient(t, e.srv)
	anon.get("/metrics").expect(404)
	s := *config.Get()
	s.MetricsToken = "scrape-token"
	config.Set(&s)
	t.Cleanup(config.Reset)
	anon.get("/metrics").expect(401)
	anon.get("/metrics", "authorization", "Bearer wrong").expect(401)
	res := anon.get("/metrics", "authorization", "Bearer scrape-token").expect(200)
	body := string(res.Raw)
	if res.Header.Get("content-type") != "text/plain; version=0.0.4; charset=utf-8" || !strings.Contains(body, "bezisync_pipeline_health") ||
		!strings.Contains(body, `bezisync_pipeline_health{project="default",pipeline="orders-cdc",lifecycle="draft"} 4`) ||
		!strings.Contains(body, `bezisync_pipeline_running{project="default",pipeline="orders-cdc",lifecycle="draft"} 0`) {
		t.Fatal(body)
	}
}

func TestStreamChangeFeed(t *testing.T) {
	e := newPipelineEnv(t)
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, e.srv.URL+"/api/v1/projects/"+e.pid+"/stream", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req = req.WithContext(ctx)
	res, err := e.admin.hc.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK || !strings.HasPrefix(res.Header.Get("content-type"), "text/event-stream") {
		t.Fatal(res.Status, res.Header)
	}
	r := bufio.NewReader(res.Body)
	read := func() (string, string) {
		var event, data string
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			line = strings.TrimRight(line, "\r\n")
			switch {
			case strings.HasPrefix(line, "event: "):
				event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				data = strings.TrimPrefix(line, "data: ")
			case line == "" && event != "":
				return event, data
			}
		}
	}
	if ev, data := read(); ev != "ready" || data != "{}" {
		t.Fatal(ev, data)
	}
	pipe := createPipeline(t, e.admin, e.pid, e.ids, "shop.orders")
	ev, data := read()
	if ev != "pipeline" || !strings.Contains(data, `"id": "`+pipe.Str("id")+`"`) || !strings.Contains(data, `"name": "orders-cdc"`) {
		t.Fatal(ev, data)
	}
	newClient(t, e.srv).get("/api/v1/projects/" + e.pid + "/stream").expect(401)
}
