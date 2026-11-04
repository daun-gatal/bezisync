package controller

import (
	"testing"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/flinkhelper"
	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

func TestFlinkMetricsReportOnlyWhatTheHelperSaw(t *testing.T) {
	lag := 1500.0
	copying := flinkhelper.Status{Phase: flinkhelper.PhaseRunning, JobState: "RUNNING", RecordsRead: 42, LagMS: &lag}
	m := flinkMetrics(copying)
	if !*m.Connected || !*m.SnapshotRunning || *m.SnapshotCompleted || *m.MSBehindSource != 1500 || *m.EventsSeenSnapshot != 42 || m.EventsSeenStreaming != nil {
		t.Fatalf("copying: %+v", m.AsDict())
	}
	copying.Streaming = true
	m = flinkMetrics(copying)
	if *m.SnapshotRunning || !*m.SnapshotCompleted || *m.EventsSeenStreaming != 42 {
		t.Fatalf("streaming: %+v", m.AsDict())
	}
	// Starting: nothing is known yet.
	if m := flinkMetrics(flinkhelper.Status{Phase: flinkhelper.PhaseSubmitting}); len(m.AsDict()) != 0 {
		t.Fatalf("starting: %+v", m.AsDict())
	}
	if m := flinkMetrics(flinkhelper.Status{Phase: flinkhelper.PhaseRestarting}); m.Connected == nil || *m.Connected {
		t.Fatal("a restarting job is not connected")
	}
}

func TestFlinkDownSaysWhy(t *testing.T) {
	for _, c := range []struct {
		st         flinkhelper.Status
		code, text string
	}{
		{flinkhelper.Status{Phase: flinkhelper.PhaseFailed, ErrorCode: "destination_renamed", Error: "renamed"}, "destination_renamed", "renamed"},
		{flinkhelper.Status{Phase: flinkhelper.PhaseFailed}, "job_failed", "The Flink job failed."},
		{flinkhelper.Status{Phase: flinkhelper.PhaseConfigError, ErrorCode: "image", Error: "This image lacks MySQL Connector/J."}, "image",
			"This image lacks MySQL Connector/J."},
		{flinkhelper.Status{Phase: flinkhelper.PhaseRestarting, Exceptions: []string{"a", "java.io.IOException: gone"}}, "job_restarting",
			"The Flink job is restarting after a failure: java.io.IOException: gone"},
		{flinkhelper.Status{Phase: flinkhelper.PhaseStarting, TaskManagers: 1, TaskManagersWanted: 2}, "health_down", "Waiting for TaskManagers: 1 of 2 connected."},
		{flinkhelper.Status{Phase: flinkhelper.PhaseSubmitting}, "health_down", "Submitting the pipeline to Flink."},
		{flinkhelper.Status{Phase: flinkhelper.PhaseStopping}, "health_down", "Saving the position and stopping."},
	} {
		if r := flinkDown(c.st); r.Code != c.code || r.Message != c.text {
			t.Errorf("%s: %+v", c.st.Phase, r)
		}
	}
}

func TestFlinkStatusKeepsTheJobAndProgress(t *testing.T) {
	at := time.Date(2026, 11, 4, 9, 0, 0, 0, time.UTC)
	st := &flinkhelper.Status{Phase: flinkhelper.PhaseRunning, JobState: "RUNNING", TaskManagers: 2, TaskManagersWanted: 2, CheckpointsDone: 5,
		LastStored: &flinkhelper.Saved{ID: 9, Kind: "checkpoint", Reason: "periodic", TakenAt: at, Size: 3000}, Chunks: map[string]int64{"shop_orders": 12, "other_t": 1}}
	obs := &runtime.Observation{Details: map[string]any{"components": []any{map[string]any{"role": "jobmanager"}}}}
	got := flinkStatus(st, obs, []string{"shop.orders", "shop.items"})
	chunks := got["chunks"].(map[string]any)
	if len(chunks) != 1 || chunks["shop.orders"] != int64(12) || got["last_saved"].(map[string]any)["id"] != int64(9) || got["components"] == nil ||
		got["last_saved"].(map[string]any)["taken_at"] != "2026-11-04T09:00:00+00:00" {
		t.Fatalf("%+v", got)
	}
}
