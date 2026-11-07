package runtime_test

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

const flinkID = "abc123def456"

func flinkWorkload(taskManagers int, hash string) runtime.Workload {
	return runtime.Workload{PipelineID: "p", ShortID: flinkID, Name: "lake", Image: "acme/flink-cdc:3.7.0", ConfigHash: hash, Running: true,
		Engine: "flink-cdc", StopGraceSeconds: 90,
		Env:   map[string]string{"BZ_STATE_PASSWORD": "s3cret", "BZ_SSH_KEY": "key"},
		Files: map[string]string{"keystore.p12.b64": "a2V5"},
		Tunnel: map[string]any{"image": "bezisync/bezisync:1", "host": "db", "port": 5432, "ssh_user": "u", "ssh_host": "bastion",
			"ssh_port": 22, "listen_port": 15432},
		Flink: &runtime.FlinkTopology{TaskManagers: taskManagers, JobManagerCPU: "500m", JobManagerMemory: "1Gi", TaskManagerCPU: "1",
			TaskManagerMemory: "2Gi", HelperImage: "bezisync/bezisync:1", StateSize: "512Mi",
			Config: map[string]string{"settings.json": "{}", "flink-conf.yaml": "a: b"}}}
}

func newFlinkDocker(t *testing.T) (*fakeDocker, *runtime.DockerDriver) {
	t.Helper()
	f, host := newFakeDocker(t)
	f.pull = f.progress(0)
	for _, img := range []string{"acme/flink-cdc:3.7.0", "bezisync/bezisync:1"} {
		f.images[img] = true
	}
	d, err := runtime.NewDocker(host, nil, "rt-net", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return f, d
}

func at(m map[string]any, path ...string) any {
	var v any = m
	for _, p := range path {
		mm, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = mm[p]
	}
	return v
}

func untar(t *testing.T, b []byte) map[string]*tar.Header {
	t.Helper()
	out := map[string]*tar.Header{}
	tr := tar.NewReader(bytes.NewReader(b))
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out[h.Name] = h
	}
}

func (f *fakeDocker) stops() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.stopped...)
}

func (f *fakeDocker) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for n := range f.containers {
		out = append(out, n)
	}
	return out
}

// A Flink CDC pipeline is a JobManager, TaskManagers and its tunnel, on a private network with the
// helper and its configuration in a shared volume; secrets reach the JobManager only.
func TestDockerFlinkCluster(t *testing.T) {
	f, d := newFlinkDocker(t)
	ctx := context.Background()
	w := flinkWorkload(2, "h1")
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(f.names(), ","); len(f.names()) != 4 || strings.Contains(got, "install") {
		t.Fatalf("containers %s", got)
	}
	jm, tm, tunnel := f.container("bz-"+flinkID), f.container("bz-"+flinkID+"-tm-2"), f.container("bz-"+flinkID+"-tunnel")
	for name, c := range map[string]fakeContainer{"jm": jm, "tm": tm, "tunnel": tunnel} {
		if c.Status != "running" || at(c.Body, "HostConfig", "ReadonlyRootfs") != true || fmt.Sprint(at(c.Body, "HostConfig", "CapDrop")) != "[ALL]" ||
			at(c.Body, "HostConfig", "NetworkMode") != "bz-"+flinkID+"-net" || fmt.Sprint(c.Networks) != "[rt-net]" {
			t.Errorf("%s: %+v %v", name, c.Body["HostConfig"], c.Networks)
		}
	}
	if jm.Labels[runtime.RoleLabel] != "jobmanager" || tm.Labels[runtime.RoleLabel] != "taskmanager" || jm.Labels["app.bezisync.io/config-hash"] != "h1" {
		t.Fatal(jm.Labels, tm.Labels)
	}
	if fmt.Sprint(at(jm.Body, "Entrypoint")) != "[/opt/bezisync/bezisync]" || fmt.Sprint(at(jm.Body, "Cmd")) != "[engine flink-cdc jobmanager]" ||
		fmt.Sprint(at(tm.Body, "Cmd")) != "[engine flink-cdc taskmanager]" || at(jm.Body, "StopTimeout") != float64(90) {
		t.Fatal(jm.Body, tm.Body)
	}
	if !strings.Contains(fmt.Sprint(at(jm.Body, "Env")), "BZ_STATE_PASSWORD=s3cret") || strings.Contains(fmt.Sprint(at(tm.Body, "Env")), "s3cret") {
		t.Fatal("secrets go to the JobManager only")
	}
	if at(jm.Body, "HostConfig", "Memory") != float64(1<<30) || at(tm.Body, "HostConfig", "NanoCpus") != float64(1e9) {
		t.Fatal(jm.Body["HostConfig"], tm.Body["HostConfig"])
	}
	if fmt.Sprint(at(tunnel.Body, "NetworkingConfig", "EndpointsConfig", "bz-"+flinkID+"-net", "Aliases")) != "[bz-tunnel]" ||
		!strings.Contains(fmt.Sprint(at(tunnel.Body, "Cmd")), "--listen-host 0.0.0.0 --listen-port 15432") {
		t.Fatal(tunnel.Body)
	}
	files := untar(t, jm.Archive)
	if h := files["secrets/keystore.p12.b64"]; h == nil || h.Mode != 0o600 || h.Uid != runtime.FlinkUID {
		t.Fatalf("keystore %+v", h)
	}
	if h := files["config/settings.json"]; h == nil || h.Mode != 0o640 || h.Uid != runtime.FlinkUID || files["config/"] == nil {
		t.Fatalf("config %+v", files)
	}
	if v := f.volumes["bz-"+flinkID+"-state"]; at(v, "DriverOpts", "type") != "tmpfs" || at(v, "DriverOpts", "o") != "uid=9999,gid=9999,size=536870912" {
		t.Fatal(v)
	}
	if f.volumes["bz-"+flinkID+"-opt"] == nil || f.networks["bz-"+flinkID+"-net"] == nil {
		t.Fatal("the opt volume and the network")
	}
	obs, err := d.Observe(ctx, flinkID)
	if err != nil || obs.Phase != runtime.PhaseRunning || !obs.Ready || len(obs.Details["components"].([]any)) != 4 {
		t.Fatalf("%+v %v", obs, err)
	}
	if ids, err := d.ListWorkloads(ctx, ""); err != nil || len(ids) != 1 || ids[0] != flinkID {
		t.Fatal(ids, err)
	}
	// Applying again changes nothing.
	if err := d.Apply(ctx, w); err != nil || len(f.stops()) != 0 {
		t.Fatal(err, f.stops())
	}
	// A TaskManager that died is started again.
	f.mu.Lock()
	f.containers["bz-"+flinkID+"-tm-1"].Status = "exited"
	f.mu.Unlock()
	if obs, _ := d.Observe(ctx, flinkID); obs.Ready {
		t.Fatal("ready with a TaskManager down")
	}
	if err := d.Apply(ctx, w); err != nil || f.container("bz-"+flinkID+"-tm-1").Status != "running" {
		t.Fatal(err)
	}
}

// Pausing stops the JobManager first, within the stop grace, in the background; changing the
// cluster stops it the same way before recreating it.
func TestDockerFlinkOrderedStopAndRecreate(t *testing.T) {
	f, d := newFlinkDocker(t)
	ctx := context.Background()
	w := flinkWorkload(2, "h1")
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.Running = false
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the stop", func() bool { return len(f.stops()) == 4 })
	if s := f.stops(); s[0] != "bz-"+flinkID+"?t=90" {
		t.Fatalf("the JobManager stops first with the grace: %v", s)
	}
	eventually(t, "the stop to be forgotten", func() bool { o, _ := d.Observe(ctx, flinkID); return o.Phase == runtime.PhaseStopped })
	if err := d.Apply(ctx, w); err != nil || len(f.stops()) != 4 {
		t.Fatal("a stopped cluster is left alone", err, f.stops())
	}

	// Resume with three TaskManagers: the cluster is recreated (it isn't running, so no stop).
	w = flinkWorkload(3, "h2")
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	if c := f.container("bz-" + flinkID + "-tm-3"); c.Status != "running" || c.Labels["app.bezisync.io/config-hash"] != "h2" {
		t.Fatalf("%+v", c)
	}
	// A new hash while running: stop first (the position is saved), then recreate.
	w.ConfigHash = "h3"
	w.Flink.TaskManagers = 1
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	if f.container("bz-" + flinkID).Labels["app.bezisync.io/config-hash"] != "h2" {
		t.Fatal("recreated before stopping")
	}
	eventually(t, "the recreate", func() bool {
		if err := d.Apply(ctx, w); err != nil {
			t.Fatal(err)
		}
		return f.container("bz-" + flinkID).Labels["app.bezisync.io/config-hash"] == "h3"
	})
	if names := f.names(); len(names) != 3 {
		t.Fatalf("one TaskManager, the JobManager and the tunnel: %v", names)
	}
}

// A failed stop is reported by the next Apply; a failed helper copy explains itself.
func TestDockerFlinkFailures(t *testing.T) {
	f, d := newFlinkDocker(t)
	ctx := context.Background()
	w := flinkWorkload(1, "h1")
	f.exit = 3
	if err := d.Apply(ctx, w); err == nil || !strings.Contains(err.Error(), "Copying the BeziSync helper from bezisync/bezisync:1 failed (exit 3)") {
		t.Fatalf("install: %v", err)
	}
	f.exit = 0
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.stopErr["bz-"+flinkID] = true
	f.mu.Unlock()
	w.Running = false
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	var err error
	eventually(t, "the failed stop", func() bool { err = d.Apply(ctx, w); return err != nil })
	if !strings.Contains(err.Error(), "Stopping the Flink cluster failed") {
		t.Fatal(err)
	}
	if f.container("bz-"+flinkID+"-tm-1").Status != "running" {
		t.Fatal("TaskManagers are stopped only after the JobManager")
	}
}

// Delete stops in order and removes the containers, volumes and network.
func TestDockerFlinkDelete(t *testing.T) {
	f, d := newFlinkDocker(t)
	ctx := context.Background()
	if err := d.Apply(ctx, flinkWorkload(2, "h1")); err != nil {
		t.Fatal(err)
	}
	if err := d.Delete(ctx, flinkID); err != nil {
		t.Fatal(err)
	}
	if len(f.names()) != 0 || len(f.volumes) != 0 || len(f.networks) != 0 {
		t.Fatal(f.names(), f.volumes, f.networks)
	}
	if s := f.stops(); len(s) != 4 || s[0] != "bz-"+flinkID+"?t=90" {
		t.Fatal(s)
	}
}

func TestDockerFlinkComponentLogs(t *testing.T) {
	_, d := newFlinkDocker(t)
	for _, bad := range []string{"taskmanager-0", "taskmanager-x", "taskmanager-65", "sidecar"} {
		if _, err := d.LogsOf(context.Background(), flinkID, bad, 10, false); err == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if out, err := d.LogsOf(context.Background(), flinkID, "taskmanager-1", 10, false); err != nil || out != "" {
		t.Fatal(out, err) // a component that doesn't exist has no logs
	}
}

// Copying the helper may outlast one API request: the wait isn't cut off by the request timeout
// (Known issue 11), only by HelperInstallTimeout.
func TestDockerFlinkSlowHelperInstall(t *testing.T) {
	defer runtime.SetRequestTimeout(100 * time.Millisecond)()
	f, d := newFlinkDocker(t)
	f.wait = 400 * time.Millisecond
	ctx := context.Background()
	if err := d.Apply(ctx, flinkWorkload(1, "h1")); err != nil {
		t.Fatalf("a slow helper copy must not fail: %v", err)
	}
	prev := runtime.HelperInstallTimeout
	runtime.HelperInstallTimeout = 50 * time.Millisecond
	t.Cleanup(func() { runtime.HelperInstallTimeout = prev })
	f2, d2 := newFlinkDocker(t)
	f2.wait = 400 * time.Millisecond
	err := d2.Apply(ctx, flinkWorkload(1, "h1"))
	if err == nil || !strings.Contains(err.Error(), "took longer than 0 seconds") {
		t.Fatalf("install timeout: %v", err)
	}
}
