package runtime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

// fakeDocker is the slice of the Docker Engine API the driver uses, with containers named by id.
type fakeDocker struct {
	mu         sync.Mutex
	images     map[string]bool
	containers map[string]*fakeContainer
	pulls      int
	// pull streams one download; it runs in the request handler, so it can be slow.
	pull func(ctx context.Context, ref string, w http.ResponseWriter)
}

type fakeContainer struct {
	Image  string
	Labels map[string]string
	Status string
}

var apiVersion = regexp.MustCompile(`^/v[0-9.]+`)

func newFakeDocker(t *testing.T) (*fakeDocker, string) {
	f := &fakeDocker{images: map[string]bool{}, containers: map[string]*fakeContainer{}}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	return f, "tcp://" + strings.TrimPrefix(srv.URL, "http://")
}

func (f *fakeDocker) serve(w http.ResponseWriter, r *http.Request) {
	path := apiVersion.ReplaceAllString(r.URL.Path, "")
	w.Header().Set("Api-Version", "1.47")
	notFound := func(what string) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "No such " + what})
	}
	switch {
	case path == "/_ping":
		_, _ = w.Write([]byte("OK"))
	case strings.HasPrefix(path, "/images/") && strings.HasSuffix(path, "/json"):
		ref := strings.TrimSuffix(strings.TrimPrefix(path, "/images/"), "/json")
		f.mu.Lock()
		ok := f.images[ref]
		f.mu.Unlock()
		if !ok {
			notFound("image: " + ref)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"Id": "sha256:" + ref})
	case path == "/images/create":
		ref := strings.TrimPrefix(r.URL.Query().Get("fromImage"), "docker.io/") + ":" + r.URL.Query().Get("tag")
		f.mu.Lock()
		f.pulls++
		f.mu.Unlock()
		f.pull(r.Context(), ref, w)
	case path == "/containers/create":
		var body struct {
			Image  string
			Labels map[string]string
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		name := r.URL.Query().Get("name")
		f.mu.Lock()
		f.containers[name] = &fakeContainer{Image: body.Image, Labels: body.Labels, Status: "created"}
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"Id": name})
	case strings.HasPrefix(path, "/containers/"):
		parts := strings.Split(strings.TrimPrefix(path, "/containers/"), "/")
		f.mu.Lock()
		defer f.mu.Unlock()
		c := f.containers[parts[0]]
		if c == nil {
			notFound("container: " + parts[0])
			return
		}
		switch {
		case r.Method == http.MethodDelete:
			delete(f.containers, parts[0])
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 2 && parts[1] == "json":
			_ = json.NewEncoder(w).Encode(map[string]any{"Id": parts[0], "Name": "/" + parts[0],
				"Config": map[string]any{"Image": c.Image, "Labels": c.Labels}, "State": map[string]any{"Status": c.Status},
				"HostConfig": map[string]any{"RestartPolicy": map[string]any{"Name": "unless-stopped"}}})
		case len(parts) == 2 && parts[1] == "archive":
			w.WriteHeader(http.StatusOK)
		case len(parts) == 2 && parts[1] == "start":
			c.Status = "running"
			w.WriteHeader(http.StatusNoContent)
		case len(parts) == 2 && parts[1] == "stop":
			c.Status = "exited"
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected "+r.Method+" "+path, http.StatusNotImplemented)
		}
	default:
		http.Error(w, "unexpected "+r.Method+" "+path, http.StatusNotImplemented)
	}
}

func (f *fakeDocker) container(name string) fakeContainer {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.containers[name]; c != nil {
		return *c
	}
	return fakeContainer{}
}

func (f *fakeDocker) pullCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pulls
}

// progress streams download progress for d, then marks ref present.
func (f *fakeDocker) progress(d time.Duration) func(context.Context, string, http.ResponseWriter) {
	return func(ctx context.Context, ref string, w http.ResponseWriter) {
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			_, _ = fmt.Fprintf(w, `{"status":"Downloading","id":"layer"}`+"\n")
			w.(http.Flusher).Flush()
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
		f.mu.Lock()
		f.images[ref] = true
		f.mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"status":"Status: Downloaded newer image for %s"}`+"\n", ref)
	}
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func dockerWorkload(image, hash string) runtime.Workload {
	return runtime.Workload{PipelineID: "p", ShortID: "abc123def456", Name: "orders", Image: image, Properties: "a=b\n",
		ConfigHash: hash, CPU: "500m", Memory: "768Mi", Heap: "512m", Running: true}
}

// A new image downloads in the background, for longer than any API request may take, while the
// current connector keeps running; it is replaced once the image is there.
func TestDockerDownloadsANewImageInTheBackground(t *testing.T) {
	defer runtime.SetRequestTimeout(100 * time.Millisecond)()
	f, host := newFakeDocker(t)
	f.pull = f.progress(400 * time.Millisecond) // four times the request timeout
	f.images["debezium/server:3.7.0.Final"] = true
	f.containers["bz-abc123def456"] = &fakeContainer{Image: "debezium/server:3.7.0.Final",
		Labels: map[string]string{"app.bezisync.io/config-hash": "h1"}, Status: "running"}
	d, err := runtime.NewDocker(host, nil, "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	w := dockerWorkload("debezium/server:3.6.0.Final", "h2")

	if err := d.Apply(ctx, w); err != nil {
		t.Fatalf("apply while the image downloads: %v", err)
	}
	if c := f.container("bz-abc123def456"); c.Status != "running" || c.Labels["app.bezisync.io/config-hash"] != "h1" {
		t.Fatalf("the current connector must keep running during the download: %+v", c)
	}
	obs, err := d.Observe(ctx, w.ShortID)
	if err != nil || obs.Phase != runtime.PhaseRunning || !strings.HasPrefix(obs.ImagePull, "Downloading the image debezium/server:3.6.0.Final (") {
		t.Fatalf("observe during the download: %+v, %v", obs, err)
	}
	if err := d.Apply(ctx, w); err != nil { // the next reconcile joins the same download
		t.Fatal(err)
	}
	eventually(t, "the download", func() bool { o, _ := d.Observe(ctx, w.ShortID); return o.ImagePull == "" })
	if n := f.pullCount(); n != 1 {
		t.Fatalf("%d downloads, want 1", n)
	}
	if err := d.Apply(ctx, w); err != nil {
		t.Fatalf("apply after the download: %v", err)
	}
	if c := f.container("bz-abc123def456"); c.Status != "running" || c.Image != "debezium/server:3.6.0.Final" || c.Labels["app.bezisync.io/config-hash"] != "h2" {
		t.Fatalf("not replaced: %+v", c)
	}
}

// A first deploy has no container while it waits, and a pause during the download stops the
// current connector without waiting for it.
func TestDockerWaitsForTheImageBeforeCreating(t *testing.T) {
	f, host := newFakeDocker(t)
	release := make(chan struct{})
	f.pull = func(ctx context.Context, ref string, w http.ResponseWriter) {
		select {
		case <-release:
			f.progress(0)(ctx, ref, w)
		case <-ctx.Done():
		}
	}
	d, err := runtime.NewDocker(host, nil, "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	w := dockerWorkload("debezium/server:3.6.0.Final", "h1")
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	if obs, _ := d.Observe(ctx, w.ShortID); obs.Phase != runtime.PhaseMissing || obs.ImagePull == "" {
		t.Fatalf("first deploy while downloading: %+v", obs)
	}

	f.mu.Lock()
	f.containers["bz-abc123def456"] = &fakeContainer{Image: "old", Labels: map[string]string{"app.bezisync.io/config-hash": "h0"}, Status: "running"}
	f.mu.Unlock()
	w.Running = false
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	if c := f.container("bz-abc123def456"); c.Status != "exited" {
		t.Fatalf("pause while downloading must stop the connector: %+v", c)
	}
	close(release)
}

// A failed download is reported (not retried) for a while, then tried again.
func TestDockerReportsAFailedDownload(t *testing.T) {
	f, host := newFakeDocker(t)
	f.pull = func(_ context.Context, ref string, w http.ResponseWriter) {
		_, _ = fmt.Fprintf(w, `{"status":"Pulling from %s"}`+"\n"+`{"errorDetail":{"message":"manifest unknown"},"error":"manifest unknown"}`+"\n", ref)
	}
	d, err := runtime.NewDocker(host, nil, "", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	w := dockerWorkload("debezium/server:9.9", "h1")
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the failed download", func() bool { o, _ := d.Observe(ctx, w.ShortID); return o.ImagePull == "" })
	for range 2 {
		err := d.Apply(ctx, w)
		if err == nil || err.Error() != "Cannot download the image debezium/server:9.9: manifest unknown" {
			t.Fatalf("apply after a failed download: %v", err)
		}
	}
	if obs, _ := d.Observe(ctx, w.ShortID); obs.ImagePull != "" {
		t.Fatalf("a failed download is not in progress: %+v", obs)
	}
	if n := f.pullCount(); n != 1 {
		t.Fatalf("%d downloads, want 1 before the retry window ends", n)
	}
	defer runtime.SetPullRetryAfter(0)()
	if err := d.Apply(ctx, w); err != nil {
		t.Fatalf("retry: %v", err)
	}
	eventually(t, "the retry", func() bool { return f.pullCount() == 2 })
}

// The runtime's image download timeout ends a download that takes too long.
func TestDockerImageDownloadTimeout(t *testing.T) {
	f, host := newFakeDocker(t)
	f.pull = f.progress(time.Hour)
	d, err := runtime.NewDocker(host, nil, "", 200*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	w := dockerWorkload("debezium/server:3.6.0.Final", "h1")
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the timeout", func() bool { o, _ := d.Observe(ctx, w.ShortID); return o.ImagePull == "" })
	err = d.Apply(ctx, w)
	if err == nil || !strings.Contains(err.Error(), "took longer than the runtime's image download timeout") ||
		!strings.Contains(err.Error(), "Settings → Runtimes") {
		t.Fatalf("apply after the timeout: %v", err)
	}
}
