package runtime_test

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

func TestEKSTokenIsAPresignedSTSCallBoundToTheCluster(t *testing.T) {
	tok, err := runtime.EKSToken(context.Background(), "orders-prod", "eu-west-1", "AKIAEXAMPLE", "secret")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(tok, "k8s-aws-v1.") {
		t.Fatalf("token %q", tok)
	}
	raw := strings.TrimPrefix(tok, "k8s-aws-v1.")
	raw += strings.Repeat("=", (4-len(raw)%4)%4)
	dec, err := base64.URLEncoding.DecodeString(raw)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(string(dec))
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if u.Host != "sts.eu-west-1.amazonaws.com" || q.Get("Action") != "GetCallerIdentity" || q.Get("X-Amz-Expires") != "60" {
		t.Fatalf("unexpected URL %s", dec)
	}
	if !strings.Contains(q.Get("X-Amz-SignedHeaders"), "x-k8s-aws-id") || strings.Contains(tok, "secret") {
		t.Fatalf("cluster header not signed or secret leaked: %s", dec)
	}
}

func TestTokenRESTConfigRefreshesBearerTokens(t *testing.T) {
	calls := 0
	source := func(context.Context) (string, time.Time, error) {
		calls++
		// The first token is almost expired, so the next request refreshes it.
		if calls == 1 {
			return "tok-1", time.Now().Add(time.Minute), nil
		}
		return "tok-2", time.Now().Add(time.Hour), nil
	}
	var seen []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"DeploymentList","apiVersion":"apps/v1","items":[]}`))
	}))
	defer srv.Close()
	ca := string(pemCert(srv))
	rc, err := runtime.TokenRESTConfig(context.Background(), srv.URL, ca, source)
	if err != nil {
		t.Fatal(err)
	}
	d, err := runtime.NewKubernetes("cdc", false, "", rc)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.ListWorkloads(context.Background(), ""); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] != "Bearer tok-2" {
		t.Fatalf("authorization headers %v (calls %d)", seen, calls)
	}
}

func TestKubernetesApplyAndObserve(t *testing.T) {
	cs := fake.NewClientset()
	d := runtime.NewKubernetesFromClient("cdc", cs)
	ctx := context.Background()
	w := runtime.Workload{PipelineID: "p", ShortID: "abc123def456", Name: "orders", Image: "debezium:3.7", Properties: "a=b\n",
		Env: map[string]string{"BZ_SRC_PASSWORD": "pw"}, ConfigHash: "h1", CPU: "500m", Memory: "768Mi", Heap: "512m", Running: true,
		Owner: "rt-1", Files: map[string]string{"k.json": "{}"},
		Tunnel: map[string]any{"host": "db", "port": 5432, "ssh_host": "bastion", "ssh_port": 22, "ssh_user": "u", "ssh_host_key": "", "listen_port": 15432, "image": "agent:1"}}
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	dep, err := cs.AppsV1().Deployments("cdc").Get(ctx, "bz-abc123def456", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if *dep.Spec.Replicas != 1 || dep.Spec.Template.Annotations["app.bezisync.io/config-hash"] != "h1" || dep.Labels[runtime.Owner] != "rt-1" {
		t.Fatalf("deployment %+v", dep.ObjectMeta)
	}
	if len(dep.Spec.Template.Spec.InitContainers) != 1 || dep.Spec.Template.Spec.InitContainers[0].Args[2] != "db:5432" {
		t.Fatalf("tunnel sidecar %+v", dep.Spec.Template.Spec.InitContainers)
	}
	sec, _ := cs.CoreV1().Secrets("cdc").Get(ctx, "bz-abc123def456", metav1.GetOptions{})
	if string(sec.Data["JAVA_OPTS"]) != "-Xmx512m -XX:+ExitOnOutOfMemoryError" || string(sec.Data["ENABLE_DEBEZIUM_SCRIPTING"]) != "true" {
		t.Fatalf("env secret %v", sec.Data)
	}
	ids, _ := d.ListWorkloads(ctx, "rt-1")
	if len(ids) != 1 || ids[0] != "abc123def456" {
		t.Fatalf("list %v", ids)
	}
	if ids, _ := d.ListWorkloads(ctx, "rt-2"); len(ids) != 0 {
		t.Fatalf("other owner sees %v", ids)
	}
	obs, _ := d.Observe(ctx, "abc123def456")
	if obs.Phase != runtime.PhasePending || *obs.ConfigHash != "h1" {
		t.Fatalf("observe without pod %+v", obs)
	}
	_, _ = cs.CoreV1().Pods("cdc").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "bz-abc-1", Labels: map[string]string{runtime.Label: "abc123def456"}, CreationTimestamp: metav1.Now()},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "debezium", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "not found"}}}}},
	}, metav1.CreateOptions{})
	obs, _ = d.Observe(ctx, "abc123def456")
	if obs.Phase != runtime.PhaseFailed || obs.Reason != "ImagePullBackOff: not found" {
		t.Fatalf("observe image pull failure %+v", obs)
	}
	w.Running = false
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	if obs, _ := d.Observe(ctx, "abc123def456"); obs.Phase != runtime.PhaseStopped {
		t.Fatalf("paused %+v", obs)
	}
	if err := d.Delete(ctx, "abc123def456"); err != nil {
		t.Fatal(err)
	}
	if obs, _ := d.Observe(ctx, "abc123def456"); obs.Phase != runtime.PhaseMissing {
		t.Fatalf("deleted %+v", obs)
	}
}

func TestParseDockerTimeAndQuantities(t *testing.T) {
	ts := runtime.ParseDockerTime("2026-01-02T03:04:05.123456789Z")
	if ts == nil || ts.Nanosecond() != 123456000 {
		t.Fatalf("parsed %v", ts)
	}
	if runtime.ParseDockerTime("0001-01-01T00:00:00Z") != nil || runtime.ParseDockerTime("") != nil {
		t.Fatal("zero time parsed")
	}
	if runtime.CPUNano("500m") != 500_000_000 || runtime.MemBytes("768Mi") != 768<<20 || runtime.QuantityMem("3Gi") != 3<<30 || runtime.QuantityCPU("250m") != 0.25 {
		t.Fatal("quantity conversion")
	}
}

func pemCert(srv *httptest.Server) []byte {
	var b strings.Builder
	b.WriteString("-----BEGIN CERTIFICATE-----\n")
	enc := base64.StdEncoding.EncodeToString(srv.Certificate().Raw)
	for len(enc) > 64 {
		b.WriteString(enc[:64] + "\n")
		enc = enc[64:]
	}
	b.WriteString(enc + "\n-----END CERTIFICATE-----\n")
	return []byte(b.String())
}

// ErrImagePull is one failed attempt the kubelet retries (often a registry hiccup): the pod is
// still starting. Only ImagePullBackOff, after repeated failures, means the image cannot be pulled.
func TestKubernetesImagePullStates(t *testing.T) {
	for reason, want := range map[string]string{"ErrImagePull": runtime.PhasePending, "ImagePullBackOff": runtime.PhaseFailed} {
		cs := fake.NewClientset()
		d := runtime.NewKubernetesFromClient("cdc", cs)
		ctx := context.Background()
		if err := d.Apply(ctx, runtime.Workload{PipelineID: "p", ShortID: "abc123def456", Name: "orders", Image: "debezium:3.6",
			ConfigHash: "h1", CPU: "500m", Memory: "768Mi", Heap: "512m", Running: true}); err != nil {
			t.Fatal(err)
		}
		_, _ = cs.CoreV1().Pods("cdc").Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "bz-abc-1", Labels: map[string]string{runtime.Label: "abc123def456"}, CreationTimestamp: metav1.Now()},
			Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{
				Name: "debezium", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: "i/o timeout"}}}}},
		}, metav1.CreateOptions{})
		obs, err := d.Observe(ctx, "abc123def456")
		if err != nil || obs.Phase != want || obs.Reason != reason+": i/o timeout" {
			t.Errorf("%s: %+v, %v (want phase %s)", reason, obs, err, want)
		}
	}
}

func TestKubernetesProbesTheWorkloadsHealthPort(t *testing.T) {
	cs := fake.NewClientset()
	d := runtime.NewKubernetesFromClient("cdc", cs)
	ctx := context.Background()
	for port, want := range map[int]string{0: "http", 9000: "health"} {
		w := runtime.Workload{PipelineID: "p", ShortID: "abc123def456", Name: "orders", Image: "dbz", Properties: "a=b\n", ConfigHash: "h",
			CPU: "500m", Memory: "768Mi", Heap: "512m", Running: true, Owner: "rt-1", HealthPort: port}
		if err := d.Apply(ctx, w); err != nil {
			t.Fatal(err)
		}
		dep, _ := cs.AppsV1().Deployments("cdc").Get(ctx, "bz-abc123def456", metav1.GetOptions{})
		c := dep.Spec.Template.Spec.Containers[0]
		if c.ReadinessProbe.HTTPGet.Port.StrVal != want || c.LivenessProbe.HTTPGet.Port.StrVal != want {
			t.Fatalf("health port %d: probes on %v", port, c.ReadinessProbe.HTTPGet.Port)
		}
		named := map[string]int32{}
		for _, p := range c.Ports {
			named[p.Name] = p.ContainerPort
		}
		if port != 0 && named["health"] != int32(port) {
			t.Fatalf("ports %v", c.Ports)
		}
	}
}
