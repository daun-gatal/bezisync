package runtime_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

func k8sFlink(t *testing.T) (*fake.Clientset, *runtime.KubernetesDriver) {
	t.Helper()
	cs := fake.NewClientset()
	return cs, runtime.NewKubernetesFromClient("cdc", cs)
}

func deployment(t *testing.T, cs *fake.Clientset, name string) *appsv1.Deployment {
	t.Helper()
	d, err := cs.AppsV1().Deployments("cdc").Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// addPod stands in for the Deployment controller.
func addPod(t *testing.T, cs *fake.Clientset, name, role string, ready bool) {
	t.Helper()
	_, err := cs.CoreV1().Pods("cdc").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{runtime.Label: flinkID, runtime.RoleLabel: role}, CreationTimestamp: metav1.Now()},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: role, Ready: ready,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}}}}},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
}

func deletePod(t *testing.T, cs *fake.Clientset, name string) {
	t.Helper()
	if err := cs.CoreV1().Pods("cdc").Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
}

// A Flink CDC pipeline is a JobManager Deployment, a TaskManager Deployment and a Service for the
// JobManager; each pod copies the helper in and has its own tunnel; secrets reach the JobManager only.
func TestKubernetesFlinkCluster(t *testing.T) {
	cs, d := k8sFlink(t)
	ctx := context.Background()
	w := flinkWorkload(3, "h1")
	w.Owner = "rt-1"
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	jm, tm := deployment(t, cs, "bz-"+flinkID), deployment(t, cs, "bz-"+flinkID+"-tm")
	if *jm.Spec.Replicas != 1 || *tm.Spec.Replicas != 3 || jm.Labels[runtime.RoleLabel] != "jobmanager" || tm.Spec.Selector.MatchLabels[runtime.RoleLabel] != "taskmanager" {
		t.Fatal(jm.Spec.Replicas, tm.Spec.Replicas, jm.Labels, tm.Spec.Selector)
	}
	for _, dep := range []*appsv1.Deployment{jm, tm} {
		spec := dep.Spec.Template.Spec
		c := spec.Containers[0]
		if c.Command[0] != "/opt/bezisync/bezisync" || !*c.SecurityContext.ReadOnlyRootFilesystem || *c.SecurityContext.RunAsUser != runtime.FlinkUID ||
			*spec.TerminationGracePeriodSeconds != 100 || *spec.SecurityContext.FSGroup != runtime.FlinkUID {
			t.Errorf("%s: %+v", dep.Name, c)
		}
		if len(spec.InitContainers) != 2 || spec.InitContainers[0].Image != "bezisync/bezisync:1" || spec.InitContainers[0].Args[0] != "install-self" ||
			spec.InitContainers[1].Name != "ssh-tunnel" || spec.HostAliases[0].Hostnames[0] != "bz-tunnel" {
			t.Errorf("%s init containers %+v", dep.Name, spec.InitContainers)
		}
		mounts := fmt.Sprint(c.VolumeMounts)
		for _, m := range []string{"/opt/bezisync ", "/opt/bezisync/config", "/opt/bezisync/secrets", "/state", "/tmp"} {
			if !strings.Contains(mounts, m) {
				t.Errorf("%s lacks %s: %s", dep.Name, m, mounts)
			}
		}
	}
	if len(jm.Spec.Template.Spec.Containers[0].EnvFrom) != 1 || len(tm.Spec.Template.Spec.Containers[0].EnvFrom) != 0 || tm.Spec.Template.Spec.Containers[0].ReadinessProbe != nil {
		t.Fatal("secrets and probes belong to the JobManager")
	}
	svc, err := cs.CoreV1().Services("cdc").Get(ctx, "bz-"+flinkID+"-jm", metav1.GetOptions{})
	if err != nil || !svc.Spec.PublishNotReadyAddresses || svc.Spec.Selector[runtime.RoleLabel] != "jobmanager" || len(svc.Spec.Ports) != 2 {
		t.Fatal(svc, err)
	}
	cm, _ := cs.CoreV1().ConfigMaps("cdc").Get(ctx, "bz-"+flinkID, metav1.GetOptions{})
	files, _ := cs.CoreV1().Secrets("cdc").Get(ctx, "bz-"+flinkID+"-files", metav1.GetOptions{})
	if cm.Data["settings.json"] != "{}" || string(files.Data["keystore.p12.b64"]) != "a2V5" {
		t.Fatal(cm.Data, files.Data)
	}
	if ids, _ := d.ListWorkloads(ctx, "rt-1"); len(ids) != 1 {
		t.Fatal(ids)
	}
	// Ready once the JobManager and every TaskManager are.
	addPod(t, cs, "jm-1", "jobmanager", true)
	addPod(t, cs, "tm-1", "taskmanager", true)
	addPod(t, cs, "tm-2", "taskmanager", true)
	obs, err := d.Observe(ctx, flinkID)
	if err != nil || obs.Phase != runtime.PhaseRunning || obs.Ready || obs.Details["taskmanagers_ready"] != 2 {
		t.Fatalf("%+v %v", obs, err)
	}
	addPod(t, cs, "tm-3", "taskmanager", true)
	if obs, _ := d.Observe(ctx, flinkID); !obs.Ready || len(obs.Details["components"].([]any)) != 4 {
		t.Fatalf("%+v", obs)
	}
	// Logs: the JobManager pod's container, the n-th TaskManager by age.
	if _, err := d.LogsOf(ctx, flinkID, "taskmanager-9", 10, false); err != nil {
		t.Fatal(err) // fewer TaskManagers than asked: empty
	}
	if _, err := d.LogsOf(ctx, flinkID, "worker", 10, false); err == nil {
		t.Fatal("unknown component")
	}
}

// Pausing scales the JobManager to zero alone; the TaskManagers stop once its pod is gone. A
// changed cluster stops the same way before anything is replaced.
func TestKubernetesFlinkOrderedStop(t *testing.T) {
	cs, d := k8sFlink(t)
	ctx := context.Background()
	w := flinkWorkload(2, "h1")
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	addPod(t, cs, "jm-1", "jobmanager", true)
	w.Running = false
	for range 2 { // the JobManager still saving: nothing else changes, however often Apply runs
		if err := d.Apply(ctx, w); err != nil {
			t.Fatal(err)
		}
		if *deployment(t, cs, "bz-"+flinkID).Spec.Replicas != 0 || *deployment(t, cs, "bz-"+flinkID+"-tm").Spec.Replicas != 2 {
			t.Fatal("the TaskManagers must outlive the JobManager")
		}
	}
	if obs, _ := d.Observe(ctx, flinkID); obs.Phase != runtime.PhasePending || obs.Reason != "Saving the position and stopping." {
		t.Fatalf("%+v", obs)
	}
	deletePod(t, cs, "jm-1")
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	if *deployment(t, cs, "bz-"+flinkID+"-tm").Spec.Replicas != 0 {
		t.Fatal("TaskManagers not stopped")
	}
	if obs, _ := d.Observe(ctx, flinkID); obs.Phase != runtime.PhaseStopped {
		t.Fatalf("%+v", obs)
	}

	// Resume, then change the configuration while running.
	w.Running = true
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	addPod(t, cs, "jm-2", "jobmanager", true)
	w.ConfigHash, w.Flink.TaskManagers = "h2", 4
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	jm, tm := deployment(t, cs, "bz-"+flinkID), deployment(t, cs, "bz-"+flinkID+"-tm")
	if *jm.Spec.Replicas != 0 || jm.Spec.Template.Annotations["app.bezisync.io/config-hash"] != "h1" || *tm.Spec.Replicas != 2 {
		t.Fatal("the change waits for the JobManager to save the position")
	}
	deletePod(t, cs, "jm-2")
	if err := d.Apply(ctx, w); err != nil {
		t.Fatal(err)
	}
	jm, tm = deployment(t, cs, "bz-"+flinkID), deployment(t, cs, "bz-"+flinkID+"-tm")
	if *jm.Spec.Replicas != 1 || jm.Spec.Template.Annotations["app.bezisync.io/config-hash"] != "h2" || *tm.Spec.Replicas != 4 {
		t.Fatal(jm.Spec.Replicas, tm.Spec.Replicas)
	}
}

// Delete waits for the JobManager's pod to go, then removes every object.
func TestKubernetesFlinkDelete(t *testing.T) {
	defer runtime.SetFlinkPoll(10 * time.Millisecond)()
	cs, d := k8sFlink(t)
	ctx := context.Background()
	if err := d.Apply(ctx, flinkWorkload(1, "h1")); err != nil {
		t.Fatal(err)
	}
	addPod(t, cs, "jm-1", "jobmanager", true)
	done := make(chan error, 1)
	go func() { done <- d.Delete(ctx, flinkID) }()
	select {
	case err := <-done:
		t.Fatal("deleted before the JobManager stopped", err)
	case <-time.After(100 * time.Millisecond):
	}
	if *deployment(t, cs, "bz-"+flinkID).Spec.Replicas != 0 {
		t.Fatal("the JobManager was not stopped first")
	}
	deletePod(t, cs, "jm-1")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	deps, _ := cs.AppsV1().Deployments("cdc").List(ctx, metav1.ListOptions{})
	svcs, _ := cs.CoreV1().Services("cdc").List(ctx, metav1.ListOptions{})
	secrets, _ := cs.CoreV1().Secrets("cdc").List(ctx, metav1.ListOptions{})
	cms, _ := cs.CoreV1().ConfigMaps("cdc").List(ctx, metav1.ListOptions{})
	if len(deps.Items)+len(svcs.Items)+len(secrets.Items)+len(cms.Items) != 0 {
		t.Fatal(deps.Items, svcs.Items, secrets.Items, cms.Items)
	}
}

// A helper image that can't be pulled fails the pipeline instead of leaving it pending.
func TestKubernetesFlinkInitFailure(t *testing.T) {
	cs, d := k8sFlink(t)
	ctx := context.Background()
	if err := d.Apply(ctx, flinkWorkload(1, "h1")); err != nil {
		t.Fatal(err)
	}
	_, _ = cs.CoreV1().Pods("cdc").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "jm-1", Labels: map[string]string{runtime.Label: flinkID, runtime.RoleLabel: "jobmanager"}},
		Status: corev1.PodStatus{Phase: corev1.PodPending, InitContainerStatuses: []corev1.ContainerStatus{{Name: "install-helper",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "not found"}}}}},
	}, metav1.CreateOptions{})
	if obs, _ := d.Observe(ctx, flinkID); obs.Phase != runtime.PhaseFailed || obs.Reason != "install-helper: ImagePullBackOff: not found" {
		t.Fatalf("%+v", obs)
	}
}

// A namespace without the right to create Services still runs Debezium; the test says Flink CDC
// pipelines can't run there.
func TestKubernetesTestReportsFlinkServices(t *testing.T) {
	for _, allowServices := range []bool{true, false} {
		cs, d := k8sFlink(t)
		cs.PrependReactor("create", "selfsubjectaccessreviews", func(a k8stesting.Action) (bool, k8sruntime.Object, error) {
			r := a.(k8stesting.CreateAction).GetObject().(*authv1.SelfSubjectAccessReview)
			r.Status.Allowed = r.Spec.ResourceAttributes.Resource != "services" || allowServices
			return true, r, nil
		})
		res := d.Test(context.Background())
		if !res.OK || res.Details["flink_cdc"] != allowServices {
			t.Fatalf("services allowed %v: %+v", allowServices, res)
		}
	}
}
