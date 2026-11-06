package runtime_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/daun-gatal/bezisync/backend/internal/runtime"
)

func node(name, cpu, mem string, unschedulable bool) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}, Spec: corev1.NodeSpec{Unschedulable: unschedulable},
		Status: corev1.NodeStatus{Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(mem)}}}
}

func quota(name string, hard, used corev1.ResourceList) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "cdc"}, Status: corev1.ResourceQuotaStatus{Hard: hard, Used: used}}
}

// The capacity a Kubernetes runtime reports: its largest schedulable node, and the headroom the
// namespace's tightest ResourceQuota leaves (none: quota false).
func TestKubernetesCapacity(t *testing.T) {
	ctx := context.Background()
	cs := fake.NewClientset(node("a", "4", "16Gi", false), node("b", "8", "32Gi", false), node("big-but-cordoned", "64", "256Gi", true))
	d := runtime.NewKubernetesFromClient("cdc", cs)
	c := d.Capacity(ctx)
	if c["node_cpu_max"] != 8.0 || c["node_memory_max"] != int64(32<<30) || c["cpu_allocatable"] != 76.0 || c["quota"] != false {
		t.Fatal(c)
	}
	if _, ok := c["quota_cpu_free"]; ok {
		t.Fatal(c)
	}
	q := func(cpu, mem string) corev1.ResourceList {
		return corev1.ResourceList{corev1.ResourceRequestsCPU: resource.MustParse(cpu), corev1.ResourceRequestsMemory: resource.MustParse(mem)}
	}
	_, _ = cs.CoreV1().ResourceQuotas("cdc").Create(ctx, quota("team", q("10", "20Gi"), q("7500m", "4Gi")), metav1.CreateOptions{})
	// A second quota on limits.memory is tighter for memory.
	_, _ = cs.CoreV1().ResourceQuotas("cdc").Create(ctx, quota("limits", corev1.ResourceList{corev1.ResourceLimitsMemory: resource.MustParse("8Gi")},
		corev1.ResourceList{corev1.ResourceLimitsMemory: resource.MustParse("6Gi")}), metav1.CreateOptions{})
	c = d.Capacity(ctx)
	if c["quota"] != true || c["quota_cpu_free"] != 2.5 || c["quota_memory_free"] != int64(2<<30) {
		t.Fatal(c)
	}
	// Used over hard (a quota lowered later) leaves nothing, never a negative.
	_, _ = cs.CoreV1().ResourceQuotas("cdc").Create(ctx, quota("lowered", q("1", "1Gi"), q("2", "2Gi")), metav1.CreateOptions{})
	if c = d.Capacity(ctx); c["quota_cpu_free"] != 0.0 || c["quota_memory_free"] != int64(0) {
		t.Fatal(c)
	}
}
