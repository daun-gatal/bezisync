package runtime

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	authv1 "k8s.io/api/authorization/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Kubernetes runtime: one Deployment (+ ConfigMap + Secret) per pipeline in a namespace.
// strategy Recreate means two connector instances never share a replication slot. Health
// and metrics are read through the API server's pod proxy.

const propsKey = "application.properties"

// TunnelProbeCommand is how the SSH sidecar's startup probe checks its local port.
var TunnelProbeCommand = func(port int) []string {
	return []string{"bezisync", "tunnel-probe", "--port", strconv.Itoa(port)}
}

// KubernetesDriver drives one namespace.
type KubernetesDriver struct {
	namespace string
	cs        kubernetes.Interface
}

// NewKubernetes builds a driver from in-cluster config, a kubeconfig, or a REST config.
func NewKubernetes(namespace string, inCluster bool, kubeconfig string, rc *rest.Config) (*KubernetesDriver, error) {
	var err error
	switch {
	case rc != nil:
	case kubeconfig != "":
		rc, err = clientcmd.RESTConfigFromKubeConfig([]byte(kubeconfig))
	case inCluster:
		rc, err = rest.InClusterConfig()
	default:
		rc, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(), nil).ClientConfig()
	}
	if err != nil {
		return nil, Errorf("Cannot initialise the kubernetes runtime: %v", err)
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, Errorf("Cannot initialise the kubernetes runtime: %v", err)
	}
	return &KubernetesDriver{namespace: namespace, cs: cs}, nil
}

// NewKubernetesFromClient wraps an existing clientset (tests).
func NewKubernetesFromClient(namespace string, cs kubernetes.Interface) *KubernetesDriver {
	return &KubernetesDriver{namespace: namespace, cs: cs}
}

func (k *KubernetesDriver) labels(w Workload) map[string]string {
	l := map[string]string{Label: w.ShortID, ManagedBy: "bezisync", "app.kubernetes.io/name": "debezium-server"}
	if w.Owner != "" {
		l[Owner] = w.Owner
	}
	return l
}

// reason is the HTTP reason phrase of an API error (ApiException.reason in Python).
func reason(err error) string {
	var st k8serrors.APIStatus
	if errors.As(err, &st) {
		return http.StatusText(int(st.Status().Code))
	}
	return err.Error()
}

func statusCode(err error) int {
	var st k8serrors.APIStatus
	if errors.As(err, &st) {
		return int(st.Status().Code)
	}
	return 0
}

func (k *KubernetesDriver) upsertSecret(ctx context.Context, s *corev1.Secret) error {
	api := k.cs.CoreV1().Secrets(k.namespace)
	if _, err := api.Get(ctx, s.Name, metav1.GetOptions{}); err != nil {
		if !k8serrors.IsNotFound(err) {
			return Errorf("Kubernetes refused to read secret %s: %s", s.Name, reason(err))
		}
		_, err = api.Create(ctx, s, metav1.CreateOptions{})
		return err
	}
	_, err := api.Update(ctx, s, metav1.UpdateOptions{})
	return err
}

func (k *KubernetesDriver) upsertConfigMap(ctx context.Context, c *corev1.ConfigMap) error {
	api := k.cs.CoreV1().ConfigMaps(k.namespace)
	if _, err := api.Get(ctx, c.Name, metav1.GetOptions{}); err != nil {
		if !k8serrors.IsNotFound(err) {
			return Errorf("Kubernetes refused to read config_map %s: %s", c.Name, reason(err))
		}
		_, err = api.Create(ctx, c, metav1.CreateOptions{})
		return err
	}
	_, err := api.Update(ctx, c, metav1.UpdateOptions{})
	return err
}

func (k *KubernetesDriver) upsertDeployment(ctx context.Context, d *appsv1.Deployment) error {
	api := k.cs.AppsV1().Deployments(k.namespace)
	if _, err := api.Get(ctx, d.Name, metav1.GetOptions{}); err != nil {
		if !k8serrors.IsNotFound(err) {
			return Errorf("Kubernetes refused to read deployment %s: %s", d.Name, reason(err))
		}
		_, err = api.Create(ctx, d, metav1.CreateOptions{})
		return err
	}
	_, err := api.Update(ctx, d, metav1.UpdateOptions{})
	return err
}

func ptr[T any](v T) *T { return &v }

// Apply makes the namespace match the workload.
func (k *KubernetesDriver) Apply(ctx context.Context, w Workload) error {
	w.Normalize()
	name := w.ResourceName()
	labels := k.labels(w)
	meta := metav1.ObjectMeta{Name: name, Labels: labels}
	wrap := func(err error) error {
		if err == nil {
			return nil
		}
		var re *Error
		if errors.As(err, &re) {
			return err
		}
		return Errorf("Kubernetes rejected the workload: %s (%d)", reason(err), statusCode(err))
	}
	if w.IsFlink() {
		return k.applyFlink(ctx, w, wrap)
	}
	env := map[string][]byte{}
	for key, v := range JVMEnv(w) {
		env[key] = []byte(v)
	}
	if err := k.upsertSecret(ctx, &corev1.Secret{ObjectMeta: meta, Type: corev1.SecretTypeOpaque, Data: env}); err != nil {
		return wrap(err)
	}
	if err := k.upsertConfigMap(ctx, &corev1.ConfigMap{ObjectMeta: meta, Data: map[string]string{propsKey: w.Properties}}); err != nil {
		return wrap(err)
	}
	// Health on its own port (the Iceberg build's management interface) is probed there.
	healthPort := intstr.FromString("http")
	if w.Health() != 8080 {
		healthPort = intstr.FromString("health")
	}
	c := corev1.Container{
		Name: "debezium", Image: w.Image, ImagePullPolicy: corev1.PullIfNotPresent,
		EnvFrom: []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: name}}}},
		Ports:   []corev1.ContainerPort{{Name: "http", ContainerPort: 8080}, {Name: "metrics", ContainerPort: 9404}},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "files", MountPath: "/debezium/secrets", ReadOnly: true},
			{Name: "config", MountPath: "/debezium/config/application.properties", SubPath: propsKey, ReadOnly: true},
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(w.CPU), corev1.ResourceMemory: resource.MustParse(w.Memory)},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(w.Memory)},
		},
		ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/q/health/ready", Port: healthPort}},
			PeriodSeconds: 10, FailureThreshold: 3},
		LivenessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/q/health/live", Port: healthPort}},
			InitialDelaySeconds: 90, PeriodSeconds: 20, FailureThreshold: 6},
		SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: ptr(false), RunAsNonRoot: ptr(true), RunAsUser: ptr(int64(185)),
			Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
	}
	if w.Health() != 8080 {
		c.Ports = append(c.Ports, corev1.ContainerPort{Name: "health", ContainerPort: int32(w.Health())})
	}
	var sidecars []corev1.Container
	if len(w.Tunnel) > 0 {
		sidecars = []corev1.Container{tunnelSidecar(w, name)}
	}
	var pullSecrets []corev1.LocalObjectReference
	for _, n := range w.ImagePullSecrets {
		pullSecrets = append(pullSecrets, corev1.LocalObjectReference{Name: n})
	}
	replicas := int32(0)
	if w.Running {
		replicas = 1
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Annotations: map[string]string{"app.bezisync.io/pipeline-name": w.Name}},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{Label: w.ShortID}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{"app.bezisync.io/config-hash": w.ConfigHash}},
				Spec: corev1.PodSpec{
					InitContainers:     sidecars,
					Containers:         []corev1.Container{c},
					ImagePullSecrets:   pullSecrets,
					ServiceAccountName: w.ServiceAccount,
					Volumes: []corev1.Volume{
						{Name: "files", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: name + "-files"}}},
						{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: name}}}},
					},
					SecurityContext:               &corev1.PodSecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
					TerminationGracePeriodSeconds: ptr(int64(30)),
					EnableServiceLinks:            ptr(false),
				},
			},
		},
	}
	files := map[string][]byte{}
	for key, v := range w.Files {
		files[key] = []byte(v)
	}
	if err := k.upsertSecret(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-files", Labels: labels}, Type: corev1.SecretTypeOpaque, Data: files}); err != nil {
		return wrap(err)
	}
	return wrap(k.upsertDeployment(ctx, dep))
}

// tunnelSidecar is the native sidecar (restartPolicy Always) holding the SSH tunnel. It
// listens on 127.0.0.1 only; the connector starts once the startup probe reaches it.
func tunnelSidecar(w Workload, secretName string) corev1.Container {
	t := w.Tunnel
	listen := toInt(t["listen_port"])
	hostKey := ""
	if v, ok := t["ssh_host_key"]; ok && v != nil {
		hostKey = fmt.Sprint(v)
	}
	always := corev1.ContainerRestartPolicyAlways
	return corev1.Container{
		Name: "ssh-tunnel", Image: fmt.Sprint(t["image"]), ImagePullPolicy: corev1.PullIfNotPresent, RestartPolicy: &always,
		Args: []string{"tunnel", "--target", fmt.Sprintf("%v:%v", t["host"], t["port"]),
			"--via", fmt.Sprintf("%v@%v:%v", t["ssh_user"], t["ssh_host"], t["ssh_port"]), "--listen-port", strconv.Itoa(listen)},
		Env: []corev1.EnvVar{
			{Name: "BZ_SSH_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secretName}, Key: "BZ_SSH_KEY"}}},
			{Name: "BZ_SSH_HOST_KEY", Value: hostKey},
		},
		StartupProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: TunnelProbeCommand(listen)}},
			PeriodSeconds: 2, FailureThreshold: 60},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("20m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
		},
		SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: ptr(false), RunAsNonRoot: ptr(true), RunAsUser: ptr(int64(10001)),
			Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
	}
}

func toInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case fmt.Stringer:
		n, _ := strconv.Atoi(x.String())
		return n
	case string:
		n, _ := strconv.Atoi(x)
		return n
	}
	return 0
}

// Delete removes the Deployment, ConfigMap and Secrets.
func (k *KubernetesDriver) Delete(ctx context.Context, shortID string) error {
	name := "bz-" + shortID
	if dep, err := k.cs.AppsV1().Deployments(k.namespace).Get(ctx, name, metav1.GetOptions{}); err == nil && dep.Labels[RoleLabel] == RoleJobManager {
		return k.deleteFlink(ctx, shortID, dep)
	} else if err != nil && !k8serrors.IsNotFound(err) {
		return Errorf("Kubernetes refused to read deployment %s: %s", name, reason(err))
	}
	fg := metav1.DeletePropagationForeground
	steps := []struct {
		obj string
		fn  func() error
	}{
		{name, func() error {
			return k.cs.AppsV1().Deployments(k.namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &fg})
		}},
		{name, func() error { return k.cs.CoreV1().ConfigMaps(k.namespace).Delete(ctx, name, metav1.DeleteOptions{}) }},
		{name, func() error { return k.cs.CoreV1().Secrets(k.namespace).Delete(ctx, name, metav1.DeleteOptions{}) }},
		{name + "-files", func() error {
			return k.cs.CoreV1().Secrets(k.namespace).Delete(ctx, name+"-files", metav1.DeleteOptions{})
		}},
	}
	for _, s := range steps {
		if err := s.fn(); err != nil && !k8serrors.IsNotFound(err) {
			return Errorf("Kubernetes refused to delete %s: %s", s.obj, reason(err))
		}
	}
	return nil
}

func (k *KubernetesDriver) pod(ctx context.Context, shortID string) (*corev1.Pod, error) {
	pods, err := k.cs.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: Label + "=" + shortID})
	if err != nil {
		return nil, err
	}
	var live []corev1.Pod
	for _, p := range pods.Items {
		if p.DeletionTimestamp == nil && p.Labels[RoleLabel] != RoleTaskManager { // a Flink CDC pipeline's pod is its JobManager's
			live = append(live, p)
		}
	}
	if len(live) == 0 {
		return nil, nil
	}
	sort.SliceStable(live, func(i, j int) bool { return live[i].CreationTimestamp.Before(&live[j].CreationTimestamp) })
	return &live[len(live)-1], nil
}

// Observe reports the Deployment and its newest pod.
func (k *KubernetesDriver) Observe(ctx context.Context, shortID string) (Observation, error) {
	dep, err := k.cs.AppsV1().Deployments(k.namespace).Get(ctx, "bz-"+shortID, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return Observation{Phase: PhaseMissing}, nil
		}
		return Observation{}, Errorf("Kubernetes error: %s", reason(err))
	}
	var hash *string
	if h, ok := dep.Spec.Template.Annotations["app.bezisync.io/config-hash"]; ok {
		hash = &h
	}
	if dep.Labels[RoleLabel] == RoleJobManager {
		return k.observeFlink(ctx, shortID, dep, hash)
	}
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas == 0 {
		return Observation{Phase: PhaseStopped, ConfigHash: hash}, nil
	}
	pod, err := k.pod(ctx, shortID)
	if err != nil {
		return Observation{}, err
	}
	if pod == nil {
		return Observation{Phase: PhasePending, ConfigHash: hash, Reason: "Waiting for the pod to be created."}, nil
	}
	return podObservation(pod, hash), nil
}

// podObservation reads a pod's first container.
func podObservation(pod *corev1.Pod, hash *string) Observation {
	var cs *corev1.ContainerStatus
	if len(pod.Status.ContainerStatuses) > 0 {
		cs = &pod.Status.ContainerStatuses[0]
	}
	restarts := 0
	ready := false
	if cs != nil {
		restarts = int(cs.RestartCount)
		ready = cs.Ready
	}
	switch {
	case cs != nil && cs.State.Running != nil:
		started := cs.State.Running.StartedAt.UTC()
		return Observation{Phase: PhaseRunning, Ready: ready, Restarts: restarts, StartedAt: &started, ConfigHash: hash,
			Details: map[string]any{"pod": pod.Name, "node": pod.Spec.NodeName}}
	case cs != nil && cs.State.Waiting != nil:
		r := cs.State.Waiting.Reason
		if r == "" {
			r = "Waiting"
		}
		phase := PhasePending
		if r == "CrashLoopBackOff" || restarts != 0 {
			phase = PhaseRestarting
		}
		switch r {
		// ErrImagePull is not among them: it is one failed attempt, often a registry hiccup, and
		// the kubelet retries; only after repeated failures does it report ImagePullBackOff.
		case "ImagePullBackOff", "InvalidImageName", "CreateContainerConfigError":
			phase = PhaseFailed
		}
		var exit *int
		if cs.LastTerminationState.Terminated != nil {
			e := int(cs.LastTerminationState.Terminated.ExitCode)
			exit = &e
		}
		return Observation{Phase: phase, Restarts: restarts, ExitCode: exit,
			Reason: strings.Trim(r+": "+cs.State.Waiting.Message, ": "), ConfigHash: hash}
	case cs != nil && cs.State.Terminated != nil:
		e := int(cs.State.Terminated.ExitCode)
		r := cs.State.Terminated.Reason
		if r == "" {
			r = "Terminated"
		}
		return Observation{Phase: PhaseRestarting, Restarts: restarts, ExitCode: &e, Reason: r, ConfigHash: hash}
	}
	if pod.Status.Phase == corev1.PodPending {
		var msgs []string
		for _, c := range pod.Status.Conditions {
			if c.Message != "" {
				msgs = append(msgs, c.Message)
			}
		}
		r := strings.Join(msgs, "; ")
		if r == "" {
			r = "Scheduling"
		}
		return Observation{Phase: PhasePending, Reason: r, ConfigHash: hash}
	}
	return Observation{Phase: PhasePending, Reason: string(pod.Status.Phase), ConfigHash: hash}
}

// Logs returns the newest pod's log tail.
func (k *KubernetesDriver) Logs(ctx context.Context, shortID string, tail int, previous bool) (string, error) {
	pod, err := k.pod(ctx, shortID)
	if err != nil || pod == nil {
		return "", err
	}
	lines := int64(tail)
	raw, err := k.cs.CoreV1().Pods(k.namespace).GetLogs(pod.Name, &corev1.PodLogOptions{TailLines: &lines, Previous: previous}).DoRaw(ctx)
	if err != nil {
		return "", nil //nolint:nilerr // like Python: logs that cannot be read are simply empty
	}
	return decodeReplace(raw), nil
}

// HTTPGet reads a URL of the connector through the API server's pod proxy.
func (k *KubernetesDriver) HTTPGet(ctx context.Context, shortID string, port int, path string, timeout time.Duration) (int, string, error) {
	pod, err := k.pod(ctx, shortID)
	if err != nil {
		return 0, "", Errorf("Cannot reach the connector: %v", err)
	}
	if pod == nil || pod.Status.Phase != corev1.PodRunning {
		return 0, "", &Error{"No running pod."}
	}
	res := k.cs.CoreV1().RESTClient().Get().Namespace(k.namespace).Resource("pods").
		Name(fmt.Sprintf("%s:%d", pod.Name, port)).SubResource("proxy").Suffix(strings.TrimLeft(path, "/")).
		Timeout(timeout).Do(ctx)
	var code int
	res.StatusCode(&code)
	body, err := res.Raw()
	if err != nil && code == 0 {
		return 0, "", Errorf("Cannot reach the connector: %v", err)
	}
	return code, decodeReplace(body), nil
}

// Test checks the permissions connectors need in the namespace.
func (k *KubernetesDriver) Test(ctx context.Context) TestResult {
	needed := [][3]string{
		{"apps", "deployments", "create"}, {"", "configmaps", "create"}, {"", "secrets", "create"},
		{"", "pods", "list"}, {"", "pods/log", "get"}, {"", "pods/proxy", "get"},
	}
	var missing []string
	for _, n := range needed {
		res, sub, _ := strings.Cut(n[1], "/")
		review := &authv1.SelfSubjectAccessReview{Spec: authv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authv1.ResourceAttributes{Namespace: k.namespace, Group: n[0], Resource: res, Subresource: sub, Verb: n[2]},
		}}
		out, err := k.cs.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{})
		if err != nil {
			if statusCode(err) != 0 {
				return TestResult{OK: false, Message: "Cannot reach the Kubernetes API: " + reason(err)}
			}
			return TestResult{OK: false, Message: fmt.Sprintf("Cannot reach the Kubernetes API: %v", err)}
		}
		if !out.Status.Allowed {
			missing = append(missing, n[2]+" "+n[1])
		}
	}
	if len(missing) > 0 {
		l := make([]any, len(missing))
		for i, m := range missing {
			l[i] = m
		}
		return TestResult{OK: false, Message: fmt.Sprintf("Missing permissions in namespace %s: %s.", k.namespace, strings.Join(missing, ", ")),
			Details: map[string]any{"missing": l}}
	}
	capacity := k.Capacity(ctx)
	if capacity == nil {
		capacity = map[string]any{}
	}
	// Flink CDC pipelines also need a Service for their JobManager; without it only they can't run.
	review := &authv1.SelfSubjectAccessReview{Spec: authv1.SelfSubjectAccessReviewSpec{
		ResourceAttributes: &authv1.ResourceAttributes{Namespace: k.namespace, Resource: "services", Verb: "create"}}}
	if out, err := k.cs.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, review, metav1.CreateOptions{}); err == nil {
		capacity["flink_cdc"] = out.Status.Allowed
	}
	return TestResult{OK: true, Message: fmt.Sprintf("Can run connectors in namespace %s.", k.namespace), Details: capacity}
}

// ListWorkloads lists BeziSync Deployments (optionally of one owner).
func (k *KubernetesDriver) ListWorkloads(ctx context.Context, owner string) ([]string, error) {
	selector := ManagedBy + "=bezisync"
	if owner != "" {
		selector += "," + Owner + "=" + owner
	}
	deps, err := k.cs.AppsV1().Deployments(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, err
	}
	out := []string{}
	seen := map[string]bool{}
	for _, d := range deps.Items {
		if id, ok := d.Labels[Label]; ok && !seen[id] { // a Flink CDC pipeline has two Deployments
			seen[id] = true
			out = append(out, id)
		}
	}
	return out, nil
}

// QuantityCPU parses a CPU quantity like Python's _quantity_cpu.
func QuantityCPU(v string) float64 {
	if strings.HasSuffix(v, "m") {
		f, _ := strconv.ParseFloat(strings.TrimSuffix(v, "m"), 64)
		return f / 1000
	}
	f, _ := strconv.ParseFloat(v, 64)
	return f
}

// QuantityMem parses a memory quantity like Python's _quantity_mem.
func QuantityMem(v string) int64 {
	for _, u := range []struct {
		suffix string
		factor float64
	}{{"Ki", 1024}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40}, {"K", 1000}, {"M", 1e6}, {"G", 1e9}} {
		if strings.HasSuffix(v, u.suffix) {
			f, _ := strconv.ParseFloat(strings.TrimSuffix(v, u.suffix), 64)
			return int64(f * u.factor)
		}
	}
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

// Capacity reports node allocatable CPU/memory and what connector pods request.
func (k *KubernetesDriver) Capacity(ctx context.Context) map[string]any {
	nodes, err := k.cs.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil
	}
	var allocCPU, nodeCPU float64
	var allocMem, nodeMem int64
	var archs []string
	for _, n := range nodes.Items {
		archs = append(archs, n.Status.NodeInfo.Architecture)
		cpu := n.Status.Allocatable[corev1.ResourceCPU]
		mem := n.Status.Allocatable[corev1.ResourceMemory]
		c, m := QuantityCPU(cpu.String()), QuantityMem(mem.String())
		allocCPU += c
		allocMem += m
		// The largest node a pod can still land on: what a Flink CDC TaskManager must fit.
		if !n.Spec.Unschedulable {
			nodeCPU, nodeMem = max(nodeCPU, c), max(nodeMem, m)
		}
	}
	pods, err := k.cs.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: ManagedBy + "=bezisync"})
	if err != nil {
		return nil
	}
	var reqCPU, reqMem float64
	for _, p := range pods.Items {
		for _, c := range p.Spec.Containers {
			if q, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
				reqCPU += QuantityCPU(q.String())
			}
			if q, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
				reqMem += float64(QuantityMem(q.String()))
			}
		}
	}
	return map[string]any{"cpu_allocatable": allocCPU, "memory_allocatable": allocMem, "cpu_requested": reqCPU,
		"memory_requested": reqMem, "workloads": len(pods.Items), "architectures": Architectures(archs...),
		"node_cpu_max": nodeCPU, "node_memory_max": nodeMem}
}

// --- managed-cluster bearer tokens (GKE, EKS) ---

// TokenSource produces a bearer token and its expiry.
type TokenSource func(ctx context.Context) (string, time.Time, error)

const refreshMargin = 120 * time.Second

type refreshingToken struct {
	mu      sync.Mutex
	source  TokenSource
	token   string
	expires time.Time
	base    http.RoundTripper
}

func (r *refreshingToken) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	if time.Now().After(r.expires.Add(-refreshMargin)) {
		tok, exp, err := r.source(req.Context())
		if err != nil {
			r.mu.Unlock()
			return nil, err
		}
		r.token, r.expires = tok, exp
	}
	tok := r.token
	r.mu.Unlock()
	clone := req.Clone(req.Context())
	clone.Header.Set("Authorization", "Bearer "+tok)
	return r.base.RoundTrip(clone)
}

// TokenRESTConfig is a REST config authenticating with a refreshed bearer token.
func TokenRESTConfig(ctx context.Context, endpoint, caPEM string, source TokenSource) (*rest.Config, error) {
	ca := []byte(caPEM)
	if !strings.Contains(caPEM, "BEGIN CERTIFICATE") {
		dec, err := base64.StdEncoding.DecodeString(strings.TrimSpace(caPEM))
		if err != nil {
			return nil, Errorf("Cannot initialise the kubernetes runtime: %v", err)
		}
		ca = dec
	}
	rt := &refreshingToken{source: source}
	// Mint the first token now, like the Python client did, so errors surface at once.
	tok, exp, err := source(ctx)
	if err != nil {
		return nil, err
	}
	rt.token, rt.expires = tok, exp
	return &rest.Config{
		Host:            strings.TrimRight(endpoint, "/"),
		TLSClientConfig: rest.TLSClientConfig{CAData: ca},
		WrapTransport: func(base http.RoundTripper) http.RoundTripper {
			rt.base = base
			return rt
		},
	}, nil
}
