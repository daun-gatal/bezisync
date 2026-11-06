package runtime

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// A Flink CDC pipeline <id> on Kubernetes:
//
//	Secret     bz-<id>        the JobManager's environment (secrets); the tunnel reads BZ_SSH_KEY
//	Secret     bz-<id>-files  /opt/bezisync/secrets (the keystore both roles use for internal TLS)
//	ConfigMap  bz-<id>        /opt/bezisync/config
//	Deployment bz-<id>        the JobManager (one replica)
//	Deployment bz-<id>-tm     the TaskManagers
//	Service    bz-<id>-jm     the JobManager's RPC and blob ports, for the TaskManagers
//
// Every pod copies the helper from BeziSync's image into an emptyDir (init container), runs as the
// Flink user with a read-only root filesystem, and, behind a bastion, has its own tunnel sidecar
// reached as bz-tunnel (hostAliases). Stopping or changing the cluster scales the JobManager to
// zero first (it saves the position within its grace period) and touches the TaskManagers only once
// its pod is gone; every Apply works this out from the cluster, so no state is kept.

// Flink's ports on the JobManager's Service (the compiler sets them in flink-conf.yaml).
const (
	FlinkRPCPort  = 6123
	FlinkBlobPort = 6124
)

func flinkTMDeployment(id string) string { return "bz-" + id + "-tm" }
func flinkJMService(id string) string    { return "bz-" + id + "-jm" }

// FlinkJobManagerHost is the JobManager's address as the TaskManagers see it on each runtime.
func FlinkJobManagerHost(runtimeType, shortID string) string {
	if runtimeType == "kubernetes" {
		return flinkJMService(shortID)
	}
	return "bz-" + shortID
}

func (k *KubernetesDriver) flinkLabels(w Workload, role string) map[string]string {
	l := k.labels(w)
	l["app.kubernetes.io/name"] = "flink-cdc"
	if role != "" {
		l[RoleLabel] = role
	}
	return l
}

// flinkPods lists a pipeline's live pods of one role.
func (k *KubernetesDriver) flinkPods(ctx context.Context, id, role string, terminating bool) ([]corev1.Pod, error) {
	pods, err := k.cs.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: Label + "=" + id + "," + RoleLabel + "=" + role})
	if err != nil {
		return nil, err
	}
	var out []corev1.Pod
	for _, p := range pods.Items {
		if terminating || p.DeletionTimestamp == nil {
			out = append(out, p)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].CreationTimestamp.Equal(&out[j].CreationTimestamp) {
			return out[i].CreationTimestamp.Before(&out[j].CreationTimestamp)
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// jobManagerQuiet reports a JobManager scaled to zero whose pods are all gone.
func (k *KubernetesDriver) jobManagerQuiet(ctx context.Context, jm *appsv1.Deployment) (bool, error) {
	if jm == nil {
		return true, nil
	}
	if jm.Spec.Replicas != nil && *jm.Spec.Replicas > 0 {
		return false, nil
	}
	pods, err := k.flinkPods(ctx, jm.Labels[Label], RoleJobManager, true)
	return len(pods) == 0, err
}

// stopJobManager scales the JobManager to zero; its pod saves the position as it stops.
func (k *KubernetesDriver) stopJobManager(ctx context.Context, jm *appsv1.Deployment) error {
	if jm.Spec.Replicas != nil && *jm.Spec.Replicas == 0 {
		return nil
	}
	jm.Spec.Replicas = ptr(int32(0))
	_, err := k.cs.AppsV1().Deployments(k.namespace).Update(ctx, jm, metav1.UpdateOptions{})
	return err
}

// applyFlink converges a Flink CDC pipeline's objects.
func (k *KubernetesDriver) applyFlink(ctx context.Context, w Workload, wrap func(error) error) error {
	id := w.ShortID
	jm, err := k.cs.AppsV1().Deployments(k.namespace).Get(ctx, w.ResourceName(), metav1.GetOptions{})
	if k8serrors.IsNotFound(err) {
		jm, err = nil, nil
	}
	if err != nil {
		return wrap(err)
	}
	if jm != nil && (!w.Running || jm.Spec.Template.Annotations["app.bezisync.io/config-hash"] != w.ConfigHash) {
		// Stopping, or a cluster to replace: the JobManager goes first and alone.
		if err := k.stopJobManager(ctx, jm); err != nil {
			return wrap(err)
		}
		quiet, err := k.jobManagerQuiet(ctx, jm)
		if err != nil || !quiet {
			return wrap(err)
		}
	}
	name := w.ResourceName()
	env := map[string][]byte{}
	for key, v := range w.Env {
		env[key] = []byte(v)
	}
	if err := k.upsertSecret(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: k.flinkLabels(w, "")}, Type: corev1.SecretTypeOpaque,
		Data: env}); err != nil {
		return wrap(err)
	}
	files := map[string][]byte{}
	for key, v := range w.Files {
		files[key] = []byte(v)
	}
	if err := k.upsertSecret(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-files", Labels: k.flinkLabels(w, "")},
		Type: corev1.SecretTypeOpaque, Data: files}); err != nil {
		return wrap(err)
	}
	if err := k.upsertConfigMap(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: k.flinkLabels(w, "")},
		Data: w.Flink.Config}); err != nil {
		return wrap(err)
	}
	if err := k.upsertService(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: flinkJMService(id), Labels: k.flinkLabels(w, "")},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{Label: id, RoleLabel: RoleJobManager},
			Ports: []corev1.ServicePort{{Name: "rpc", Port: FlinkRPCPort, TargetPort: intstr.FromInt32(FlinkRPCPort)},
				{Name: "blob", Port: FlinkBlobPort, TargetPort: intstr.FromInt32(FlinkBlobPort)}},
			// TaskManagers register before the JobManager is ready (it is ready once its job runs).
			PublishNotReadyAddresses: true,
		}}); err != nil {
		return wrap(err)
	}
	jmReplicas, tmReplicas := int32(0), int32(0)
	if w.Running {
		jmReplicas, tmReplicas = 1, int32(w.Flink.TaskManagers)
	}
	// The TaskManagers first: on a stop the JobManager is already gone, on a start they wait for it.
	if err := k.upsertDeployment(ctx, k.flinkDeployment(w, RoleTaskManager, tmReplicas)); err != nil {
		return wrap(err)
	}
	return wrap(k.upsertDeployment(ctx, k.flinkDeployment(w, RoleJobManager, jmReplicas)))
}

func (k *KubernetesDriver) upsertService(ctx context.Context, s *corev1.Service) error {
	api := k.cs.CoreV1().Services(k.namespace)
	old, err := api.Get(ctx, s.Name, metav1.GetOptions{})
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return Errorf("Kubernetes refused to read service %s: %s", s.Name, reason(err))
		}
		_, err = api.Create(ctx, s, metav1.CreateOptions{})
		return err
	}
	s.ResourceVersion = old.ResourceVersion
	s.Spec.ClusterIP, s.Spec.ClusterIPs = old.Spec.ClusterIP, old.Spec.ClusterIPs // immutable once set
	_, err = api.Update(ctx, s, metav1.UpdateOptions{})
	return err
}

// flinkDeployment is the JobManager's or the TaskManagers' Deployment.
func (k *KubernetesDriver) flinkDeployment(w Workload, role string, replicas int32) *appsv1.Deployment {
	name, cpu, memory := w.ResourceName(), w.Flink.JobManagerCPU, w.Flink.JobManagerMemory
	if role == RoleTaskManager {
		name, cpu, memory = flinkTMDeployment(w.ShortID), w.Flink.TaskManagerCPU, w.Flink.TaskManagerMemory
	}
	labels := k.flinkLabels(w, role)
	mounts := []corev1.VolumeMount{
		{Name: "opt", MountPath: FlinkOptDir},
		{Name: "config", MountPath: FlinkConfigDir, ReadOnly: true},
		{Name: "secrets", MountPath: FlinkSecretsDir, ReadOnly: true},
		{Name: "state", MountPath: FlinkStateDir},
		{Name: "tmp", MountPath: "/tmp"},
	}
	locked := func(user int64) *corev1.SecurityContext {
		return &corev1.SecurityContext{AllowPrivilegeEscalation: ptr(false), RunAsNonRoot: ptr(true), RunAsUser: ptr(user), RunAsGroup: ptr(user),
			ReadOnlyRootFilesystem: ptr(true), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}}
	}
	c := corev1.Container{
		Name: role, Image: w.Image, ImagePullPolicy: corev1.PullIfNotPresent,
		Command: []string{FlinkOptDir + "/bezisync"}, Args: []string{"engine", "flink-cdc", role},
		VolumeMounts: mounts,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(memory)},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(memory)},
		},
		SecurityContext: locked(FlinkUID),
	}
	if role == RoleJobManager {
		c.EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: name}}}}
		c.Ports = []corev1.ContainerPort{{Name: "rpc", ContainerPort: FlinkRPCPort}, {Name: "blob", ContainerPort: FlinkBlobPort},
			{Name: "http", ContainerPort: int32(w.Health())}}
		c.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/q/health/ready", Port: intstr.FromString("http")}},
			PeriodSeconds: 10, FailureThreshold: 3}
		c.LivenessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/q/health/live", Port: intstr.FromString("http")}},
			InitialDelaySeconds: 90, PeriodSeconds: 20, FailureThreshold: 6}
	}
	install := corev1.Container{
		Name: "install-helper", Image: w.Flink.HelperImage, ImagePullPolicy: corev1.PullIfNotPresent, Args: []string{"install-self", FlinkOptDir},
		VolumeMounts: []corev1.VolumeMount{{Name: "opt", MountPath: FlinkOptDir}},
		Resources: corev1.ResourceRequirements{
			// Writing the binary fills the page cache, which counts against the limit.
			Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("20m"), corev1.ResourceMemory: resource.MustParse("64Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
		},
		SecurityContext: locked(10001),
	}
	inits := []corev1.Container{install}
	var aliases []corev1.HostAlias
	if len(w.Tunnel) > 0 {
		inits = append(inits, tunnelSidecar(w, w.ResourceName()))
		aliases = []corev1.HostAlias{{IP: "127.0.0.1", Hostnames: []string{"bz-tunnel"}}}
	}
	var pullSecrets []corev1.LocalObjectReference
	for _, n := range w.ImagePullSecrets {
		pullSecrets = append(pullSecrets, corev1.LocalObjectReference{Name: n})
	}
	stateSize := w.Flink.StateSize
	if stateSize == "" {
		stateSize = "1Gi"
	}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels, Annotations: map[string]string{"app.bezisync.io/pipeline-name": w.Name}},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Strategy: appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType},
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{Label: w.ShortID, RoleLabel: role}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{"app.bezisync.io/config-hash": w.ConfigHash}},
				Spec: corev1.PodSpec{
					InitContainers:     inits,
					Containers:         []corev1.Container{c},
					HostAliases:        aliases,
					ImagePullSecrets:   pullSecrets,
					ServiceAccountName: w.ServiceAccount,
					Volumes: []corev1.Volume{
						{Name: "opt", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("512Mi"))}}},
						{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: w.ResourceName()}, DefaultMode: ptr(int32(0o440))}}},
						{Name: "secrets", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: w.ResourceName() + "-files",
							DefaultMode: ptr(int32(0o440))}}},
						{Name: "state", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory,
							SizeLimit: ptr(resource.MustParse(stateSize))}}},
						{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr(resource.MustParse("2Gi"))}}},
					},
					SecurityContext: &corev1.PodSecurityContext{SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
						FSGroup: ptr(int64(FlinkUID))},
					TerminationGracePeriodSeconds: ptr(int64(w.StopGrace() + 10)),
					EnableServiceLinks:            ptr(false),
				},
			},
		},
	}
	applyPlacement(dep, w.Placement)
	return dep
}

// observeFlink reports the JobManager's pod, with every pod of the cluster in Details.
func (k *KubernetesDriver) observeFlink(ctx context.Context, id string, jm *appsv1.Deployment, hash *string) (Observation, error) {
	if jm.Spec.Replicas == nil || *jm.Spec.Replicas == 0 {
		pods, err := k.flinkPods(ctx, id, RoleJobManager, true)
		if err != nil {
			return Observation{}, err
		}
		if len(pods) > 0 {
			return Observation{Phase: PhasePending, Reason: "Saving the position and stopping.", ConfigHash: hash}, nil
		}
		return Observation{Phase: PhaseStopped, ConfigHash: hash}, nil
	}
	jmPods, err := k.flinkPods(ctx, id, RoleJobManager, false)
	if err != nil {
		return Observation{}, err
	}
	var obs Observation
	if len(jmPods) == 0 {
		obs = Observation{Phase: PhasePending, ConfigHash: hash, Reason: "Waiting for the pod to be created."}
	} else {
		obs = podObservation(&jmPods[len(jmPods)-1], hash)
		if obs.Phase == PhasePending {
			obs = initObservation(&jmPods[len(jmPods)-1], obs)
		}
	}
	tmPods, err := k.flinkPods(ctx, id, RoleTaskManager, false)
	if err != nil {
		return Observation{}, err
	}
	wanted := 0
	if tm, err := k.cs.AppsV1().Deployments(k.namespace).Get(ctx, flinkTMDeployment(id), metav1.GetOptions{}); err == nil && tm.Spec.Replicas != nil {
		wanted = int(*tm.Spec.Replicas)
	}
	var components []any
	readyTMs := 0
	for _, p := range append(jmPods, tmPods...) {
		ready, restarts := false, 0
		for _, cs := range p.Status.ContainerStatuses {
			ready, restarts = cs.Ready, int(cs.RestartCount)
		}
		if p.Labels[RoleLabel] == RoleTaskManager {
			obs.Restarts += restarts
			if ready {
				readyTMs++
			}
		}
		components = append(components, map[string]any{"name": p.Name, "role": p.Labels[RoleLabel], "state": strings.ToLower(string(p.Status.Phase)),
			"ready": ready, "restarts": restarts, "node": p.Spec.NodeName})
	}
	obs.Ready = obs.Ready && readyTMs >= wanted
	if obs.Details == nil {
		obs.Details = map[string]any{}
	}
	obs.Details["components"] = components
	obs.Details["taskmanagers_ready"] = readyTMs
	obs.Details["taskmanagers_wanted"] = wanted
	return obs, nil
}

// initObservation reports an init container (the helper copy, the tunnel) that keeps a pending pod
// from starting.
func initObservation(pod *corev1.Pod, obs Observation) Observation {
	for _, cs := range pod.Status.InitContainerStatuses {
		w := cs.State.Waiting
		if w == nil || w.Reason == "" || w.Reason == "PodInitializing" {
			continue
		}
		obs.Reason = strings.Trim(cs.Name+": "+w.Reason+": "+w.Message, ": ")
		switch w.Reason {
		case "ImagePullBackOff", "InvalidImageName", "CreateContainerConfigError":
			obs.Phase = PhaseFailed
		case "CrashLoopBackOff":
			obs.Phase = PhaseRestarting
		}
		return obs
	}
	return obs
}

// deleteFlink stops the JobManager first (waiting up to its grace for it to save the position),
// then removes every object.
func (k *KubernetesDriver) deleteFlink(ctx context.Context, id string, jm *appsv1.Deployment) error {
	if err := k.stopJobManager(ctx, jm); err != nil && !k8serrors.IsNotFound(err) {
		return Errorf("Kubernetes refused to stop %s: %s", jm.Name, reason(err))
	}
	grace := 30
	if g := jm.Spec.Template.Spec.TerminationGracePeriodSeconds; g != nil {
		grace = int(*g)
	}
	deadline := time.Now().Add(time.Duration(grace+30) * time.Second)
	for {
		quiet, err := k.jobManagerQuiet(ctx, jm)
		if err != nil {
			return err
		}
		if quiet || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(flinkPoll):
		}
	}
	fg := metav1.DeletePropagationForeground
	name := "bz-" + id
	steps := []struct {
		obj string
		fn  func() error
	}{
		{name, func() error {
			return k.cs.AppsV1().Deployments(k.namespace).Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &fg})
		}},
		{flinkTMDeployment(id), func() error {
			return k.cs.AppsV1().Deployments(k.namespace).Delete(ctx, flinkTMDeployment(id), metav1.DeleteOptions{PropagationPolicy: &fg})
		}},
		{flinkJMService(id), func() error {
			return k.cs.CoreV1().Services(k.namespace).Delete(ctx, flinkJMService(id), metav1.DeleteOptions{})
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

// flinkPoll is how often Delete checks that the JobManager is gone (a variable for tests).
var flinkPoll = 2 * time.Second

// LogsOf returns the last lines of one component: "" or "jobmanager", "taskmanager-<n>" (the n-th
// TaskManager pod by age) or "tunnel" (the JobManager pod's tunnel).
func (k *KubernetesDriver) LogsOf(ctx context.Context, shortID, component string, tail int, previous bool) (string, error) {
	if _, err := componentName(shortID, component); err != nil {
		return "", err
	}
	role, container, index := RoleJobManager, RoleJobManager, 0
	switch {
	case component == RoleTunnel:
		container = "ssh-tunnel"
	case strings.HasPrefix(component, RoleTaskManager+"-"):
		role, container = RoleTaskManager, RoleTaskManager
		_, _ = fmt.Sscanf(strings.TrimPrefix(component, RoleTaskManager+"-"), "%d", &index)
		index--
	}
	pods, err := k.flinkPods(ctx, shortID, role, false)
	if err != nil || index >= len(pods) {
		return "", err
	}
	lines := int64(tail)
	raw, err := k.cs.CoreV1().Pods(k.namespace).GetLogs(pods[index].Name, &corev1.PodLogOptions{Container: container, TailLines: &lines, Previous: previous}).DoRaw(ctx)
	if err != nil {
		return "", nil //nolint:nilerr // logs that cannot be read are simply empty, as for Logs
	}
	return decodeReplace(raw), nil
}
