package runtime

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// A Flink CDC pipeline <id> on Docker:
//
//	network bz-<id>-net          private to the pipeline (the runtime's network is joined too)
//	volume  bz-<id>-opt          /opt/bezisync: the helper (copied from the BeziSync image),
//	                             config/ and secrets/
//	volume  bz-<id>-state        /state: checkpoints, in memory (tmpfs), shared by the cluster
//	bz-<id>                      the JobManager (its helper serves health; HTTPGet and Logs use it)
//	bz-<id>-tm-1 … bz-<id>-tm-N  TaskManagers
//	bz-<id>-tunnel               the SSH tunnel, on the private network as bz-tunnel (optional)
//
// Every container has a read-only root filesystem, no capabilities and a private /tmp. Stopping
// stops the JobManager first (it saves the position, within the stop grace), then the rest; it
// runs in the background, and Observe reports it meanwhile.

func flinkNet(id string) string   { return "bz-" + id + "-net" }
func flinkOpt(id string) string   { return "bz-" + id + "-opt" }
func flinkState(id string) string { return "bz-" + id + "-state" }
func flinkTM(id string, n int) string {
	return "bz-" + id + "-tm-" + strconv.Itoa(n)
}

// flinkStop is an ordered stop running in the background.
type flinkStop struct {
	done chan struct{}
	err  error
}

func (s *flinkStop) finished() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// stopping reports a stop still in progress; a finished one is forgotten and its error returned
// once.
func (d *DockerDriver) stopping(id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.stops[id]
	if s == nil {
		return false, nil
	}
	if s.finished() {
		delete(d.stops, id)
		if s.err != nil {
			return false, Errorf("Stopping the Flink cluster failed: %s", explanation(s.err))
		}
		return false, nil
	}
	return true, nil
}

// stopInProgress reports a stop still running, leaving a finished one's error for Apply.
func (d *DockerDriver) stopInProgress(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.stops[id]
	return s != nil && !s.finished()
}

// flinkContainers lists a pipeline's containers by name.
func (d *DockerDriver) flinkContainers(ctx context.Context, id string) (map[string]container.Summary, error) {
	list, err := d.cli.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: client.Filters{}.Add("label", Label+"="+id)})
	if err != nil {
		return nil, err
	}
	out := map[string]container.Summary{}
	for _, c := range list.Items {
		if len(c.Names) == 0 {
			continue
		}
		name := strings.TrimPrefix(c.Names[0], "/")
		if name != "bz-"+id+"-install" { // the short-lived helper copy is not part of the cluster
			out[name] = c
		}
	}
	return out, nil
}

func active(state container.ContainerState) bool {
	return state == "running" || state == "restarting"
}

// applyFlink converges a Flink CDC pipeline's containers.
func (d *DockerDriver) applyFlink(ctx context.Context, w Workload) error {
	id := w.ShortID
	if busy, err := d.stopping(id); busy || err != nil {
		return err
	}
	have, err := d.flinkContainers(ctx, id)
	if err != nil {
		return d.apiErr(err)
	}
	anyActive := false
	for _, c := range have {
		anyActive = anyActive || active(c.State)
	}
	if !w.Running {
		if anyActive {
			d.startStop(ctx, id, w.StopGrace())
		}
		return nil
	}
	if !d.flinkCurrent(w, have) {
		images := []string{w.Image, w.Flink.HelperImage}
		if len(w.Tunnel) > 0 {
			images = append(images, fmt.Sprint(w.Tunnel["image"]))
		}
		for _, img := range images {
			ready, err := d.ensureImage(ctx, id, img)
			if err != nil {
				return d.apiErr(err)
			}
			if !ready {
				return nil // the current cluster keeps running while the images download
			}
		}
		if anyActive {
			// Save the position first; the next pass recreates the cluster.
			d.startStop(ctx, id, w.StopGrace())
			return nil
		}
		if err := d.recreateFlink(ctx, w, have); err != nil {
			return d.apiErr(err)
		}
		if have, err = d.flinkContainers(ctx, id); err != nil {
			return d.apiErr(err)
		}
	}
	// Start what is not running: the tunnel and the JobManager first, then the TaskManagers.
	order := []string{"bz-" + id + "-tunnel", w.ResourceName()}
	for n := 1; n <= w.Flink.TaskManagers; n++ {
		order = append(order, flinkTM(id, n))
	}
	for _, name := range order {
		c, ok := have[name]
		if !ok || active(c.State) {
			continue
		}
		if _, err := d.cli.ContainerStart(ctx, c.ID, client.ContainerStartOptions{}); err != nil {
			return d.apiErr(err)
		}
	}
	return nil
}

// flinkCurrent reports a cluster that matches the workload: every container on its config hash and
// exactly the TaskManagers it asks for.
func (d *DockerDriver) flinkCurrent(w Workload, have map[string]container.Summary) bool {
	want := map[string]bool{w.ResourceName(): true}
	for n := 1; n <= w.Flink.TaskManagers; n++ {
		want[flinkTM(w.ShortID, n)] = true
	}
	if len(w.Tunnel) > 0 {
		want["bz-"+w.ShortID+"-tunnel"] = true
	}
	if len(have) != len(want) {
		return false
	}
	for name, c := range have {
		if !want[name] || c.Labels["app.bezisync.io/config-hash"] != w.ConfigHash {
			return false
		}
	}
	return true
}

// startStop stops the cluster in order in the background: the JobManager within grace (it saves
// the position), then the TaskManagers and the tunnel.
func (d *DockerDriver) startStop(ctx context.Context, id string, grace int) {
	d.mu.Lock()
	if s := d.stops[id]; s != nil && !s.finished() {
		d.mu.Unlock()
		return
	}
	s := &flinkStop{done: make(chan struct{})}
	d.stops[id] = s
	d.mu.Unlock()
	bg := context.WithoutCancel(ctx)
	go func() {
		defer close(s.done)
		s.err = d.stopFlink(bg, id, grace)
	}()
}

// stopFlink stops a pipeline's cluster in order and waits for it.
func (d *DockerDriver) stopFlink(ctx context.Context, id string, grace int) error {
	have, err := d.flinkContainers(ctx, id)
	if err != nil {
		return err
	}
	stop := func(name string, seconds int) error {
		c, ok := have[name]
		if !ok || !active(c.State) {
			return nil
		}
		sctx, cancel := context.WithTimeout(ctx, time.Duration(seconds+30)*time.Second)
		defer cancel()
		// pullCli has no request timeout: a JobManager may take its whole grace to save.
		_, err := d.pullCli.ContainerStop(sctx, c.ID, client.ContainerStopOptions{Timeout: &seconds})
		if err != nil && !cerrdefs.IsNotFound(err) {
			return err
		}
		return nil
	}
	if err := stop("bz-"+id, grace); err != nil {
		return err
	}
	var errs []error
	for name, c := range have {
		if c.Labels[RoleLabel] == RoleTaskManager || c.Labels[RoleLabel] == RoleTunnel {
			errs = append(errs, stop(name, 10))
		}
	}
	return errors.Join(errs...)
}

// recreateFlink removes the pipeline's containers and volumes and creates them again for w.
func (d *DockerDriver) recreateFlink(ctx context.Context, w Workload, have map[string]container.Summary) error {
	id := w.ShortID
	for _, c := range have {
		if err := d.remove(ctx, c.ID); err != nil {
			return err
		}
	}
	labels := func(role string) map[string]string {
		l := labelsWithOwner(map[string]string{Label: id, ManagedBy: "bezisync", "app.bezisync.io/config-hash": w.ConfigHash,
			"app.bezisync.io/pipeline-name": w.Name}, w.Owner)
		if role != "" {
			l[RoleLabel] = role
		}
		return l
	}
	if err := d.ensureNetwork(ctx, flinkNet(id), labels("")); err != nil {
		return err
	}
	for _, v := range []string{flinkOpt(id), flinkState(id)} {
		if _, err := d.cli.VolumeRemove(ctx, v, client.VolumeRemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
			return err
		}
	}
	if _, err := d.cli.VolumeCreate(ctx, client.VolumeCreateOptions{Name: flinkOpt(id), Labels: labels("")}); err != nil {
		return err
	}
	size := w.Flink.StateSize
	if size == "" {
		size = "1Gi"
	}
	if _, err := d.cli.VolumeCreate(ctx, client.VolumeCreateOptions{Name: flinkState(id), Driver: "local", Labels: labels(""),
		DriverOpts: map[string]string{"type": "tmpfs", "device": "tmpfs",
			"o": fmt.Sprintf("uid=%d,gid=%d,size=%d", FlinkUID, FlinkUID, MemBytes(size))}}); err != nil {
		return err
	}
	if err := d.installHelper(ctx, w, labels("")); err != nil {
		return err
	}
	if len(w.Tunnel) > 0 {
		if err := d.flinkTunnel(ctx, w, labels(RoleTunnel)); err != nil {
			return err
		}
	}
	env := envList(w.Env)
	jm, err := d.createFlinkContainer(ctx, w, w.ResourceName(), labels(RoleJobManager), RoleJobManager, env, w.Flink.JobManagerCPU, w.Flink.JobManagerMemory)
	if err != nil {
		return err
	}
	// Configuration and secrets go into the opt volume through the (not yet started) JobManager.
	files := map[string]string{}
	for name, content := range w.Flink.Config {
		files["config/"+name] = content
	}
	for name, content := range w.Files {
		files["secrets/"+name] = content
	}
	if _, err := d.cli.CopyToContainer(ctx, jm, client.CopyToContainerOptions{DestinationPath: FlinkOptDir, Content: bytes.NewReader(flinkTar(files))}); err != nil {
		return err
	}
	for n := 1; n <= w.Flink.TaskManagers; n++ {
		if _, err := d.createFlinkContainer(ctx, w, flinkTM(id, n), labels(RoleTaskManager), RoleTaskManager, nil,
			w.Flink.TaskManagerCPU, w.Flink.TaskManagerMemory); err != nil {
			return err
		}
	}
	return nil
}

func (d *DockerDriver) ensureNetwork(ctx context.Context, name string, labels map[string]string) error {
	if _, err := d.cli.NetworkInspect(ctx, name, client.NetworkInspectOptions{}); err == nil {
		return nil
	} else if !cerrdefs.IsNotFound(err) {
		return err
	}
	_, err := d.cli.NetworkCreate(ctx, name, client.NetworkCreateOptions{Driver: "bridge", Labels: labels})
	return err
}

// HelperInstallTimeout is how long copying the helper may take (tests shorten it).
var HelperInstallTimeout = 2 * time.Minute

// installHelper copies the bezisync binary of the BeziSync image into the opt volume.
func (d *DockerDriver) installHelper(ctx context.Context, w Workload, labels map[string]string) error {
	name := "bz-" + w.ShortID + "-install"
	if old, err := d.get(ctx, name); err != nil {
		return err
	} else if old != nil {
		if err := d.remove(ctx, old.ID); err != nil {
			return err
		}
	}
	res, err := d.cli.ContainerCreate(ctx, client.ContainerCreateOptions{Name: name,
		Config: &container.Config{Image: w.Flink.HelperImage, Cmd: []string{"install-self", FlinkOptDir}, User: "0", Labels: labels},
		HostConfig: &container.HostConfig{NetworkMode: "none", SecurityOpt: []string{"no-new-privileges"}, CapDrop: []string{"ALL"},
			Mounts: []mount.Mount{{Type: mount.TypeVolume, Source: flinkOpt(w.ShortID), Target: FlinkOptDir}}}})
	if err != nil {
		return err
	}
	defer func() { _ = d.remove(context.WithoutCancel(ctx), res.ID) }()
	// pullCli has no request timeout: the copy may take longer than one API request may.
	wctx, cancel := context.WithTimeout(ctx, HelperInstallTimeout)
	defer cancel()
	wait := d.pullCli.ContainerWait(wctx, res.ID, client.ContainerWaitOptions{Condition: container.WaitConditionNextExit})
	if _, err := d.cli.ContainerStart(ctx, res.ID, client.ContainerStartOptions{}); err != nil {
		return err
	}
	select {
	case r := <-wait.Result:
		if r.StatusCode != 0 {
			return Errorf("Copying the BeziSync helper from %s failed (exit %d).", w.Flink.HelperImage, r.StatusCode)
		}
		return nil
	case err := <-wait.Error:
		if errors.Is(wctx.Err(), context.DeadlineExceeded) {
			return Errorf("Copying the BeziSync helper from %s took longer than %d seconds.", w.Flink.HelperImage, int(HelperInstallTimeout.Seconds()))
		}
		return err
	}
}

// flinkTunnel runs the SSH tunnel on the pipeline's network as bz-tunnel.
func (d *DockerDriver) flinkTunnel(ctx context.Context, w Workload, labels map[string]string) error {
	t := w.Tunnel
	hostKey := ""
	if v, ok := t["ssh_host_key"]; ok && v != nil {
		hostKey = fmt.Sprint(v)
	}
	res, err := d.cli.ContainerCreate(ctx, client.ContainerCreateOptions{Name: "bz-" + w.ShortID + "-tunnel",
		Config: &container.Config{Image: fmt.Sprint(t["image"]), Labels: labels,
			Cmd: []string{"tunnel", "--target", fmt.Sprintf("%v:%v", t["host"], t["port"]),
				"--via", fmt.Sprintf("%v@%v:%v", t["ssh_user"], t["ssh_host"], t["ssh_port"]),
				"--listen-host", "0.0.0.0", "--listen-port", fmt.Sprint(t["listen_port"])},
			Env: []string{"BZ_SSH_KEY=" + w.Env["BZ_SSH_KEY"], "BZ_SSH_HOST_KEY=" + hostKey}},
		HostConfig: &container.HostConfig{RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
			SecurityOpt: []string{"no-new-privileges"}, CapDrop: []string{"ALL"}, ReadonlyRootfs: true, NetworkMode: container.NetworkMode(flinkNet(w.ShortID))},
		NetworkingConfig: &network.NetworkingConfig{EndpointsConfig: map[string]*network.EndpointSettings{flinkNet(w.ShortID): {Aliases: []string{"bz-tunnel"}}}}})
	if err != nil {
		return err
	}
	if d.network != "" { // the bastion may be reachable only there
		if _, err := d.cli.NetworkConnect(ctx, d.network, client.NetworkConnectOptions{Container: res.ID}); err != nil {
			return err
		}
	}
	return nil
}

// createFlinkContainer creates one JobManager or TaskManager (not started) and joins it to the
// runtime's network when the runtime has one.
func (d *DockerDriver) createFlinkContainer(ctx context.Context, w Workload, name string, labels map[string]string, role string, env []string,
	cpu, memory string) (string, error) {
	grace := w.StopGrace()
	res, err := d.cli.ContainerCreate(ctx, client.ContainerCreateOptions{Name: name,
		Config: &container.Config{Image: w.Image, Entrypoint: []string{FlinkOptDir + "/bezisync"}, Cmd: []string{"engine", "flink-cdc", role},
			Env: env, Labels: labels, StopTimeout: &grace},
		HostConfig: &container.HostConfig{
			RestartPolicy:  container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
			Resources:      container.Resources{NanoCPUs: CPUNano(cpu), Memory: MemBytes(memory)},
			SecurityOpt:    []string{"no-new-privileges"},
			CapDrop:        []string{"ALL"},
			ReadonlyRootfs: true,
			Tmpfs:          map[string]string{"/tmp": "exec,mode=1777,size=1g"},
			NetworkMode:    container.NetworkMode(flinkNet(w.ShortID)),
			Mounts: []mount.Mount{
				{Type: mount.TypeVolume, Source: flinkOpt(w.ShortID), Target: FlinkOptDir},
				{Type: mount.TypeVolume, Source: flinkState(w.ShortID), Target: FlinkStateDir},
			},
		}})
	if err != nil {
		return "", err
	}
	if d.network != "" {
		if _, err := d.cli.NetworkConnect(ctx, d.network, client.NetworkConnectOptions{Container: res.ID}); err != nil {
			return "", err
		}
	}
	return res.ID, nil
}

// flinkTar packs files (path → content) under directories the Flink user owns: config readable,
// secrets private.
func flinkTar(files map[string]string) []byte {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	dirs := map[string]bool{}
	now := time.Now()
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		dir := name[:strings.LastIndex(name, "/")+1]
		if dir != "" && !dirs[dir] {
			dirs[dir] = true
			_ = tw.WriteHeader(&tar.Header{Name: dir, Mode: 0o750, Typeflag: tar.TypeDir, Uid: FlinkUID, Gid: FlinkUID, ModTime: now})
		}
		mode := int64(0o640)
		if strings.HasPrefix(name, "secrets/") {
			mode = 0o600
		}
		_ = tw.WriteHeader(&tar.Header{Name: name, Mode: mode, Size: int64(len(files[name])), Uid: FlinkUID, Gid: FlinkUID, Typeflag: tar.TypeReg, ModTime: now})
		_, _ = tw.Write([]byte(files[name]))
	}
	_ = tw.Close()
	return buf.Bytes()
}

// observeFlink reports the JobManager's state, with every component in Details.
func (d *DockerDriver) observeFlink(ctx context.Context, id string, jm *container.InspectResponse) (Observation, error) {
	obs, err := d.observe(ctx, id)
	if err != nil {
		return obs, err
	}
	have, err := d.flinkContainers(ctx, id)
	if err != nil {
		return obs, err
	}
	names := make([]string, 0, len(have))
	for name := range have {
		names = append(names, name)
	}
	sort.Strings(names)
	var components []any
	allUp := true
	for _, name := range names {
		c := have[name]
		restarts := 0
		if name != "bz-"+id {
			if full, err := d.get(ctx, c.ID); err == nil && full != nil {
				restarts = full.RestartCount
				obs.Restarts += restarts
			}
		} else {
			restarts = jm.RestartCount
		}
		components = append(components, map[string]any{"name": name, "role": c.Labels[RoleLabel], "state": string(c.State), "restarts": restarts})
		allUp = allUp && c.State == "running"
	}
	obs.Ready = obs.Ready && allUp
	if obs.Details == nil {
		obs.Details = map[string]any{}
	}
	obs.Details["components"] = components
	if d.stopInProgress(id) {
		obs.Phase, obs.Ready, obs.Reason = PhasePending, false, "Saving the position and stopping."
	}
	return obs, nil
}

// deleteFlink stops a cluster in order (waiting for it) and removes everything it had.
func (d *DockerDriver) deleteFlink(ctx context.Context, id string, have map[string]container.Summary) error {
	grace := 30
	if c, ok := have["bz-"+id]; ok {
		if full, err := d.get(ctx, c.ID); err == nil && full != nil && full.Config.StopTimeout != nil {
			grace = *full.Config.StopTimeout
		}
	}
	d.mu.Lock()
	s := d.stops[id]
	delete(d.stops, id)
	d.mu.Unlock()
	if s != nil && !s.finished() {
		select { // a stop already running saves the position; wait for it
		case <-s.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	} else if err := d.stopFlink(ctx, id, grace); err != nil {
		return err
	}
	for _, c := range have {
		if err := d.remove(ctx, c.ID); err != nil && !cerrdefs.IsNotFound(err) {
			return err
		}
	}
	for _, v := range []string{flinkOpt(id), flinkState(id)} {
		if _, err := d.cli.VolumeRemove(ctx, v, client.VolumeRemoveOptions{Force: true}); err != nil && !cerrdefs.IsNotFound(err) {
			return err
		}
	}
	if _, err := d.cli.NetworkRemove(ctx, flinkNet(id), client.NetworkRemoveOptions{}); err != nil && !cerrdefs.IsNotFound(err) {
		return err
	}
	return nil
}

// LogsOf returns the last lines of one component: "" or "jobmanager", "taskmanager-<n>", "tunnel".
func (d *DockerDriver) LogsOf(ctx context.Context, shortID, component string, tail int, _ bool) (string, error) {
	name, err := componentName(shortID, component)
	if err != nil {
		return "", err
	}
	return d.logsOf(ctx, name, tail)
}

// componentName maps a component to its container name.
func componentName(shortID, component string) (string, error) {
	switch {
	case component == "" || component == RoleJobManager:
		return "bz-" + shortID, nil
	case component == RoleTunnel:
		return "bz-" + shortID + "-tunnel", nil
	case strings.HasPrefix(component, RoleTaskManager+"-"):
		n, err := strconv.Atoi(strings.TrimPrefix(component, RoleTaskManager+"-"))
		if err != nil || n < 1 || n > 64 {
			return "", Errorf("Unknown component %q.", component)
		}
		return flinkTM(shortID, n), nil
	}
	return "", Errorf("Unknown component %q.", component)
}
