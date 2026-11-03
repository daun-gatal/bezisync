package runtime

import (
	"archive/tar"
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// Docker runtime: one container per pipeline on a local or remote Docker Engine.
// Configuration is copied into the container before start; health and metrics are read
// with `docker exec curl`, so no network path to the container is needed.

// DockerDriver drives one Docker Engine.
type DockerDriver struct {
	cli     *client.Client
	pullCli *client.Client // no request timeout: an image download takes minutes, bounded by pullTimeout
	network string
	// pullTimeout bounds one image download; it then fails and the next apply starts again.
	pullTimeout time.Duration

	mu      sync.Mutex
	pulls   map[string]*imagePull // image ref → download in progress or just finished
	pulling map[string]string     // workload short id → image ref it waits for
}

// imagePull is one image download running in the background.
type imagePull struct {
	started time.Time
	done    chan struct{} // closed when the download ends; err and ended are set before
	err     error
	ended   time.Time
}

// DefaultPullTimeout is the image download timeout when the runtime sets none.
const DefaultPullTimeout = 30 * time.Minute

// requestTimeout bounds every other Docker API call (a variable for tests).
var requestTimeout = 30 * time.Second

// DockerTLS are PEM blocks for a TLS-protected daemon.
type DockerTLS struct {
	CA, Cert, Key string
	Verify        bool
}

// NewDocker connects to host ("" = the environment / local socket).
// pullTimeout bounds one image download (0 = DefaultPullTimeout).
func NewDocker(host string, tlsCfg *DockerTLS, network string, pullTimeout time.Duration) (*DockerDriver, error) {
	if pullTimeout <= 0 {
		pullTimeout = DefaultPullTimeout
	}
	cli, err := dockerClient(host, tlsCfg, requestTimeout)
	if err != nil {
		return nil, err
	}
	pullCli, err := dockerClient(host, tlsCfg, 0)
	if err != nil {
		return nil, err
	}
	return &DockerDriver{cli: cli, pullCli: pullCli, network: network, pullTimeout: pullTimeout,
		pulls: map[string]*imagePull{}, pulling: map[string]string{}}, nil
}

// dockerClient builds an API client; timeout 0 leaves requests bounded only by their context.
func dockerClient(host string, tlsCfg *DockerTLS, timeout time.Duration) (*client.Client, error) {
	opts := []client.Opt{client.WithTimeout(timeout)} // API-version negotiation is the default
	if host == "" {
		opts = append(opts, client.FromEnv)
	} else {
		opts = append(opts, client.WithHost(host))
		if tlsCfg != nil && (tlsCfg.CA != "" || tlsCfg.Cert != "" || tlsCfg.Key != "") {
			tc := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !tlsCfg.Verify} //nolint:gosec // the runtime's own tls_verify setting
			if tlsCfg.CA != "" {
				pool := x509.NewCertPool()
				pool.AppendCertsFromPEM([]byte(tlsCfg.CA))
				tc.RootCAs = pool
			}
			if tlsCfg.Cert != "" && tlsCfg.Key != "" {
				pair, err := tls.X509KeyPair([]byte(tlsCfg.Cert), []byte(tlsCfg.Key))
				if err != nil {
					return nil, Errorf("Cannot initialise the docker runtime: %v", err)
				}
				tc.Certificates = []tls.Certificate{pair}
			}
			opts = append(opts, client.WithScheme("https"), client.WithHTTPClient(&http.Client{
				Transport: &http.Transport{TLSClientConfig: tc}, Timeout: timeout,
			}))
		}
	}
	cli, err := client.New(opts...)
	if err != nil {
		return nil, Errorf("Cannot connect to Docker: %v", err)
	}
	return cli, nil
}

func tarFile(name, content string, mode int64, uid int) []byte {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	_ = tw.WriteHeader(&tar.Header{Name: name, Size: int64(len(content)), Mode: mode, ModTime: time.Now(), Uid: uid})
	_, _ = tw.Write([]byte(content))
	_ = tw.Close()
	return buf.Bytes()
}

func tarMany(files map[string]string) []byte {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range sortedFileNames(files) {
		content := files[name]
		_ = tw.WriteHeader(&tar.Header{Name: name, Size: int64(len(content)), Mode: 0o440, ModTime: time.Now(), Uid: 185})
		_, _ = tw.Write([]byte(content))
	}
	_ = tw.Close()
	return buf.Bytes()
}

// CPUNano converts "500m" / "2" to nano CPUs.
func CPUNano(value string) int64 {
	var cores float64
	if strings.HasSuffix(value, "m") {
		f, _ := strconv.ParseFloat(strings.TrimSuffix(value, "m"), 64)
		cores = f / 1000
	} else {
		cores, _ = strconv.ParseFloat(value, 64)
	}
	return int64(cores * 1e9)
}

// MemBytes converts "768Mi" / "3Gi" to bytes (Docker's "768m" / "3g").
func MemBytes(value string) int64 {
	v := strings.ReplaceAll(strings.ReplaceAll(value, "Mi", "m"), "Gi", "g")
	unit := int64(1)
	switch {
	case strings.HasSuffix(v, "g"):
		unit, v = 1<<30, strings.TrimSuffix(v, "g")
	case strings.HasSuffix(v, "m"):
		unit, v = 1<<20, strings.TrimSuffix(v, "m")
	case strings.HasSuffix(v, "k"):
		unit, v = 1<<10, strings.TrimSuffix(v, "k")
	}
	n, _ := strconv.ParseFloat(v, 64)
	return int64(n * float64(unit))
}

var dockerTime = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$`)

// ParseDockerTime reads Docker timestamps (nanoseconds) at microsecond precision.
func ParseDockerTime(value string) *time.Time {
	if value == "" || strings.HasPrefix(value, "0001") {
		return nil
	}
	m := dockerTime.FindStringSubmatch(value)
	if m == nil {
		return nil
	}
	frac := m[2]
	if frac == "" {
		frac = "0"
	}
	if len(frac) > 6 {
		frac = frac[:6]
	}
	frac += strings.Repeat("0", 6-len(frac))
	zone := m[3]
	if zone == "Z" {
		zone = "+00:00"
	}
	t, err := time.Parse("2006-01-02T15:04:05.000000-07:00", m[1]+"."+frac+zone)
	if err != nil {
		return nil
	}
	t = t.UTC()
	return &t
}

func (d *DockerDriver) get(ctx context.Context, name string) (*container.InspectResponse, error) {
	res, err := d.cli.ContainerInspect(ctx, name, client.ContainerInspectOptions{})
	if err != nil {
		if cerrdefs.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &res.Container, nil
}

func (d *DockerDriver) remove(ctx context.Context, id string) error {
	_, err := d.cli.ContainerRemove(ctx, id, client.ContainerRemoveOptions{Force: true})
	return err
}

// ensureImage reports whether ref is on the daemon. When it is not, it starts downloading it in
// the background (once per ref, whoever asks) and returns false: a download takes minutes, far
// longer than one reconcile, so the caller tries again on its next pass. A failed download keeps
// returning its error for pullRetryAfter, then the next call starts a new one.
func (d *DockerDriver) ensureImage(ctx context.Context, shortID, ref string) (bool, error) {
	if _, err := d.cli.ImageInspect(ctx, ref); err == nil {
		d.mu.Lock()
		delete(d.pulling, shortID)
		if p := d.pulls[ref]; p != nil && p.finished() {
			delete(d.pulls, ref)
		}
		d.mu.Unlock()
		return true, nil
	} else if !cerrdefs.IsNotFound(err) {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	p := d.pulls[ref]
	if p != nil && p.finished() {
		if p.err != nil && time.Since(p.ended) < pullRetryAfter {
			delete(d.pulling, shortID)
			return false, d.pullErr(ref, p.err)
		}
		delete(d.pulls, ref) // retry; or downloaded yet gone again (removed meanwhile)
		p = nil
	}
	if p == nil {
		p = &imagePull{started: time.Now(), done: make(chan struct{})}
		d.pulls[ref] = p
		// The download outlives this reconcile: keep ctx's values, not its cancellation.
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), d.pullTimeout)
		go func() {
			defer cancel()
			p.err = d.pull(pctx, ref)
			p.ended = time.Now()
			close(p.done)
		}()
	}
	d.pulling[shortID] = ref
	return false, nil
}

// pullRetryAfter is how long a failed download is reported before it is tried again (a variable for tests).
var pullRetryAfter = 2 * time.Minute

func (d *DockerDriver) pullErr(ref string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return Errorf("Downloading the image %s took longer than the runtime's image download timeout (%.0f minutes). "+
			"Raise it under Settings → Runtimes, or pre-pull the image on the Docker host.", ref, d.pullTimeout.Minutes())
	}
	return Errorf("Cannot download the image %s: %s", ref, explanation(err))
}

func (p *imagePull) finished() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// pull downloads ref; the daemon streams progress until the image is complete.
func (d *DockerDriver) pull(ctx context.Context, ref string) error {
	rc, err := d.pullCli.ImagePull(ctx, ref, client.ImagePullOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	for sc.Scan() {
		var msg struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(sc.Bytes(), &msg) == nil && msg.Error != "" {
			return errors.New(msg.Error)
		}
	}
	return sc.Err()
}

// imagePullNote describes the download a workload waits for ("" = none).
func (d *DockerDriver) imagePullNote(shortID string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	ref, ok := d.pulling[shortID]
	if !ok {
		return ""
	}
	p := d.pulls[ref]
	if p == nil || p.finished() {
		return ""
	}
	return fmt.Sprintf("Downloading the image %s (%s so far).", ref, time.Since(p.started).Round(time.Second))
}

func labelsWithOwner(base map[string]string, owner string) map[string]string {
	if owner != "" {
		base[Owner] = owner
	}
	return base
}

// startTunnel runs the companion container holding the SSH tunnel; the connector joins
// its network namespace.
func (d *DockerDriver) startTunnel(ctx context.Context, w Workload) (string, error) {
	t := w.Tunnel
	name := "bz-" + w.ShortID + "-tunnel"
	if stale, err := d.get(ctx, name); err != nil {
		return "", err
	} else if stale != nil {
		if err := d.remove(ctx, stale.ID); err != nil {
			return "", err
		}
	}
	img := fmt.Sprint(t["image"]) // on the daemon: Apply waits for it before replacing anything
	hostKey := ""
	if v, ok := t["ssh_host_key"]; ok && v != nil {
		hostKey = fmt.Sprint(v)
	}
	cfg := &container.Config{
		Image: img,
		Cmd: []string{"tunnel", "--target", fmt.Sprintf("%v:%v", t["host"], t["port"]),
			"--via", fmt.Sprintf("%v@%v:%v", t["ssh_user"], t["ssh_host"], t["ssh_port"]),
			"--listen-port", fmt.Sprint(t["listen_port"])},
		Labels: labelsWithOwner(map[string]string{ManagedBy: "bezisync", "app.bezisync.io/tunnel-for": w.ShortID}, w.Owner),
		Env:    []string{"BZ_SSH_KEY=" + w.Env["BZ_SSH_KEY"], "BZ_SSH_HOST_KEY=" + hostKey},
	}
	host := &container.HostConfig{
		RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
		SecurityOpt:   []string{"no-new-privileges"},
	}
	if d.network != "" {
		host.NetworkMode = container.NetworkMode(d.network)
	}
	res, err := d.cli.ContainerCreate(ctx, client.ContainerCreateOptions{Config: cfg, HostConfig: host, Name: name})
	if err != nil {
		return "", err
	}
	if _, err := d.cli.ContainerStart(ctx, res.ID, client.ContainerStartOptions{}); err != nil {
		return "", err
	}
	return name, nil
}

func envList(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, k := range sortedFileNames(m) {
		out = append(out, k+"="+m[k])
	}
	return out
}

func sortedFileNames(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sortStrings(out)
	return out
}

// Apply creates or replaces the container when its config hash changes and sets its
// running state.
func (d *DockerDriver) Apply(ctx context.Context, w Workload) error {
	existing, err := d.get(ctx, w.ResourceName())
	if err != nil {
		return d.apiErr(err)
	}
	stale := existing != nil && existing.Config.Labels["app.bezisync.io/config-hash"] != w.ConfigHash
	if existing == nil || stale {
		// Images first: a new image downloads in the background while the current container
		// keeps running, and is replaced only once everything it needs is on the daemon.
		images := []string{w.Image}
		if len(w.Tunnel) > 0 {
			images = append(images, fmt.Sprint(w.Tunnel["image"]))
		}
		for _, img := range images {
			ready, err := d.ensureImage(ctx, w.ShortID, img)
			if err != nil {
				return d.apiErr(err)
			}
			if !ready {
				if existing != nil && !w.Running && existing.State.Status == "running" {
					return d.stop(ctx, w.ShortID, existing.ID)
				}
				return nil
			}
		}
	}
	if stale {
		if err := d.remove(ctx, existing.ID); err != nil {
			return d.apiErr(err)
		}
		existing = nil
	}
	if existing == nil {
		if len(w.Tunnel) == 0 {
			if orphan, err := d.get(ctx, "bz-"+w.ShortID+"-tunnel"); err != nil {
				return d.apiErr(err)
			} else if orphan != nil {
				if err := d.remove(ctx, orphan.ID); err != nil { // the connection no longer uses a bastion
					return d.apiErr(err)
				}
			}
		}
		host := &container.HostConfig{
			RestartPolicy: container.RestartPolicy{Name: container.RestartPolicyUnlessStopped},
			Resources:     container.Resources{NanoCPUs: CPUNano(w.CPU), Memory: MemBytes(w.Memory)},
			SecurityOpt:   []string{"no-new-privileges"},
		}
		if len(w.Tunnel) > 0 {
			// Behind a bastion: Debezium shares the tunnel container's network namespace.
			name, err := d.startTunnel(ctx, w)
			if err != nil {
				return d.apiErr(err)
			}
			host.NetworkMode = container.NetworkMode("container:" + name)
		} else if d.network != "" {
			host.NetworkMode = container.NetworkMode(d.network)
		}
		cfg := &container.Config{
			Image: w.Image,
			Labels: labelsWithOwner(map[string]string{
				Label: w.ShortID, ManagedBy: "bezisync", "app.bezisync.io/config-hash": w.ConfigHash, "app.bezisync.io/pipeline-name": w.Name,
			}, w.Owner),
			Env: envList(JVMEnv(w)),
		}
		var netCfg *network.NetworkingConfig
		res, err := d.cli.ContainerCreate(ctx, client.ContainerCreateOptions{Config: cfg, HostConfig: host, NetworkingConfig: netCfg, Name: w.ResourceName()})
		if err != nil {
			return d.apiErr(err)
		}
		if _, err := d.cli.CopyToContainer(ctx, res.ID, client.CopyToContainerOptions{DestinationPath: "/debezium/config",
			Content: bytes.NewReader(tarFile("application.properties", w.Properties, 0o444, 0))}); err != nil {
			return d.apiErr(err)
		}
		if len(w.Files) > 0 {
			files := map[string]string{}
			for k, v := range w.Files {
				files["secrets/"+k] = v
			}
			if _, err := d.cli.CopyToContainer(ctx, res.ID, client.CopyToContainerOptions{DestinationPath: "/debezium", Content: bytes.NewReader(tarMany(files))}); err != nil {
				return d.apiErr(err)
			}
		}
		existing, err = d.get(ctx, res.ID)
		if err != nil {
			return d.apiErr(err)
		}
	}
	existing, err = d.get(ctx, existing.ID)
	if err != nil || existing == nil {
		return d.apiErr(err)
	}
	var tunnel *container.InspectResponse
	if len(w.Tunnel) > 0 {
		if tunnel, err = d.get(ctx, "bz-"+w.ShortID+"-tunnel"); err != nil {
			return d.apiErr(err)
		}
	}
	status := existing.State.Status
	switch {
	case w.Running && status != "running":
		if tunnel != nil && tunnel.State.Status != "running" {
			if _, err := d.cli.ContainerStart(ctx, tunnel.ID, client.ContainerStartOptions{}); err != nil {
				return d.apiErr(err)
			}
		}
		if _, err := d.cli.ContainerStart(ctx, existing.ID, client.ContainerStartOptions{}); err != nil {
			return d.apiErr(err)
		}
	case !w.Running && status == "running":
		return d.stop(ctx, w.ShortID, existing.ID)
	}
	return nil
}

// stop stops the connector, then its tunnel when it has one.
func (d *DockerDriver) stop(ctx context.Context, shortID, id string) error {
	t30, t10 := 30, 10
	if _, err := d.cli.ContainerStop(ctx, id, client.ContainerStopOptions{Timeout: &t30}); err != nil {
		return d.apiErr(err)
	}
	tunnel, err := d.get(ctx, "bz-"+shortID+"-tunnel")
	if err != nil {
		return d.apiErr(err)
	}
	if tunnel != nil {
		if _, err := d.cli.ContainerStop(ctx, tunnel.ID, client.ContainerStopOptions{Timeout: &t10}); err != nil {
			return d.apiErr(err)
		}
	}
	return nil
}

func (d *DockerDriver) apiErr(err error) error {
	if err == nil {
		return nil
	}
	var re *Error
	if errors.As(err, &re) {
		return err
	}
	return Errorf("Docker rejected the workload: %s", explanation(err))
}

func explanation(err error) string {
	s := err.Error()
	if i := strings.Index(s, "Error response from daemon: "); i >= 0 {
		return s[i+len("Error response from daemon: "):]
	}
	return s
}

// Delete removes the connector and its tunnel.
func (d *DockerDriver) Delete(ctx context.Context, shortID string) error {
	d.mu.Lock()
	delete(d.pulling, shortID) // a download it started finishes for whoever needs the image next
	d.mu.Unlock()
	for _, name := range []string{"bz-" + shortID, "bz-" + shortID + "-tunnel"} {
		c, err := d.get(ctx, name)
		if err != nil {
			return err
		}
		if c != nil {
			if err := d.remove(ctx, c.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

// Observe reports the container state, and the image download it waits for, if any.
func (d *DockerDriver) Observe(ctx context.Context, shortID string) (Observation, error) {
	obs, err := d.observe(ctx, shortID)
	if err == nil {
		obs.ImagePull = d.imagePullNote(shortID)
	}
	return obs, err
}

func (d *DockerDriver) observe(ctx context.Context, shortID string) (Observation, error) {
	c, err := d.get(ctx, "bz-"+shortID)
	if err != nil {
		return Observation{}, err
	}
	if c == nil {
		return Observation{Phase: PhaseMissing}, nil
	}
	var hash *string
	if h, ok := c.Config.Labels["app.bezisync.io/config-hash"]; ok {
		hash = &h
	}
	st := c.State
	restarts := c.RestartCount
	code := st.ExitCode
	switch st.Status {
	case "running":
		return Observation{Phase: PhaseRunning, Ready: true, Restarts: restarts, StartedAt: ParseDockerTime(st.StartedAt), ConfigHash: hash}, nil
	case "restarting":
		return Observation{Phase: PhaseRestarting, Restarts: restarts, ExitCode: &code, ConfigHash: hash}, nil
	case "exited", "dead":
		if c.HostConfig != nil && c.HostConfig.RestartPolicy.Name != "" && code != 0 {
			return Observation{Phase: PhaseRestarting, Restarts: restarts, ExitCode: &code, Reason: st.Error, ConfigHash: hash}, nil
		}
		return Observation{Phase: PhaseStopped, ExitCode: &code, ConfigHash: hash}, nil
	}
	return Observation{Phase: PhasePending, Reason: string(st.Status), ConfigHash: hash}, nil
}

// Logs returns the last lines of the connector's output.
func (d *DockerDriver) Logs(ctx context.Context, shortID string, tail int, _ bool) (string, error) {
	c, err := d.get(ctx, "bz-"+shortID)
	if err != nil || c == nil {
		return "", err
	}
	rc, err := d.cli.ContainerLogs(ctx, c.ID, client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Tail: strconv.Itoa(tail)})
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()
	var out bytes.Buffer
	if c.Config.Tty {
		_, err = out.ReadFrom(rc)
	} else {
		_, err = stdcopy.StdCopy(&out, &out, rc)
	}
	if err != nil {
		return "", err
	}
	return decodeReplace(out.Bytes()), nil
}

// HTTPGet runs curl inside the container.
func (d *DockerDriver) HTTPGet(ctx context.Context, shortID string, port int, path string, timeout time.Duration) (int, string, error) {
	c, err := d.get(ctx, "bz-"+shortID)
	if err != nil {
		return 0, "", err
	}
	if c == nil || c.State.Status != "running" {
		return 0, "", &Error{"Container is not running."}
	}
	exec, err := d.cli.ExecCreate(ctx, c.ID, client.ExecCreateOptions{
		AttachStdout: true, AttachStderr: true,
		Cmd: []string{"curl", "-s", "-m", strconv.Itoa(int(timeout.Seconds())), "-w", "\n%{http_code}", fmt.Sprintf("http://127.0.0.1:%d%s", port, path)},
	})
	if err != nil {
		return 0, "", err
	}
	att, err := d.cli.ExecAttach(ctx, exec.ID, client.ExecAttachOptions{})
	if err != nil {
		return 0, "", err
	}
	defer att.Close()
	var out bytes.Buffer
	if _, err := stdcopy.StdCopy(&out, &out, att.Reader); err != nil {
		return 0, "", err
	}
	text := decodeReplace(out.Bytes())
	i := strings.LastIndex(text, "\n")
	body, code := "", text
	if i >= 0 {
		body, code = text[:i], text[i+1:]
	}
	n := 0
	if isDigits(code) {
		n, _ = strconv.Atoi(code)
	}
	return n, body, nil
}

// Test checks the daemon (and the configured network).
func (d *DockerDriver) Test(ctx context.Context) TestResult {
	res, err := d.cli.Info(ctx, client.InfoOptions{})
	info := res.Info
	if err != nil {
		return TestResult{OK: false, Message: fmt.Sprintf("Cannot reach the Docker daemon: %v", err)}
	}
	if d.network != "" {
		if _, err := d.cli.NetworkInspect(ctx, d.network, client.NetworkInspectOptions{}); err != nil {
			if cerrdefs.IsNotFound(err) {
				return TestResult{OK: false, Message: fmt.Sprintf("Docker network '%s' does not exist on this host.", d.network)}
			}
		}
	}
	capacity := d.Capacity(ctx)
	if capacity == nil {
		capacity = map[string]any{}
	}
	return TestResult{OK: true, Message: fmt.Sprintf("Docker %s on %s.", info.ServerVersion, info.Name), Details: capacity}
}

// ListWorkloads lists BeziSync connector containers (optionally of one owner).
func (d *DockerDriver) ListWorkloads(ctx context.Context, owner string) ([]string, error) {
	f := client.Filters{}.Add("label", ManagedBy+"=bezisync")
	if owner != "" {
		f = f.Add("label", Owner+"="+owner)
	}
	list, err := d.cli.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: f})
	if err != nil {
		return nil, err
	}
	out := []string{}
	for _, c := range list.Items {
		if id, ok := c.Labels[Label]; ok {
			out = append(out, id)
		}
	}
	return out, nil
}

// Capacity reports host CPU/memory and what running connectors reserve.
func (d *DockerDriver) Capacity(ctx context.Context) map[string]any {
	res, err := d.cli.Info(ctx, client.InfoOptions{})
	if err != nil {
		return nil
	}
	info := res.Info
	listed, err := d.cli.ContainerList(ctx, client.ContainerListOptions{Filters: client.Filters{}.Add("label", ManagedBy+"=bezisync")})
	if err != nil {
		return nil
	}
	list := listed.Items
	var nano, mem int64
	for _, c := range list {
		inspected, err := d.cli.ContainerInspect(ctx, c.ID, client.ContainerInspectOptions{})
		full := inspected.Container
		if err != nil || full.HostConfig == nil {
			continue
		}
		nano += full.HostConfig.NanoCPUs
		mem += full.HostConfig.Memory
	}
	return map[string]any{
		"cpu_allocatable":    float64(info.NCPU),
		"memory_allocatable": info.MemTotal,
		"cpu_requested":      float64(nano) / 1e9,
		"memory_requested":   mem,
		"workloads":          len(list),
		"architectures":      Architectures(info.Architecture),
	}
}
