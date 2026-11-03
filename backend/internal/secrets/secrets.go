// Package secrets reads connection secrets kept in external stores.
//
// A secret field holds `ref+<store>://<path>#<key>` instead of the value. Only the
// reference is stored; the value is read from the store when a connector needs it and is
// never persisted by BeziSync. Stores are per project.
package secrets

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/smithy-go"
	"golang.org/x/oauth2/google"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/daun-gatal/bezisync/backend/internal/om"
	"github.com/daun-gatal/bezisync/backend/internal/pyjson"
	"github.com/daun-gatal/bezisync/backend/internal/ssrf"
)

// Ref is a parsed secret reference.
type Ref struct {
	Store, Path string
	Key         *string
}

func (r Ref) String() string {
	s := "ref+" + r.Store + "://" + r.Path
	if r.Key != nil && *r.Key != "" {
		s += "#" + *r.Key
	}
	return s
}

var refRe = regexp.MustCompile(`^ref\+([a-z0-9][a-z0-9-]{0,62})://([^#\s]+)(?:#([^\s#]+))?$`)

// Error means reading from a store failed; the message never contains secret values.
type Error struct{ Msg string }

func (e *Error) Error() string { return e.Msg }

func errf(format string, a ...any) error { return &Error{fmt.Sprintf(format, a...)} }

// ParseRef returns the reference in value, nil when value is not a reference.
func ParseRef(value any) (*Ref, error) {
	s, ok := value.(string)
	if !ok || !strings.HasPrefix(s, "ref+") {
		return nil, nil
	}
	m := refRe.FindStringSubmatch(s)
	if m == nil {
		return nil, &Error{"A secret reference must look like ref+<store>://<path>#<key>, e.g. ref+vault://cdc/orders#password."}
	}
	r := &Ref{Store: m[1], Path: m[2]}
	if m[3] != "" {
		k := m[3]
		r.Key = &k
	}
	return r, nil
}

// Client reads secrets from one store.
type Client interface {
	Read(ctx context.Context, path string) (string, error)
	Test(ctx context.Context) (string, error)
	// List returns the paths of the store's secrets, sorted, at most limit of them (truncated
	// says more exist). It never reads a value.
	List(ctx context.Context, limit int) (paths []string, truncated bool, err error)
}

// ListLimit is how many secrets a picker is offered; beyond it, people type the path.
const ListLimit = 1000

// Keys returns the key names of the secret at path, sorted; nil when its value is not a JSON
// object (then a reference uses the whole value). Values never leave this function.
func Keys(ctx context.Context, c Client, path string) ([]string, error) {
	raw, err := c.Read(ctx, path)
	if err != nil {
		return nil, err
	}
	// Anything but a JSON object (plain text, a number, an array) has no keys.
	var obj map[string]json.RawMessage
	if !strings.HasPrefix(strings.TrimSpace(raw), "{") || json.Unmarshal([]byte(raw), &obj) != nil {
		obj = nil
	}
	if obj == nil {
		return nil, nil
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, nil
}

func capped(paths []string, limit int) ([]string, bool) {
	sort.Strings(paths)
	if len(paths) > limit {
		return paths[:limit], true
	}
	return paths, false
}

// Get reads path and optionally one JSON key of it.
func Get(ctx context.Context, c Client, path string, key *string) (string, error) {
	raw, err := c.Read(ctx, path)
	if err != nil {
		return "", err
	}
	if key == nil {
		return raw, nil
	}
	data, err := om.DecodeOrdered([]byte(raw))
	if err != nil {
		return "", errf("'%s' is not a JSON object, so it has no key '%s'.", path, *key)
	}
	obj, ok := data.(*om.Map)
	if !ok {
		return "", errf("'%s' has no key '%s'.", path, *key)
	}
	value, ok := obj.Get(*key)
	if !ok {
		return "", errf("'%s' has no key '%s'.", path, *key)
	}
	if s, ok := value.(string); ok {
		return s, nil
	}
	return DumpsUnsorted(value)
}

// DumpsUnsorted is json.dumps(value) (insertion order, default separators, ensure_ascii).
func DumpsUnsorted(v any) (string, error) {
	return pyjson.Encode(v, pyjson.Options{EnsureASCII: true})
}

const timeout = 8 * time.Second

func guard(ctx context.Context, raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return errf("'%s' is not an http(s) URL.", raw)
	}
	port := 80
	if u.Scheme == "https" {
		port = 443
	}
	if p := u.Port(); p != "" {
		port, _ = strconv.Atoi(p)
	}
	if _, err := ssrf.ResolveChecked(ctx, u.Hostname(), port); err != nil {
		return &Error{err.Error()}
	}
	return nil
}

// --- Vault (KV v2) ---

type vault struct {
	address, mount, namespace, authMethod, approleMount string
	token, roleID, secretID                             *string
	cached                                              string
}

func optStr(m map[string]any, k string) *string {
	v, ok := m[k]
	if !ok || v == nil {
		return nil
	}
	s := fmt.Sprint(v)
	return &s
}

func str(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}

func (v *vault) headers(ctx context.Context) (map[string]string, error) {
	h := map[string]string{}
	if v.namespace != "" {
		h["X-Vault-Namespace"] = v.namespace
	}
	if v.cached == "" {
		if v.authMethod == "token" {
			if v.token == nil || *v.token == "" {
				return nil, &Error{"No Vault token is set."}
			}
			v.cached = *v.token
		} else {
			if v.roleID == nil || *v.roleID == "" || v.secretID == nil || *v.secretID == "" {
				return nil, &Error{"AppRole needs a role ID and a secret ID."}
			}
			res, err := v.call(ctx, http.MethodPost, "/v1/auth/"+v.approleMount+"/login", h,
				map[string]string{"role_id": *v.roleID, "secret_id": *v.secretID})
			if err != nil {
				return nil, err
			}
			authV, _ := res.Get("auth")
			auth, _ := authV.(*om.Map)
			if auth == nil {
				return nil, &Error{"Vault returned no client token."}
			}
			tok, _ := auth.Get("client_token")
			v.cached = fmt.Sprint(tok)
		}
	}
	out := map[string]string{"X-Vault-Token": v.cached}
	for k, val := range h {
		out[k] = val
	}
	return out, nil
}

func (v *vault) call(ctx context.Context, method, path string, headers map[string]string, body any) (*om.Map, error) {
	full := strings.TrimRight(v.address, "/") + path
	if err := guard(ctx, full); err != nil {
		return nil, err
	}
	var reader io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		reader = strings.NewReader(string(raw))
	}
	req, err := http.NewRequestWithContext(ctx, method, full, reader)
	if err != nil {
		return nil, errf("Cannot reach Vault at %s: %v", v.address, err)
	}
	for k, val := range headers {
		req.Header.Set(k, val)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: timeout}).Do(req)
	if err != nil {
		return nil, errf("Cannot reach Vault at %s: %v", v.address, err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, &Error{"Vault has no secret at this path."}
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return nil, &Error{"Vault denied access (check the token or policy)."}
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return nil, errf("Vault answered HTTP %d.", resp.StatusCode)
	}
	raw, _ := io.ReadAll(resp.Body)
	g, err := om.DecodeOrdered(raw)
	if err != nil {
		return nil, err
	}
	root, ok := g.(*om.Map)
	if !ok {
		root = om.New()
	}
	return root, nil
}

// pathEscape is urllib.parse.quote(path) (safe "/").
func pathEscape(p string) string {
	var b strings.Builder
	for _, c := range []byte(p) {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.IndexByte("_.-~/", c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func (v *vault) Read(ctx context.Context, path string) (string, error) {
	h, err := v.headers(ctx)
	if err != nil {
		return "", err
	}
	body, err := v.call(ctx, http.MethodGet, "/v1/"+v.mount+"/data/"+pathEscape(strings.Trim(path, "/")), h, nil)
	if err != nil {
		return "", err
	}
	outerV, _ := body.Get("data")
	outer, _ := outerV.(*om.Map)
	if outer == nil {
		return "", &Error{"Vault returned no data (is the mount KV version 2?)."}
	}
	inner, _ := outer.Get("data")
	data, ok := inner.(*om.Map)
	if !ok {
		return "", &Error{"Vault returned no data (is the mount KV version 2?)."}
	}
	return DumpsUnsorted(data)
}

// List walks the KV v2 metadata tree (folders end in "/").
func (v *vault) List(ctx context.Context, limit int) ([]string, bool, error) {
	h, err := v.headers(ctx)
	if err != nil {
		return nil, false, err
	}
	var out []string
	folders := []string{""}
	for len(folders) > 0 && len(out) <= limit {
		dir := folders[0]
		folders = folders[1:]
		body, err := v.call(ctx, "LIST", "/v1/"+v.mount+"/metadata/"+pathEscape(dir), h, nil)
		if err != nil {
			var se *Error
			if dir == "" && errors.As(err, &se) && se.Msg == "Vault has no secret at this path." {
				return []string{}, false, nil // an empty mount
			}
			if dir == "" {
				return nil, false, err
			}
			continue
		}
		dataV, _ := body.Get("data")
		data, _ := dataV.(*om.Map)
		var keysV any
		if data != nil {
			keysV, _ = data.Get("keys")
		}
		keys, _ := keysV.([]any)
		for _, k := range keys {
			name := dir + fmt.Sprint(k)
			if strings.HasSuffix(name, "/") {
				folders = append(folders, name)
			} else {
				out = append(out, name)
			}
		}
	}
	paths, truncated := capped(out, limit)
	return paths, truncated || len(folders) > 0, nil
}

func (v *vault) Test(ctx context.Context) (string, error) {
	h, err := v.headers(ctx)
	if err != nil {
		return "", err
	}
	info, err := v.call(ctx, http.MethodGet, "/v1/auth/token/lookup-self", h, nil)
	if err != nil {
		return "", err
	}
	dataV, _ := info.Get("data")
	data, _ := dataV.(*om.Map)
	var names []string
	var policiesV any
	if data != nil {
		policiesV, _ = data.Get("policies")
	}
	if l, ok := policiesV.([]any); ok {
		for _, p := range l {
			names = append(names, fmt.Sprint(p))
		}
	}
	policies := strings.Join(names, ", ")
	if policies == "" {
		policies = "none"
	}
	return fmt.Sprintf("Signed in to Vault (policies: %s).", policies), nil
}

// --- AWS Secrets Manager ---

type awsStore struct{ client *secretsmanager.Client }

func newAWS(ctx context.Context, cfg, sec map[string]any) (Client, error) {
	endpoint := str(cfg, "endpoint")
	if endpoint != "" {
		if err := guard(ctx, endpoint); err != nil {
			return nil, err
		}
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(str(cfg, "region"))}
	if ak := optStr(sec, "access_key_id"); ak != nil && *ak != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*ak, derefStr(optStr(sec, "secret_access_key")), "")))
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, errf("Cannot reach AWS Secrets Manager: %v", err)
	}
	return &awsStore{secretsmanager.NewFromConfig(ac, func(o *secretsmanager.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})}, nil
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func (a *awsStore) Read(ctx context.Context, path string) (string, error) {
	res, err := a.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(path)})
	if err != nil {
		var nf *smtypes.ResourceNotFoundException
		if errors.As(err, &nf) {
			return "", errf("AWS Secrets Manager has no secret '%s'.", path)
		}
		var ae smithy.APIError
		if errors.As(err, &ae) {
			code := ae.ErrorCode()
			if code == "" {
				code = "error"
			}
			return "", errf("AWS Secrets Manager refused: %s.", code)
		}
		return "", errf("Cannot reach AWS Secrets Manager: %v", err)
	}
	if res.SecretString != nil {
		return *res.SecretString, nil
	}
	return string(res.SecretBinary), nil
}

func (a *awsStore) List(ctx context.Context, limit int) ([]string, bool, error) {
	var out []string
	in := &secretsmanager.ListSecretsInput{MaxResults: aws.Int32(100)}
	for {
		res, err := a.client.ListSecrets(ctx, in)
		if err != nil {
			var ae smithy.APIError
			if errors.As(err, &ae) {
				return nil, false, errf("AWS Secrets Manager refused to list secrets: %s.", ae.ErrorCode())
			}
			return nil, false, errf("Cannot reach AWS Secrets Manager: %v", err)
		}
		for _, s := range res.SecretList {
			if s.Name != nil {
				out = append(out, *s.Name)
			}
		}
		if res.NextToken == nil || len(out) > limit {
			paths, truncated := capped(out, limit)
			return paths, truncated || res.NextToken != nil, nil
		}
		in.NextToken = res.NextToken
	}
}

func (a *awsStore) Test(ctx context.Context) (string, error) {
	if _, err := a.client.ListSecrets(ctx, &secretsmanager.ListSecretsInput{MaxResults: aws.Int32(1)}); err != nil {
		return "", errf("AWS Secrets Manager: %v", err)
	}
	return "Connected to AWS Secrets Manager.", nil
}

// --- Google Secret Manager ---

type gcpStore struct {
	projectID string
	keyJSON   *string
	// endpoint replaces Google's API (a private endpoint or an emulator); empty: Google itself.
	endpoint string
}

const gcpAPI = "https://secretmanager.googleapis.com/v1"

func (g *gcpStore) token(ctx context.Context) (string, error) {
	scopes := []string{"https://www.googleapis.com/auth/cloud-platform"}
	var creds *google.Credentials
	var err error
	// The messages name the failure like Python's google-auth exception types did.
	if g.keyJSON != nil && *g.keyJSON != "" {
		creds, err = google.CredentialsFromJSON(ctx, []byte(*g.keyJSON), scopes...) //nolint:staticcheck // the user's own key
		if err != nil {
			return "", &Error{"Cannot sign in to Google Cloud: ValueError"}
		}
	} else {
		creds, err = google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			return "", &Error{"Cannot sign in to Google Cloud: DefaultCredentialsError"}
		}
	}
	tok, err := creds.TokenSource.Token()
	if err != nil {
		return "", &Error{"Cannot sign in to Google Cloud: RefreshError"}
	}
	return tok.AccessToken, nil
}

func (g *gcpStore) get(ctx context.Context, path string) (map[string]any, error) {
	base, hc := gcpAPI, &http.Client{Timeout: timeout}
	if g.endpoint != "" {
		// A user-supplied address: every dial goes through the guard.
		base, hc = strings.TrimRight(g.endpoint, "/")+"/v1", ssrf.HTTPClient(timeout)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/"+path, nil)
	if err != nil {
		return nil, errf("Cannot reach Google Secret Manager: %v", err)
	}
	// An endpoint without a key is an emulator: it takes no sign-in.
	if g.endpoint == "" || (g.keyJSON != nil && *g.keyJSON != "") {
		tok, err := g.token(ctx)
		if err != nil {
			return nil, err
		}
		req.Header.Set("authorization", "Bearer "+tok)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, errf("Cannot reach Google Secret Manager: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil, &Error{"Google Secret Manager has no such secret or version."}
	case resp.StatusCode == http.StatusForbidden:
		return nil, &Error{"Google Secret Manager denied access (needs roles/secretmanager.secretAccessor)."}
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return nil, errf("Google Secret Manager answered HTTP %d.", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}
	return body, nil
}

func (g *gcpStore) Read(ctx context.Context, path string) (string, error) {
	name, version, _ := strings.Cut(path, "/versions/")
	if version == "" {
		version = "latest"
	}
	body, err := g.get(ctx, fmt.Sprintf("projects/%s/secrets/%s/versions/%s:access", g.projectID, pathEscape(name), version))
	if err != nil {
		return "", err
	}
	payload, _ := body["payload"].(map[string]any)
	data, _ := payload["data"].(string)
	raw, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (g *gcpStore) List(ctx context.Context, limit int) ([]string, bool, error) {
	var out []string
	prefix := "projects/" + g.projectID + "/secrets/"
	token := ""
	for {
		q := url.Values{"pageSize": {"250"}}
		if token != "" {
			q.Set("pageToken", token)
		}
		body, err := g.get(ctx, fmt.Sprintf("projects/%s/secrets?%s", g.projectID, q.Encode()))
		if err != nil {
			return nil, false, err
		}
		list, _ := body["secrets"].([]any)
		for _, item := range list {
			m, _ := item.(map[string]any)
			if name, _ := m["name"].(string); name != "" {
				out = append(out, strings.TrimPrefix(name, prefix))
			}
		}
		token, _ = body["nextPageToken"].(string)
		if token == "" || len(out) > limit {
			paths, truncated := capped(out, limit)
			return paths, truncated || token != "", nil
		}
	}
}

func (g *gcpStore) Test(ctx context.Context) (string, error) {
	if _, err := g.get(ctx, fmt.Sprintf("projects/%s/secrets?pageSize=1", g.projectID)); err != nil {
		return "", err
	}
	return fmt.Sprintf("Connected to Secret Manager in %s.", g.projectID), nil
}

// --- Kubernetes Secrets ---

type k8sStore struct {
	namespace string
	core      kubernetes.Interface
}

func newK8s(cfg, sec map[string]any) (Client, error) {
	namespace := str(cfg, "namespace")
	inCluster, _ := cfg["in_cluster"].(bool)
	var rc *rest.Config
	var err error
	if inCluster {
		rc, err = rest.InClusterConfig()
	} else {
		kc := derefStr(optStr(sec, "kubeconfig"))
		if kc == "" {
			return nil, &Error{"A kubeconfig is required when not using the in-cluster account."}
		}
		if rc, err = kubeconfigREST(kc); err != nil {
			return nil, err
		}
	}
	if err != nil {
		return nil, errf("Cannot configure the Kubernetes client: %v", err)
	}
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, errf("Cannot configure the Kubernetes client: %v", err)
	}
	return &k8sStore{namespace: namespace, core: cs}, nil
}

// kubeconfigREST is the client config of a pasted kubeconfig. Its API server is user-supplied, so
// every dial goes through the SSRF guard (no proxy from the environment); exec and auth-provider
// sign-ins would run a program, which the image doesn't have and BeziSync never runs.
func kubeconfigREST(kc string) (*rest.Config, error) {
	cfg, err := clientcmd.Load([]byte(kc))
	if err != nil {
		return nil, errf("Cannot read the kubeconfig: %v", err)
	}
	name := cfg.CurrentContext
	if name == "" {
		return nil, &Error{"The kubeconfig has no current-context."}
	}
	kctx, ok := cfg.Contexts[name]
	if !ok {
		return nil, errf("The kubeconfig's current-context '%s' doesn't exist.", name)
	}
	if user, ok := cfg.AuthInfos[kctx.AuthInfo]; ok {
		switch {
		case user.Exec != nil:
			return nil, errf("The kubeconfig signs in by running '%s', which BeziSync can't do. Use a ServiceAccount token instead.", user.Exec.Command)
		case user.AuthProvider != nil:
			return nil, errf("The kubeconfig signs in with the '%s' auth provider, which BeziSync can't use. Use a ServiceAccount token instead.", user.AuthProvider.Name)
		}
	}
	rc, err := clientcmd.NewDefaultClientConfig(*cfg, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, errf("Cannot configure the Kubernetes client: %v", err)
	}
	rc.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	rc.Dial = ssrf.DialContext
	return rc, nil
}

func (k *k8sStore) Read(ctx context.Context, path string) (string, error) {
	s, err := k.core.CoreV1().Secrets(k.namespace).Get(ctx, path, metav1.GetOptions{})
	if err != nil {
		switch {
		case k8serrors.IsNotFound(err):
			return "", errf("No Secret '%s' in namespace %s.", path, k.namespace)
		case k8serrors.IsForbidden(err):
			return "", errf("Not allowed to read Secrets in namespace %s.", k.namespace)
		}
		var status k8serrors.APIStatus
		if errors.As(err, &status) {
			return "", errf("Kubernetes answered %d.", status.Status().Code)
		}
		return "", err
	}
	// Python iterated secret.data in the API's key order (sorted by the API server).
	data := om.New()
	for _, key := range sortedKeys(s.Data) {
		data.Set(key, string(s.Data[key]))
	}
	return DumpsUnsorted(data)
}

func sortedKeys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// skippedSecretTypes hold no connection secrets: tokens, Helm releases, image pull secrets.
var skippedSecretTypes = map[string]bool{
	"kubernetes.io/service-account-token": true, "helm.sh/release.v1": true,
	"kubernetes.io/dockerconfigjson": true, "kubernetes.io/dockercfg": true,
}

func (k *k8sStore) List(ctx context.Context, limit int) ([]string, bool, error) {
	var out []string
	opts := metav1.ListOptions{Limit: 250}
	for {
		res, err := k.core.CoreV1().Secrets(k.namespace).List(ctx, opts)
		if err != nil {
			if k8serrors.IsForbidden(err) {
				return nil, false, errf("Not allowed to list Secrets in namespace %s.", k.namespace)
			}
			return nil, false, errf("Cannot list Secrets in %s: %v", k.namespace, err)
		}
		for _, s := range res.Items {
			if !skippedSecretTypes[string(s.Type)] {
				out = append(out, s.Name)
			}
		}
		if res.Continue == "" || len(out) > limit {
			paths, truncated := capped(out, limit)
			return paths, truncated || res.Continue != "", nil
		}
		opts.Continue = res.Continue
	}
}

func (k *k8sStore) Test(ctx context.Context) (string, error) {
	if _, err := k.core.CoreV1().Secrets(k.namespace).List(ctx, metav1.ListOptions{Limit: 1}); err != nil {
		code := 0
		var status k8serrors.APIStatus
		if errors.As(err, &status) {
			code = int(status.Status().Code)
		}
		return "", errf("Cannot list Secrets in %s: HTTP %d.", k.namespace, code)
	}
	return fmt.Sprintf("Can read Secrets in namespace %s.", k.namespace), nil
}

// Kinds are the supported store kinds in display order.
var Kinds = []string{"vault", "aws", "gcp", "kubernetes"}

// Build creates a client for a store (config already validated against its schema).
func Build(ctx context.Context, kind string, cfg, sec map[string]any) (Client, error) {
	switch kind {
	case "vault":
		return &vault{address: str(cfg, "address"), mount: str(cfg, "mount"), namespace: str(cfg, "namespace"),
			authMethod: str(cfg, "auth_method"), approleMount: str(cfg, "approle_mount"),
			token: optStr(sec, "token"), roleID: optStr(sec, "role_id"), secretID: optStr(sec, "secret_id")}, nil
	case "aws":
		return newAWS(ctx, cfg, sec)
	case "gcp":
		endpoint := str(cfg, "endpoint")
		if endpoint != "" {
			if err := guard(ctx, endpoint); err != nil {
				return nil, err
			}
		}
		return &gcpStore{projectID: str(cfg, "project_id"), keyJSON: optStr(sec, "service_account_json"), endpoint: endpoint}, nil
	case "kubernetes":
		return newK8s(cfg, sec)
	}
	return nil, errf("Unknown secret store kind '%s'.", kind)
}
