package connectors

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	pubsubadmin "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/problem"
)

// Google Cloud Pub/Sub sink: one topic per table, ordered per row.

var pubsubTopicRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9\-_.~+%]{2,254}$`)

// PubSubKeyFile is the mounted service-account key's file name.
const PubSubKeyFile = "gcp-service-account.json"

// SecretDir is where secret files are mounted in connector workloads.
const SecretDir = "/debezium/secrets"

type pubsubSink struct{ BaseSink }

func init() { Register(&pubsubSink{BaseSink{Title: "Google Pub/Sub"}}) }

func (*pubsubSink) Kind() string { return "sink" }
func (*pubsubSink) Type() string { return "pubsub" }

type pubsubConfig struct {
	ProjectID              string
	Ordering, CreateTopics bool
	Emulator, KeyJSON      *string
}

func pubsubCfg(conn Resolved) (pubsubConfig, error) {
	d, _ := Describe("sink", "pubsub")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return pubsubConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return pubsubConfig{}, err
	}
	return pubsubConfig{ProjectID: Str(cfg, "project_id"), Ordering: Bool(cfg, "ordering"), CreateTopics: Bool(cfg, "create_topics"),
		Emulator: OptStr(cfg, "emulator_host"), KeyJSON: OptStr(sec, "service_account_json")}, nil
}

func pubsubPublisher(ctx context.Context, cfg pubsubConfig) (*pubsubadmin.TopicAdminClient, error) {
	var opts []option.ClientOption
	switch {
	case orEmpty(cfg.Emulator) != "":
		opts = append(opts, option.WithEndpoint(*cfg.Emulator), option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
			option.WithGRPCDialOption(grpc.WithContextDialer(grpcDial))) // the SSRF guard (ADR 0004)
	case orEmpty(cfg.KeyJSON) != "":
		opts = append(opts, option.WithCredentialsJSON([]byte(*cfg.KeyJSON))) //nolint:staticcheck // the key is the user's own credential
	}
	return pubsubadmin.NewTopicAdminClient(ctx, opts...)
}

func listTopics(ctx context.Context, pub *pubsubadmin.TopicAdminClient, project string) ([]string, error) {
	it := pub.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: "projects/" + project})
	var out []string
	for {
		t, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		name := t.GetName()
		out = append(out, name[strings.LastIndex(name, "/")+1:])
	}
}

// PubSubCheckTimeout bounds the Pub/Sub check's sign-in and topic listing (tests shorten it).
var PubSubCheckTimeout = 10 * time.Second

func (s *pubsubSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	cfg, err := pubsubCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	err = func() error {
		lctx, cancel := context.WithTimeout(ctx, PubSubCheckTimeout)
		defer cancel()
		pub, err := pubsubPublisher(lctx, cfg)
		if err != nil {
			return err
		}
		defer func() { _ = pub.Close() }()
		_, err = listTopics(lctx, pub, cfg.ProjectID)
		return err
	}()
	if err == nil {
		run.Passed("pubsub_auth", "Sign in", checks.CatAuth, fmt.Sprintf("Can list topics in %s.", cfg.ProjectID), "")
	} else {
		text := err.Error()
		denied := strings.Contains(text, "403") || strings.Contains(text, "PermissionDenied") || strings.Contains(strings.ToLower(text), "permission")
		cat, msg := checks.CatNetwork, "Could not reach Pub/Sub."
		if denied {
			cat, msg = checks.CatPermissions, "Google Cloud denied access to Pub/Sub."
		}
		run.Add(checks.Check{Code: "pubsub_auth", Title: "Sign in", Status: checks.Fail, Category: cat, Blocking: true,
			Message: msg, Technical: firstRunes(text, 300),
			Fix: &checks.Fix{Kind: "shell", Who: "GCP admin", Code: fmt.Sprintf("gcloud projects add-iam-policy-binding %s ", cfg.ProjectID) +
				"--member=serviceAccount:<sa-email> --role=roles/pubsub.editor"}})
	}
	return checks.Summarize(run.Checks, started), nil
}

func (s *pubsubSink) CompileSink(conn Resolved, c CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := pubsubCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	var bad []string
	for _, t := range c.Spec.Source.Tables.Include {
		schema, table, _ := strings.Cut(t, ".")
		if d := c.Destination(schema, table); !pubsubTopicRe.MatchString(d) {
			bad = append(bad, d)
		}
	}
	if len(bad) > 0 {
		return nil, nil, problem.New(422, "invalid_destination", fmt.Sprintf("Not valid Pub/Sub topic names: %s.", strings.Join(first(bad, 5), ", ")))
	}
	props := map[string]string{
		"debezium.sink.type":                    "pubsub",
		"debezium.sink.pubsub.project.id":       cfg.ProjectID,
		"debezium.sink.pubsub.ordering.enabled": boolStr(cfg.Ordering),
		"debezium.sink.pubsub.null.key":         "null",
	}
	env := map[string]string{}
	if e := orEmpty(cfg.Emulator); e != "" {
		props["debezium.sink.pubsub.address"] = e
	} else if orEmpty(cfg.KeyJSON) != "" {
		env["GOOGLE_APPLICATION_CREDENTIALS"] = SecretDir + "/" + PubSubKeyFile
	}
	return props, env, nil
}

func (s *pubsubSink) SecretFiles(conn Resolved) (map[string]string, error) {
	cfg, err := pubsubCfg(conn)
	if err != nil {
		return nil, err
	}
	if k := orEmpty(cfg.KeyJSON); k != "" {
		return map[string]string{PubSubKeyFile: k}, nil
	}
	return map[string]string{}, nil
}

func (s *pubsubSink) PrepareSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := pubsubCfg(conn)
	if err != nil {
		return nil, err
	}
	pub, err := pubsubPublisher(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = pub.Close() }()
	names, err := listTopics(ctx, pub, cfg.ProjectID)
	if err != nil {
		return nil, err
	}
	existing := map[string]bool{}
	for _, n := range names {
		existing[n] = true
	}
	var results []ArtifactResult
	for _, topic := range sortedUnique(destinations) {
		switch {
		case existing[topic]:
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "exists"})
		case cfg.CreateTopics:
			if _, err := pub.CreateTopic(ctx, &pubsubpb.Topic{Name: fmt.Sprintf("projects/%s/topics/%s", cfg.ProjectID, topic)}); err != nil {
				return nil, err
			}
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "created"})
		default:
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "failed", Detail: "Topic does not exist."})
		}
	}
	return results, nil
}

func (s *pubsubSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := pubsubCfg(conn)
	if err != nil {
		return nil, err
	}
	pub, err := pubsubPublisher(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer func() { _ = pub.Close() }()
	var results []ArtifactResult
	for _, topic := range sortedUnique(destinations) {
		err := pub.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: fmt.Sprintf("projects/%s/topics/%s", cfg.ProjectID, topic)})
		if err != nil {
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "not_found", Detail: firstRunes(err.Error(), 200)})
			continue
		}
		results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "removed"})
	}
	return results, nil
}
