package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
)

// Amazon SNS sink: one topic per table (the ARN prefix of the account plus the destination;
// FIFO topics end in ".fifo"), published in batches of 10. SNS keeps nothing to read back, so
// BeziSync subscribes a small queue of its own to each topic (bz-preview-<topic>, 15-minute
// retention); the Events tab and the delivery canary peek at it, and deleting the pipeline
// removes it. Delete tombstones are off: SNS refuses empty messages.

type snsSink struct{ BaseSink }

func init() { Register(&snsSink{BaseSink{Title: "Amazon SNS"}}) }

func (*snsSink) Kind() string { return "sink" }
func (*snsSink) Type() string { return "sns" }

type snsConfig struct {
	Region, AccountID, Endpoint, AccessKey, SecretKey string
	CreateTopics                                      bool
}

func snsCfg(conn Resolved) (snsConfig, error) {
	d, _ := Describe("sink", "sns")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return snsConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return snsConfig{}, err
	}
	return snsConfig{Region: Str(cfg, "region"), AccountID: Str(cfg, "account_id"), Endpoint: Str(cfg, "endpoint"),
		AccessKey: Str(cfg, "access_key_id"), SecretKey: Str(sec, "secret_access_key"), CreateTopics: Bool(cfg, "create_topics")}, nil
}

func (c snsConfig) partition() string {
	switch {
	case strings.HasPrefix(c.Region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(c.Region, "us-gov-"):
		return "aws-us-gov"
	}
	return "aws"
}

func (c snsConfig) arnPrefix(service string) string {
	return fmt.Sprintf("arn:%s:%s:%s:%s:", c.partition(), service, c.Region, c.AccountID)
}

func (c snsConfig) topicARN(topic string) string { return c.arnPrefix("sns") + topic }

func (c snsConfig) clients(ctx context.Context) (*sns.Client, *sqs.Client, error) {
	ac, err := awsClientConfig(ctx, c.Region, c.AccessKey, c.SecretKey)
	if err != nil {
		return nil, nil, err
	}
	snsClient := sns.NewFromConfig(ac, func(o *sns.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
	sqsClient := sqs.NewFromConfig(ac, func(o *sqs.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	})
	return snsClient, sqsClient, nil
}

func (s *snsSink) CompileSink(conn Resolved, _ CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := snsCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	props := map[string]string{
		"debezium.sink.type":                 "sns",
		"debezium.sink.sns.region":           cfg.Region,
		"debezium.sink.sns.topic.arn.prefix": cfg.arnPrefix("sns"),
	}
	if cfg.Endpoint != "" {
		props["debezium.sink.sns.endpoint"] = cfg.Endpoint
	}
	return props, awsEnv(cfg.Region, cfg.AccessKey, cfg.SecretKey), nil
}

func (s *snsSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	cfg, err := snsCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	snsClient, sqsClient, err := cfg.clients(ctx)
	if err == nil {
		_, err = snsClient.ListTopics(ctx, &sns.ListTopicsInput{})
	}
	if err != nil {
		awsFailure(run, err, "sns", "SNS", "Allow sns:ListTopics, sns:CreateTopic, sns:GetTopicAttributes, sns:Publish, sns:Subscribe and sns:Unsubscribe "+
			"on the topics, and sqs:CreateQueue, sqs:SetQueueAttributes, sqs:GetQueueAttributes, sqs:ReceiveMessage and sqs:DeleteQueue on bz-preview-* queues.")
		return checks.Summarize(run.Checks, started), nil
	}
	run.Passed("sns_auth", "Sign in", checks.CatAuth, "AWS accepted the credentials for SNS.", "")
	if _, err := sqsClient.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: aws.String("bz-preview-"), MaxResults: aws.Int32(1)}); err != nil {
		run.Add(checks.Check{Code: "sns_preview", Title: "Events tab", Status: checks.Warn, Category: checks.CatPermissions,
			Message: "BeziSync cannot use SQS with these credentials: the Events tab and the delivery canary stay empty.", Technical: firstRunes(err.Error(), 300),
			Fix: &checks.Fix{Kind: "text", Who: "AWS admin", Code: "Allow sqs:CreateQueue, sqs:SetQueueAttributes, sqs:GetQueueAttributes, sqs:ReceiveMessage and sqs:DeleteQueue on bz-preview-* queues."}})
	} else {
		run.Passed("sns_preview", "Events tab", checks.CatPermissions, "BeziSync can keep a preview queue for the Events tab.", "")
	}
	return checks.Summarize(run.Checks, started), nil
}

// snsPreviewQueue is the preview queue of a topic (SQS names have at most 80 characters).
func snsPreviewQueue(topic string) string {
	base, fifo := strings.CutSuffix(topic, ".fifo")
	name := "bz-preview-" + base
	if len(name) > 75 {
		name = "bz-preview-" + sha256Hex(topic)[:40]
	}
	if fifo {
		name += ".fifo"
	}
	return name
}

// PrepareSink creates missing topics (FIFO ones deduplicate by content: Debezium sends no
// deduplication ID) and subscribes the preview queue to them.
func (s *snsSink) PrepareSink(ctx context.Context, conn Resolved, c CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := snsCfg(conn)
	if err != nil {
		return nil, err
	}
	snsClient, sqsClient, err := cfg.clients(ctx)
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	topics := sortedUnique(destinations)
	for _, topic := range topics {
		arn := cfg.topicARN(topic)
		if _, err := snsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(arn)}); err == nil {
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "exists"})
			continue
		}
		if !cfg.CreateTopics {
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "failed", Detail: "Topic does not exist."})
			continue
		}
		attrs := map[string]string{}
		if strings.HasSuffix(topic, ".fifo") {
			attrs["FifoTopic"], attrs["ContentBasedDeduplication"] = "true", "true"
		}
		if _, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(topic), Attributes: attrs}); err != nil {
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "failed", Detail: firstRunes(err.Error(), 200)})
			continue
		}
		results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "created"})
	}
	for _, topic := range topics {
		results = append(results, snsSubscribePreview(ctx, cfg, snsClient, sqsClient, topic))
	}
	return results, nil
}

func snsSubscribePreview(ctx context.Context, cfg snsConfig, snsClient *sns.Client, sqsClient *sqs.Client, topic string) ArtifactResult {
	name := snsPreviewQueue(topic)
	fifo := strings.HasSuffix(topic, ".fifo")
	fail := func(err error) ArtifactResult {
		return ArtifactResult{Kind: "preview queue", Name: name, Outcome: "failed", Detail: "The Events tab stays empty: " + firstRunes(err.Error(), 200)}
	}
	queueARN := cfg.arnPrefix("sqs") + name
	arn := cfg.topicARN(topic)
	policy, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
		"Effect": "Allow", "Principal": map[string]string{"Service": "sns.amazonaws.com"}, "Action": "sqs:SendMessage", "Resource": queueARN,
		"Condition": map[string]any{"ArnEquals": map[string]any{"aws:SourceArn": arn}}}}})
	attrs := map[string]string{"MessageRetentionPeriod": "900", "Policy": string(policy)}
	if fifo {
		attrs["FifoQueue"], attrs["ContentBasedDeduplication"] = "true", "true"
	}
	q, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
	if err != nil {
		return fail(err)
	}
	if _, err := sqsClient.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: q.QueueUrl, Attributes: map[string]string{"Policy": string(policy)}}); err != nil {
		return fail(err)
	}
	if _, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(arn), Protocol: aws.String("sqs"), Endpoint: aws.String(queueARN)}); err != nil {
		return fail(err)
	}
	return ArtifactResult{Kind: "preview queue", Name: name, Outcome: "created", Detail: "The Events tab reads the last 15 minutes of " + topic + " from it."}
}

func (s *snsSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := snsCfg(conn)
	if err != nil {
		return nil, err
	}
	snsClient, _, err := cfg.clients(ctx)
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	for _, topic := range sortedUnique(destinations) {
		if _, err := snsClient.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(cfg.topicARN(topic))}); err != nil {
			results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "failed", Detail: firstRunes(err.Error(), 200)})
			continue
		}
		results = append(results, ArtifactResult{Kind: "topic", Name: topic, Outcome: "removed"})
	}
	return results, nil
}

// ReleaseSink removes the preview queues and their subscriptions.
func (s *snsSink) ReleaseSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := snsCfg(conn)
	if err != nil {
		return nil, err
	}
	snsClient, sqsClient, err := cfg.clients(ctx)
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	for _, topic := range sortedUnique(destinations) {
		name := snsPreviewQueue(topic)
		queueARN := cfg.arnPrefix("sqs") + name
		if subs, err := snsClient.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{TopicArn: aws.String(cfg.topicARN(topic))}); err == nil {
			for _, sub := range subs.Subscriptions {
				if aws.ToString(sub.Endpoint) == queueARN {
					_, _ = snsClient.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: sub.SubscriptionArn})
				}
			}
		}
		q, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
		var missing *sqstypes.QueueDoesNotExist
		switch {
		case errors.As(err, &missing):
			results = append(results, ArtifactResult{Kind: "preview queue", Name: name, Outcome: "not_found"})
		case err != nil:
			results = append(results, ArtifactResult{Kind: "preview queue", Name: name, Outcome: "failed", Detail: firstRunes(err.Error(), 200)})
		default:
			if _, err := sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: q.QueueUrl}); err != nil {
				results = append(results, ArtifactResult{Kind: "preview queue", Name: name, Outcome: "failed", Detail: firstRunes(err.Error(), 200)})
			} else {
				results = append(results, ArtifactResult{Kind: "preview queue", Name: name, Outcome: "removed"})
			}
		}
	}
	return results, nil
}

// Tail peeks at the preview queue (SNS's envelope names the topic) for this destination.
func (s *snsSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := snsCfg(conn)
	if err != nil {
		return nil, err
	}
	_, sqsClient, err := cfg.clients(ctx)
	if err != nil {
		return nil, err
	}
	q, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(snsPreviewQueue(destination))})
	if err != nil {
		var missing *sqstypes.QueueDoesNotExist
		if errors.As(err, &missing) {
			return nil, &ErrNotSupported{"No preview queue for " + destination + ": deploy the pipeline again (BeziSync needs SQS rights) to see events."}
		}
		return nil, err
	}
	queueURL := aws.ToString(q.QueueUrl)
	arn := cfg.topicARN(destination)
	return sqsPeek(ctx, sqsClient, queueURL, destination, limit, timeout, func(body string) (any, *string, bool) {
		var env struct {
			TopicArn, Message, Timestamp string
		}
		if json.Unmarshal([]byte(body), &env) != nil || env.TopicArn != arn {
			return nil, nil, false
		}
		var value any = env.Message
		if v, err := decodeJSON([]byte(env.Message)); err == nil {
			value = v
		}
		ts := env.Timestamp
		return value, &ts, true
	})
}
