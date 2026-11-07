package connectors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Amazon SQS sink: every table goes to one queue (Debezium's sink ignores the destination), one
// SendMessage per event. Consumers tell tables apart by the event (source.table, or __table with
// the row-after-change shape). Delete tombstones are off: SQS refuses empty messages.

type sqsSink struct{ BaseSink }

func init() { Register(&sqsSink{BaseSink{Title: "Amazon SQS"}}) }

func (*sqsSink) Kind() string { return "sink" }
func (*sqsSink) Type() string { return "sqs" }

type sqsConfig struct {
	Region, QueueURL, Endpoint, AccessKey, SecretKey string
}

func sqsCfg(conn Resolved) (sqsConfig, error) {
	d, _ := Describe("sink", "sqs")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return sqsConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return sqsConfig{}, err
	}
	return sqsConfig{Region: Str(cfg, "region"), QueueURL: Str(cfg, "queue_url"), Endpoint: Str(cfg, "endpoint"),
		AccessKey: Str(cfg, "access_key_id"), SecretKey: Str(sec, "secret_access_key")}, nil
}

// awsClientConfig is an AWS SDK configuration for checks and previews: service calls through
// the SSRF guard (GuardAWS), two attempts, and static keys when given (else the default chain:
// IAM role, env).
func awsClientConfig(ctx context.Context, region, accessKey, secretKey string) (aws.Config, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
		awsconfig.WithRetryer(func() aws.Retryer { return retry.AddWithMaxAttempts(retry.NewStandard(), 2) }),
	}
	if accessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	return GuardAWS(ac, 15*time.Second), err
}

func (c sqsConfig) client(ctx context.Context) (*sqs.Client, error) {
	ac, err := awsClientConfig(ctx, c.Region, c.AccessKey, c.SecretKey)
	if err != nil {
		return nil, err
	}
	return sqs.NewFromConfig(ac, func(o *sqs.Options) {
		if c.Endpoint != "" {
			o.BaseEndpoint = aws.String(c.Endpoint)
		}
	}), nil
}

// queueName is the last segment of the queue URL.
func (c sqsConfig) queueName() string {
	u, err := url.Parse(c.QueueURL)
	if err != nil {
		return c.QueueURL
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	return parts[len(parts)-1]
}

func awsEnv(region, accessKey, secretKey string) map[string]string {
	if accessKey == "" {
		return map[string]string{"AWS_REGION": region}
	}
	return map[string]string{"AWS_ACCESS_KEY_ID": accessKey, "AWS_SECRET_ACCESS_KEY": secretKey, "AWS_REGION": region}
}

func (s *sqsSink) CompileSink(conn Resolved, _ CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := sqsCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	props := map[string]string{
		"debezium.sink.type":          "sqs",
		"debezium.sink.sqs.region":    cfg.Region,
		"debezium.sink.sqs.queue.url": cfg.QueueURL,
		// The sink logs every event at INFO: row data must not reach the logs.
		`quarkus.log.category."io.debezium.server.sqs".level`: "WARN",
	}
	if cfg.Endpoint != "" {
		props["debezium.sink.sqs.endpoint"] = cfg.Endpoint
	}
	return props, awsEnv(cfg.Region, cfg.AccessKey, cfg.SecretKey), nil
}

// Destination: every table goes to the queue.
func (s *sqsSink) Destination(conn Resolved, _ CompileContext, _ string) string {
	cfg := sqsConfig{QueueURL: Str(conn.Config, "queue_url")}
	return cfg.queueName()
}

type sqsQueue struct {
	fifo, contentDedup bool
	maxBytes           int
	maxReceives        int // 0: no redrive policy
}

func (c sqsConfig) attributes(ctx context.Context, client *sqs.Client) (sqsQueue, error) {
	out, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: aws.String(c.QueueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll}})
	if err != nil {
		return sqsQueue{}, err
	}
	a := out.Attributes
	q := sqsQueue{fifo: a["FifoQueue"] == "true", contentDedup: a["ContentBasedDeduplication"] == "true"}
	q.maxBytes, _ = strconv.Atoi(a["MaximumMessageSize"])
	if rp := a["RedrivePolicy"]; rp != "" {
		var policy struct {
			MaxReceiveCount json.Number `json:"maxReceiveCount"`
		}
		if json.Unmarshal([]byte(rp), &policy) == nil {
			n, _ := policy.MaxReceiveCount.Int64()
			q.maxReceives = int(n)
		}
	}
	return q, nil
}

// sqsPreviewMinReceives: peeking adds one receive per look, so a dead-letter redrive after fewer
// receives could move messages early.
const sqsPreviewMinReceives = 10

func (s *sqsSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	cfg, err := sqsCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	client, err := cfg.client(ctx)
	var q sqsQueue
	if err == nil {
		q, err = cfg.attributes(ctx, client)
	}
	if err != nil {
		awsFailure(run, err, "sqs", "SQS", "Allow sqs:GetQueueAttributes, sqs:SendMessage and sqs:ReceiveMessage on the queue for the BeziSync and runtime principals.")
		return checks.Summarize(run.Checks, started), nil
	}
	run.Passed("sqs_queue", "Queue", checks.CatDestination, fmt.Sprintf("Queue %s exists; messages up to %d KiB.", cfg.queueName(), q.maxBytes/1024), "")
	if q.fifo && !q.contentDedup {
		run.Add(checks.Check{Code: "sqs_fifo_dedup", Title: "FIFO deduplication", Status: checks.Fail, Category: checks.CatDestination, Blocking: true,
			Message: "The FIFO queue does not deduplicate by content; Debezium sends no deduplication ID, so every message would be refused.",
			Fix:     &checks.Fix{Kind: "text", Who: "AWS admin", Code: fmt.Sprintf("aws sqs set-queue-attributes --queue-url %s --attributes ContentBasedDeduplication=true", cfg.QueueURL)}})
	}
	if q.maxReceives > 0 && q.maxReceives < sqsPreviewMinReceives {
		run.Add(checks.Check{Code: "sqs_preview", Title: "Events tab", Status: checks.Warn, Category: checks.CatDestination,
			Message: fmt.Sprintf("The queue moves messages to its dead-letter queue after %d receives, so the Events tab does not peek at it.", q.maxReceives),
			Why:     "Each look at the queue counts as a receive."})
	}
	return checks.Summarize(run.Checks, started), nil
}

// awsFailure records a failed AWS call as a sign-in or reachability failure.
func awsFailure(run *checks.Run, err error, prefix, service, grant string) {
	if code, ok := apiErrorCode(err); ok {
		msg := fmt.Sprintf("%s refused the request (%s).", service, code)
		if strings.Contains(code, "Denied") || strings.Contains(code, "Unrecognized") || strings.Contains(code, "Invalid") || strings.Contains(code, "Signature") {
			msg = "AWS rejected the request."
		}
		if strings.Contains(code, "NonExistent") || strings.Contains(code, "NotFound") {
			msg = "The queue or topic does not exist."
		}
		run.Add(checks.Check{Code: prefix + "_auth", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: msg, Technical: firstRunes(err.Error(), 300), Fix: &checks.Fix{Kind: "text", Who: "AWS admin", Code: grant}})
		return
	}
	run.Add(checks.Check{Code: prefix + "_connect", Title: "Reach " + service, Status: checks.Fail, Category: checks.CatNetwork, Blocking: true,
		Message: "Could not reach the " + service + " endpoint.", Technical: firstRunes(err.Error(), 300)})
}

// Tail peeks at messages still in the queue (visibility timeout 0: they stay available to
// consumers), newest first. It refuses when a low dead-letter redrive count makes peeking unsafe.
func (s *sqsSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := sqsCfg(conn)
	if err != nil {
		return nil, err
	}
	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}
	q, err := cfg.attributes(ctx, client)
	if err != nil {
		return nil, err
	}
	if q.maxReceives > 0 && q.maxReceives < sqsPreviewMinReceives {
		return nil, &ErrNotSupported{fmt.Sprintf("The queue moves messages to its dead-letter queue after %d receives; BeziSync does not peek at it.", q.maxReceives)}
	}
	return sqsPeek(ctx, client, cfg.QueueURL, destination, limit, timeout, nil)
}

// sqsPeek receives without hiding messages, a few times, and returns distinct ones newest first.
// accept filters and unwraps bodies (nil: every body as is).
func sqsPeek(ctx context.Context, client *sqs.Client, queueURL, destination string, limit int, timeout time.Duration,
	accept func(body string) (value any, ts *string, ok bool)) ([]TailRecord, error) {
	deadline := time.Now().Add(max(timeout, 3*time.Second))
	seen := map[string]bool{}
	var out []TailRecord
	for round := 0; round < 5 && len(out) < limit && time.Now().Before(deadline); round++ {
		res, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 10,
			VisibilityTimeout: 0, WaitTimeSeconds: 1, MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameSentTimestamp}})
		if err != nil {
			return nil, err
		}
		fresh := 0
		for _, m := range res.Messages {
			id := aws.ToString(m.MessageId)
			if seen[id] {
				continue
			}
			seen[id], fresh = true, fresh+1
			body := aws.ToString(m.Body)
			var value any
			var ts *string
			if accept != nil {
				v, t, ok := accept(body)
				if !ok {
					continue
				}
				value, ts = v, t
			} else if v, err := decodeJSON([]byte(body)); err == nil {
				value = v
			} else {
				value = body
			}
			if ts == nil {
				if ms, err := strconv.ParseInt(m.Attributes[string(sqstypes.MessageSystemAttributeNameSentTimestamp)], 10, 64); err == nil {
					t := pytime.ISOFormat(time.UnixMilli(ms))
					ts = &t
				}
			}
			out = append(out, TailRecord{Destination: destination, Value: value, Timestamp: ts})
		}
		if fresh == 0 {
			break
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return orEmpty(out[i].Timestamp) < orEmpty(out[j].Timestamp) })
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	if out == nil {
		out = []TailRecord{}
	}
	return out, nil
}
