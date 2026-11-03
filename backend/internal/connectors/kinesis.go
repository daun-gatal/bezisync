package connectors

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/aws/smithy-go"

	"github.com/daun-gatal/bezisync/backend/internal/checks"
	"github.com/daun-gatal/bezisync/backend/internal/pytime"
)

// Amazon Kinesis Data Streams sink: one stream per table.

type kinesisSink struct{ BaseSink }

func init() { Register(&kinesisSink{BaseSink{Title: "AWS Kinesis"}}) }

func (*kinesisSink) Kind() string { return "sink" }
func (*kinesisSink) Type() string { return "kinesis" }

type kinesisConfig struct {
	Region              string
	AccessKey, Endpoint *string
	CreateStreams       bool
	Shards              int
	SecretKey           *string
}

func kinesisCfg(conn Resolved) (kinesisConfig, error) {
	d, _ := Describe("sink", "kinesis")
	cfg, err := ValidateConfig(d.ConfigSchema, conn.Config)
	if err != nil {
		return kinesisConfig{}, err
	}
	sec, err := ValidateConfig(d.SecretSchema, conn.Secrets)
	if err != nil {
		return kinesisConfig{}, err
	}
	return kinesisConfig{Region: Str(cfg, "region"), AccessKey: OptStr(cfg, "access_key_id"), Endpoint: OptStr(cfg, "endpoint"),
		CreateStreams: Bool(cfg, "create_streams"), Shards: Int(cfg, "shards"), SecretKey: OptStr(sec, "secret_access_key")}, nil
}

func kinesisClient(ctx context.Context, cfg kinesisConfig) (*kinesis.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
		awsconfig.WithHTTPClient(&http.Client{Timeout: 15 * time.Second}),
		awsconfig.WithRetryer(func() aws.Retryer { return retry.AddWithMaxAttempts(retry.NewStandard(), 2) }),
	}
	if ak := orEmpty(cfg.AccessKey); ak != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(ak, orEmpty(cfg.SecretKey), "")))
	}
	ac, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return kinesis.NewFromConfig(ac, func(o *kinesis.Options) {
		if ep := orEmpty(cfg.Endpoint); ep != "" {
			o.BaseEndpoint = aws.String(ep)
		}
	}), nil
}

func apiErrorCode(err error) (string, bool) {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode(), true
	}
	return "", false
}

func (s *kinesisSink) Check(ctx context.Context, conn Resolved, _ []string) (checks.Report, error) {
	started := time.Now()
	cfg, err := kinesisCfg(conn)
	if err != nil {
		return checks.Report{}, err
	}
	run := &checks.Run{}
	err = func() error {
		kc, err := kinesisClient(ctx, cfg)
		if err != nil {
			return err
		}
		_, err = kc.ListStreams(ctx, &kinesis.ListStreamsInput{Limit: aws.Int32(1)})
		return err
	}()
	if err == nil {
		run.Passed("kinesis_auth", "Sign in", checks.CatAuth, "AWS accepted the credentials for Kinesis.", "")
		return checks.Summarize(run.Checks, started), nil
	}
	if code, ok := apiErrorCode(err); ok {
		msg := fmt.Sprintf("Kinesis refused the request (%s).", code)
		if strings.Contains(code, "Denied") || strings.Contains(code, "Unrecognized") || strings.Contains(code, "Invalid") {
			msg = "AWS rejected the request."
		}
		run.Add(checks.Check{Code: "kinesis_auth", Title: "Sign in", Status: checks.Fail, Category: checks.CatAuth, Blocking: true,
			Message: msg, Technical: firstRunes(err.Error(), 300),
			Fix: &checks.Fix{Kind: "text", Who: "AWS admin", Code: "Allow kinesis:ListStreams, kinesis:DescribeStreamSummary, kinesis:CreateStream, " +
				"kinesis:PutRecord, kinesis:PutRecords for the BeziSync and runtime principals."}})
	} else {
		run.Add(checks.Check{Code: "kinesis_connect", Title: "Reach Kinesis", Status: checks.Fail, Category: checks.CatNetwork, Blocking: true,
			Message: "Could not reach the Kinesis endpoint.", Technical: firstRunes(err.Error(), 300)})
	}
	return checks.Summarize(run.Checks, started), nil
}

func (s *kinesisSink) CompileSink(conn Resolved, _ CompileContext) (map[string]string, map[string]string, error) {
	cfg, err := kinesisCfg(conn)
	if err != nil {
		return nil, nil, err
	}
	props := map[string]string{
		"debezium.sink.type":             "kinesis",
		"debezium.sink.kinesis.region":   cfg.Region,
		"debezium.sink.kinesis.null.key": "null",
	}
	if ep := orEmpty(cfg.Endpoint); ep != "" {
		props["debezium.sink.kinesis.endpoint"] = ep
	}
	env := map[string]string{}
	if ak, sk := orEmpty(cfg.AccessKey), orEmpty(cfg.SecretKey); ak != "" && sk != "" {
		env = map[string]string{"AWS_ACCESS_KEY_ID": ak, "AWS_SECRET_ACCESS_KEY": sk, "AWS_REGION": cfg.Region}
	}
	return props, env, nil
}

func (s *kinesisSink) PrepareSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := kinesisCfg(conn)
	if err != nil {
		return nil, err
	}
	kc, err := kinesisClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	for _, stream := range sortedUnique(destinations) {
		_, err := kc.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{StreamName: aws.String(stream)})
		if err == nil {
			results = append(results, ArtifactResult{Kind: "stream", Name: stream, Outcome: "exists"})
			continue
		}
		var nf *types.ResourceNotFoundException
		if !errors.As(err, &nf) {
			if _, ok := apiErrorCode(err); !ok {
				return nil, err
			}
			results = append(results, ArtifactResult{Kind: "stream", Name: stream, Outcome: "failed", Detail: firstRunes(err.Error(), 200)})
			continue
		}
		if !cfg.CreateStreams {
			results = append(results, ArtifactResult{Kind: "stream", Name: stream, Outcome: "failed", Detail: "Stream does not exist."})
			continue
		}
		if _, err := kc.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String(stream), ShardCount: aws.Int32(int32(cfg.Shards))}); err != nil {
			return nil, err
		}
		waiter := kinesis.NewStreamExistsWaiter(kc, func(o *kinesis.StreamExistsWaiterOptions) {
			o.MinDelay, o.MaxDelay = 2*time.Second, 2*time.Second
		})
		if err := waiter.Wait(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(stream)}, 60*time.Second); err != nil {
			return nil, err
		}
		results = append(results, ArtifactResult{Kind: "stream", Name: stream, Outcome: "created"})
	}
	return results, nil
}

func (s *kinesisSink) CleanupSink(ctx context.Context, conn Resolved, _ CompileContext, destinations map[string]string) ([]ArtifactResult, error) {
	cfg, err := kinesisCfg(conn)
	if err != nil {
		return nil, err
	}
	kc, err := kinesisClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var results []ArtifactResult
	for _, stream := range sortedUnique(destinations) {
		_, err := kc.DeleteStream(ctx, &kinesis.DeleteStreamInput{StreamName: aws.String(stream), EnforceConsumerDeletion: aws.Bool(true)})
		if err != nil {
			if _, ok := apiErrorCode(err); !ok {
				return nil, err
			}
			results = append(results, ArtifactResult{Kind: "stream", Name: stream, Outcome: "not_found", Detail: firstRunes(err.Error(), 200)})
			continue
		}
		results = append(results, ArtifactResult{Kind: "stream", Name: stream, Outcome: "removed"})
	}
	return results, nil
}

func (s *kinesisSink) Tail(ctx context.Context, conn Resolved, destination string, limit int, timeout time.Duration) ([]TailRecord, error) {
	cfg, err := kinesisCfg(conn)
	if err != nil {
		return nil, err
	}
	kc, err := kinesisClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	shards, err := kc.ListShards(ctx, &kinesis.ListShardsInput{StreamName: aws.String(destination)})
	if err != nil {
		return nil, err
	}
	var records []TailRecord
	deadline := time.Now().Add(timeout)
	for _, shard := range shards.Shards {
		it, err := kc.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{StreamName: aws.String(destination), ShardId: shard.ShardId,
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon})
		if err != nil {
			return nil, err
		}
		iterator := it.ShardIterator
		for iterator != nil && *iterator != "" && time.Now().Before(deadline) {
			page, err := kc.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: iterator, Limit: aws.Int32(1000)})
			if err != nil {
				return nil, err
			}
			for _, r := range page.Records {
				var value any
				if v, err := decodeJSON(r.Data); err == nil {
					value = v
				} else {
					value = decodeText(r.Data)
				}
				var ts *string
				if r.ApproximateArrivalTimestamp != nil {
					t := pytime.ISOFormat(*r.ApproximateArrivalTimestamp)
					ts = &t
				}
				var key any
				if r.PartitionKey != nil {
					key = *r.PartitionKey
				}
				records = append(records, TailRecord{Destination: destination, Key: key, Value: value, Timestamp: ts})
			}
			if len(page.Records) == 0 || page.MillisBehindLatest == nil || *page.MillisBehindLatest == 0 {
				break
			}
			iterator = page.NextShardIterator
		}
	}
	sort.SliceStable(records, func(i, j int) bool { return orEmpty(records[i].Timestamp) < orEmpty(records[j].Timestamp) })
	if len(records) > limit {
		records = records[len(records)-limit:]
	}
	if records == nil {
		records = []TailRecord{}
	}
	return records, nil
}
