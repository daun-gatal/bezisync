package runtime

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"golang.org/x/oauth2/google"
)

// Bearer tokens for managed Kubernetes (GKE, EKS) in direct mode: BeziSync mints the same
// short-lived tokens the usual exec plugins would (they are not in the image).

// EKSTokenLifetime is how long an EKS token is considered valid.
const EKSTokenLifetime = 15 * time.Minute

// GKETokenSource returns OAuth access tokens from a service-account key or the
// workload's default credentials.
func GKETokenSource(ctx context.Context, serviceAccountJSON string) (TokenSource, error) {
	scopes := []string{"https://www.googleapis.com/auth/cloud-platform"}
	var creds *google.Credentials
	var err error
	if serviceAccountJSON != "" {
		creds, err = google.CredentialsFromJSON(ctx, []byte(serviceAccountJSON), scopes...) //nolint:staticcheck // the runtime's own key
		if err != nil {
			return nil, &Error{"Cannot load Google credentials for GKE: ValueError"}
		}
	} else {
		creds, err = google.FindDefaultCredentials(ctx, scopes...)
		if err != nil {
			return nil, &Error{"Cannot load Google credentials for GKE: DefaultCredentialsError"}
		}
	}
	return func(context.Context) (string, time.Time, error) {
		tok, err := creds.TokenSource.Token()
		if err != nil {
			return "", time.Time{}, &Error{"Cannot get a GKE access token: RefreshError"}
		}
		exp := tok.Expiry
		if exp.IsZero() {
			exp = time.Now().Add(3000 * time.Second)
		}
		return tok.AccessToken, exp, nil
	}, nil
}

// EKSToken is what `aws eks get-token` produces: k8s-aws-v1.<base64url presigned STS URL>.
func EKSToken(ctx context.Context, cluster, region, accessKey, secretKey string) (string, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if accessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return "", Errorf("Cannot sign an EKS token (check AWS credentials): %T", err)
	}
	client := sts.NewFromConfig(cfg, func(o *sts.Options) {
		o.BaseEndpoint = aws.String(fmt.Sprintf("https://sts.%s.amazonaws.com", region))
	})
	presign := sts.NewPresignClient(client, func(po *sts.PresignOptions) {
		po.ClientOptions = append(po.ClientOptions, func(o *sts.Options) {
			o.APIOptions = append(o.APIOptions, smithyhttp.AddHeaderValue("x-k8s-aws-id", cluster), addExpires(60))
		})
	})
	req, err := presign.PresignGetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", &Error{"Cannot sign an EKS token (check AWS credentials): " + errName(err)}
	}
	return "k8s-aws-v1." + strings.TrimRight(base64.URLEncoding.EncodeToString([]byte(req.URL)), "="), nil
}

func errName(err error) string {
	if strings.Contains(err.Error(), "credential") {
		return "NoCredentialsError"
	}
	return "ClientError"
}

// addExpires sets X-Amz-Expires on the presigned URL (ExpiresIn=60 in boto3).
func addExpires(seconds int) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Build.Add(middleware.BuildMiddlewareFunc("bzExpires",
			func(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (middleware.BuildOutput, middleware.Metadata, error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					q := req.URL.Query()
					q.Set("X-Amz-Expires", fmt.Sprint(seconds))
					req.URL.RawQuery = q.Encode()
				}
				return next.HandleBuild(ctx, in)
			}), middleware.After)
	}
}

// EKSTokenSource refreshes EKS tokens.
func EKSTokenSource(cluster, region, accessKey, secretKey string) TokenSource {
	return func(ctx context.Context) (string, time.Time, error) {
		tok, err := EKSToken(ctx, cluster, region, accessKey, secretKey)
		return tok, time.Now().Add(EKSTokenLifetime), err
	}
}
