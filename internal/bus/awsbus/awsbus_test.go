package awsbus_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/awsbus"
	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/bus/bustest/awsfake"
)

// clients are SQS and SNS clients of a fake of both in the test's process.
func clients(t *testing.T) (*sqs.Client, *sns.Client) {
	t.Helper()
	endpoint := bustest.AWS(t)
	cfg := aws.Config{Region: awsfake.Region, Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) }),
		sns.NewFromConfig(cfg, func(o *sns.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func TestStream(t *testing.T) {
	q, n := clients(t)
	bustest.RunStream(t, func(t *testing.T) bus.Stream { return awsbus.New(q, n, awsbus.Options{}) }, bustest.StreamOptions{Concurrent: 8})
}

func TestQueue(t *testing.T) {
	q, n := clients(t)
	bustest.RunQueue(t, func(t *testing.T, maxAttempts int) bus.Queue {
		return awsbus.New(q, n, awsbus.Options{MaxAttempts: maxAttempts})
	}, bustest.QueueOptions{DetectsLostLease: true})
}

func TestSignals(t *testing.T) {
	q, n := clients(t)
	bustest.RunSignals(t, func(t *testing.T) bus.Signals { return awsbus.New(q, n, awsbus.Options{}) })
}
