package awsbus_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/awsbus"
	"github.com/Cidan/ok/internal/bus/bustest"
	"github.com/Cidan/ok/internal/bus/bustest/awsfake"
)

// client is an SQS client of a fake in the test's process.
func client(t *testing.T) *sqs.Client {
	t.Helper()
	endpoint := bustest.AWS(t)
	cfg := aws.Config{Region: awsfake.Region, Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func TestQueue(t *testing.T) {
	q := client(t)
	bustest.RunQueue(t, func(t *testing.T, maxAttempts int) bus.Queue {
		return awsbus.New(q, awsbus.Options{MaxAttempts: maxAttempts})
	}, bustest.QueueOptions{DetectsLostLease: true, Shared: true})
}
