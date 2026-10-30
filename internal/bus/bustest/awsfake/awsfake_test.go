package awsfake_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Cidan/ok/internal/bus/bustest/awsfake"
)

func client(t *testing.T) (*sqs.Client, *awsfake.Server) {
	t.Helper()
	s, err := awsfake.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	cfg := aws.Config{Region: awsfake.Region, Credentials: credentials.NewStaticCredentialsProvider("k", "s", "")}
	return sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(s.URL) }), s
}

func TestSQS(t *testing.T) {
	ctx := context.Background()
	q, srv := client(t)
	if _, err := q.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("none")}); !errors.As(err, new(*sqstypes.QueueDoesNotExist)) {
		t.Fatalf("a missing queue: %v", err)
	}
	dead, err := q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("dead")})
	if err != nil {
		t.Fatal(err)
	}
	da, err := q.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: dead.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"}})
	if err != nil {
		t.Fatal(err)
	}
	cq, err := q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("work"), Attributes: map[string]string{
		"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":"2"}`, da.Attributes["QueueArn"])}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: cq.QueueUrl, Entries: []sqstypes.SendMessageBatchRequestEntry{
		{Id: aws.String("0"), MessageBody: aws.String("hello"), MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"b": {DataType: aws.String("Binary"), BinaryValue: []byte{0, 1, 2, 255}}, "k": {DataType: aws.String("Number"), StringValue: aws.String("42")}}},
		{Id: aws.String("1"), MessageBody: aws.String("later"), DelaySeconds: 1},
	}}); err != nil {
		t.Fatal(err)
	}
	recv := func(vis int32, wait int32) []sqstypes.Message {
		t.Helper()
		out, err := q.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: cq.QueueUrl, MaxNumberOfMessages: 10, VisibilityTimeout: vis, WaitTimeSeconds: wait,
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{"ApproximateReceiveCount"}, MessageAttributeNames: []string{"All"}})
		if err != nil {
			t.Fatal(err)
		}
		return out.Messages
	}
	got := recv(1, 0)
	if len(got) != 1 || *got[0].Body != "hello" || string(got[0].MessageAttributes["b"].BinaryValue) != "\x00\x01\x02\xff" ||
		*got[0].MessageAttributes["k"].StringValue != "42" || got[0].Attributes["ApproximateReceiveCount"] != "1" {
		t.Fatalf("the first receive: %+v", got)
	}
	start := time.Now()
	again := recv(1, 5)
	if len(again) == 0 || time.Since(start) < 500*time.Millisecond {
		t.Fatalf("a long poll returned %d after %v", len(again), time.Since(start))
	}
	// Received twice: the third receive moves it to the dead letters.
	for len(again) < 2 {
		again = append(again, recv(1, 2)...)
	}
	time.Sleep(1100 * time.Millisecond)
	for range 3 {
		for _, m := range recv(1, 1) {
			if *m.Body == "hello" {
				t.Fatalf("received past its redrive policy: %s", m.Attributes["ApproximateReceiveCount"])
			}
		}
	}
	// "later", received twice by the same loops, went too.
	if n := srv.Messages("dead"); n != 2 {
		t.Fatalf("the dead letters hold %d", n)
	}
	// Visibility, deletion, and a stale handle.
	if _, err := q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: cq.QueueUrl, MessageBody: aws.String("x")}); err != nil {
		t.Fatal(err)
	}
	var x sqstypes.Message
	for x.Body == nil || *x.Body != "x" {
		for _, m := range recv(30, 1) {
			if *m.Body == "x" {
				x = m
			}
		}
	}
	if _, err := q.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: cq.QueueUrl, ReceiptHandle: x.ReceiptHandle, VisibilityTimeout: 0}); err != nil {
		t.Fatal(err)
	}
	y := recv(30, 1)
	if len(y) != 1 || *y[0].Body != "x" {
		t.Fatalf("after visibility 0: %v", y)
	}
	if _, err := q.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: cq.QueueUrl, ReceiptHandle: x.ReceiptHandle, VisibilityTimeout: 5}); err == nil {
		t.Fatal("a stale handle changed visibility")
	}
	q.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: cq.QueueUrl, ReceiptHandle: x.ReceiptHandle})
	if srv.Messages("work") != 1 {
		t.Fatal("a stale handle deleted")
	}
	if _, err := q.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: cq.QueueUrl, ReceiptHandle: y[0].ReceiptHandle}); err != nil || srv.Messages("work") != 0 {
		t.Fatalf("deleting: %v, %d left", err, srv.Messages("work"))
	}
}
