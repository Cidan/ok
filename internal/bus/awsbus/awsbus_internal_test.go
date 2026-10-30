package awsbus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/bustest/awsfake"
)

func fake(t *testing.T) (*Bus, *awsfake.Server) {
	t.Helper()
	s, err := awsfake.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	cfg := aws.Config{Region: awsfake.Region, Credentials: credentials.NewStaticCredentialsProvider("test", "test", "")}
	return New(sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(s.URL) }), Options{MaxAttempts: 3}), s
}

// A job received before it is due — one due past SQS's fifteen-minute delay — is sent on, not
// leased, its receive no attempt; a job sent again carries its attempts; and the last attempt's Nack
// leaves the job in the dead letters, its attempts with it.
func TestDelaysAndAttempts(t *testing.T) {
	ctx := context.Background()
	b, srv := fake(t)
	url, _, err := b.jobQueue(ctx, "work")
	if err != nil {
		t.Fatal(err)
	}
	early := jobEntry("0", bus.Job{ID: "early", Queue: "work", Body: []byte("e"), NotBefore: time.Now().Add(time.Hour)}, 1)
	if early.DelaySeconds != 900 || aws.ToString(early.MessageAttributes[attrNotBefore].StringValue) == "" {
		t.Fatalf("a job due in an hour: delay %d, not before %v", early.DelaySeconds, early.MessageAttributes[attrNotBefore].StringValue)
	}
	early.DelaySeconds = 0 // as if its fifteen minutes passed
	if err := b.send(ctx, url, []sqstypes.SendMessageBatchRequestEntry{early}); err != nil {
		t.Fatal(err)
	}
	if ls, err := b.Lease(ctx, "work", bus.LeaseOptions{Max: 10, Visibility: time.Minute}); err != nil || len(ls) != 0 {
		t.Fatalf("a job not due was leased: %d, %v", len(ls), err)
	}
	if n := srv.Messages(name("ok", "q", "work")); n != 1 {
		t.Fatalf("the job not due, sent on: the queue holds %d", n)
	}

	// Due now, having had two deliveries before it was sent again.
	again := jobEntry("0", bus.Job{ID: "again", Queue: "work", Body: []byte("a")}, 2)
	if err := b.send(ctx, url, []sqstypes.SendMessageBatchRequestEntry{again}); err != nil {
		t.Fatal(err)
	}
	var l bus.Leased
	for l == nil {
		ls, err := b.Lease(ctx, "work", bus.LeaseOptions{Max: 10, Visibility: time.Minute, Wait: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		for _, x := range ls {
			if x.Job().ID == "again" {
				l = x
			}
		}
	}
	if l.Attempt() != 3 || string(l.Job().Body) != "a" {
		t.Fatalf("the job sent again: attempt %d, body %q", l.Attempt(), l.Job().Body)
	}
	if err := l.Nack(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if n := srv.Messages(name("ok", "dead", "work")); n != 1 {
		t.Fatalf("after the last attempt's Nack the dead letters hold %d", n)
	}
	if err := l.Ack(ctx); err != bus.ErrLeaseLost {
		t.Fatalf("an Ack after the Nack: %v", err)
	}
}

// Names keep to SQS's alphabet and length, and a long one is cut the same way every time.
func TestNames(t *testing.T) {
	if got := name("ok", "ev", "scm.events", "control_follow"); got != "ok-ev-scm_2eevents-control_5ffollow" {
		t.Errorf("a name: %q", got)
	}
	long := name("ok", "ev", strings.Repeat("t", 60), strings.Repeat("g", 60))
	if len(long) != maxName || long != name("ok", "ev", strings.Repeat("t", 60), strings.Repeat("g", 60)) ||
		long == name("ok", "ev", strings.Repeat("t", 60), strings.Repeat("g", 61)) {
		t.Errorf("a long name: %q (%d)", long, len(long))
	}
	if strings.Trim(long, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") != "" {
		t.Errorf("a name outside SQS's alphabet: %q", long)
	}
}
