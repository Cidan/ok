// Package awsfake is a fake of Amazon SQS — as much as ok's bus uses (internal/bus/awsbus) — served
// over HTTP on the test's loopback, speaking what the AWS SDK for Go speaks: SQS's JSON 1.0
// protocol.
//
// It keeps SQS's semantics where the bus relies on them: a received message is invisible for its
// visibility timeout, a ReceiveMessage long-polls, messages are delayed, each receive counts, a
// message received a redrive policy's maxReceiveCount times moves to its dead-letter queue on the
// next receive, and visibility reaches at most twelve hours past the first receive.
package awsfake

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Region and Account are every ARN's.
const (
	Region  = "us-east-1"
	Account = "000000000000"
)

// Server is the fake, serving until Close.
type Server struct {
	URL string
	srv *http.Server
	ln  net.Listener

	mu      sync.Mutex
	queues  map[string]*queue // by name
	changed chan struct{}     // closed and replaced when a queue gains a message
}

// Start serves a fake on the loopback.
func Start() (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &Server{URL: "http://" + ln.Addr().String(), ln: ln, queues: map[string]*queue{}, changed: make(chan struct{})}
	s.srv = &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second}
	go s.srv.Serve(ln)
	return s, nil
}

// Close stops serving.
func (s *Server) Close() { s.srv.Close() }

// Messages counts the messages a queue holds, by name, in flight or not.
func (s *Server) Messages(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.queues[name]
	if q == nil {
		return 0
	}
	return len(q.msgs)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target, ok := strings.CutPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.")
	if !ok {
		http.Error(w, "the fake serves SQS alone", http.StatusBadRequest)
		return
	}
	s.serveSQS(w, r, target)
}

// wake tells long polls a queue gained a message.
func (s *Server) wake() {
	close(s.changed)
	s.changed = make(chan struct{})
}

func newID() string {
	var b [16]byte
	rand.Read(b[:])
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// ─── SQS ───

type attribute struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue,omitempty"`
	BinaryValue []byte `json:"BinaryValue,omitempty"`
}

type message struct {
	id, body  string
	attrs     map[string]attribute
	sent      time.Time
	visibleAt time.Time
	receives  int
	first     time.Time // the first receive's
	receipt   string    // the latest receive's handle
}

type queue struct {
	name, url, arn string
	attrs          map[string]string
	tags           map[string]string
	msgs           []*message
}

// redrive is a queue's redrive policy: its dead-letter queue's ARN and receives before moving.
func (q *queue) redrive() (string, int) {
	var p struct {
		Target string          `json:"deadLetterTargetArn"`
		Max    json.RawMessage `json:"maxReceiveCount"`
	}
	if json.Unmarshal([]byte(q.attrs["RedrivePolicy"]), &p) != nil || p.Target == "" {
		return "", 0
	}
	n, err := strconv.Atoi(strings.Trim(string(p.Max), `"`))
	if err != nil {
		return "", 0
	}
	return p.Target, n
}

// delete deletes the message whose latest receive's handle is handle: a handle of an earlier
// receive deletes nothing, as SQS promises no more of it.
func (q *queue) delete(handle string) {
	for i, m := range q.msgs {
		if m.receipt == handle {
			q.msgs = append(q.msgs[:i], q.msgs[i+1:]...)
			return
		}
	}
}

func (q *queue) seconds(attr string, def int) int {
	if n, err := strconv.Atoi(q.attrs[attr]); err == nil {
		return n
	}
	return def
}

type sqsErr struct {
	status     int
	typ, query string // the error's shape, and its query-protocol code
	msg        string
}

func (e *sqsErr) Error() string { return e.msg }

func noQueue() *sqsErr {
	return &sqsErr{http.StatusBadRequest, "QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist."}
}

func invalid(format string, args ...any) *sqsErr {
	return &sqsErr{http.StatusBadRequest, "InvalidParameterValue", "InvalidParameterValue", fmt.Sprintf(format, args...)}
}

func (s *Server) serveSQS(w http.ResponseWriter, r *http.Request, op string) {
	var in map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		in = map[string]json.RawMessage{}
	}
	out, err := s.sqsOp(r, op, in)
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	if err != nil {
		e, ok := err.(*sqsErr)
		if !ok {
			e = &sqsErr{http.StatusBadRequest, "InvalidAction", "InvalidAction", err.Error()}
		}
		w.Header().Set("X-Amzn-Query-Error", e.query+";Sender")
		w.WriteHeader(e.status)
		json.NewEncoder(w).Encode(map[string]string{"__type": "com.amazonaws.sqs#" + e.typ, "message": e.msg})
		return
	}
	json.NewEncoder(w).Encode(out)
}

func field[T any](in map[string]json.RawMessage, name string) T {
	var v T
	if raw, ok := in[name]; ok {
		json.Unmarshal(raw, &v)
	}
	return v
}

func (s *Server) queueOf(in map[string]json.RawMessage) (*queue, error) {
	u := field[string](in, "QueueUrl")
	name := u[strings.LastIndexByte(u, '/')+1:]
	q := s.queues[name]
	if q == nil {
		return nil, noQueue()
	}
	return q, nil
}

func (s *Server) sqsOp(r *http.Request, op string, in map[string]json.RawMessage) (any, error) {
	if op == "ReceiveMessage" {
		return s.receive(r, in)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	switch op {
	case "CreateQueue":
		name := field[string](in, "QueueName")
		if name == "" || len(name) > 80 || strings.Trim(name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") != "" {
			return nil, invalid("queue name %q", name)
		}
		if q := s.queues[name]; q != nil {
			return map[string]string{"QueueUrl": q.url}, nil
		}
		q := &queue{name: name, url: s.URL + "/" + Account + "/" + name, arn: "arn:aws:sqs:" + Region + ":" + Account + ":" + name, attrs: map[string]string{}}
		for k, v := range field[map[string]string](in, "Attributes") {
			q.attrs[k] = v
		}
		s.queues[name] = q
		return map[string]string{"QueueUrl": q.url}, nil
	case "DeleteQueue":
		q, err := s.queueOf(in)
		if err != nil {
			return nil, err
		}
		delete(s.queues, q.name)
		return map[string]string{}, nil
	case "ListQueues":
		prefix := field[string](in, "QueueNamePrefix")
		var urls []string
		for name, q := range s.queues {
			if strings.HasPrefix(name, prefix) {
				urls = append(urls, q.url)
			}
		}
		sort.Strings(urls)
		return map[string]any{"QueueUrls": urls}, nil
	case "TagQueue":
		q, err := s.queueOf(in)
		if err != nil {
			return nil, err
		}
		if q.tags == nil {
			q.tags = map[string]string{}
		}
		for k, v := range field[map[string]string](in, "Tags") {
			q.tags[k] = v
		}
		return map[string]any{}, nil
	case "ListQueueTags":
		q, err := s.queueOf(in)
		if err != nil {
			return nil, err
		}
		return map[string]any{"Tags": q.tags}, nil
	case "GetQueueUrl":
		q := s.queues[field[string](in, "QueueName")]
		if q == nil {
			return nil, noQueue()
		}
		return map[string]string{"QueueUrl": q.url}, nil
	case "GetQueueAttributes":
		q, err := s.queueOf(in)
		if err != nil {
			return nil, err
		}
		attrs := map[string]string{"QueueArn": q.arn, "ApproximateNumberOfMessages": strconv.Itoa(len(q.msgs))}
		for k, v := range q.attrs {
			attrs[k] = v
		}
		return map[string]any{"Attributes": attrs}, nil
	case "SetQueueAttributes":
		q, err := s.queueOf(in)
		if err != nil {
			return nil, err
		}
		for k, v := range field[map[string]string](in, "Attributes") {
			q.attrs[k] = v
		}
		return map[string]any{}, nil
	case "SendMessage":
		q, err := s.queueOf(in)
		if err != nil {
			return nil, err
		}
		m, err := s.send(q, field[string](in, "MessageBody"), field[map[string]attribute](in, "MessageAttributes"), field[*int](in, "DelaySeconds"))
		if err != nil {
			return nil, err
		}
		return map[string]string{"MessageId": m.id, "MD5OfMessageBody": md5Of(m.body)}, nil
	case "SendMessageBatch":
		q, err := s.queueOf(in)
		if err != nil {
			return nil, err
		}
		type entry struct {
			ID                string               `json:"Id"`
			MessageBody       string               `json:"MessageBody"`
			DelaySeconds      *int                 `json:"DelaySeconds"`
			MessageAttributes map[string]attribute `json:"MessageAttributes"`
		}
		entries := field[[]entry](in, "Entries")
		if len(entries) == 0 || len(entries) > 10 {
			return nil, invalid("%d entries in a batch", len(entries))
		}
		var ok []map[string]string
		for _, e := range entries {
			m, err := s.send(q, e.MessageBody, e.MessageAttributes, e.DelaySeconds)
			if err != nil {
				return nil, err
			}
			ok = append(ok, map[string]string{"Id": e.ID, "MessageId": m.id, "MD5OfMessageBody": md5Of(m.body)})
		}
		return map[string]any{"Successful": ok, "Failed": []any{}}, nil
	case "DeleteMessage":
		q, err := s.queueOf(in)
		if err != nil {
			return nil, err
		}
		q.delete(field[string](in, "ReceiptHandle"))
		return map[string]any{}, nil
	case "DeleteMessageBatch":
		q, err := s.queueOf(in)
		if err != nil {
			return nil, err
		}
		type entry struct {
			ID            string `json:"Id"`
			ReceiptHandle string `json:"ReceiptHandle"`
		}
		entries := field[[]entry](in, "Entries")
		if len(entries) == 0 || len(entries) > 10 {
			return nil, invalid("%d entries in a batch", len(entries))
		}
		var ok []map[string]string
		for _, e := range entries {
			q.delete(e.ReceiptHandle)
			ok = append(ok, map[string]string{"Id": e.ID})
		}
		return map[string]any{"Successful": ok, "Failed": []any{}}, nil
	case "ChangeMessageVisibility":
		q, err := s.queueOf(in)
		if err != nil {
			return nil, err
		}
		handle, secs := field[string](in, "ReceiptHandle"), field[int](in, "VisibilityTimeout")
		now := time.Now()
		for _, m := range q.msgs {
			if m.receipt != handle {
				continue
			}
			if !m.visibleAt.After(now) {
				return nil, &sqsErr{http.StatusBadRequest, "MessageNotInflight", "AWS.SimpleQueueService.MessageNotInflight", "The message is not in flight."}
			}
			if secs < 0 || now.Add(time.Duration(secs)*time.Second).After(m.first.Add(12*time.Hour)) {
				return nil, invalid("visibility %ds reaches past twelve hours from the first receive", secs)
			}
			m.visibleAt = now.Add(time.Duration(secs) * time.Second)
			if secs == 0 {
				s.wake()
			}
			return map[string]any{}, nil
		}
		return nil, &sqsErr{http.StatusBadRequest, "ReceiptHandleIsInvalid", "ReceiptHandleIsInvalid", "The receipt handle is not the message's latest."}
	}
	return nil, &sqsErr{http.StatusBadRequest, "InvalidAction", "InvalidAction", "the fake does not do " + op}
}

func md5Of(body string) string {
	sum := md5.Sum([]byte(body))
	return hex.EncodeToString(sum[:])
}

// send adds a message to q.
func (s *Server) send(q *queue, body string, attrs map[string]attribute, delay *int) (*message, error) {
	if body == "" {
		return nil, invalid("an empty message body")
	}
	secs := q.seconds("DelaySeconds", 0)
	if delay != nil {
		secs = *delay
	}
	if secs < 0 || secs > 900 {
		return nil, invalid("DelaySeconds %d", secs)
	}
	now := time.Now()
	m := &message{id: newID(), body: body, attrs: attrs, sent: now, visibleAt: now.Add(time.Duration(secs) * time.Second)}
	q.msgs = append(q.msgs, m)
	s.wake()
	return m, nil
}

// receive long-polls for a queue's visible messages, moving those past their redrive policy's
// receives to its dead-letter queue as it finds them.
func (s *Server) receive(r *http.Request, in map[string]json.RawMessage) (any, error) {
	wait := time.Duration(field[int](in, "WaitTimeSeconds")) * time.Second
	if wait > 20*time.Second {
		return nil, invalid("WaitTimeSeconds over 20")
	}
	n := field[int](in, "MaxNumberOfMessages")
	if n == 0 {
		n = 1
	}
	if n < 1 || n > 10 {
		return nil, invalid("MaxNumberOfMessages %d", n)
	}
	deadline := time.Now().Add(wait)
	for {
		s.mu.Lock()
		q, err := s.queueOf(in)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		vis := q.seconds("VisibilityTimeout", 30)
		if v := field[*int](in, "VisibilityTimeout"); v != nil {
			vis = *v
		}
		if vis < 0 || vis > 12*60*60 {
			s.mu.Unlock()
			return nil, invalid("VisibilityTimeout %d", vis)
		}
		got := s.take(q, n, time.Duration(vis)*time.Second)
		changed := s.changed
		s.mu.Unlock()
		if len(got) > 0 || !time.Now().Before(deadline) {
			return map[string]any{"Messages": got}, nil
		}
		select {
		case <-changed:
		case <-time.After(min(time.Until(deadline), 50*time.Millisecond)):
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}
}

// take receives up to n of q's visible messages, invisible then for vis.
func (s *Server) take(q *queue, n int, vis time.Duration) []map[string]any {
	now := time.Now()
	dead, maxReceives := q.redrive()
	var got []map[string]any
	for i := 0; i < len(q.msgs) && len(got) < n; i++ {
		m := q.msgs[i]
		if m.visibleAt.After(now) {
			continue
		}
		if maxReceives > 0 && m.receives >= maxReceives {
			for _, d := range s.queues {
				if d.arn == dead {
					d.msgs = append(d.msgs, &message{id: m.id, body: m.body, attrs: m.attrs, sent: m.sent, visibleAt: now, receives: m.receives})
				}
			}
			q.msgs = append(q.msgs[:i], q.msgs[i+1:]...)
			i--
			continue
		}
		m.receives++
		if m.receives == 1 {
			m.first = now
		}
		m.receipt = newID() + "#" + strconv.Itoa(m.receives)
		m.visibleAt = now.Add(vis)
		attrs := map[string]attribute{}
		for k, v := range m.attrs {
			attrs[k] = v
		}
		got = append(got, map[string]any{
			"MessageId": m.id, "ReceiptHandle": m.receipt, "Body": m.body, "MD5OfBody": md5Of(m.body),
			"Attributes":        map[string]string{"ApproximateReceiveCount": strconv.Itoa(m.receives), "SentTimestamp": strconv.FormatInt(m.sent.UnixMilli(), 10)},
			"MessageAttributes": attrs,
		})
	}
	return got
}
