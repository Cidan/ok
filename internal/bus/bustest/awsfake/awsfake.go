// Package awsfake is a fake of Amazon SQS and SNS — as much of each as ok's bus uses
// (internal/bus/awsbus) — served over HTTP on the test's loopback, speaking what the AWS SDK for Go
// speaks: SQS's JSON 1.0 protocol and SNS's query protocol, on one endpoint.
//
// It keeps SQS's semantics where the bus relies on them: a received message is invisible for its
// visibility timeout, a ReceiveMessage long-polls, messages are delayed, each receive counts, a
// message received a redrive policy's maxReceiveCount times moves to its dead-letter queue on the
// next receive, and visibility reaches at most twelve hours past the first receive. SNS delivers a
// topic's messages to its SQS subscribers raw or in SNS's JSON envelope, and only to a queue whose
// policy lets SNS send from the topic, as SNS does.
package awsfake

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
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
	topics  map[string]*topic // by ARN
	changed chan struct{}     // closed and replaced when a queue gains a message
}

// Start serves a fake on the loopback.
func Start() (*Server, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &Server{URL: "http://" + ln.Addr().String(), ln: ln, queues: map[string]*queue{}, topics: map[string]*topic{}, changed: make(chan struct{})}
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
	if target := r.Header.Get("X-Amz-Target"); strings.HasPrefix(target, "AmazonSQS.") {
		s.serveSQS(w, r, strings.TrimPrefix(target, "AmazonSQS."))
		return
	}
	if err := r.ParseForm(); err != nil {
		snsError(w, http.StatusBadRequest, "InvalidParameter", err.Error())
		return
	}
	s.serveSNS(w, r.Form)
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

// ─── SNS ───

type subscription struct {
	arn, endpoint string // the queue's ARN
	raw           bool
	// filter is its filter policy, of the plain form alone: each attribute named must be a string
	// one of the values listed.
	filter map[string][]string
}

// admits reports whether a message's attributes pass the subscription's filter policy.
func (sub *subscription) admits(attrs map[string]attribute) bool {
	for k, values := range sub.filter {
		a, ok := attrs[k]
		if !ok || a.DataType != "String" || !slices.Contains(values, a.StringValue) {
			return false
		}
	}
	return true
}

type topic struct {
	arn  string
	subs []*subscription
}

func snsOK(w http.ResponseWriter, action string, result any) {
	w.Header().Set("Content-Type", "text/xml")
	fmt.Fprintf(w, `<%sResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">`, action)
	enc := xml.NewEncoder(w)
	enc.EncodeElement(result, xml.StartElement{Name: xml.Name{Local: action + "Result"}})
	enc.Flush()
	fmt.Fprintf(w, `<ResponseMetadata><RequestId>%s</RequestId></ResponseMetadata></%sResponse>`, newID(), action)
}

func snsError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "text/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, `<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><Error><Type>Sender</Type><Code>%s</Code><Message>%s</Message></Error><RequestId>%s</RequestId></ErrorResponse>`,
		code, xmlEscape(msg), newID())
}

func xmlEscape(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

// entries reads a query map — prefix.entry.N.key and prefix.entry.N.value — as names to the keys
// under each entry's value.
func entries(form url.Values, prefix, keyName, valueName string) map[string]map[string]string {
	out := map[string]map[string]string{}
	for i := 1; ; i++ {
		base := fmt.Sprintf("%s.entry.%d.", prefix, i)
		name := form.Get(base + keyName)
		if name == "" {
			return out
		}
		vals := map[string]string{}
		for k, v := range form {
			if strings.HasPrefix(k, base+valueName) {
				vals[strings.TrimPrefix(strings.TrimPrefix(k, base+valueName), ".")] = v[0]
			}
		}
		out[name] = vals
	}
}

// messageAttributes reads a query request's message attributes under prefix.
func messageAttributes(form url.Values, prefix string) (map[string]attribute, error) {
	out := map[string]attribute{}
	for name, v := range entries(form, prefix, "Name", "Value") {
		a := attribute{DataType: v["DataType"], StringValue: v["StringValue"]}
		if b, ok := v["BinaryValue"]; ok {
			bin, err := base64.StdEncoding.DecodeString(b)
			if err != nil {
				return nil, err
			}
			a.BinaryValue = bin
		}
		out[name] = a
	}
	return out, nil
}

func (s *Server) serveSNS(w http.ResponseWriter, form url.Values) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch action := form.Get("Action"); action {
	case "CreateTopic":
		name := form.Get("Name")
		if name == "" || len(name) > 256 || strings.Trim(name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") != "" {
			snsError(w, http.StatusBadRequest, "InvalidParameter", "Invalid parameter: Topic Name")
			return
		}
		arn := "arn:aws:sns:" + Region + ":" + Account + ":" + name
		if s.topics[arn] == nil {
			s.topics[arn] = &topic{arn: arn}
		}
		snsOK(w, action, struct {
			TopicArn string `xml:"TopicArn"`
		}{arn})
	case "Subscribe":
		t := s.topics[form.Get("TopicArn")]
		if t == nil {
			snsError(w, http.StatusNotFound, "NotFound", "Topic does not exist")
			return
		}
		if form.Get("Protocol") != "sqs" {
			snsError(w, http.StatusBadRequest, "InvalidParameter", "the fake delivers to sqs alone")
			return
		}
		endpoint := form.Get("Endpoint")
		subAttrs := entries(form, "Attributes", "key", "value")
		raw := subAttrs["RawMessageDelivery"][""] == "true"
		var filter map[string][]string
		if f := subAttrs["FilterPolicy"][""]; f != "" {
			if err := json.Unmarshal([]byte(f), &filter); err != nil {
				snsError(w, http.StatusBadRequest, "InvalidParameter", "Invalid parameter: FilterPolicy: the fake takes attribute names and string values alone")
				return
			}
		}
		var sub *subscription
		for _, x := range t.subs {
			if x.endpoint == endpoint {
				sub = x
			}
		}
		if sub == nil {
			sub = &subscription{arn: t.arn + ":" + newID(), endpoint: endpoint, raw: raw, filter: filter}
			t.subs = append(t.subs, sub)
		}
		snsOK(w, action, struct {
			SubscriptionArn string `xml:"SubscriptionArn"`
		}{sub.arn})
	case "Unsubscribe":
		arn := form.Get("SubscriptionArn")
		for _, t := range s.topics {
			t.subs = slices.DeleteFunc(t.subs, func(sub *subscription) bool { return sub.arn == arn })
		}
		snsOK(w, action, struct{}{})
	case "Publish":
		t := s.topics[form.Get("TopicArn")]
		if t == nil {
			snsError(w, http.StatusNotFound, "NotFound", "Topic does not exist")
			return
		}
		attrs, err := messageAttributes(form, "MessageAttributes")
		if err != nil || form.Get("Message") == "" {
			snsError(w, http.StatusBadRequest, "InvalidParameter", "Invalid parameter: Message")
			return
		}
		id := newID()
		s.fanOut(t, id, form.Get("Message"), attrs)
		snsOK(w, action, struct {
			MessageID string `xml:"MessageId"`
		}{id})
	case "PublishBatch":
		t := s.topics[form.Get("TopicArn")]
		if t == nil {
			snsError(w, http.StatusNotFound, "NotFound", "Topic does not exist")
			return
		}
		type ok struct {
			ID        string `xml:"Id"`
			MessageID string `xml:"MessageId"`
		}
		var done []ok
		for i := 1; ; i++ {
			base := fmt.Sprintf("PublishBatchRequestEntries.member.%d", i)
			entryID := form.Get(base + ".Id")
			if entryID == "" {
				break
			}
			if i > 10 {
				snsError(w, http.StatusBadRequest, "TooManyEntriesInBatchRequest", "more than ten entries")
				return
			}
			attrs, err := messageAttributes(form, base+".MessageAttributes")
			if err != nil || form.Get(base+".Message") == "" {
				snsError(w, http.StatusBadRequest, "InvalidParameter", "Invalid parameter: Message")
				return
			}
			id := newID()
			s.fanOut(t, id, form.Get(base+".Message"), attrs)
			done = append(done, ok{entryID, id})
		}
		snsOK(w, action, struct {
			Successful []ok `xml:"Successful>member"`
			Failed     struct{}
		}{Successful: done})
	default:
		snsError(w, http.StatusBadRequest, "InvalidAction", "the fake does not do "+action)
	}
}

// fanOut delivers a published message to each of a topic's subscribed queues whose policy lets SNS
// send from it.
func (s *Server) fanOut(t *topic, id, msg string, attrs map[string]attribute) {
	for _, sub := range t.subs {
		var q *queue
		for _, x := range s.queues {
			if x.arn == sub.endpoint {
				q = x
			}
		}
		if q == nil || !allowsSNS(q.attrs["Policy"], q.arn, t.arn) || !sub.admits(attrs) {
			continue
		}
		body, mattrs := msg, attrs
		if !sub.raw {
			env, _ := json.Marshal(map[string]any{"Type": "Notification", "MessageId": id, "TopicArn": t.arn,
				"Message": msg, "Timestamp": time.Now().UTC().Format(time.RFC3339Nano), "MessageAttributes": envelopeAttrs(attrs)})
			body, mattrs = string(env), nil
		}
		s.send(q, body, mattrs, nil)
	}
}

func envelopeAttrs(attrs map[string]attribute) map[string]map[string]string {
	out := map[string]map[string]string{}
	for k, a := range attrs {
		v := a.StringValue
		if a.DataType == "Binary" {
			v = base64.StdEncoding.EncodeToString(a.BinaryValue)
		}
		out[k] = map[string]string{"Type": a.DataType, "Value": v}
	}
	return out
}

// allowsSNS reports whether a queue's policy lets SNS's service principal send to it from topic:
// a statement allowing sqs:SendMessage (or sqs:*) on the queue to sns.amazonaws.com (or anyone),
// conditioned, if at all, on aws:SourceArn equal to or like the topic's.
func allowsSNS(policy, queueArn, topicArn string) bool {
	var p struct {
		Statement []struct {
			Effect    string
			Principal any
			Action    any
			Resource  any
			Condition map[string]map[string]any
		}
	}
	if json.Unmarshal([]byte(policy), &p) != nil {
		return false
	}
	list := func(v any) []string {
		switch v := v.(type) {
		case string:
			return []string{v}
		case []any:
			var out []string
			for _, x := range v {
				if s, ok := x.(string); ok {
					out = append(out, s)
				}
			}
			return out
		}
		return nil
	}
	has := func(vs []string, want ...string) bool {
		for _, v := range vs {
			for _, w := range want {
				if v == w {
					return true
				}
			}
		}
		return false
	}
	for _, st := range p.Statement {
		if st.Effect != "Allow" || !has(list(st.Action), "sqs:SendMessage", "sqs:*", "*") ||
			(st.Resource != nil && !has(list(st.Resource), queueArn, "*")) {
			continue
		}
		principal := false
		switch pr := st.Principal.(type) {
		case string:
			principal = pr == "*"
		case map[string]any:
			principal = has(list(pr["Service"]), "sns.amazonaws.com") || has(list(pr["AWS"]), "*")
		}
		if !principal {
			continue
		}
		matched := true
		for op, conds := range st.Condition {
			for key, v := range conds {
				if !strings.EqualFold(key, "aws:SourceArn") {
					continue
				}
				ok := false
				for _, want := range list(v) {
					switch op {
					case "ArnEquals", "StringEquals":
						ok = ok || want == topicArn
					case "ArnLike", "StringLike":
						ok = ok || like(want, topicArn)
					}
				}
				matched = matched && ok
			}
		}
		if matched {
			return true
		}
	}
	return false
}

// like matches s against a pattern of '*' and '?' wildcards.
func like(pattern, s string) bool {
	if pattern == "" {
		return s == ""
	}
	switch pattern[0] {
	case '*':
		for i := 0; i <= len(s); i++ {
			if like(pattern[1:], s[i:]) {
				return true
			}
		}
		return false
	case '?':
		return s != "" && like(pattern[1:], s[1:])
	}
	return s != "" && s[0] == pattern[0] && like(pattern[1:], s[1:])
}
