package bus

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// A job put off — looked at again later, what it names not ready yet — goes back on its queue as a
// copy under an ID of its own, base~n: the nth copy of the job whose ID is base. What names a job
// names its base, which every copy of it stands for. A backend that drops a job whose ID it holds
// (the queue's dedupe) takes each copy as new, and no copy spends the attempts a backend counts
// toward a dead letter: putting a job off any number of times never loses it.

// JobBase is the ID of the job id is a copy of, or id itself.
func JobBase(id string) string {
	base, _, _ := strings.Cut(id, "~")
	return base
}

// Copies is how many times the job id stands for was put off before it: 0 for the job itself.
func Copies(id string) int {
	_, n, ok := strings.Cut(id, "~")
	if !ok {
		return 0
	}
	c, _ := strconv.Atoi(n)
	return c
}

// PutOff has l's job looked at again after delay: a copy of it, its body as it is, enqueued on q
// before l is acknowledged — or, that failing, l returned, due after delay, which spends an attempt.
func PutOff(ctx context.Context, q Queue, l Leased, delay time.Duration) error {
	ctx = context.WithoutCancel(ctx)
	j := l.Job()
	again := j
	again.ID = fmt.Sprintf("%s~%d", JobBase(j.ID), Copies(j.ID)+1)
	again.NotBefore = time.Now().Add(delay)
	if err := q.Enqueue(ctx, again); err != nil {
		if nerr := l.Nack(ctx, delay); nerr != nil && !errors.Is(nerr, ErrLeaseLost) {
			return errors.Join(err, nerr)
		}
		return nil
	}
	if err := l.Ack(ctx); err != nil && !errors.Is(err, ErrLeaseLost) {
		return err
	}
	return nil
}

// Backoff is how long the nth copy of a job waits: first, doubling each copy, up to most.
func Backoff(copies int, first, most time.Duration) time.Duration {
	d := first << min(copies, 30)
	if d <= 0 || d > most {
		return most
	}
	return d
}
