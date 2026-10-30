package bus_test

import (
	"testing"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/bustest"
)

func TestLocalQueue(t *testing.T) {
	bustest.RunQueue(t, func(t *testing.T, maxAttempts int) bus.Queue {
		q := bus.NewLocalQueue()
		q.MaxAttempts = maxAttempts
		return q
	}, bustest.QueueOptions{Dedupe: true, DetectsLostLease: true})
}
