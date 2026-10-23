package bus_test

import (
	"testing"

	"github.com/Cidan/ok/internal/bus"
	"github.com/Cidan/ok/internal/bus/bustest"
)

func TestLocalSignals(t *testing.T) {
	l := bus.NewLocal()
	bustest.RunSignals(t, func(*testing.T) bus.Signals { return l })
}
