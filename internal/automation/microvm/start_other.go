//go:build !linux

package microvm

import (
	"context"
	"errors"
)

// Start boots a run's microVM: on Linux, with KVM, alone.
func Start(ctx context.Context, cfg Config, o Options) (*VM, error) {
	return nil, errors.New("microVMs run on Linux, with KVM")
}
