package spannerstore

import "time"

// SetStageLogBytes sets the bytes of content log a transaction of b's writes itself before it
// stages the rest.
func SetStageLogBytes(b *Backend, n int) { b.stageBytes = n }

// SetStagedOrphanAge sets how long before a fold's place a staged row with no marker is deleted, and
// returns what restores it.
func SetStagedOrphanAge(d time.Duration) (restore func()) {
	was := stagedOrphanAge
	stagedOrphanAge = d
	return func() { stagedOrphanAge = was }
}
