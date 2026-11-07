package work

import (
	"time"

	"github.com/Cidan/ok/internal/bus"
)

// JobOfMark is the job of a mark due at due, enqueued to come at once: as a bus whose clock runs
// ahead brings it.
func JobOfMark(j Jobs, kind string, key int64, due time.Time) bus.Job {
	return j.jobWith(kind, key, time.Time{}, due, newToken())
}
