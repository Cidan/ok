package idclient

import "time"

// SetNow fixes the time the workload credential sees.
func SetNow(w *Workload, at time.Time) { w.now = func() time.Time { return at } }

// SetLocalNow fixes the time a local credential sees.
func SetLocalNow(l *Local, at time.Time) { l.now = func() time.Time { return at } }
