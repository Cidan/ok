package spannerstore

// CLRunStatements are the statements over a CL's pre runs, by name.
var CLRunStatements = map[string]string{
	"CancelQueuedPre": cancelQueuedPre,
	"StopRunningPre":  stopRunningPre,
	"ClearPreStops":   clearPreStops,
}
