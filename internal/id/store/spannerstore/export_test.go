package spannerstore

// SetsHoldingSQL and SetRecordsSQL are SetsHolding's statement and SetRecords', for a test of
// their plans.
var (
	SetsHoldingSQL = setsHolding
	SetRecordsSQL  = setRecords(true)
)
