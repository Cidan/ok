package load

import (
	"fmt"
	"math"
	"os"

	"gopkg.in/yaml.v3"
)

// Prices are a cloud's list prices, each with where it came from.
type Prices struct {
	Region            string  `yaml:"region"`
	Fetched           string  `yaml:"fetched"`
	CoreHour          Price   `yaml:"core_hour"`
	MemoryGiBHour     Price   `yaml:"memory_gib_hour"`
	MemoryPerCoreGiB  float64 `yaml:"memory_per_core_gib"`
	ObjectGiBMonth    Price   `yaml:"object_gib_month"`
	ObjectClassA      Price   `yaml:"object_class_a"`
	ObjectClassB      Price   `yaml:"object_class_b"`
	SpannerNodeHour   Price   `yaml:"spanner_node_hour"`
	SpannerGiBMonth   Price   `yaml:"spanner_gib_month"`
	SpannerNodeReads  float64 `yaml:"spanner_node_reads"`
	SpannerNodeWrites float64 `yaml:"spanner_node_writes"`
	SpannerCPUCeiling float64 `yaml:"spanner_cpu_ceiling"`
}

// Price is a unit's price, and its SKU.
type Price struct {
	Price float64 `yaml:"price"`
	SKU   string  `yaml:"sku"`
}

// LoadPrices reads a prices file.
func LoadPrices(path string) (Prices, error) {
	var p Prices
	data, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}
	if err := yaml.Unmarshal(data, &p); err != nil {
		return p, fmt.Errorf("%s: %w", path, err)
	}
	if p.CoreHour.Price <= 0 || p.SpannerNodeHour.Price <= 0 || p.SpannerNodeWrites <= 0 || p.SpannerNodeReads <= 0 || p.SpannerCPUCeiling <= 0 {
		return p, fmt.Errorf("%s names no price of a core, of a Spanner node, or a node's throughput", path)
	}
	return p, nil
}

// Measured is what an installation used over a run: its servers' and its database's cores, the
// database's rows and bytes, and the bucket's bytes and objects written.
type Measured struct {
	Seconds             float64 `json:"seconds"`
	OKCoreSeconds       float64 `json:"ok_core_seconds"`
	DatabaseCoreSeconds float64 `json:"database_core_seconds"`
	DatabaseRowsWritten float64 `json:"database_rows_written"`
	DatabaseRowsRead    float64 `json:"database_rows_read"`
	DatabaseBytes       float64 `json:"database_bytes"`
	ObjectBytes         float64 `json:"object_bytes"`
	ObjectsWritten      float64 `json:"objects_written"`
}

// Assumptions are what a cost takes that a run does not measure.
type Assumptions struct {
	// Headroom is the share of a core ok's servers run at: the rest absorbs peaks and a lost zone.
	Headroom float64
	// Compression is how much smaller a repository's bytes are stored than written: the target
	// assumes 1.5–3 for a repository of mostly binaries (docs/hosted.md, "The target").
	Compression float64
	// DatabaseBytesPerRepository is what a repository keeps in its cell's database at steady state:
	// ~50 MB (docs/hosted.md, "What follows from the numbers").
	DatabaseBytesPerRepository float64
}

// Line is a term of a cost: what it is, how much of it a cell uses, and what that costs a month.
type Line struct {
	What     string  `json:"what"`
	Quantity float64 `json:"quantity"`
	Unit     string  `json:"unit"`
	Monthly  float64 `json:"monthly_usd"`
	Basis    string  `json:"basis"`
}

// Cost is a cell's cost at its full share of the target, a month, and the target's.
type Cost struct {
	Lines       []Line   `json:"cell_lines"`
	Cell        float64  `json:"cell_monthly_usd"`
	Target      float64  `json:"target_monthly_usd"`
	PerAccount  float64  `json:"account_monthly_usd"`
	Excluded    []string `json:"excluded"`
	Assumptions []string `json:"assumptions"`
}

const (
	hoursAMonth   = 730
	secondsAMonth = hoursAMonth * 3600
	gib           = 1 << 30
	// TargetAccounts are the target's accounts; RepositoryBytes each one's repository's.
	TargetAccounts  = 1_000_000
	RepositoryBytes = 1e12
)

// Price is a cell's monthly cost at its full share of the target, scaling a run's measurements
// linearly by the fraction of a cell's share the run offered.
func (m Measured) Price(rep *Report, p Prices, a Assumptions) (Cost, error) {
	if m.Seconds <= 0 || rep.CellFraction <= 0 {
		return Cost{}, fmt.Errorf("load: a run of %.0fs at %g of a cell's share prices nothing", m.Seconds, rep.CellFraction)
	}
	scale := 1 / rep.CellFraction
	perSecond := func(v float64) float64 { return v / m.Seconds * scale }
	c := Cost{}
	add := func(l Line) { c.Lines = append(c.Lines, l); c.Cell += l.Monthly }

	cores := perSecond(m.OKCoreSeconds) / a.Headroom
	add(Line{What: "ok's servers", Quantity: cores, Unit: "cores", Monthly: cores * (p.CoreHour.Price + p.MemoryPerCoreGiB*p.MemoryGiBHour.Price) * hoursAMonth,
		Basis: fmt.Sprintf("%.2f cores measured at the run's load, ×%.0f to a cell's, at %.0f%% of each; N2, %g GiB a core", perSecond(m.OKCoreSeconds)/scale, scale,
			a.Headroom*100, p.MemoryPerCoreGiB)})

	writes, reads := perSecond(m.DatabaseRowsWritten), perSecond(m.DatabaseRowsRead)
	nodes := math.Max(1, math.Ceil(math.Max(writes/p.SpannerNodeWrites, reads/p.SpannerNodeReads)/p.SpannerCPUCeiling))
	add(Line{What: "the cell's Spanner compute", Quantity: nodes, Unit: "nodes", Monthly: nodes * p.SpannerNodeHour.Price * hoursAMonth,
		Basis: fmt.Sprintf("%.0f rows written and %.0f read a second at a cell's load — a node writes %.0f and reads %.0f at full CPU — at %.0f%% CPU",
			writes, reads, p.SpannerNodeWrites, p.SpannerNodeReads, p.SpannerCPUCeiling*100)})

	dbGiB := CellAccounts * a.DatabaseBytesPerRepository / gib
	add(Line{What: "the cell's Spanner storage", Quantity: dbGiB, Unit: "GiB", Monthly: dbGiB * p.SpannerGiBMonth.Price,
		Basis: fmt.Sprintf("%.0f MB a repository at steady state (assumed); the run grew the database %.0f MB a day at a cell's load",
			a.DatabaseBytesPerRepository/1e6, perSecond(m.DatabaseBytes)*86400/1e6)})

	objGiB := CellAccounts * RepositoryBytes / a.Compression / gib
	add(Line{What: "the cell's object storage", Quantity: objGiB, Unit: "GiB", Monthly: objGiB * p.ObjectGiBMonth.Price,
		Basis: fmt.Sprintf("a 1 TB repository an account, stored %g× smaller (assumed); the run stored %.2f bytes a byte it sent",
			a.Compression, m.ObjectBytes/math.Max(rep.sentBytes(), 1))})

	classA := perSecond(m.ObjectsWritten) * secondsAMonth
	add(Line{What: "the cell's object writes", Quantity: classA, Unit: "Class A operations", Monthly: classA * p.ObjectClassA.Price,
		Basis: fmt.Sprintf("%.1f objects written a second at a cell's load", perSecond(m.ObjectsWritten))})

	cells := float64(TargetAccounts) / CellAccounts
	c.Target = c.Cell * cells
	c.PerAccount = c.Cell / CellAccounts
	c.Excluded = []string{
		"CI's compute: the workers' runs, metered as CI minutes",
		"CI's reads, from caches near workers and from object storage on signed URLs: within the region, no egress; Class B operations not measured",
		"the bus (NATS) and the edge: in-cluster, their cores not measured here",
		"the directory: global, measured beside a cell",
		"egress to people",
	}
	c.Assumptions = []string{
		fmt.Sprintf("linear scaling from the run's %g of a cell's share", rep.CellFraction),
		fmt.Sprintf("list prices, %s, fetched %s", p.Region, p.Fetched),
		"a PostgreSQL row written or read is a Spanner row operation",
	}
	return c, nil
}

// sentBytes are the bytes a run's publishes, checkpoints, and CLs sent, as offered and done.
func (rep *Report) sentBytes() float64 {
	b := rep.Bytes
	return float64(rep.Operations["publish"].OK)*b.Publish + float64(rep.Operations["checkpoint"].OK)*b.Checkpoint +
		float64(rep.Operations["cl.upload"].OK)*b.CL
}
