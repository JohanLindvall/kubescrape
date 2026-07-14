package promscrape

import "github.com/JohanLindvall/kubescrape/pkg/promparse"

// The exposition parser lives in pkg/promparse (it is useful on its own — a
// constant-memory Prometheus text/OpenMetrics parser). These aliases keep the
// scrape pipeline reading in its own vocabulary rather than qualifying every
// Sample and Label.
type (
	Sample     = promparse.Sample
	Label      = promparse.Label
	Exemplar   = promparse.Exemplar
	SampleRole = promparse.SampleRole
	MetricType = promparse.MetricType
	Parser     = promparse.Parser
)

const (
	TypeUntyped   = promparse.TypeUntyped
	TypeCounter   = promparse.TypeCounter
	TypeGauge     = promparse.TypeGauge
	TypeHistogram = promparse.TypeHistogram
	TypeSummary   = promparse.TypeSummary

	RoleGauge           = promparse.RoleGauge
	RoleCounter         = promparse.RoleCounter
	RoleHistogramBucket = promparse.RoleHistogramBucket
	RoleHistogramSum    = promparse.RoleHistogramSum
	RoleHistogramCount  = promparse.RoleHistogramCount
	RoleSummaryQuantile = promparse.RoleSummaryQuantile
	RoleSummarySum      = promparse.RoleSummarySum
	RoleSummaryCount    = promparse.RoleSummaryCount

	// Bounds for the per-scrape memos in filter.go, shared with the parser's
	// own intern tables so one pathological endpoint cannot grow either.
	maxTrackedFamilies = promparse.MaxTrackedFamilies
	maxInternedValues  = promparse.MaxInternedValues
)

// ErrTooManySamples is returned when a scrape exceeds its sample budget.
var ErrTooManySamples = promparse.ErrTooManySamples

var (
	newParser    = promparse.New
	copyExemplar = promparse.CopyExemplar
)
