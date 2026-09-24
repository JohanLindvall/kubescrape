package promscrape

import "github.com/JohanLindvall/kubescrape/pkg/promparse"

// Sample is pkg/promparse's sample, and SampleRole, Label and Exemplar are its
// siblings: the exposition parser lives there (it is useful on its own — a
// constant-memory Prometheus text/OpenMetrics parser), and the aliases keep the
// scrape pipeline reading in its own vocabulary rather than qualifying every
// Sample and Label.
type (
	Sample     = promparse.Sample
	SampleRole = promparse.SampleRole
	Label      = promparse.Label
	Exemplar   = promparse.Exemplar
)

// The parser's sample roles and memo bounds, aliased for the same reason.
const (
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

	// The per-sample label ceiling, shared with the text parser so BOTH fronts
	// bound the same quadratic dedupe scan. See promparse.MaxLabelsPerSample.
	maxLabelsPerSample = promparse.MaxLabelsPerSample

	// The OpenMetrics exemplar label-set bound and the per-exposition HELP/UNIT
	// budget, shared with the text parser. The protobuf front applies the same
	// exemplar bound, and charges the HELP/UNIT budget the way the text parser
	// does so both fronts describe the same families (protoMetaBudget, which
	// names the one shape where they still differ).
	maxExemplarLabelSetRunes = promparse.MaxExemplarLabelSetRunes
	maxMetaBytes             = promparse.MaxMetaBytes
)

// ErrTooManySamples is returned when a scrape exceeds its sample budget.
var ErrTooManySamples = promparse.ErrTooManySamples

var copyExemplar = promparse.CopyExemplar
