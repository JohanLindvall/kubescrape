// Package promparse is a streaming parser for the Prometheus text exposition
// format, classic and OpenMetrics, including the Prometheus 3 quoted UTF-8
// name syntax ({"my.metric",code="200"} 1, quoted label names, and quoted
// families on # TYPE/# HELP/# UNIT).
//
// It never buffers more than one line, so memory stays independent of the
// scrape size — a 100k-series endpoint parses in constant memory, which is why
// it exists rather than using a family-buffering parser. Names and label values
// are interned per parse and consecutive-line memcmp caches short-circuit the
// intern probes, so a warm parser from the pool parses a large scrape in a
// handful of allocations.
//
// Basic use:
//
//	p := promparse.New(promparse.Options{})
//	malformed, err := p.Parse(body, func(s promparse.Sample) error {
//	    fmt.Println(s.Name, s.Labels, s.Value)
//	    return nil // Sample and its Labels are only valid during the callback
//	})
//
// On a hot path, take a parser from the pool instead — it keeps the intern
// tables and the read buffer warm across scrapes:
//
//	p := promparse.Get(promparse.Options{OpenMetrics: true, Exemplars: true})
//	defer promparse.Put(p)
//	malformed, err := p.Parse(body, emit)
package promparse

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// MetricType is the declared type of a metric family (# TYPE line).
type MetricType int

// The family types a # TYPE line declares; TypeUntyped is a family without one.
const (
	TypeUntyped MetricType = iota
	TypeCounter
	TypeGauge
	TypeHistogram
	TypeSummary
)

// SampleRole classifies how one series sample participates in its family,
// derived from the family type and the series suffix.
type SampleRole int

const (
	// RoleGauge covers gauges and untyped series.
	RoleGauge SampleRole = iota
	// RoleCounter covers counter samples (with or without _total suffix).
	RoleCounter
	// RoleHistogramBucket is a histogram's _bucket series (one per le). A sample
	// named after a histogram FAMILY itself (the bare `lat` of `# TYPE lat
	// histogram`, which neither exposition format defines) is classified here
	// too, with Name == Family — never as a gauge that would claim the family's
	// name — so a consumer can refuse it: a real bucket series is always
	// `<family>_bucket`. The summary sibling, RoleSummaryQuantile, carries the
	// family name by definition.
	RoleHistogramBucket
	// RoleHistogramSum is a histogram's _sum series.
	RoleHistogramSum
	// RoleHistogramCount is a histogram's _count series.
	RoleHistogramCount
	// RoleSummaryQuantile is a summary's quantile series (one per quantile).
	RoleSummaryQuantile
	// RoleSummarySum is a summary's _sum series.
	RoleSummarySum
	// RoleSummaryCount is a summary's _count series.
	RoleSummaryCount
)

// Label is one label pair (excluding __name__).
type Label struct {
	Name  string
	Value string
}

// Exemplar is an OpenMetrics exemplar attached to a sample.
type Exemplar struct {
	Labels      []Label // valid only during the callback
	Value       float64
	TimestampMs int64 // 0 when absent
}

// CopyExemplar returns a deep copy of an exemplar, so it can outlive the emit
// callback that produced it (Sample, its Labels and its Exemplar all alias
// parser-owned memory that the next line reuses).
func CopyExemplar(e Exemplar) Exemplar {
	e.Labels = append([]Label(nil), e.Labels...)
	return e
}

// Sample is one parsed series sample.
type Sample struct {
	// Name is the full series name (e.g. "http_duration_bucket").
	Name string
	// Family is the metric family the sample belongs to (e.g.
	// "http_duration"); equal to Name for gauges/untyped series.
	Family string
	Role   SampleRole
	Labels []Label // valid only during the callback
	Value  float64
	// TimestampMs is 0 when the exposition carries no timestamp.
	TimestampMs int64
	// Exemplar is non-nil when the sample carries one and exemplar parsing
	// is enabled; valid only during the callback.
	Exemplar *Exemplar
	// Help and Unit are the family's "# HELP" text and "# UNIT" (OpenMetrics)
	// declarations, empty when the exposition carried none. They are stored
	// once per family and repeated on every sample of it, so a consumer that
	// only wants them when it first meets a family need not track state; they
	// outlive the callback (unlike Labels and Exemplar).
	Help string
	Unit string
}

// MaxTrackedFamilies bounds the ENTRY COUNT of the TYPE table. It is exported
// because callers that memoize per parse (a filter caching decisions per series
// name, say) want the same bound.
//
// A count is only half a memory bound, and this doc used to claim it was the
// whole one: the KEYS are family names the target chooses, bounded only by the
// line bound, so the count alone permits MaxTrackedFamilies x MaxLineBytes of
// retained heap. maxTypeBytes and maxFamilyNameBytes are the other half.
const MaxTrackedFamilies = 100_000

// maxFamilyNameBytes bounds ONE family token on a TYPE/HELP/UNIT line. The line
// bound already caps what is read; this caps what is RETAINED per declaration,
// and it exists so that a single absurd name cannot spend the whole
// maxTypeBytes budget and leave every later family of the exposition untyped —
// histogram buckets exported as gauges — through a cap that reports nothing.
// A TYPE line refused here is counted MALFORMED (the existing verdict for a
// TYPE line this parser will not honour), so a pathological target moves
// kubescrape_scrape_malformed_total instead of degrading silently. Real names
// are nowhere near it: the intern table already treats 256 bytes as long.
const maxFamilyNameBytes = 1024

// maxTypeBytes bounds the family names the TYPE table RETAINS for one
// exposition, the way maxMetaBytes bounds the HELP/UNIT text. Without it, a
// ~2 MiB gzip response carrying 100k `# TYPE <16 KiB name> counter` lines and
// no samples at all inflated to >1.5 GB of live heap in the agent — an OOMKill
// of the node's DaemonSet pod, recorded as a scrape with outcome "ok", zero
// samples and nothing malformed. Past the budget a family simply keeps no
// declared type (it classifies as untyped), which is exactly what the count cap
// above already does.
const maxTypeBytes = 1 << 20

// Interning bounds: metric and label names are low-cardinality and repeat on
// nearly every line; label values (namespace, pod, le, code, ...) repeat
// heavily in Kubernetes-style expositions. Exemplar label values (trace and
// span IDs) never repeat and are not interned. A name/value longer than its
// length cap or arriving after the table is full is allocated normally, so
// pathological inputs degrade to the non-interned cost instead of growing
// memory.
//
// The tables do NOT live for one scrape: a pooled parser keeps them WARM across
// parses on purpose (the same names repeat every scrape), and Get clears one
// only once it passes HALF of either bound. So what a table may retain between
// scrapes is half its bound, per pooled parser, for as long as the pool keeps
// the parser — which is why each is bounded in BYTES and not only in entries.
// The value table's bytes follow from its two caps (8192 x 128 B, 1 MiB); the
// name table's did not (100,000 x 256 B is ~25 MiB, measured 29.5 MiB retained
// after one scrape of 99,990 distinct 256-byte names), so it carries its own
// byte budget, maxInternedNameBytes — the same 1 MiB every other
// per-exposition table in this package and in its callers is held to.
const (
	maxInternedNames   = MaxTrackedFamilies
	maxInternedNameLen = 256
	// maxInternedNameBytes bounds the text the name table retains. Past it a
	// new name is allocated normally, exactly like one over the length cap.
	maxInternedNameBytes = 1 << 20
	// MaxInternedValues bounds the label-value intern table; exported for the
	// same reason as MaxTrackedFamilies.
	MaxInternedValues   = 8192
	maxInternedValueLen = 128
)

// maxLongCacheBytes bounds the text the positional last-seen caches (lastKV,
// exLastKV) may hold in names and values too LONG to intern. Everything the
// caches hold at intern size is bounded by position count x length cap; a long
// string is bounded only by the line, and a slot keeps its string until a later
// line reaches that position again. An exposition whose lines carry a
// DECREASING label count therefore left one near-MaxLineBytes value behind at
// every position — measured 127 MiB retained from a 126 MiB body of 128
// malformed lines, and up to MaxLabelsPerSample x MaxLineBytes (~4 GiB) in
// principle, with no sample, budget or counter ever moving.
//
// A long value is still worth caching when it repeats on consecutive lines (a
// systemd-driver cadvisor `id` is ~190 bytes and repeats across a container's
// device and operation rows), so it is cached WITHIN this budget rather than
// never: past it a long string is simply not remembered, which costs that
// line the allocation an uncached long value always costs and nothing else.
const maxLongCacheBytes = 64 << 10

// MaxLabelsPerSample bounds the label count of ONE sample line. The
// duplicate-name check in parseLabels is a linear scan over the labels seen so
// far, run on every appended pair — O(labels²) for one line — and the label
// count is otherwise bounded only by MaxLineBytes (≈116k 5-byte labels in the
// 1 MiB default). That parse is CPU-bound with no ctx check, so the scrape
// timeout cannot abort it (measured: a single ~800 KB line takes ~30s, one
// scrape cycle 1m22s against a 1s timeout) and one hostile/broken target
// freezes the whole node's scrape loop (cycle() waits for every scrape it
// started). Prometheus enforces a label-count limit; past this ceiling the
// line is dropped as malformed, exactly as a duplicate label name is. Real
// exposition rarely exceeds a few dozen labels; this is far above any
// legitimate use.
// It is EXPORTED because the protobuf front in internal/agent/promscrape runs
// the same quadratic scan over its own label slice and must apply the same
// ceiling — capping only this one left the other door wide open, and the proto
// side is the worse of the two: it materialises the whole message before
// scanning, so there is no interleaved socket read for the scrape deadline to
// land on.
const MaxLabelsPerSample = 4096

// MaxMetaBytes bounds the "# HELP"/"# UNIT" text retained for one exposition.
// The meta table is per-exposition like the TYPE table, but its VALUES are free
// text (a whole line each), which MaxTrackedFamilies alone would not bound —
// past the budget later families simply carry no description. Exported so the
// protobuf front in internal/agent/promscrape, which has no table but stamps
// the same text on every point it emits, applies the same per-exposition
// budget and the two fronts describe one target alike.
const MaxMetaBytes = 1 << 20

// maxMetaBytes is the internal spelling used below.
const maxMetaBytes = MaxMetaBytes

// MaxExemplarLabelSetRunes is the OpenMetrics bound on one exemplar's label
// set: "the combined length of the label names and values of an Exemplar's
// LabelSet MUST NOT exceed 128 UTF-8 character code points". It is Prometheus's
// exemplar.ExemplarMaxLabelSetLength, which its exemplar storage enforces, so an
// exemplar past it is one no Prometheus-compatible backend keeps. Counted in
// RUNES, never bytes: a byte bound would refuse spec-valid non-ASCII sets.
//
// An exemplar over it is refused (counted by MalformedExemplars) and its sample
// kept, like any other unusable exemplar. The bound is also what keeps an
// exemplar CHEAP downstream: every accepted label becomes an OTLP attribute
// written by an insert that probes the map linearly, so a label COUNT bounded
// only by MaxLabelsPerSample made that write quadratic. Exported because the
// protobuf front applies the same rule.
const MaxExemplarLabelSetRunes = 128

// familyMeta is one family's HELP text and UNIT.
type familyMeta struct{ help, unit string }

// Parser parses one scrape body. Not safe for concurrent use; create one per
// scrape.
type Parser struct {
	maxLineBytes int
	// openMetrics switches timestamp units to seconds (float), honors the
	// "# EOF" terminator and allows exemplars.
	openMetrics bool
	// exemplars enables parsing of "# {...} v [ts]" exemplar suffixes
	// (OpenMetrics only).
	exemplars bool

	types map[string]MetricType
	// typeBytes is the retained key text of the TYPE table; see maxTypeBytes.
	typeBytes int
	names     map[string]string // interned metric/label names
	// nameBytes is the retained text of the name table; see
	// maxInternedNameBytes. It moves with the table: charged on insert, zeroed
	// wherever the table is cleared.
	nameBytes int
	values    map[string]string // interned sample label values (never exemplar ones)
	// metas holds each family's HELP/UNIT, recorded once per family from its
	// comment lines and stamped on every sample of it via lastClass below.
	// metaBytes is the retained text budget; see maxMetaBytes.
	metas     map[string]familyMeta
	metaBytes int
	// lastClass memoizes the previous line's classification (role, family and
	// the family's HELP/UNIT) by metric name: consecutive lines share a name
	// and names are interned, so the repeat check is normally a pointer
	// comparison — it replaces classify's map probe and suffix walks as well as
	// the meta lookup. A TYPE/HELP/UNIT line that CHANGES a table invalidates
	// it; an unchanged redeclaration does not.
	lastClass   classified
	lastClassOK bool
	// Consecutive lines of a family are near-identical: lastMetric and the
	// per-position lastKV short-circuit the intern-map probes with a plain
	// memcmp (string(b) == s does not allocate), which is ~5x cheaper.
	// Exemplar labels have their own positional cache (exLastKV) so
	// exemplar-bearing lines do not evict the sample-label entries, and their
	// VALUES are never interned, so they do not evict the sample values from
	// the shared table either (see parseLabels).
	lastMetric string
	lastKV     []lastKV
	exLastKV   []lastKV
	// longCacheBytes is what lastKV and exLastKV currently hold in strings too
	// long to intern; see maxLongCacheBytes and cacheString.
	longCacheBytes int
	// labels and exLabels are reused between lines, and are CLEARED before each
	// reuse rather than only resliced: an entry past the current line's length
	// still references the string an earlier, longer line put there.
	labels   []Label
	exLabels []Label
	exemplar Exemplar
	scratch  []byte // for lines spanning bufio reads
	eof      bool   // saw "# EOF"
	// badExemplars counts this parse's unparseable (or refused, see
	// MaxExemplarLabelSetRunes) exemplar suffixes. It is deliberately NOT
	// folded into the malformed count: those samples were emitted (finishSample
	// keeps them), so a reader of the two numbers can tell a target with broken
	// exemplars from one losing data.
	badExemplars int
	// detail breaks this parse's malformed count down by the four reasons that
	// are diagnosable from where they are noticed (see MalformedDetail).
	detail MalformedDetail
}

// MalformedDetail names the parts of one parse's malformed count that can be
// attributed to a CAUSE at the point they are noticed. The total that Parse
// returns stays the number to alert on; this is what turns it into an action.
//
// The four are not interchangeable and lead to opposite remedies: over-long
// and truncated lines are about the BODY (a bound of ours, or a connection cut
// mid-stream), while duplicate and excess labels are about the target's
// EXPOSITION and mean the exporter itself is producing series Prometheus would
// reject the whole scrape over. Everything else — an unparseable value, a
// broken TYPE line, a bad quoted name — stays in the total only: those share
// one shape (this line does not parse) and splitting them further would name
// parser internals rather than anything an operator can act on.
type MalformedDetail struct {
	// OverLongLines is lines dropped for exceeding Options.MaxLineBytes. The
	// remedy is the EXPORTER, which is emitting a single enormous line (a
	// runaway label value, a missing newline); a caller may also raise
	// Options.MaxLineBytes, at the cost of that much memory per parse.
	OverLongLines int
	// TruncatedLines is the partial line left by a body that ended mid-stream
	// (a read error other than a clean EOF). It means the SCRAPE was cut, so
	// series are missing beyond it whatever the total says.
	TruncatedLines int
	// DuplicateLabels is sample lines dropped for repeating a label NAME.
	// Prometheus rejects the whole scrape for this; this parser drops the line
	// (see parseLabels), so a nonzero count is a target bug that costs series.
	DuplicateLabels int
	// TooManyLabels is sample lines dropped for exceeding MaxLabelsPerSample.
	TooManyLabels int
}

// Any reports whether any reason was attributed, so a caller can skip the
// per-reason report entirely on the ordinary path.
func (d MalformedDetail) Any() bool {
	return d.OverLongLines != 0 || d.TruncatedLines != 0 || d.DuplicateLabels != 0 || d.TooManyLabels != 0
}

// DefaultMaxLineBytes bounds one physical line when Options leaves
// MaxLineBytes zero. The parser holds at most one line, so this is what keeps
// memory constant against an endpoint that never emits a newline.
const DefaultMaxLineBytes = 1 << 20

// Options configure a parser. The zero value parses classic Prometheus text
// with the default line bound and no exemplars.
type Options struct {
	// MaxLineBytes skips physical lines longer than this
	// (0 = DefaultMaxLineBytes). Over-long lines are consumed and counted
	// malformed rather than returned.
	MaxLineBytes int
	// OpenMetrics selects OpenMetrics semantics — float-second timestamps,
	// the "# EOF" terminator — typically decided from the response
	// Content-Type.
	OpenMetrics bool
	// Exemplars additionally parses "# {...}" exemplar suffixes onto samples
	// (OpenMetrics only; ignored otherwise).
	Exemplars bool
}

// normLineBytes maps the zero value onto the default: 0 read literally would
// make every line "too long" and silently skip the whole exposition.
func normLineBytes(maxLineBytes int) int {
	if maxLineBytes <= 0 {
		return DefaultMaxLineBytes
	}
	return maxLineBytes
}

// New creates a parser.
func New(opts Options) *Parser {
	return &Parser{
		maxLineBytes: normLineBytes(opts.MaxLineBytes),
		openMetrics:  opts.OpenMetrics,
		exemplars:    opts.OpenMetrics && opts.Exemplars,
		types:        make(map[string]MetricType),
		names:        make(map[string]string),
	}
}

// parserPool recycles parsers (and their bufio readers) across parses: the
// interned name/value tables stay warm — the same names repeat every scrape —
// and the 64KiB read buffer stops being per-scrape garbage. The TYPE and
// HELP/UNIT tables are cleared per parse (their semantics are per-exposition);
// the intern tables are only cleared once they pass half of a bound, which is
// what bounds their retention.
var parserPool = sync.Pool{New: func() any {
	return &Pooled{
		p:      New(Options{}),
		reader: bufio.NewReaderSize(nil, parseBufSize),
	}
}}

const parseBufSize = 64 * 1024

// Pooled is a parser taken from the shared pool: its intern tables stay warm
// across parses (the same names repeat every scrape) and it carries a reusable
// read buffer, so a large scrape parses in a handful of allocations. Obtain
// one with Get and return it with Put. Each Parse is one exposition, exactly as
// Parser.Parse is; a caller normally makes one.
type Pooled struct {
	p      *Parser
	reader *bufio.Reader
}

// Get returns a pooled parser configured for one parse. Return it with Put
// when the parse is done; the parser must not be used afterwards.
func Get(opts Options) *Pooled {
	pp := parserPool.Get().(*Pooled)
	pp.reset(opts)
	return pp
}

// reset is Get's half of a pool round trip, apart from the pool itself: tests
// drive release+reset on ONE *Pooled to exercise exactly what a recycled parser
// goes through, which a Put followed by a Get cannot promise (the pool may hand
// back a fresh parser, and under -race it drops one Put in four on purpose).
func (pp *Pooled) reset(opts Options) {
	p := pp.p
	p.maxLineBytes = normLineBytes(opts.MaxLineBytes)
	p.openMetrics = opts.OpenMetrics
	p.exemplars = opts.OpenMetrics && opts.Exemplars
	p.releaseLineRefs()
	// The per-exposition tables (TYPE, HELP/UNIT, the EOF flag) are reset by
	// every parse, in parseFrom; only the warm intern tables are decided here.
	if len(p.names) >= maxInternedNames/2 || p.nameBytes >= maxInternedNameBytes/2 {
		clear(p.names)
		p.nameBytes = 0
	}
	if len(p.values) >= MaxInternedValues/2 {
		clear(p.values)
	}
}

// Put returns a pooled parser for reuse.
func Put(pp *Pooled) {
	pp.release()
	parserPool.Put(pp)
}

// release is Put's half of a pool round trip; see reset.
func (pp *Pooled) release() {
	pp.reader.Reset(nil) // drop the response body reference
	// And every string the last scrape left behind: a parser can sit in the
	// pool for as long as the GC leaves it there, and nothing of a finished
	// parse needs to outlive it except the intern tables (the reuse buffers
	// keep their capacity, not the strings they referenced). That is the
	// per-line state AND the per-exposition TYPE and HELP/UNIT tables, up to a
	// MiB of text each, which the next parse would clear anyway — parseFrom's
	// second clear then finds an empty map and costs nothing.
	pp.p.releaseLineRefs()
	pp.p.resetTypes()
	pp.p.resetMeta()
}

// releaseLineRefs empties the per-line reuse state — the positional caches, the
// label buffers, the last metric name, the classification memo and the exemplar
// scratch — dropping every string reference in them, and releases the
// long-cache charge with them.
// clear, never a bare reslice: a [:0] keeps its backing array's strings alive
// (see Parser.labels). Each buffer is cleared across its whole CAPACITY, not
// just its length, so this does not depend on every per-line reuse site having
// cleared before it resliced — once per scrape, over at most a few thousand
// entries.
func (p *Parser) releaseLineRefs() {
	p.lastMetric = ""
	// The memo's name and family alias the last line's metric name, which is
	// not interned past maxInternedNameLen and may be up to MaxLineBytes long,
	// and its help/unit alias the meta table's text.
	p.lastClass, p.lastClassOK = classified{}, false
	clear(p.lastKV[:cap(p.lastKV)])
	p.lastKV = p.lastKV[:0]
	clear(p.exLastKV[:cap(p.exLastKV)])
	p.exLastKV = p.exLastKV[:0]
	p.longCacheBytes = 0
	clear(p.labels[:cap(p.labels)])
	p.labels = p.labels[:0]
	clear(p.exLabels[:cap(p.exLabels)])
	p.exLabels = p.exLabels[:0]
	p.exemplar = Exemplar{}
}

// Parse reads the exposition from r through the pooled reader, invoking emit
// for every sample (see Parser.Parse — including its per-exposition reset, so a
// second call is a second exposition and a previous "# EOF" does not end it).
func (pp *Pooled) Parse(r io.Reader, emit func(Sample) error) (int, error) {
	pp.reader.Reset(r)
	return pp.p.parseFrom(pp.reader, emit)
}

// MalformedExemplars reports the last parse's unparseable or refused exemplar
// suffixes (see Parser.MalformedExemplars). Read it before Put: the next user of the
// pooled parser resets the count.
func (pp *Pooled) MalformedExemplars() int { return pp.p.MalformedExemplars() }

// MalformedDetail breaks the last parse's malformed count down by cause (see
// Parser.MalformedDetail). Read it before Put, like MalformedExemplars.
func (pp *Pooled) MalformedDetail() MalformedDetail { return pp.p.MalformedDetail() }

// lastKV caches the previous line's interned name/value at one label
// position.
type lastKV struct {
	name, value string
}

// internName returns a canonical string for a metric or label name. The
// map[string(b)] lookup does not allocate; only a first-seen name does.
func (p *Parser) internName(b []byte) string {
	if len(b) > maxInternedNameLen {
		return string(b)
	}
	if s, ok := p.names[string(b)]; ok {
		return s
	}
	s := string(b)
	if len(p.names) < maxInternedNames && p.nameBytes+len(s) <= maxInternedNameBytes {
		p.names[s] = s
		p.nameBytes += len(s)
	}
	return s
}

// internValue returns a canonical string for a label value, deduplicating the
// heavy repetition of k8s-style values across a scrape's series.
func (p *Parser) internValue(b []byte) string {
	if len(b) > maxInternedValueLen {
		return string(b)
	}
	if s, ok := p.values[string(b)]; ok {
		return s
	}
	s := string(b)
	if p.values == nil {
		p.values = make(map[string]string, 256)
	}
	if len(p.values) < MaxInternedValues {
		p.values[s] = s
	}
	return s
}

// skipSpaceTab trims leading spaces and tabs (a hand-rolled bytes.TrimLeft:
// the stdlib builds an ASCII set per call, which dominated parse CPU).
func skipSpaceTab(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		b = b[1:]
	}
	return b
}

// trimSeparator drops the single blank that ends a directive's family token.
// One separator is all that belongs to the syntax: HELP text is free text, so
// trimming every leading blank swallows indentation the exposition put in a
// description on purpose (the reference parser keeps everything past that one
// byte, and the text becomes the OTLP Description verbatim).
func trimSeparator(b []byte) []byte {
	if len(b) > 0 && (b[0] == ' ' || b[0] == '\t') {
		return b[1:]
	}
	return b
}

// Parse reads the exposition text from r, invoking emit for every sample.
// The Sample (including Labels and Exemplar) is only valid during the
// callback. A non-nil error from emit aborts the parse. Malformed lines are
// skipped, counted and reported; a malformed count with a nil error means a
// partially usable scrape.
func (p *Parser) Parse(r io.Reader, emit func(Sample) error) (malformed int, err error) {
	return p.parseFrom(bufio.NewReaderSize(r, parseBufSize), emit)
}

// MalformedExemplars reports how many exemplar suffixes of the last parse were
// unparseable or refused (a label set over MaxExemplarLabelSetRunes). Their
// samples were emitted intact, so this is a count of lost ANNOTATIONS and never
// of lost data — it belongs on its own counter, not on the caller's
// malformed-sample one. It is 0 unless Options.Exemplars asked for exemplars: a
// parser that is not attaching them does not parse them either.
func (p *Parser) MalformedExemplars() int { return p.badExemplars }

// MalformedDetail reports which causes the last parse's malformed count can be
// attributed to. The sum of its fields never exceeds the count Parse returned;
// the remainder is lines that simply did not parse (see MalformedDetail).
func (p *Parser) MalformedDetail() MalformedDetail { return p.detail }

// resetTypes drops the previous exposition's TYPE declarations together with
// the byte charge that bounds them. The two must move as one: a charge left
// standing over a cleared table would refuse to type the NEXT exposition's
// families, so it lives here rather than beside the call that clears the table
// (parseFrom's per-exposition reset, the one both entry points run).
func (p *Parser) resetTypes() {
	clear(p.types)
	p.typeBytes = 0
}

// resetMeta drops the previous exposition's HELP/UNIT declarations (they are
// per-exposition, exactly like the TYPE table) and the classification memo
// built from them.
func (p *Parser) resetMeta() {
	clear(p.metas)
	p.metaBytes = 0
	p.lastClass, p.lastClassOK = classified{}, false
}

func (p *Parser) parseFrom(br *bufio.Reader, emit func(Sample) error) (malformed int, err error) {
	// Each parse is one exposition: clear the previous one's terminal state,
	// family classifications and statistics. HERE, the one function both entry
	// points (Parser.Parse and Pooled.Parse) run, and nowhere else — the reset
	// used to live in Parse and in Get, so a second Pooled.Parse on one parser
	// skipped it and a stale `# EOF` silently ended the new exposition after
	// its first sample, while New()+Parse+Parse (a legal use of the public API)
	// only worked because Parse had its own copy. A count carried over from
	// the previous parse would likewise be charged to this target. (Put also
	// empties the two tables, but only to release their text while pooled;
	// no parse relies on that.)
	p.eof = false
	p.resetTypes()
	p.resetMeta()
	p.badExemplars = 0
	p.detail = MalformedDetail{}
	for {
		line, tooLong, rerr := p.readLine(br)
		// A read error other than io.EOF cut the body mid-line, so what came
		// back is a PREFIX and not a line: a value token severed mid-number
		// parses as a smaller, entirely plausible number, and a caller that
		// keeps what was converted before an abort then ships it as real. Only
		// a clean EOF completes an unterminated final line.
		truncated := rerr != nil && !errors.Is(rerr, io.EOF)
		if tooLong || (truncated && len(line) > 0) {
			malformed++
			// Attributed here rather than folded into the total alone: a body
			// cut mid-stream and a line over the bound look identical in the
			// count and want opposite responses (see MalformedDetail).
			if tooLong {
				p.detail.OverLongLines++
			} else {
				p.detail.TruncatedLines++
			}
		} else if len(line) > 0 {
			if ok := p.parseLine(line, emit, &err); err != nil {
				return malformed, err
			} else if !ok {
				malformed++
			}
			if p.eof {
				return malformed, nil
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return malformed, nil
			}
			return malformed, rerr
		}
	}
}

// readLine returns the next line without its trailing newline. Lines longer
// than maxLineBytes are consumed and flagged rather than returned.
func (p *Parser) readLine(br *bufio.Reader) (line []byte, tooLong bool, err error) {
	p.scratch = p.scratch[:0]
	for {
		chunk, rerr := br.ReadSlice('\n')
		// !tooLong is load-bearing: an over-long FIRST chunk that did not end
		// in a newline leaves scratch empty (nothing is appended once the
		// budget is blown), so without it this early-out returned the line's
		// short trailing chunk as a complete, in-budget line — emitting a
		// SAMPLE that does not exist in the exposition, with malformed=0 and
		// no error. The flag has to survive the whole physical line.
		if !tooLong && len(p.scratch) == 0 && rerr == nil {
			// The bound is on the LINE — what this function returns, terminator
			// excluded. Measuring the ReadSlice chunk instead charged the '\n',
			// so a line of exactly MaxLineBytes was dropped as malformed when it
			// was terminated and parsed when it happened to be the final
			// unterminated line: the same bytes, two verdicts, and the sample
			// lost in one of them.
			if line := trimEOL(chunk); len(line) <= p.maxLineBytes {
				// Whole line inside the buffer: no copy needed.
				return line, false, nil
			}
			return nil, true, nil
		}
		n := len(chunk)
		if rerr == nil {
			n = len(trimEOL(chunk)) // the terminating chunk: see above
		}
		if len(p.scratch)+n <= p.maxLineBytes {
			p.scratch = append(p.scratch, chunk...)
		} else {
			tooLong = true
		}
		switch {
		case rerr == nil:
			if tooLong {
				return nil, true, nil
			}
			return trimEOL(p.scratch), false, nil
		case errors.Is(rerr, bufio.ErrBufferFull):
			continue
		default:
			if tooLong {
				return nil, true, rerr
			}
			return trimEOL(p.scratch), false, rerr
		}
	}
}

func trimEOL(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// parseLine handles one line; ok is false for malformed sample lines.
func (p *Parser) parseLine(line []byte, emit func(Sample) error, emitErr *error) bool {
	line = skipSpaceTab(line)
	if len(line) == 0 {
		return true
	}
	if line[0] == '#' {
		return p.parseComment(line)
	}

	s, ok := p.parseSample(line)
	if !ok {
		return false
	}
	c := p.classifyCached(s.Name)
	s.Role, s.Family, s.Help, s.Unit = c.role, c.family, c.help, c.unit
	if err := emit(s); err != nil {
		*emitErr = err
	}
	return true
}

// nextField returns the next space/tab-delimited token and the remainder
// after it.
func nextField(b []byte) (tok, rest []byte) {
	b = skipSpaceTab(b)
	i := 0
	for i < len(b) && b[i] != ' ' && b[i] != '\t' {
		i++
	}
	return b[:i], b[i:]
}

// parseComment records # TYPE, # HELP and # UNIT declarations and the
// OpenMetrics # EOF terminator; other comments are ignored after their second
// token rather than being tokenized whole. It reports false for a malformed
// TYPE line (missing family/type, or trailing garbage) so the caller counts it;
// a HELP/UNIT line without a family is ignored (it declares nothing, and
// counting free-text comments malformed would be noise).
func (p *Parser) parseComment(line []byte) bool {
	_, rest := nextField(line) // the leading "#" token
	directive, rest := nextField(rest)
	switch {
	case p.openMetrics && string(directive) == "EOF": // no alloc: memcmp
		if len(skipSpaceTab(rest)) == 0 {
			p.eof = true
		}
	case string(directive) == "HELP":
		// The remainder of the line is free text (spaces included), escaped
		// for backslash and newline.
		bare, family, rest, ok := familyTokenRaw(rest)
		if !ok {
			break
		}
		text := trimSeparator(rest)
		if len(text) == 0 {
			break // declares nothing (setMeta ignores an empty pair)
		}
		// An unchanged redeclaration — exporters that repeat a family's HELP
		// before every sample exist — is recognised BEFORE the family name and
		// the text are materialised, so it costs two memcmps and no allocation.
		// setMeta would return early for it anyway; this only skips the copies
		// it would have been handed. An escaped text takes the slow path (its
		// bytes are not its value).
		if bare != nil && bytes.IndexByte(text, '\\') < 0 {
			if m, seen := p.metas[string(bare)]; seen && m.help == string(text) {
				break
			}
		}
		if bare != nil {
			family = string(bare)
		}
		p.setMeta(family, unescapeHelp(text), "")
	case string(directive) == "UNIT":
		// OpenMetrics only, one token. Carried VERBATIM into the OTLP unit:
		// the exposition is the only authority on what its values measure, and
		// guessing a UCUM translation would be lossy in a way the raw string
		// is not.
		family, rest, ok := familyToken(rest)
		unit, rest := nextField(rest)
		if ok && len(unit) > 0 && len(skipSpaceTab(rest)) == 0 {
			p.setMeta(family, "", string(unit))
		}
	case string(directive) == "TYPE":
		bare, family, rest, ok := familyTokenRaw(rest)
		typ, rest := nextField(rest)
		if !ok || len(typ) == 0 || len(skipSpaceTab(rest)) != 0 {
			return false // malformed TYPE: counted, not silently ignored
		}
		t := metricTypeOf(typ)
		var old MetricType
		var seen bool
		if bare != nil {
			old, seen = p.types[string(bare)] // keyed lookup: no allocation
		} else {
			old, seen = p.types[family]
		}
		if seen && old == t {
			// An unchanged redeclaration — exporters that repeat a family's TYPE
			// before every one of its samples exist — changes no classify()
			// answer, so it must neither copy the name nor invalidate the
			// per-name memo: an unconditional invalidation re-ran classify's map
			// probe and suffix walks for every sample of such a family, the
			// cost setMeta already refuses for a repeated HELP.
			return true
		}
		if bare != nil {
			family = string(bare)
		}
		// The charge is per NEW key and never per line: exporters that repeat a
		// family's TYPE before every one of its samples exist, and charging them
		// again each time would spend the budget on one legitimate family and
		// leave the rest of the exposition untyped. A map assignment to an
		// existing key keeps the ORIGINAL key string, so the first charge is
		// exactly what stays retained.
		if !seen {
			if len(p.types) >= MaxTrackedFamilies || p.typeBytes+len(family) > maxTypeBytes {
				return true // over the table bound: a deliberate cap, not malformed
			}
			p.typeBytes += len(family)
		}
		p.types[family] = t
		p.lastClassOK = false // the memo may hold the family just retyped
	}
	return true
}

// metricTypeOf maps a TYPE line's type token; anything unrecognised is
// untyped, as in Prometheus.
func metricTypeOf(typ []byte) MetricType {
	switch string(typ) {
	case "counter":
		return TypeCounter
	case "gauge":
		return TypeGauge
	case "histogram":
		return TypeHistogram
	case "summary":
		return TypeSummary
	default:
		return TypeUntyped
	}
}

// familyToken reads a family-name token after a TYPE/HELP/UNIT directive: the
// Prometheus 3 quoted form ("my.metric" — escaped, may contain any UTF-8
// including spaces) or a bare field. The UNESCAPED name is the table key, so
// it matches the names quoted sample lines produce. It materialises the name
// on every call, which is only right on a path that stores it every time: the
// TYPE and HELP arms, which an exporter may repeat before every sample, go
// through familyTokenRaw and copy the name only when the table changes.
//
// A token past maxFamilyNameBytes is refused rather than returned: it would be
// RETAINED for the whole exposition as a TYPE-table key (or charged to the meta
// budget), and the bare arm checks the length before materializing the string
// so an absurd name is not even copied once.
func familyToken(rest []byte) (string, []byte, bool) {
	bare, name, rem, ok := familyTokenRaw(rest)
	if ok && bare != nil {
		name = string(bare)
	}
	return name, rem, ok
}

// familyTokenRaw is familyToken without materialising a BARE name: that is
// returned as bare, a view into the line valid until the next one, so a caller
// can probe a table with m[string(bare)] — which does not allocate — and copy
// the name only when it actually stores it. A quoted name is decoded eagerly
// (the cold path) and returned in name, with bare nil.
func familyTokenRaw(rest []byte) (bare []byte, name string, rem []byte, ok bool) {
	rest = skipSpaceTab(rest)
	if len(rest) > 0 && rest[0] == '"' {
		fam, rem, ok := parseQuotedSlow(rest[1:])
		return nil, fam, rem, ok && fam != "" && len(fam) <= maxFamilyNameBytes
	}
	tok, rem := nextField(rest)
	if len(tok) == 0 || len(tok) > maxFamilyNameBytes {
		return nil, "", rem, false
	}
	return tok, "", rem, true
}

// setMeta records a family's HELP text or UNIT (the empty string leaves the
// other field alone). Called once per family — the per-sample path only reads
// the table, through classifyCached — so the key and text allocations here are
// per family, never per line.
func (p *Parser) setMeta(family string, help, unit string) {
	if help == "" && unit == "" {
		return
	}
	m, seen := p.metas[family]
	// The empty string leaves the other field alone, so what this call RETAINS
	// is the merged pair.
	if help == "" {
		help = m.help
	}
	if unit == "" {
		unit = m.unit
	}
	if seen && help == m.help && unit == m.unit {
		// A redeclaration of the same text retains nothing new and changes
		// nothing, so it must neither charge the budget nor invalidate the
		// per-name memo (an unconditional invalidation restored classifyCached's
		// map probe and suffix walks for every sample of a family whose exporter
		// repeats its HELP).
		return
	}
	// Bounded like the TYPE table, and by BYTES as well: these values are free
	// text, which a count cap alone would not bound. The charge is what this
	// call newly RETAINS — the key on first insert (a map assignment to an
	// existing key keeps the ORIGINAL key string) plus the growth of the text —
	// and never one charge per DECLARATION: exporters that repeat a family's
	// HELP before every one of its samples exist, the same ones that repeat its
	// TYPE, and charging them again each time spent the 1 MiB budget on one
	// legitimate family after ~10k lines and left every family declared later in
	// the exposition with no OTLP Description and no Unit — silently, with
	// nothing counted, and with the missing unit changing what the downstream
	// OTLP→Prometheus rewrite names those series.
	add := len(help) + len(unit) - len(m.help) - len(m.unit)
	if !seen {
		if len(p.metas) >= MaxTrackedFamilies {
			return // over the table bound: a deliberate cap, not malformed
		}
		add += len(family)
	}
	if add > 0 && p.metaBytes+add > maxMetaBytes {
		return
	}
	p.metaBytes += add
	if p.metas == nil {
		p.metas = make(map[string]familyMeta, 16)
	}
	p.metas[family] = familyMeta{help: help, unit: unit}
	p.lastClassOK = false // the memo may hold the family just redeclared
}

// unescapeHelp decodes the escapes the exposition formats allow in HELP text:
// \\ and \n in both, plus \" which OpenMetrics adds (accepting it in classic
// text too costs nothing — a literal backslash-quote in HELP is not a thing
// either format can express otherwise). The escape-free case, which is nearly
// all of them, is one allocation and no scan beyond IndexByte.
func unescapeHelp(b []byte) string {
	i := bytes.IndexByte(b, '\\')
	if i < 0 {
		return string(b)
	}
	var sb strings.Builder
	sb.Grow(len(b))
	sb.Write(b[:i])
	for ; i < len(b); i++ {
		// A trailing lone backslash is kept as text: HELP is free text, so
		// there is nothing to fail (parseQuotedSlow fails the value instead).
		if b[i] != '\\' || i+1 >= len(b) {
			sb.WriteByte(b[i])
			continue
		}
		i++
		writeUnescaped(&sb, b[i])
	}
	return sb.String()
}

// writeUnescaped writes the decoding of the escape sequence `\c` — the ONE
// escape table both the HELP text (unescapeHelp) and a quoted label value
// (parseQuotedSlow) decode through, so the two cannot disagree: \n, \\ and \"
// decode, and any other escape is kept verbatim, backslash included. What each
// caller does with a backslash that ends its input differs on purpose and stays
// at the caller.
func writeUnescaped(sb *strings.Builder, c byte) {
	switch c {
	case 'n':
		sb.WriteByte('\n')
	case '\\', '"':
		sb.WriteByte(c)
	default:
		sb.WriteByte('\\')
		sb.WriteByte(c)
	}
}

// classified is one metric name's resolved role, family and family metadata.
type classified struct {
	name, family, help, unit string
	role                     SampleRole
}

// classifyCached resolves a metric name through the last-line memo. Consecutive
// lines of a family repeat the name and names are interned, so the hit is
// normally a pointer comparison — it skips classify's TYPE probe and suffix
// walks along with the HELP/UNIT lookup, both of which are per-FAMILY facts
// that a per-sample path should not recompute. The returned pointer is the
// parser's own memo: read it out before the next line.
func (p *Parser) classifyCached(name string) *classified {
	if !p.lastClassOK || name != p.lastClass.name {
		c := classified{name: name}
		c.role, c.family = p.classify(name)
		if len(p.metas) > 0 { // an exposition without HELP/UNIT pays nothing
			m := p.metas[c.family]
			c.help, c.unit = m.help, m.unit
		}
		p.lastClass, p.lastClassOK = c, true
	}
	return &p.lastClass
}

// classify resolves the sample role and family from the TYPE table,
// accounting for the _bucket/_sum/_count/_total series suffixes.
func (p *Parser) classify(name string) (SampleRole, string) {
	if t, ok := p.types[name]; ok {
		switch t {
		case TypeCounter:
			return RoleCounter, name
		case TypeSummary:
			// Quantile series carry the family name itself.
			return RoleSummaryQuantile, name
		case TypeHistogram:
			// No histogram series carries the bare family name. Classified as
			// the family's own (see RoleHistogramBucket) rather than as a gauge
			// named like it: a gauge claims the OTLP name first and every
			// histogram point of the family then collides and is dropped.
			return RoleHistogramBucket, name
		default:
			return RoleGauge, name
		}
	}
	if base, found := strings.CutSuffix(name, "_bucket"); found {
		if p.types[base] == TypeHistogram {
			return RoleHistogramBucket, base
		}
	}
	if base, found := strings.CutSuffix(name, "_sum"); found {
		switch p.types[base] {
		case TypeHistogram:
			return RoleHistogramSum, base
		case TypeSummary:
			return RoleSummarySum, base
		}
	}
	if base, found := strings.CutSuffix(name, "_count"); found {
		switch p.types[base] {
		case TypeHistogram:
			return RoleHistogramCount, base
		case TypeSummary:
			return RoleSummaryCount, base
		}
	}
	if base, found := strings.CutSuffix(name, "_total"); found {
		if p.types[base] == TypeCounter {
			return RoleCounter, base
		}
	}
	return RoleGauge, name
}

// parseSample parses
//
//	name{labels} value [timestamp] [# {exemplar-labels} value [timestamp]]
func (p *Parser) parseSample(line []byte) (Sample, bool) {
	var s Sample

	// Metric name. Consecutive lines of a family share it, so matching the
	// previous line's name plus its terminator in one memcmp skips the byte
	// scan entirely.
	i := 0
	if n := len(p.lastMetric); n > 0 && n < len(line) && string(line[:n]) == p.lastMetric {
		switch line[n] {
		case '{', ' ', '\t':
			i = n
			s.Name = p.lastMetric
		}
	}
	if s.Name == "" {
		for i < len(line) && line[i] != '{' && line[i] != ' ' && line[i] != '\t' {
			i++
		}
		if i == 0 {
			// No bare name. The Prometheus 3 quoted form carries the name as
			// the first brace entry instead: {"my.metric",label="v"} 1
			return p.parseQuotedNameSample(line)
		}
		if string(line[:i]) == p.lastMetric { // memcmp fast path, no alloc
			s.Name = p.lastMetric
		} else {
			s.Name = p.internName(line[:i])
			p.lastMetric = s.Name
		}
	}
	rest := line[i:]
	// Classic exposition allows blanks between the name and the label block —
	// both reference parsers skip them — and rejecting the line loses every
	// series of a target that writes it. OpenMetrics forbids the blank, so
	// there it stays a parse error. Consuming it costs the value path nothing:
	// parseFloatToken skips leading blanks itself.
	if !p.openMetrics && len(rest) > 0 && rest[0] != '{' {
		rest = skipSpaceTab(rest)
	}

	// Labels. Cleared, not just resliced: see Parser.labels.
	clear(p.labels)
	p.labels = p.labels[:0]
	if len(rest) > 0 && rest[0] == '{' {
		var ok bool
		rest, ok = p.parseLabels(rest[1:], &p.labels, &p.lastKV, &p.detail, true)
		if !ok {
			return s, false
		}
	}
	s.Labels = p.labels
	// Sequenced deliberately: finishSample MUTATES s (value, timestamp,
	// exemplar), and `return s, p.finishSample(&s, rest)` leaves it to the
	// compiler whether the returned copy of s is taken before or after that
	// call. The Go spec orders function CALLS within a return statement but
	// explicitly leaves the order of the other operands unspecified (the
	// `[]int{a, f()}` case), so the one-line form is only correct by luck of
	// gc's current evaluation order — and the failure it permits is silent:
	// every sample would ship with a zero Value/Timestamp/Exemplar, with
	// malformed=0 and no error, on the hot path of every scrape.
	ok := p.finishSample(&s, rest)
	return s, ok
}

// parseQuotedNameSample parses the Prometheus 3 quoted-name sample form,
// where the metric name is the first (bare, quoted) entry of the brace block:
//
//	{"my.metric",code="200","my.label"="v"} 1 [ts]
//
// A quoted first entry FOLLOWED by '=' is a label, not the name, and a brace
// block without a name entry names nothing — both stay malformed, exactly as
// prometheus/common's parser treats them. Cold path: dotted-name targets are
// the exception, not the rule, so no last-seen caches participate here.
func (p *Parser) parseQuotedNameSample(line []byte) (Sample, bool) {
	var s Sample
	if len(line) < 2 || line[0] != '{' {
		return s, false
	}
	rest := skipSpaceTab(line[1:])
	if len(rest) == 0 || rest[0] != '"' {
		return s, false
	}
	name, rem, ok := p.parseQuoted(rest[1:], nil, true)
	if !ok || name == "" {
		return s, false
	}
	rem = skipSpaceTab(rem)
	if len(rem) > 0 && rem[0] == '=' {
		return s, false // the quoted entry was a label; the block has no name
	}
	s.Name = name
	if len(rem) > 0 && rem[0] == ',' {
		rem = rem[1:]
	}
	clear(p.labels) // see Parser.labels
	p.labels = p.labels[:0]
	rem, ok = p.parseLabels(rem, &p.labels, &p.lastKV, &p.detail, true)
	if !ok {
		return s, false
	}
	s.Labels = p.labels
	// Sequenced for the reason parseSample's identical tail spells out: the
	// call mutates s, and its order against the returned copy is unspecified.
	ok = p.finishSample(&s, rem)
	return s, ok
}

// finishSample parses the value, optional timestamp and optional exemplar
// after the name/labels — shared by the classic and quoted-name forms. It
// takes s by POINTER: Sample is a wide struct and this runs once per sample,
// so passing it (and returning it) by value cost ~9% of parse throughput.
func (p *Parser) finishSample(s *Sample, rest []byte) bool {
	var ok bool
	s.Value, rest, ok = p.parseFloatToken(rest)
	if !ok {
		return false
	}

	// Optional timestamp.
	rest = skipSpaceTab(rest)
	if len(rest) > 0 && rest[0] != '#' {
		s.TimestampMs, rest, ok = p.parseTimestampToken(rest)
		if !ok {
			return false
		}
		rest = skipSpaceTab(rest)
	}

	// Optional exemplar (OpenMetrics).
	if len(rest) > 0 {
		if rest[0] != '#' || !p.openMetrics {
			return false
		}
		// Options.Exemplars decides whether an exemplar ATTACHES, never whether
		// the line is VALID: a sample's value is the primary datum and its
		// optional annotation is not worth dropping it for. Gating validity on
		// the flag would make enabling exemplars drop samples from a target
		// whose trailing "#" text the same parser had always tolerated, with
		// the malformed counter as the only signal.
		//
		// A suffix that fails to parse is COUNTED (MalformedExemplars), or a
		// target whose exemplars are syntactically broken is indistinguishable
		// from one that emits none. The count is only reachable with the flag
		// on, which is what keeps the flag-off path free of parseExemplar's
		// per-exemplar allocations.
		if p.exemplars {
			if ex, ok := p.parseExemplar(rest[1:]); ok {
				s.Exemplar = ex
			} else {
				p.badExemplars++
			}
		}
	}
	return true
}

// parseExemplar parses "{labels} value [timestamp]" into the parser's
// reusable exemplar.
func (p *Parser) parseExemplar(rest []byte) (*Exemplar, bool) {
	rest = skipSpaceTab(rest)
	if len(rest) == 0 || rest[0] != '{' {
		return nil, false
	}
	clear(p.exLabels) // see Parser.labels
	p.exLabels = p.exLabels[:0]
	rest, ok := p.parseLabels(rest[1:], &p.exLabels, &p.exLastKV, nil, false)
	if !ok {
		return nil, false
	}
	if !exemplarLabelsFit(p.exLabels) {
		return nil, false
	}
	p.exemplar = Exemplar{Labels: p.exLabels}
	p.exemplar.Value, rest, ok = p.parseFloatToken(rest)
	if !ok {
		return nil, false
	}
	rest = skipSpaceTab(rest)
	if len(rest) > 0 {
		p.exemplar.TimestampMs, rest, ok = p.parseTimestampToken(rest)
		if !ok || len(skipSpaceTab(rest)) > 0 {
			return nil, false
		}
	}
	return &p.exemplar, true
}

// exemplarLabelsFit applies MaxExemplarLabelSetRunes, stopping at the first
// label that crosses it.
func exemplarLabelsFit(labels []Label) bool {
	runes := 0
	for _, l := range labels {
		runes += utf8.RuneCountInString(l.Name) + utf8.RuneCountInString(l.Value)
		if runes > MaxExemplarLabelSetRunes {
			return false
		}
	}
	return true
}

// goOnlyFloatSyntax reports whether a token uses float syntax that Go accepts
// and the Prometheus exposition format does not.
//
// strconv.ParseFloat has, since Go 1.13, accepted digit separators (`1_000`)
// and hexadecimal floating-point literals (`0x1p-3`). The exposition format
// defines neither, and Prometheus itself rejects both — so accepting them makes
// this parser read a value where the reference implementation reads a parse
// error, and read it as a DIFFERENT NUMBER than the text suggests to anyone
// looking at the exposition. Rejecting keeps the two in step.
//
// "NaN", "+Inf" and "-Inf" are legal exposition values and are unaffected: they
// contain neither an underscore nor an x.
func goOnlyFloatSyntax(tok []byte) bool {
	for i := range tok {
		switch tok[i] {
		case '_', 'x', 'X':
			return true
		}
	}
	return false
}

// parseFloatToken reads one whitespace-delimited float.
func (p *Parser) parseFloatToken(rest []byte) (float64, []byte, bool) {
	tok, rest := nextField(rest)
	if len(tok) == 0 || goOnlyFloatSyntax(tok) {
		return 0, nil, false
	}
	v, err := strconv.ParseFloat(string(tok), 64)
	if err != nil {
		return 0, nil, false
	}
	return v, rest, true
}

// parseTimestampToken reads one timestamp token: integer milliseconds in the
// classic format, (possibly fractional) seconds in OpenMetrics. Returns
// milliseconds.
func (p *Parser) parseTimestampToken(rest []byte) (int64, []byte, bool) {
	raw, rest := nextField(rest)
	if len(raw) == 0 || goOnlyFloatSyntax(raw) {
		return 0, nil, false
	}
	tok := string(raw)
	if p.openMetrics {
		f, err := strconv.ParseFloat(tok, 64)
		if err != nil || math.IsNaN(f) || f*1000 < math.MinInt64 || f*1000 >= math.MaxInt64 {
			// NaN/±Inf parse without error, and a finite-but-huge value (1e300)
			// overflows the int64 millisecond conversion to implementation-defined
			// garbage; reject both like Prometheus. The bound check covers ±Inf.
			return 0, nil, false
		}
		return int64(f * 1000), rest, true
	}
	ts, err := strconv.ParseInt(tok, 10, 64)
	if err != nil {
		return 0, nil, false
	}
	return ts, rest, true
}

// parseLabels parses the label pairs after '{' into dst and returns the
// remainder after the closing '}'. cache is the positional last-seen table
// for this label block kind (sample labels vs exemplar labels — separate, so
// exemplars do not evict the sample-label fast path).
//
// detail is where a refusal is ATTRIBUTED, and it is nil for the EXEMPLAR
// block. MalformedDetail describes sample LINES DROPPED, and a broken exemplar
// drops nothing — finishSample still emits its sample, and the fault is already
// counted on badExemplars / kubescrape_scrape_exemplars_malformed_total. Bumping
// the shared struct from here made the sum of MalformedDetail's fields exceed
// the malformed total it is documented never to exceed, and made the operator's
// line read `malformed=1 duplicateLabels=5000` for a scrape that lost one line.
//
// internValues is false for the EXEMPLAR block too: an exemplar's values are
// trace and span IDs, which never repeat, so interning them only filled the
// value table the sample labels share — a pooled parser clears that table at
// Get once it is half full, so every exemplar-bearing scrape pushed the next
// borrower's warm sample values out, and one carrying more IDs than the table
// holds filled it mid-parse, after which every later sample value allocated on
// every occurrence. Names stay interned: exemplar label NAMES repeat.
func (p *Parser) parseLabels(rest []byte, dst *[]Label, cache *[]lastKV, detail *MalformedDetail, internValues bool) ([]byte, bool) {
	for {
		rest = skipSpaceTab(rest)
		if len(rest) == 0 {
			return nil, false
		}
		if rest[0] == '}' {
			return rest[1:], true
		}
		pos := len(*dst)
		if pos == len(*cache) {
			*cache = append(*cache, lastKV{})
		}
		last := &(*cache)[pos]
		// Label name. Consecutive lines of a family repeat names positionally,
		// so matching the previous line's name plus its '=' terminator in one
		// memcmp skips the byte scan. A quoted name (the Prometheus 3 UTF-8
		// form, "my.label"="v") takes its own cold branch — rare enough that it
		// deliberately skips the positional cache.
		var name string
		if rest[0] == '"' {
			v, rem, qok := p.parseQuoted(rest[1:], nil, true)
			if !qok || v == "" {
				return nil, false
			}
			name = v
			rest = skipSpaceTab(rem)
		} else {
			var i int
			if n := len(last.name); n > 0 && n < len(rest) && rest[n] == '=' && string(rest[:n]) == last.name {
				name = last.name
				i = n
			} else {
				for i < len(rest) && rest[i] != '=' && rest[i] != ' ' && rest[i] != '\t' {
					i++
				}
				if i == 0 {
					return nil, false
				}
				if string(rest[:i]) == last.name { // memcmp fast path, no alloc
					name = last.name
				} else {
					name = p.internName(rest[:i])
					p.cacheString(&last.name, name, maxInternedNameLen)
				}
			}
			rest = skipSpaceTab(rest[i:])
		}
		if len(rest) == 0 || rest[0] != '=' {
			return nil, false
		}
		rest = skipSpaceTab(rest[1:])
		if len(rest) == 0 || rest[0] != '"' {
			return nil, false
		}
		value, rem, ok := p.parseQuoted(rest[1:], last, internValues)
		if !ok {
			return nil, false
		}
		// A duplicate label name on one sample is MALFORMED. Prometheus rejects
		// the WHOLE SCRAPE for it; failing only this line is the deliberate
		// divergence — this parser degrades per line — but accepting the pair
		// silently produced byte-identical duplicate data points in one payload
		// (OTLP attribute maps upsert, so both pairs collapse to one attribute
		// while the series key downstream kept both) with malformed=0. The scan
		// runs on every APPENDED pair, so the positional lastKV fast path
		// cannot admit a duplicate: it only skips re-interning a name, never
		// this check. The quoted-form metric name is not a dst entry
		// (parseQuotedNameSample keeps it on Sample.Name, exactly as the
		// classic path does), so a label merely named like its metric does not
		// false-positive. Names are interned, so the O(n²) compare over a
		// sample's few labels is usually a pointer equality and never
		// allocates.
		// A pathological label count turns the dedupe scan below quadratic and
		// is uninterruptible by the scrape timeout; drop the line as malformed
		// past the ceiling (see MaxLabelsPerSample) before the scan runs again.
		if len(*dst) >= MaxLabelsPerSample {
			if detail != nil {
				detail.TooManyLabels++
			}
			return nil, false
		}
		for i := range *dst {
			if (*dst)[i].Name == name {
				if detail != nil {
					detail.DuplicateLabels++
				}
				return nil, false
			}
		}
		*dst = append(*dst, Label{Name: name, Value: value})
		rest = skipSpaceTab(rem)
		if len(rest) > 0 && rest[0] == ',' {
			rest = rest[1:]
		}
	}
}

// parseQuoted parses an escaped label value after the opening quote,
// returning the value and the remainder after the closing quote. The common
// escape-free case checks the previous line's value at this position (a
// memcmp) before interning, so a repeated value costs neither a hash nor an
// allocation. last is nil for a quoted NAME (the metric name of the quoted-name
// form, or a quoted label name), which skips the positional cache. intern is
// false for exemplar values (see parseLabels): a positional-cache miss then
// copies the value instead of entering it in the shared intern table.
func (p *Parser) parseQuoted(rest []byte, last *lastKV, intern bool) (string, []byte, bool) {
	// Fast path: SIMD-scan for the closing quote; any backslash before it
	// (including one escaping a quote) routes to the slow path. IndexByte on
	// purpose, not bytes.Cut: this is the per-label hot path the allocation
	// budget pins, and the index serves three slices below.
	i := bytes.IndexByte(rest, '"') //nolint:modernize // see above
	if i < 0 {
		return "", nil, false
	}
	if bytes.IndexByte(rest[:i], '\\') >= 0 {
		return parseQuotedSlow(rest)
	}
	if last != nil && string(rest[:i]) == last.value {
		return last.value, rest[i+1:], true
	}
	var v string
	if intern {
		v = p.internValue(rest[:i])
	} else {
		v = string(rest[:i])
	}
	if last != nil {
		p.cacheString(&last.value, v, maxInternedValueLen)
	}
	return v, rest[i+1:], true
}

// cacheString stores s in one positional-cache slot. A string longer than
// internable (the intern table's own length cap for that kind) is charged to
// maxLongCacheBytes and, past it, not remembered at all — the slot is emptied
// rather than left holding its previous long string, whose charge is released
// either way. Only a cache MISS reaches this, so it costs the hit path nothing.
func (p *Parser) cacheString(slot *string, s string, internable int) {
	if old := *slot; len(old) > internable {
		p.longCacheBytes -= len(old)
	}
	if len(s) > internable {
		if p.longCacheBytes+len(s) > maxLongCacheBytes {
			*slot = ""
			return
		}
		p.longCacheBytes += len(s)
	}
	*slot = s
}

func parseQuotedSlow(rest []byte) (string, []byte, bool) {
	var sb strings.Builder
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '"':
			return sb.String(), rest[i+1:], true
		case '\\':
			i++
			if i >= len(rest) {
				return "", nil, false // an unterminated escape is an unterminated value
			}
			writeUnescaped(&sb, rest[i])
		default:
			sb.WriteByte(rest[i])
		}
	}
	return "", nil, false
}

// ErrTooManySamples can be returned by an emit callback to abort a scrape
// that exceeds a sample budget.
var ErrTooManySamples = errors.New("promparse: sample limit exceeded")

func (t MetricType) String() string {
	switch t {
	case TypeCounter:
		return "counter"
	case TypeGauge:
		return "gauge"
	case TypeHistogram:
		return "histogram"
	case TypeSummary:
		return "summary"
	default:
		return "untyped"
	}
}
