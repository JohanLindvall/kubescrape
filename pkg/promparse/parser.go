// Package promparse is a streaming parser for the Prometheus text exposition
// format, classic and OpenMetrics.
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
)

// MetricType is the declared type of a metric family (# TYPE line).
type MetricType int

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
	RoleHistogramBucket
	RoleHistogramSum
	RoleHistogramCount
	RoleSummaryQuantile
	RoleSummarySum
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
}

// MaxTrackedFamilies bounds the TYPE table so a pathological endpoint cannot
// exhaust memory through unique # TYPE lines. It is exported because callers
// that memoize per parse (a filter caching decisions per series name, say)
// want the same bound.
const MaxTrackedFamilies = 100_000

// Interning bounds: metric and label names are low-cardinality and repeat on
// nearly every line; label values (namespace, pod, le, code, ...) repeat
// heavily in Kubernetes-style expositions. Both tables live for one scrape.
// A name/value longer than its length cap or arriving after the table is full
// is allocated normally, so pathological inputs degrade to the non-interned
// cost instead of growing memory.
const (
	maxInternedNames   = MaxTrackedFamilies
	maxInternedNameLen = 256
	// MaxInternedValues bounds the label-value intern table; exported for the
	// same reason as MaxTrackedFamilies.
	MaxInternedValues   = 8192
	maxInternedValueLen = 128
)

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

	types  map[string]MetricType
	names  map[string]string // interned metric/label names
	values map[string]string // interned label values
	// Consecutive lines of a family are near-identical: lastMetric and the
	// per-position lastKV short-circuit the intern-map probes with a plain
	// memcmp (string(b) == s does not allocate), which is ~5x cheaper.
	// Exemplar labels have their own positional cache (exLastKV) so
	// exemplar-bearing lines do not evict the sample-label entries.
	lastMetric string
	lastKV     []lastKV
	exLastKV   []lastKV
	labels     []Label // reused between lines
	exLabels   []Label // reused between lines
	exemplar   Exemplar
	scratch    []byte // for lines spanning bufio reads
	eof        bool   // saw "# EOF"
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
// and the 64KiB read buffer stops being per-scrape garbage. The TYPE table is
// cleared per scrape (its semantics are per-exposition); the intern tables are
// only cleared once they near their caps, bounding retention.
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
// one with Get, use it for a single Parse, and return it with Put.
type Pooled struct {
	p      *Parser
	reader *bufio.Reader
}

// Get returns a pooled parser configured for one parse. Return it with Put
// when the parse is done; the parser must not be used afterwards.
func Get(opts Options) *Pooled {
	pp := parserPool.Get().(*Pooled)
	p := pp.p
	p.maxLineBytes = normLineBytes(opts.MaxLineBytes)
	p.openMetrics = opts.OpenMetrics
	p.exemplars = opts.OpenMetrics && opts.Exemplars
	p.eof = false
	p.lastMetric = ""
	p.lastKV = p.lastKV[:0]
	p.exLastKV = p.exLastKV[:0]
	clear(p.types) // family types are per-exposition
	if len(p.names) >= maxInternedNames/2 {
		clear(p.names)
	}
	if len(p.values) >= MaxInternedValues/2 {
		clear(p.values)
	}
	return pp
}

// Put returns a pooled parser for reuse.
func Put(pp *Pooled) {
	pp.reader.Reset(nil) // drop the response body reference
	parserPool.Put(pp)
}

// Parse reads the exposition from r through the pooled reader, invoking emit
// for every sample (see Parser.Parse).
func (pp *Pooled) Parse(r io.Reader, emit func(Sample) error) (int, error) {
	pp.reader.Reset(r)
	return pp.p.parseFrom(pp.reader, emit)
}

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
	if len(p.names) < maxInternedNames {
		p.names[s] = s
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

// Parse reads the exposition text from r, invoking emit for every sample.
// The Sample (including Labels and Exemplar) is only valid during the
// callback. A non-nil error from emit aborts the parse. Malformed lines are
// skipped, counted and reported; a malformed count with a nil error means a
// partially usable scrape.
func (p *Parser) Parse(r io.Reader, emit func(Sample) error) (malformed int, err error) {
	return p.parseFrom(bufio.NewReaderSize(r, parseBufSize), emit)
}

func (p *Parser) parseFrom(br *bufio.Reader, emit func(Sample) error) (malformed int, err error) {
	for {
		line, tooLong, rerr := p.readLine(br)
		if len(line) > 0 && !tooLong {
			if ok := p.parseLine(line, emit, &err); err != nil {
				return malformed, err
			} else if !ok {
				malformed++
			}
			if p.eof {
				return malformed, nil
			}
		} else if tooLong {
			malformed++
		}
		if rerr != nil {
			if rerr == io.EOF {
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
		if len(p.scratch) == 0 && rerr == nil {
			if len(chunk) > p.maxLineBytes {
				return nil, true, nil
			}
			// Whole line inside the buffer: no copy needed.
			return trimEOL(chunk), false, nil
		}
		if len(p.scratch)+len(chunk) <= p.maxLineBytes {
			p.scratch = append(p.scratch, chunk...)
		} else {
			tooLong = true
		}
		switch rerr {
		case nil:
			if tooLong {
				return nil, true, nil
			}
			return trimEOL(p.scratch), false, nil
		case bufio.ErrBufferFull:
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
	s.Role, s.Family = p.classify(s.Name)
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

// parseComment records # TYPE declarations and the OpenMetrics # EOF
// terminator; HELP/UNIT and other comments are ignored. Only the leading
// tokens are examined — a non-directive comment (HELP with its free text,
// typically the bulk of the comment bytes) returns after the second token
// instead of being tokenized whole. It reports false for a malformed TYPE
// line (missing family/type, or trailing garbage) so the caller counts it.
func (p *Parser) parseComment(line []byte) bool {
	_, rest := nextField(line) // the leading "#" token
	directive, rest := nextField(rest)
	switch {
	case p.openMetrics && string(directive) == "EOF": // no alloc: memcmp
		if len(skipSpaceTab(rest)) == 0 {
			p.eof = true
		}
	case string(directive) == "TYPE":
		family, rest := nextField(rest)
		typ, rest := nextField(rest)
		if len(family) == 0 || len(typ) == 0 || len(skipSpaceTab(rest)) != 0 {
			return false // malformed TYPE: counted, not silently ignored
		}
		if len(p.types) >= MaxTrackedFamilies {
			return true // over the table bound: a deliberate cap, not malformed
		}
		var t MetricType
		switch string(typ) {
		case "counter":
			t = TypeCounter
		case "gauge":
			t = TypeGauge
		case "histogram":
			t = TypeHistogram
		case "summary":
			t = TypeSummary
		default:
			t = TypeUntyped
		}
		p.types[string(family)] = t
	}
	return true
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
		for i = 0; i < len(line) && line[i] != '{' && line[i] != ' ' && line[i] != '\t'; i++ {
		}
		if i == 0 {
			return s, false
		}
		if string(line[:i]) == p.lastMetric { // memcmp fast path, no alloc
			s.Name = p.lastMetric
		} else {
			s.Name = p.internName(line[:i])
			p.lastMetric = s.Name
		}
	}
	rest := line[i:]

	// Labels.
	p.labels = p.labels[:0]
	if len(rest) > 0 && rest[0] == '{' {
		var ok bool
		rest, ok = p.parseLabels(rest[1:], &p.labels, &p.lastKV)
		if !ok {
			return s, false
		}
	}
	s.Labels = p.labels

	// Value.
	var ok bool
	s.Value, rest, ok = p.parseFloatToken(rest)
	if !ok {
		return s, false
	}

	// Optional timestamp.
	rest = skipSpaceTab(rest)
	if len(rest) > 0 && rest[0] != '#' {
		s.TimestampMs, rest, ok = p.parseTimestampToken(rest)
		if !ok {
			return s, false
		}
		rest = skipSpaceTab(rest)
	}

	// Optional exemplar (OpenMetrics).
	if len(rest) > 0 {
		if rest[0] != '#' || !p.openMetrics {
			return s, false
		}
		if !p.exemplars {
			return s, true // valid line; exemplar intentionally ignored
		}
		ex, ok := p.parseExemplar(rest[1:])
		if !ok {
			return s, false
		}
		s.Exemplar = ex
	}
	return s, true
}

// parseExemplar parses "{labels} value [timestamp]" into the parser's
// reusable exemplar.
func (p *Parser) parseExemplar(rest []byte) (*Exemplar, bool) {
	rest = skipSpaceTab(rest)
	if len(rest) == 0 || rest[0] != '{' {
		return nil, false
	}
	p.exLabels = p.exLabels[:0]
	rest, ok := p.parseLabels(rest[1:], &p.exLabels, &p.exLastKV)
	if !ok {
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

// parseFloatToken reads one whitespace-delimited float.
func (p *Parser) parseFloatToken(rest []byte) (float64, []byte, bool) {
	rest = skipSpaceTab(rest)
	i := 0
	for i < len(rest) && rest[i] != ' ' && rest[i] != '\t' {
		i++
	}
	if i == 0 {
		return 0, nil, false
	}
	v, err := strconv.ParseFloat(string(rest[:i]), 64)
	if err != nil {
		return 0, nil, false
	}
	return v, rest[i:], true
}

// parseTimestampToken reads one timestamp token: integer milliseconds in the
// classic format, (possibly fractional) seconds in OpenMetrics. Returns
// milliseconds.
func (p *Parser) parseTimestampToken(rest []byte) (int64, []byte, bool) {
	i := 0
	for i < len(rest) && rest[i] != ' ' && rest[i] != '\t' {
		i++
	}
	if i == 0 {
		return 0, nil, false
	}
	tok := string(rest[:i])
	if p.openMetrics {
		f, err := strconv.ParseFloat(tok, 64)
		if err != nil || math.IsNaN(f) || f*1000 < math.MinInt64 || f*1000 >= math.MaxInt64 {
			// NaN/±Inf parse without error, and a finite-but-huge value (1e300)
			// overflows the int64 millisecond conversion to implementation-defined
			// garbage; reject both like Prometheus. The bound check covers ±Inf.
			return 0, nil, false
		}
		return int64(f * 1000), rest[i:], true
	}
	ts, err := strconv.ParseInt(tok, 10, 64)
	if err != nil {
		return 0, nil, false
	}
	return ts, rest[i:], true
}

// parseLabels parses the label pairs after '{' into dst and returns the
// remainder after the closing '}'. cache is the positional last-seen table
// for this label block kind (sample labels vs exemplar labels — separate, so
// exemplars do not evict the sample-label fast path).
func (p *Parser) parseLabels(rest []byte, dst *[]Label, cache *[]lastKV) ([]byte, bool) {
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
		// memcmp skips the byte scan.
		var name string
		i := 0
		if n := len(last.name); n > 0 && n < len(rest) && rest[n] == '=' && string(rest[:n]) == last.name {
			name = last.name
			i = n
		} else {
			for i = 0; i < len(rest) && rest[i] != '=' && rest[i] != ' ' && rest[i] != '\t'; i++ {
			}
			if i == 0 {
				return nil, false
			}
			if string(rest[:i]) == last.name { // memcmp fast path, no alloc
				name = last.name
			} else {
				name = p.internName(rest[:i])
				last.name = name
			}
		}
		rest = skipSpaceTab(rest[i:])
		if len(rest) == 0 || rest[0] != '=' {
			return nil, false
		}
		rest = skipSpaceTab(rest[1:])
		if len(rest) == 0 || rest[0] != '"' {
			return nil, false
		}
		value, rem, ok := p.parseQuoted(rest[1:], last)
		if !ok {
			return nil, false
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
// allocation. last may be nil (exemplar labels).
func (p *Parser) parseQuoted(rest []byte, last *lastKV) (string, []byte, bool) {
	// Fast path: SIMD-scan for the closing quote; any backslash before it
	// (including one escaping a quote) routes to the slow path.
	i := bytes.IndexByte(rest, '"')
	if i < 0 {
		return "", nil, false
	}
	if bytes.IndexByte(rest[:i], '\\') >= 0 {
		return parseQuotedSlow(rest)
	}
	if last != nil && string(rest[:i]) == last.value {
		return last.value, rest[i+1:], true
	}
	v := p.internValue(rest[:i])
	if last != nil {
		last.value = v
	}
	return v, rest[i+1:], true
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
				return "", nil, false
			}
			switch rest[i] {
			case 'n':
				sb.WriteByte('\n')
			case '\\', '"':
				sb.WriteByte(rest[i])
			default:
				sb.WriteByte('\\')
				sb.WriteByte(rest[i])
			}
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
