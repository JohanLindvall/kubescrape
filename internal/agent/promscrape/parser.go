// Package promscrape scrapes Prometheus endpoints and converts the samples
// to OTLP metrics.
//
// The parser is a single-pass streaming parser for the Prometheus text
// exposition format (classic and OpenMetrics): it never buffers more than
// one line, so memory use is independent of the scrape size (relevant for
// endpoints exposing 100k+ series).
package promscrape

import (
	"bufio"
	"bytes"
	"errors"
	"io"
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

// maxTrackedFamilies bounds the TYPE table so a pathological endpoint cannot
// exhaust memory through unique # TYPE lines.
const maxTrackedFamilies = 100_000

// Interning bounds: metric and label names are low-cardinality and repeat on
// nearly every line; label values (namespace, pod, le, code, ...) repeat
// heavily in Kubernetes-style expositions. Both tables live for one scrape.
// A value longer than maxInternedValueLen or arriving after the table is full
// is allocated normally, so pathological inputs degrade to the non-interned
// cost instead of growing memory.
const (
	maxInternedNames    = maxTrackedFamilies
	maxInternedValues   = 8192
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
	lastMetric string
	lastKV     []lastKV
	labels     []Label // reused between lines
	exLabels   []Label // reused between lines
	exemplar   Exemplar
	scratch    []byte // for lines spanning bufio reads
	eof        bool   // saw "# EOF"
}

// NewParser creates a parser that skips physical lines longer than
// maxLineBytes. openMetrics selects OpenMetrics semantics (typically from
// the response Content-Type); withExemplars additionally parses exemplars.
func NewParser(maxLineBytes int, openMetrics, withExemplars bool) *Parser {
	return &Parser{
		maxLineBytes: maxLineBytes,
		openMetrics:  openMetrics,
		exemplars:    openMetrics && withExemplars,
		types:        make(map[string]MetricType),
		names:        make(map[string]string),
	}
}

// parserPool recycles parsers (and their bufio readers) across scrapes: the
// interned name/value tables stay warm — the same names repeat every scrape —
// and the 64KiB read buffer stops being per-scrape garbage. The TYPE table is
// cleared per scrape (its semantics are per-exposition); the intern tables are
// only cleared once they near their caps, bounding retention.
var parserPool = sync.Pool{New: func() any {
	return &pooledParser{
		Parser: NewParser(0, false, false),
		reader: bufio.NewReaderSize(nil, parseBufSize),
	}
}}

const parseBufSize = 64 * 1024

type pooledParser struct {
	*Parser
	reader *bufio.Reader
}

// getParser returns a recycled parser configured for one scrape.
func getParser(maxLineBytes int, openMetrics, withExemplars bool) *pooledParser {
	pp := parserPool.Get().(*pooledParser)
	p := pp.Parser
	p.maxLineBytes = maxLineBytes
	p.openMetrics = openMetrics
	p.exemplars = openMetrics && withExemplars
	p.eof = false
	p.lastMetric = ""
	p.lastKV = p.lastKV[:0]
	clear(p.types) // family types are per-exposition
	if len(p.names) >= maxInternedNames/2 {
		clear(p.names)
	}
	if len(p.values) >= maxInternedValues/2 {
		clear(p.values)
	}
	return pp
}

func putParser(pp *pooledParser) {
	pp.reader.Reset(nil) // drop the response body reference
	parserPool.Put(pp)
}

// parse runs Parse over r through the pooled reader.
func (pp *pooledParser) parse(r io.Reader, emit func(Sample) error) (int, error) {
	pp.reader.Reset(r)
	return pp.parseFrom(pp.reader, emit)
}

// lastKV caches the previous line's interned name/value at one label
// position.
type lastKV struct {
	name, value string
}

// internName returns a canonical string for a metric or label name. The
// map[string(b)] lookup does not allocate; only a first-seen name does.
func (p *Parser) internName(b []byte) string {
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
	if len(p.values) < maxInternedValues {
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
		p.parseComment(line)
		return true
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

// parseComment records # TYPE declarations and the OpenMetrics # EOF
// terminator; HELP/UNIT and other comments are ignored.
func (p *Parser) parseComment(line []byte) {
	fields := bytes.Fields(line)
	if p.openMetrics && len(fields) == 2 && string(fields[1]) == "EOF" {
		p.eof = true
		return
	}
	if len(fields) != 4 || string(fields[1]) != "TYPE" {
		return
	}
	if len(p.types) >= maxTrackedFamilies {
		return
	}
	var t MetricType
	switch string(fields[3]) {
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
	p.types[string(fields[2])] = t
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

	// Metric name.
	i := 0
	for i < len(line) && line[i] != '{' && line[i] != ' ' && line[i] != '\t' {
		i++
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
	rest := line[i:]

	// Labels.
	p.labels = p.labels[:0]
	if len(rest) > 0 && rest[0] == '{' {
		var ok bool
		rest, ok = p.parseLabels(rest[1:], &p.labels)
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
	rest, ok := p.parseLabels(rest[1:], &p.exLabels)
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
		if err != nil {
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
// remainder after the closing '}'.
func (p *Parser) parseLabels(rest []byte, dst *[]Label) ([]byte, bool) {
	for {
		rest = skipSpaceTab(rest)
		if len(rest) == 0 {
			return nil, false
		}
		if rest[0] == '}' {
			return rest[1:], true
		}
		// Label name.
		i := 0
		for i < len(rest) && rest[i] != '=' && rest[i] != ' ' && rest[i] != '\t' {
			i++
		}
		if i == 0 {
			return nil, false
		}
		pos := len(*dst)
		if pos == len(p.lastKV) {
			p.lastKV = append(p.lastKV, lastKV{})
		}
		last := &p.lastKV[pos]
		var name string
		if string(rest[:i]) == last.name { // memcmp fast path, no alloc
			name = last.name
		} else {
			name = p.internName(rest[:i])
			last.name = name
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
	// Fast path: no backslashes before the closing quote.
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '"':
			if last != nil && string(rest[:i]) == last.value {
				return last.value, rest[i+1:], true
			}
			v := p.internValue(rest[:i])
			if last != nil {
				last.value = v
			}
			return v, rest[i+1:], true
		case '\\':
			return parseQuotedSlow(rest)
		}
	}
	return "", nil, false
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
var ErrTooManySamples = errors.New("sample limit exceeded")

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
