// Package promscrape scrapes Prometheus endpoints and converts the samples
// to OTLP metrics.
//
// The parser is a single-pass streaming parser for the Prometheus text
// exposition format: it never buffers more than one line, so memory use is
// independent of the scrape size (relevant for endpoints exposing 100k+
// series).
package promscrape

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
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

// SampleKind is how an individual sample should be represented, derived from
// the family type and the series suffix.
type SampleKind int

const (
	// KindGauge covers gauges, untyped series and summary quantiles.
	KindGauge SampleKind = iota
	// KindSum covers counters and the cumulative _bucket/_sum/_count series
	// of histograms and summaries.
	KindSum
)

// Label is one label pair (excluding __name__).
type Label struct {
	Name  string
	Value string
}

// Sample is one parsed series sample.
type Sample struct {
	Name        string
	Kind        SampleKind
	Labels      []Label // valid only during the callback
	Value       float64
	TimestampMs int64 // 0 when the exposition carries no timestamp
}

// maxTrackedFamilies bounds the TYPE table so a pathological endpoint cannot
// exhaust memory through unique # TYPE lines.
const maxTrackedFamilies = 100_000

// Parser parses one scrape body. Not safe for concurrent use; create one per
// scrape.
type Parser struct {
	maxLineBytes int
	types        map[string]MetricType
	labels       []Label // reused between lines
	scratch      []byte  // for lines spanning bufio reads
}

// NewParser creates a parser that skips physical lines longer than
// maxLineBytes.
func NewParser(maxLineBytes int) *Parser {
	return &Parser{
		maxLineBytes: maxLineBytes,
		types:        make(map[string]MetricType),
	}
}

// Parse reads the exposition text from r, invoking emit for every sample.
// The Sample (including Labels) is only valid during the callback. A non-nil
// error from emit aborts the parse. Malformed lines are skipped, counted and
// reported; a malformed count with a nil error means a partially usable
// scrape.
func (p *Parser) Parse(r io.Reader, emit func(Sample) error) (malformed int, err error) {
	br := bufio.NewReaderSize(r, 64*1024)
	for {
		line, tooLong, rerr := p.readLine(br)
		if len(line) > 0 && !tooLong {
			if ok := p.parseLine(line, emit, &err); err != nil {
				return malformed, err
			} else if !ok {
				malformed++
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
	line = bytes.TrimLeft(line, " \t")
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
	s.Kind = p.kindOf(s.Name)
	if err := emit(s); err != nil {
		*emitErr = err
	}
	return true
}

// parseComment records # TYPE declarations; HELP and other comments are
// ignored.
func (p *Parser) parseComment(line []byte) {
	fields := bytes.Fields(line)
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

// kindOf resolves the sample kind from the family type table, accounting for
// the _bucket/_sum/_count series of histograms and summaries.
func (p *Parser) kindOf(name string) SampleKind {
	if t, ok := p.types[name]; ok {
		switch t {
		case TypeCounter:
			return KindSum
		case TypeSummary:
			// Quantile series carry the family name itself.
			return KindGauge
		default:
			return KindGauge
		}
	}
	if base, found := strings.CutSuffix(name, "_bucket"); found {
		if p.types[base] == TypeHistogram {
			return KindSum
		}
	}
	if base, found := strings.CutSuffix(name, "_sum"); found {
		if t, ok := p.types[base]; ok && (t == TypeHistogram || t == TypeSummary) {
			return KindSum
		}
	}
	if base, found := strings.CutSuffix(name, "_count"); found {
		if t, ok := p.types[base]; ok && (t == TypeHistogram || t == TypeSummary) {
			return KindSum
		}
	}
	if base, found := strings.CutSuffix(name, "_total"); found {
		if p.types[base] == TypeCounter {
			return KindSum
		}
	}
	return KindGauge
}

// parseSample parses `name{labels} value [timestamp_ms]`.
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
	s.Name = string(line[:i])
	rest := line[i:]

	// Labels.
	p.labels = p.labels[:0]
	if len(rest) > 0 && rest[0] == '{' {
		var ok bool
		rest, ok = p.parseLabels(rest[1:])
		if !ok {
			return s, false
		}
	}
	s.Labels = p.labels

	// Value.
	rest = bytes.TrimLeft(rest, " \t")
	i = 0
	for i < len(rest) && rest[i] != ' ' && rest[i] != '\t' {
		i++
	}
	if i == 0 {
		return s, false
	}
	v, err := strconv.ParseFloat(string(rest[:i]), 64)
	if err != nil {
		return s, false
	}
	s.Value = v

	// Optional timestamp (milliseconds).
	rest = bytes.TrimLeft(rest[i:], " \t")
	if len(rest) > 0 {
		ts, err := strconv.ParseInt(string(rest), 10, 64)
		if err != nil {
			return s, false
		}
		s.TimestampMs = ts
	}
	return s, true
}

// parseLabels parses the label pairs after '{' and returns the remainder
// after the closing '}'.
func (p *Parser) parseLabels(rest []byte) ([]byte, bool) {
	for {
		rest = bytes.TrimLeft(rest, " \t")
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
		name := string(rest[:i])
		rest = bytes.TrimLeft(rest[i:], " \t")
		if len(rest) == 0 || rest[0] != '=' {
			return nil, false
		}
		rest = bytes.TrimLeft(rest[1:], " \t")
		if len(rest) == 0 || rest[0] != '"' {
			return nil, false
		}
		value, rem, ok := parseQuoted(rest[1:])
		if !ok {
			return nil, false
		}
		p.labels = append(p.labels, Label{Name: name, Value: value})
		rest = bytes.TrimLeft(rem, " \t")
		if len(rest) > 0 && rest[0] == ',' {
			rest = rest[1:]
		}
	}
}

// parseQuoted parses an escaped label value after the opening quote,
// returning the value and the remainder after the closing quote. The common
// escape-free case does not allocate beyond the final string.
func parseQuoted(rest []byte) (string, []byte, bool) {
	// Fast path: no backslashes before the closing quote.
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '"':
			return string(rest[:i]), rest[i+1:], true
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
