// Package logattrs lifts configured keys out of a structured log line (JSON
// or logfmt) onto the exported record — as resource, scope, or log-record
// attributes. Resource and scope attributes affect how records group into
// OTLP ResourceLogs/ScopeLogs, so the extractor returns them separately and
// the caller keys its grouping on them.
package logattrs

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	ljson "github.com/JohanLindvall/lightning/pkg/json"
	"github.com/JohanLindvall/logfmt"
)

// Target selects where an extracted attribute lands.
type Target string

const (
	TargetLog      Target = "log"      // the log record (default)
	TargetScope    Target = "scope"    // the scope
	TargetResource Target = "resource" // the resource
)

// Rule maps one line key to an exported attribute.
type Rule struct {
	// Key is the line's key; dotted keys descend into nested JSON objects
	// (e.g. "http.status"). For logfmt only flat keys apply.
	//
	// Value TYPES follow the line format: JSON scalars lift typed (int/double/
	// bool/string — `"status":503` is an int64 attribute) while logfmt has no
	// type syntax, so every logfmt value lifts as a string (`status=503` is
	// "503"). Escape DECODING is format-independent; the type model is not.
	Key string `json:"key"`
	// Attribute is the exported attribute name; defaults to Key.
	Attribute string `json:"attribute,omitempty"`
	// Target is resource, scope, or log (default).
	Target Target `json:"target,omitempty"`
}

// Config is the list of extraction rules.
type Config struct {
	Rules []Rule `json:"rules"`
}

// Attr is one extracted key/value; Val is a string, float64, bool, or int64
// as decoded from the line.
type Attr struct {
	Key string
	Val any
}

// Result holds the extracted attributes grouped by target.
type Result struct {
	Resource []Attr
	Scope    []Attr
	Log      []Attr
}

// Empty reports whether nothing was extracted.
func (r Result) Empty() bool {
	return len(r.Resource) == 0 && len(r.Scope) == 0 && len(r.Log) == 0
}

// Extractor applies a compiled Config to log lines.
type Extractor struct {
	rules []compiledRule
	// paths mirrors rules, one dotted path each, for a single-scan
	// lightning JSON extraction of every rule at once.
	paths [][]string
	// want maps each rule's (dotted) key to the rules using it, so the logfmt
	// scan captures only configured keys instead of building a map of every
	// pair.
	want map[string][]int
	// scratch pools per-call state: the tailer and journald goroutines share
	// one Extractor and call Extract per exported line.
	scratch sync.Pool
}

// scratch is the reusable per-Extract state.
type scratch struct {
	raws  [][]byte
	vals  []string
	found []bool
}

type compiledRule struct {
	attr string
	tgt  Target
}

// New compiles an Extractor from cfg (nil or empty = a nil Extractor, which
// extracts nothing).
func New(cfg *Config) (*Extractor, error) {
	if cfg == nil || len(cfg.Rules) == 0 {
		return nil, nil
	}
	e := &Extractor{want: map[string][]int{}}
	for i, r := range cfg.Rules {
		if r.Key == "" {
			return nil, fmt.Errorf("logattrs rule %d: empty key", i)
		}
		tgt := r.Target
		if tgt == "" {
			tgt = TargetLog
		}
		if tgt != TargetLog && tgt != TargetScope && tgt != TargetResource {
			return nil, fmt.Errorf("logattrs rule %d: bad target %q (want resource, scope or log)", i, tgt)
		}
		attr := r.Attribute
		if attr == "" {
			attr = r.Key
		}
		path := strings.Split(r.Key, ".")
		e.rules = append(e.rules, compiledRule{attr: attr, tgt: tgt})
		e.paths = append(e.paths, path)
		e.want[r.Key] = append(e.want[r.Key], i)
	}
	return e, nil
}

// Extract parses line (JSON when it starts with '{', else logfmt) and returns
// the configured attributes. A nil Extractor returns an empty Result. JSON is
// scanned once for all rule paths with the lightning toolkit; logfmt uses the
// logfmt reader. Per-call state is pooled and scalars decode straight off the
// raw tokens (string values alias the line where escape-free), keeping the
// per-line allocations to the extracted values themselves.
//
// DUPLICATE KEYS resolve differently by format, and deliberately stay that way:
// JSON keeps the FIRST occurrence (lightning's GetPaths fills a path's slot
// only while it is still nil — its documented contract, and re-resolving it
// would mean a second pass over the line) while the logfmt scan keeps the LAST
// (the callback overwrites the slot on every match). So `{"level":"info",
// "level":"warn"}` lifts info and `level=info level=warn` lifts warn. Both
// inputs are malformed, and neither reader dictates an answer — the logfmt
// reader's own Get resolves first-NON-EMPTY-wins, a third rule again — so
// unifying it would mean inventing a rule here and keeping the twin extractor
// in internal/logline (identical asymmetry, identical reason) in step with it
// forever. It is written down instead.
func (e *Extractor) Extract(line string) Result {
	var res Result
	if e == nil {
		return res
	}
	sc, _ := e.scratch.Get().(*scratch)
	if sc == nil {
		sc = &scratch{vals: make([]string, len(e.rules)), found: make([]bool, len(e.rules))}
	}
	defer e.scratch.Put(sc)
	if t := strings.TrimSpace(line); strings.HasPrefix(t, "{") {
		// Read-only view of the line: lightning never mutates its input, so
		// the string→[]byte copy is avoidable.
		buf := unsafe.Slice(unsafe.StringData(t), len(t))
		raws, err := ljson.GetPaths(buf, e.paths, sc.raws[:0])
		sc.raws = raws[:0]
		if err != nil {
			return res
		}
		for i, raw := range raws {
			if raw == nil {
				continue
			}
			if v, ok := decodeScalar(raw); ok {
				e.add(&res, i, v)
			}
		}
		return res
	}
	// A line with no '=' carries no logfmt pair; the scan is skipped as a pure
	// fast path, which is only sound because bare keys lift nothing (see the
	// IsBareKey guard below).
	if strings.IndexByte(line, '=') < 0 {
		return res
	}
	// Only the configured keys are captured (a duplicate key keeps its last
	// value, matching the former all-pairs map — and NOT what the JSON arm
	// above does, which keeps the first: see the note on the asymmetry); results
	// are emitted in rule order so equal attribute sets always yield equal
	// grouping keys.
	vals, found := sc.vals, sc.found
	for i := range found {
		vals[i], found[i] = "", false
	}
	buf := unsafe.Slice(unsafe.StringData(line), len(line))
	_ = logfmt.Iterate(buf, func(key, val []byte) bool {
		// A BARE key yields the sentinel "true", which is prose, not a field:
		// `weight=10 disk error` would lift error="true" onto every record
		// carrying that sentence. Whether the words are seen at all depends on
		// an unrelated '=' elsewhere on the line (the fast path above), so
		// admitting them makes an attribute appear and disappear with the rest
		// of the sentence.
		if logfmt.IsBareKey(val) {
			return true
		}
		if idxs, ok := e.want[string(key)]; ok { // string(key) lookup: no alloc
			// Iterate yields RAW values (quotes stripped, escapes intact).
			// Decode them, as the JSON arm below and the twin extractor in
			// internal/logline both do: the same logical value must not read
			// differently depending on the line format, and in the tailer a
			// record attribute lifted here is consulted BEFORE the line, so an
			// unrelated logAttributes rule would otherwise change what a
			// logMetrics label or a logs.rules selector matches. QUOTED values
			// only — see DecodeLogfmtValue.
			// The no-escape path (the common one) costs one byte scan, no copy.
			decoded := DecodeLogfmtValue(buf, val)
			for _, i := range idxs {
				vals[i] = decoded
				found[i] = true
			}
		}
		return true
	})
	for i := range e.rules {
		if found[i] {
			e.add(&res, i, vals[i])
		}
	}
	return res
}

// decodeScalar renders a raw JSON scalar token as its typed value; objects,
// arrays and null are not attribute-worthy and report false. Numbers decode as
// int64 when integral (float64 cannot hold a 64-bit id exactly) and float64
// otherwise; escape-free strings alias the input line, which outlives
// the extracted attributes (they are copied into pdata at flush).
func decodeScalar(raw []byte) (any, bool) {
	if len(raw) == 0 {
		return nil, false // shape check at the parse seam; GetPaths never yields this
	}
	switch raw[0] {
	case '"':
		if len(raw) < 2 || raw[len(raw)-1] != '"' {
			return nil, false
		}
		s, err := ljson.UnescapeString(raw[1 : len(raw)-1])
		if err != nil {
			return nil, false
		}
		return s, true
	case 't':
		return true, string(raw) == "true" // comparison does not allocate
	case 'f':
		return false, string(raw) == "false"
	case '{', '[', 'n':
		return nil, false
	default: // number
		// Integral tokens parse as int64 first: float64 loses precision above
		// 2^53, so a 64-bit id (snowflake, order/user id) lifted from a JSON log
		// silently landed one or more off — and looked exact afterwards, since
		// whole floats are stored with PutInt anyway.
		if IsIntegerToken(raw) {
			if i, err := strconv.ParseInt(string(raw), 10, 64); err == nil {
				return i, true
			}
		}
		f, err := ljson.ParseFloat(raw)
		if err != nil {
			return nil, false
		}
		return f, true
	}
}

// IsIntegerToken reports whether a JSON number token is a plain integer (an
// optional sign followed by digits only — no fraction, no exponent), so its
// text is the exact decimal value. It is the ONE classifier shared by this
// package's decodeScalar and internal/logline's RawScalarString: the two
// render the same line field as an attribute and as a metric label, and two
// classifiers here drifted once (a lax reject-.eE version beside a strict
// digits-only one). The strict form is required by the label path, which
// returns the token text verbatim on true.
//
// Known residual divergence between the two consumers, accepted: a token
// exceeding int64's range is verbatim text as a label (exact) but falls back
// through float64 as an attribute (rounded past 2^53) — same line, same key,
// two renderings at the extreme. Unifying it would mean string attributes for
// huge integers, a type change not worth the edge.
func IsIntegerToken(raw []byte) bool {
	if len(raw) == 0 {
		return false
	}
	i := 0
	if raw[0] == '-' {
		i = 1
	}
	if i == len(raw) {
		return false
	}
	for ; i < len(raw); i++ {
		if raw[i] < '0' || raw[i] > '9' {
			return false // '.', 'e', 'E', or not a number token at all
		}
	}
	return true
}

// FloatString renders a float64 the way pcommon.Value.AsString does: the ES6
// number-to-string algorithm — 'f' inside [1e-6, 1e21), 'e' with an unpadded
// exponent outside it, NaN/Infinity spelled out.
//
// It sits beside IsIntegerToken because it is the other half of the same
// contract: one line field must render identically however it reaches an
// exported string. A bare FormatFloat('f') diverged from pcommon outside that
// range, so the same value read 0.0000005 through one path and 5e-7 through
// another — two metric series for one number, and a rule matching one spelling
// and not the other. The consumers are internal/agent/logchain (a lifted
// attribute rendered as a label, which must equal what AsString gives for the
// same value put on a record) and internal/logline (a JSON number token
// rendered as a line-field value). Pinned against the real pcommon rendering by
// logchain's TestAttrStringMatchesPcommon.
func FloatString(f float64) string {
	if math.IsNaN(f) {
		return "NaN"
	}
	if math.IsInf(f, 1) {
		return "Infinity"
	}
	if math.IsInf(f, -1) {
		return "-Infinity"
	}
	scratch := [64]byte{}
	b := scratch[:0]
	abs := math.Abs(f)
	format := byte('f')
	if abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	b = strconv.AppendFloat(b, f, format, -1, 64)
	if format == 'e' {
		// clean up e-09 to e-9
		n := len(b)
		if n >= 4 && b[n-4] == 'e' && b[n-3] == '-' && b[n-2] == '0' {
			b[n-2] = b[n-1]
			b = b[:n-1]
		}
	}
	return string(b)
}

// add appends the extracted value for rule i to the right target bucket.
func (e *Extractor) add(res *Result, i int, v any) {
	a := Attr{Key: e.rules[i].attr, Val: v}
	switch e.rules[i].tgt {
	case TargetResource:
		res.Resource = append(res.Resource, a)
	case TargetScope:
		res.Scope = append(res.Scope, a)
	default:
		res.Log = append(res.Log, a)
	}
}
