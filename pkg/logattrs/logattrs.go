// Package logattrs lifts configured keys out of a structured log line (JSON
// or logfmt) onto the exported record — as resource, scope, or log-record
// attributes. Resource and scope attributes affect how records group into
// OTLP ResourceLogs/ScopeLogs, so the extractor returns them separately and
// the caller keys its grouping on them.
package logattrs

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"unsafe"

	ljson "github.com/JohanLindvall/lightning/pkg/json"
	"github.com/JohanLindvall/logfmt"
	"sigs.k8s.io/yaml"
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
	path []string // Key split on '.'
	attr string
	tgt  Target
}

// LoadConfig reads a Config from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &cfg, nil
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
		e.rules = append(e.rules, compiledRule{path: path, attr: attr, tgt: tgt})
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
	if strings.IndexByte(line, '=') < 0 {
		return res
	}
	// Only the configured keys are captured (a duplicate key keeps its last
	// value, matching the former all-pairs map); results are emitted in rule
	// order so equal attribute sets always yield equal grouping keys.
	vals, found := sc.vals, sc.found
	for i := range found {
		vals[i], found[i] = "", false
	}
	buf := unsafe.Slice(unsafe.StringData(line), len(line))
	_ = logfmt.Iterate(buf, func(key, val []byte) bool {
		if idxs, ok := e.want[string(key)]; ok { // string(key) lookup: no alloc
			for _, i := range idxs {
				vals[i] = string(val)
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
// float64 (matching DecodeAny, which this replaces — apply.Put converts whole
// floats to ints); escape-free strings alias the input line, which outlives
// the extracted attributes (they are copied into pdata at flush).
func decodeScalar(raw []byte) (any, bool) {
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
		f, err := ljson.ParseFloat(raw)
		if err != nil {
			return nil, false
		}
		return f, true
	}
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
