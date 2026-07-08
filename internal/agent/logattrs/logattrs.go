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
	e := &Extractor{}
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
	}
	return e, nil
}

// Extract parses line (JSON when it starts with '{', else logfmt) and returns
// the configured attributes. A nil Extractor returns an empty Result. JSON is
// scanned once for all rule paths with the lightning toolkit; logfmt uses the
// logfmt reader.
func (e *Extractor) Extract(line string) Result {
	var res Result
	if e == nil {
		return res
	}
	if t := strings.TrimSpace(line); strings.HasPrefix(t, "{") {
		vals, err := ljson.GetPaths([]byte(t), e.paths, nil)
		if err != nil {
			return res
		}
		for i, raw := range vals {
			if raw == nil {
				continue
			}
			v, err := ljson.DecodeAny(raw)
			if err != nil {
				continue
			}
			if !scalar(v) {
				continue
			}
			e.add(&res, i, v)
		}
		return res
	}
	if strings.IndexByte(line, '=') < 0 {
		return res
	}
	kv := map[string]string{}
	buf := unsafe.Slice(unsafe.StringData(line), len(line))
	_ = logfmt.Iterate(buf, func(key, val []byte) bool {
		kv[string(key)] = string(val)
		return true
	})
	for i, r := range e.rules {
		if v, ok := kv[strings.Join(r.path, ".")]; ok {
			e.add(&res, i, v)
		}
	}
	return res
}

// scalar reports whether a lightning-decoded value is an attribute-worthy
// scalar (objects, arrays and null are not).
func scalar(v any) bool {
	switch v.(type) {
	case string, float64, bool:
		return true
	default:
		return false
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
