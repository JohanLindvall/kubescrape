package tailer

import (
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/bmatcuk/doublestar/v4"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/JohanLindvall/kubescrape/internal/config"
	"github.com/JohanLindvall/kubescrape/internal/logline"
)

// Source selects a set of log files by glob and declares how to handle them.
// Containerd sources parse the CRI filename for the container ID, resolve pod
// metadata, and use the CRI log format; plain sources tail arbitrary files
// with static resource attributes. Both use the identical rotation, offset
// and multi-line machinery.
type Source struct {
	// Name labels the source in logs (optional).
	Name string `json:"name,omitempty"`
	// Include lists doublestar globs (`**` supported, e.g. /var/log/**/*.log).
	Include []string `json:"include"`
	// Exclude removes matches (e.g. /var/log/azure/*.log). A file matching any
	// Exclude glob is skipped even if it matches Include.
	Exclude []string `json:"exclude,omitempty"`
	// Containerd tails CRI container logs: the filename gives the container ID,
	// metadata is resolved from the service, and the CRI format is parsed.
	Containerd bool `json:"containerd,omitempty"`
	// Compressed reads matched files as gzip, decompressing on the fly. Files
	// ending in .gz are treated as compressed automatically. Compressed files
	// are archives (read once to completion, not tailed), so — unlike plain
	// tailing — pre-existing ones ARE ingested; scope Include to avoid
	// re-reading unwanted history.
	Compressed bool `json:"compressed,omitempty"`
	// Namespaces, when set, restricts a CONTAINERD source to pods in these
	// namespaces (path.Match globs, e.g. "team-*"); ExcludeNamespaces removes
	// matches. Both are read from the CRI FILENAME at discovery, so a
	// non-matching file is never opened, tracked or read — unlike a
	// logs.rules drop, which pays the read, parse and enrich first and only
	// saves egress. Ignored for non-containerd sources, whose files carry no
	// namespace.
	Namespaces        []string `json:"namespaces,omitempty"`
	ExcludeNamespaces []string `json:"excludeNamespaces,omitempty"`
	// Selector, when set, restricts a containerd source to pods whose LABELS
	// match every key=value here. Labels are only known once the metadata
	// resolves, so unlike Namespaces this is applied at resolve time: the file
	// is tracked but no data is ever read from it. "Collect logs only for pods
	// labeled logging=true" is otherwise expressible only as a filename glob.
	Selector map[string]string `json:"selector,omitempty"`
	// IgnoreOlder skips files whose mtime is older than this Go duration (e.g.
	// "24h"), so a source pointed at a directory of retained history reads only
	// what is recent. It applies at DISCOVERY: an ignored file is never opened,
	// tracked or read. A file already being tailed, or one with a stored
	// offset, is NEVER ignored however stale it looks — dropping those would
	// abandon unshipped bytes and re-ingest the file whole if it were ever
	// touched again. Unset (or 0) reads every matched file.
	IgnoreOlder string `json:"ignoreOlder,omitempty"`
	// Multiline overrides the tailer default for this source (nil = default).
	Multiline *bool `json:"multiline,omitempty"`
	// Attributes are static resource attributes stamped on records from
	// non-containerd files (ignored for containerd sources, which derive them
	// from pod metadata). Node attributes from the builder are added too.
	Attributes map[string]string `json:"attributes,omitempty"`
	// ParseScript runs the transforms file's parse: hook (parse(line)) on
	// every line of this source before the shared chain: exotic formats a
	// script can parse (body rewrite, severity, timestamp) without teaching
	// the tailer another format. Plain sources only — containerd lines are
	// CRI, already parsed — and the cost is one Starlark call per line ON
	// THIS SOURCE only, which is why it is per-source opt-in rather than
	// global (see the refusals note in CONFIGURATION.md: the containerd hot
	// path never pays it).
	ParseScript bool `json:"parseScript,omitempty"`
	// PathAttributes derive per-file resource attributes from the file's PATH
	// (a build agent's diagnostic tree encodes job identity in directory
	// names; nothing on the line carries it). Rules are evaluated once per
	// file at resolve time, in order, each contributing its captures; they
	// win over Attributes. Refused on containerd sources, whose path encodes
	// pod identity that already arrives as metadata.
	PathAttributes []PathAttribute `json:"pathAttributes,omitempty"`
}

// PathAttribute is one path-derivation rule: a regexp matched (unanchored)
// against the file's full path. A non-matching rule contributes nothing.
type PathAttribute struct {
	// Regexp is the pattern; a bad one fails startup.
	Regexp string `json:"regexp"`
	// Attributes maps attribute key → capture group (a group name, or a group
	// number as a string). Empty: every NAMED capture group becomes an
	// attribute keyed by its own name — which cannot express dotted keys
	// (Go group names are word characters only), so `azdo.agent: agent`
	// belongs here. A referenced group that does not exist fails startup; a
	// group that matched empty is skipped.
	Attributes map[string]string `json:"attributes,omitempty"`
}

// SourcesConfig is the shape of the `logs` section of the agent config.
type SourcesConfig struct {
	Sources []Source `json:"sources"`
	// Rules are ordered first-match-wins keep/drop/sample rules applied to
	// every exported log record (all sources; journald is not filtered). No
	// match keeps. Compiled via logline.NewLineFilter; key resolution matches
	// the logMetrics selectors, plus the synthetic __severity__ key.
	Rules []logline.LineRule `json:"rules,omitempty"`
}

// ValidateSources checks include patterns and globs, returning the sources
// unchanged on success. It is shared by LoadSourcesConfig and the unified agent
// config loader.
func ValidateSources(sources []Source) ([]Source, error) {
	for i, s := range sources {
		if len(s.Include) == 0 {
			return nil, fmt.Errorf("source %d (%q): no include patterns", i, s.Name)
		}
		for _, g := range slices.Concat(s.Include, s.Exclude) {
			if !doublestar.ValidatePattern(g) {
				return nil, fmt.Errorf("source %d (%q): invalid glob %q", i, s.Name, g)
			}
		}
		// Namespace patterns are path.Match; config.Glob carries the rationale
		// (a malformed pattern reads as silent no-match at runtime, so an
		// allowlist typo would collect NOTHING with -check-config green).
		for _, g := range slices.Concat(s.Namespaces, s.ExcludeNamespaces) {
			if err := config.Glob(g); err != nil {
				return nil, fmt.Errorf("source %d (%q): invalid namespace pattern %q: %w", i, s.Name, g, err)
			}
		}
		if _, err := parseIgnoreOlder(s.IgnoreOlder); err != nil {
			return nil, fmt.Errorf("source %d (%q): %w", i, s.Name, err)
		}
		// Refused rather than ignored: a containerd path encodes pod identity
		// that already arrives as metadata, so a pathAttributes section there
		// is a misconfiguration, and this repo does not apply configs
		// partially in silence.
		if len(s.PathAttributes) > 0 && s.Containerd {
			return nil, fmt.Errorf("source %d (%q): pathAttributes is not supported on containerd sources", i, s.Name)
		}
		if s.ParseScript && s.Containerd {
			return nil, fmt.Errorf("source %d (%q): parseScript is not supported on containerd sources (CRI lines are already parsed)", i, s.Name)
		}
		if _, err := compilePathAttributes(s.PathAttributes); err != nil {
			return nil, fmt.Errorf("source %d (%q): %w", i, s.Name, err)
		}
	}
	return sources, nil
}

// parseIgnoreOlder resolves the source's mtime cutoff; empty — and an explicit
// zero — means none. A negative duration would ignore everything, which is
// never what anyone means; config.Duration is where that policy lives now.
func parseIgnoreOlder(v string) (time.Duration, error) {
	return config.Duration("ignoreOlder", v, 0, config.ZeroDisables())
}

// compiledSource is a Source with its per-source options resolved.
type compiledSource struct {
	name       string
	include    []string
	exclude    []string
	containerd bool
	compressed bool
	multiline  bool
	// ignoreOlder is the resolved mtime cutoff (0 = read everything).
	ignoreOlder time.Duration
	attributes  map[string]string
	pathAttrs   []compiledPathAttr
	parseScript bool
	// namespaces/excludeNamespaces gate a containerd source by the namespace
	// in the CRI filename (discovery time — the file is never opened);
	// selector gates it by pod labels (resolve time — nothing is ever read).
	namespaces        []string
	excludeNamespaces []string
	selector          map[string]string
	// startingUp reports that THIS source's listing has not yet succeeded, so
	// the files it discovers are still its startup set and
	// -logs-unknown-files decides where a checkpoint-less one starts. It is
	// per source, not per tailer: a source stuck on a failing glob (an
	// unreadable mount, a not-yet-created include base) would otherwise keep
	// every HEALTHY source in startup forever, and each genuinely new file
	// they discover would be skipped to its end as "history" — losing a
	// container's first lines silently. What a failed glob proves is only
	// about ITS OWN source; the checks that need "absence is proven"
	// (gone-detection, checkpoint pruning) still need every source's listing,
	// because a path is not attributable to one source before it is globbed.
	//
	// It is raised by the INITIAL scan (scanDir) and not here, so the zero value
	// is the fail-safe one: a discovery pass that never declared itself the
	// startup scan reads what it finds whole (at-least-once tolerates the
	// duplicates) rather than skipping a file's backlog as history.
	startingUp bool
}

// compiledPathAttr is one PathAttribute with its regexp compiled and every
// attribute mapping resolved to a submatch index, so applying it is one
// FindStringSubmatch and no per-file name resolution.
type compiledPathAttr struct {
	re   *regexp.Regexp
	maps []pathAttrMapping
}

// pathAttrMapping is one attribute the rule produces, in deterministic order
// (declaration order of named groups, sorted key order for an explicit map).
type pathAttrMapping struct {
	key   string
	group int
}

// apply stamps the rule's captures onto the attribute map; a non-matching
// rule, or a group that matched empty, contributes nothing.
func (c *compiledPathAttr) apply(path string, a pcommon.Map) {
	m := c.re.FindStringSubmatch(path)
	if m == nil {
		return
	}
	for _, pm := range c.maps {
		if v := m[pm.group]; v != "" {
			a.PutStr(pm.key, v)
		}
	}
}

// compilePathAttributes resolves each rule's group references at startup, so a
// typo'd group name is a -check-config failure rather than a silently absent
// attribute on every record.
func compilePathAttributes(rules []PathAttribute) ([]compiledPathAttr, error) {
	if len(rules) == 0 {
		return nil, nil
	}
	out := make([]compiledPathAttr, 0, len(rules))
	for i, r := range rules {
		re, err := regexp.Compile(r.Regexp)
		if err != nil {
			return nil, fmt.Errorf("pathAttributes[%d]: %w", i, err)
		}
		var ms []pathAttrMapping
		if len(r.Attributes) == 0 {
			for idx, name := range re.SubexpNames() {
				if name != "" {
					ms = append(ms, pathAttrMapping{key: name, group: idx})
				}
			}
			if len(ms) == 0 {
				return nil, fmt.Errorf("pathAttributes[%d] (%q): no named capture groups and no attributes mapping — the rule can produce nothing", i, r.Regexp)
			}
		} else {
			for _, key := range slices.Sorted(maps.Keys(r.Attributes)) {
				idx, err := subexpIndex(re, r.Attributes[key])
				if err != nil {
					return nil, fmt.Errorf("pathAttributes[%d] (%q): attribute %q: %w", i, r.Regexp, key, err)
				}
				ms = append(ms, pathAttrMapping{key: key, group: idx})
			}
		}
		out = append(out, compiledPathAttr{re: re, maps: ms})
	}
	return out, nil
}

// subexpIndex resolves a capture-group reference: a name, or a 1-based group
// number spelled as a string.
func subexpIndex(re *regexp.Regexp, ref string) (int, error) {
	if n, err := strconv.Atoi(ref); err == nil {
		if n < 1 || n > re.NumSubexp() {
			return 0, fmt.Errorf("capture group %d out of range (the regexp has %d)", n, re.NumSubexp())
		}
		return n, nil
	}
	if idx := re.SubexpIndex(ref); idx >= 0 {
		return idx, nil
	}
	return 0, fmt.Errorf("no capture group named %q", ref)
}

// wantNamespace reports whether a containerd source accepts this namespace.
// An empty allowlist accepts everything; the denylist wins.
func (s *compiledSource) wantNamespace(ns string) bool {
	for _, pat := range s.excludeNamespaces {
		if ok, _ := path.Match(pat, ns); ok {
			return false
		}
	}
	if len(s.namespaces) == 0 {
		return true
	}
	for _, pat := range s.namespaces {
		if ok, _ := path.Match(pat, ns); ok {
			return true
		}
	}
	return false
}

// wantLabels reports whether a pod's labels satisfy the source's selector
// (every key=value must match; an empty selector accepts everything).
//
// Kubernetes selector semantics: the key must be PRESENT and equal. A bare
// `lbls[k] != v` — which this was — reads a MISSING key as "", so a selector
// entry with an empty value matched every pod lacking the label entirely, i.e.
// the opposite of the intent, and indistinguishably from a pod that genuinely
// carries `key: ""`.
//
// The metadata service's services.selects was born with the SAME bare form and
// was corrected later; this copy was not, because nothing connected the two.
// That is the whole drift story in one function: a bug fixed once in a codebase
// that contained it twice, with no compile error, no failing test and no
// grep-able link to say the other copy existed.
//
// Fixed in place rather than shared: two call sites in two binaries for four
// lines does not earn a package, and the semantic is now written down at both.
func (s *compiledSource) wantLabels(lbls map[string]string) bool {
	for k, v := range s.selector {
		got, ok := lbls[k]
		if !ok || got != v {
			return false
		}
	}
	return true
}

// compileSources resolves the per-source multiline default against the global
// one. An empty list yields the default containerd source over dir.
func compileSources(sources []Source, dir string, defaultMultiline bool) []*compiledSource {
	if len(sources) == 0 {
		sources = []Source{{
			Name:       "containerd",
			Include:    []string{filepath.Join(dir, "*.log")},
			Containerd: true,
		}}
	}
	out := make([]*compiledSource, 0, len(sources))
	for _, s := range sources {
		ml := defaultMultiline
		if s.Multiline != nil {
			ml = *s.Multiline
		}
		// Validated by ValidateSources; a parse error here cannot happen and
		// degrades to "no cutoff" rather than dropping the source.
		ignoreOlder, _ := parseIgnoreOlder(s.IgnoreOlder)
		pathAttrs, _ := compilePathAttributes(s.PathAttributes)
		out = append(out, &compiledSource{
			name:        s.Name,
			include:     s.Include,
			exclude:     s.Exclude,
			containerd:  s.Containerd,
			compressed:  s.Compressed,
			multiline:   ml,
			ignoreOlder: ignoreOlder,
			attributes:  s.Attributes,
			pathAttrs:   pathAttrs,
			parseScript: s.ParseScript,

			namespaces:        s.Namespaces,
			excludeNamespaces: s.ExcludeNamespaces,
			selector:          s.Selector,
		})
	}
	return out
}

// excluded reports whether path hits one of the source's exclude globs. The
// scan loop uses it directly: glob() output satisfies the includes by
// construction, and PathMatch re-parses its pattern per call — re-proving
// inclusion for every listed file every 2s scan was pure waste.
func (s *compiledSource) excluded(path string) bool {
	for _, g := range s.exclude {
		if ok, _ := doublestar.PathMatch(g, path); ok {
			return true
		}
	}
	return false
}

// glob returns the paths currently matching this source's include patterns
// (before exclude filtering, which matches() applies per file). Directories
// are filtered by the caller; container logs are symlinks to files, so
// symlink following (os.Stat) is left to the caller.
func (s *compiledSource) glob() ([]string, bool) {
	var out []string
	ok := true
	for _, g := range s.include {
		// WithFailOnIOErrors is the whole point of the bool. By default
		// FilepathGlob documents itself as IGNORING filesystem errors — its
		// only error is ErrBadPattern, which compileSources already rejected
		// at startup — so this never once reported false, and the two guards
		// it gates (gone-detection and checkpoint pruning) were dead code. An
		// unreadable or vanished include base then listed as EMPTY: every
		// tracked file marked gone, and saveCheckpoints pruned the persisted
		// offsets of files it merely could not see. A node-wide re-ingest on
		// the next successful listing, or outright loss if a restart landed in
		// between.
		// WithFailOnIOErrors does NOT cover the most common failure of all: an
		// include base that is not mounted yet. In doublestar v4.10.0 a missing
		// directory is fs.ErrNotExist, which glob/globDoubleStar route to
		// handlePatternNotExist — and that returns nil unless
		// WithFailOnPatternNotExist is set (globoptions.go:131). Only a genuine
		// EIO/EACCES reached forwardErrIfFailOnIOErrors. So an absent log
		// directory listed as a SUCCESSFUL EMPTY result, which is the one
		// answer this bool exists to prevent.
		//
		// The base is checked directly rather than by adding
		// WithFailOnPatternNotExist, which is too broad: that option also fires
		// for a WILDCARD-FREE include whose file simply does not exist right
		// now (a plain source naming one file, or any file mid-rotation), and
		// turning that into "the listing failed" would disable gone-detection
		// on an ordinary steady state. Statting the fixed prefix separates the
		// two exactly — "I could not look" versus "I looked and there is
		// nothing".
		if base, _ := doublestar.SplitPattern(g); base != "" && base != "." {
			if _, serr := os.Stat(base); serr != nil {
				ok = false
				continue
			}
		}
		m, err := doublestar.FilepathGlob(g, doublestar.WithFailOnIOErrors())
		if err != nil {
			// An IO error (a single unreadable subdirectory anywhere under a
			// `**` include — an SELinux denial on one container's dir, a
			// transient EACCES) makes WithFailOnIOErrors abort the WHOLE walk
			// and return NO matches, which would blind the tailer to every
			// readable file under this include for as long as the bad dir
			// persists. So: mark the listing incomplete (ok=false, which
			// disables gone-detection and checkpoint pruning — absence here
			// proves nothing about removal) AND still collect whatever IS
			// readable, by re-globbing WITHOUT the fail-on-IO option. That
			// second call ignores the unreadable subtree and returns the
			// readable matches, so one bad directory degrades to "skip that
			// subtree", not "collect nothing under this include".
			ok = false
			if partial, perr := doublestar.FilepathGlob(g); perr == nil {
				out = append(out, partial...)
			}
			continue
		}
		out = append(out, m...)
	}
	return out, ok
}

// scanBaseDirs returns the fixed directory prefixes of the include globs (the
// part before the first wildcard), used to watch for newly appearing files.
func (s *compiledSource) scanBaseDirs() []string {
	var out []string
	for _, g := range s.include {
		base, _ := doublestar.SplitPattern(g)
		if base != "" && base != "." {
			out = append(out, base)
		}
	}
	return out
}
