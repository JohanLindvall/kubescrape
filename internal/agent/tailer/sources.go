package tailer

import (
	"fmt"
	"path"
	"path/filepath"

	"github.com/bmatcuk/doublestar/v4"

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
	// Multiline overrides the tailer default for this source (nil = default).
	Multiline *bool `json:"multiline,omitempty"`
	// Attributes are static resource attributes stamped on records from
	// non-containerd files (ignored for containerd sources, which derive them
	// from pod metadata). Node attributes from the builder are added too.
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
		for _, g := range append(append([]string{}, s.Include...), s.Exclude...) {
			if !doublestar.ValidatePattern(g) {
				return nil, fmt.Errorf("source %d (%q): invalid glob %q", i, s.Name, g)
			}
		}
		// Namespace patterns are path.Match, which returns ErrBadPattern for
		// EVERY input when the pattern is malformed. wantNamespace reads that as
		// "no match", so an unvalidated typo in an allowlist silently collects
		// NOTHING for the source — no warning, no metric, and -check-config
		// green. Reject it here, exactly as routing does with its globs.
		for _, g := range append(append([]string{}, s.Namespaces...), s.ExcludeNamespaces...) {
			if _, err := path.Match(g, ""); err != nil {
				return nil, fmt.Errorf("source %d (%q): invalid namespace pattern %q: %w", i, s.Name, g, err)
			}
		}
	}
	return sources, nil
}

// compiledSource is a Source with its per-source options resolved.
type compiledSource struct {
	name       string
	include    []string
	exclude    []string
	containerd bool
	compressed bool
	multiline  bool
	attributes map[string]string
	// namespaces/excludeNamespaces gate a containerd source by the namespace
	// in the CRI filename (discovery time — the file is never opened);
	// selector gates it by pod labels (resolve time — nothing is ever read).
	namespaces        []string
	excludeNamespaces []string
	selector          map[string]string
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
func (s *compiledSource) wantLabels(lbls map[string]string) bool {
	for k, v := range s.selector {
		if lbls[k] != v {
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
		out = append(out, &compiledSource{
			name:       s.Name,
			include:    s.Include,
			exclude:    s.Exclude,
			containerd: s.Containerd,
			compressed: s.Compressed,
			multiline:  ml,
			attributes: s.Attributes,

			namespaces:        s.Namespaces,
			excludeNamespaces: s.ExcludeNamespaces,
			selector:          s.Selector,
		})
	}
	return out
}

// matches reports whether path is included by this source and not excluded.
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
		m, err := doublestar.FilepathGlob(g)
		if err != nil {
			// An errored pattern proves nothing about which files are gone;
			// the caller must not treat absence from this listing as removal.
			ok = false
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
