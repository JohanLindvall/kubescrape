package tailer

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/htmlindex"
	"sigs.k8s.io/yaml"
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
	// Multiline overrides the tailer default for this source (nil = default).
	Multiline *bool `json:"multiline,omitempty"`
	// Encoding transcodes line content to UTF-8 from the named character set
	// (IANA/WHATWG names, e.g. windows-1252, iso-8859-1, gbk, shift_jis).
	// Empty or utf-8 means no transcoding. Lines are still split on \n, so
	// encodings where \n is not a single 0x0A byte (UTF-16/32) are unsupported.
	Encoding string `json:"encoding,omitempty"`
	// Attributes are static resource attributes stamped on records from
	// non-containerd files (ignored for containerd sources, which derive them
	// from pod metadata). Node attributes from the builder are added too.
	Attributes map[string]string `json:"attributes,omitempty"`
}

// SourcesConfig is the -logs-config file shape.
type SourcesConfig struct {
	Sources []Source `json:"sources"`
}

// LoadSourcesConfig reads log sources from a YAML file.
func LoadSourcesConfig(path string) ([]Source, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg SourcesConfig
	if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	for i, s := range cfg.Sources {
		if len(s.Include) == 0 {
			return nil, fmt.Errorf("%s: source %d (%q): no include patterns", path, i, s.Name)
		}
		for _, g := range append(append([]string{}, s.Include...), s.Exclude...) {
			if !doublestar.ValidatePattern(g) {
				return nil, fmt.Errorf("%s: source %d (%q): invalid glob %q", path, i, s.Name, g)
			}
		}
		if _, err := decoderFor(s.Encoding); err != nil {
			return nil, fmt.Errorf("%s: source %d (%q): %w", path, i, s.Name, err)
		}
	}
	return cfg.Sources, nil
}

// decoderFor resolves a character-set name to a decoder to UTF-8; a nil
// decoder means no transcoding (empty name or UTF-8).
func decoderFor(name string) (*encoding.Decoder, error) {
	if name == "" || strings.EqualFold(name, "utf-8") || strings.EqualFold(name, "utf8") {
		return nil, nil
	}
	enc, err := htmlindex.Get(name)
	if err != nil {
		return nil, fmt.Errorf("unknown encoding %q", name)
	}
	return enc.NewDecoder(), nil
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
	decoder    *encoding.Decoder // nil = UTF-8 passthrough
}

// decode transcodes a raw line to a UTF-8 string per the source's encoding.
// On any decode error it falls back to the raw bytes. The single sweep
// goroutine calls this serially, so resetting the shared decoder is safe.
func (s *compiledSource) decode(line []byte) string {
	if s.decoder == nil {
		return string(line)
	}
	s.decoder.Reset()
	out, err := s.decoder.Bytes(line)
	if err != nil {
		return string(line)
	}
	return string(out)
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
		dec, _ := decoderFor(s.Encoding) // validated at load; nil on any error
		out = append(out, &compiledSource{
			name:       s.Name,
			include:    s.Include,
			exclude:    s.Exclude,
			containerd: s.Containerd,
			compressed: s.Compressed,
			multiline:  ml,
			attributes: s.Attributes,
			decoder:    dec,
		})
	}
	return out
}

// matches reports whether path is included by this source and not excluded.
func (s *compiledSource) matches(path string) bool {
	included := false
	for _, g := range s.include {
		if ok, _ := doublestar.PathMatch(g, path); ok {
			included = true
			break
		}
	}
	if !included {
		return false
	}
	for _, g := range s.exclude {
		if ok, _ := doublestar.PathMatch(g, path); ok {
			return false
		}
	}
	return true
}

// glob returns the paths currently matching this source's include patterns
// (before exclude filtering, which matches() applies per file). Directories
// are filtered by the caller; container logs are symlinks to files, so
// symlink following (os.Stat) is left to the caller.
func (s *compiledSource) glob() []string {
	var out []string
	for _, g := range s.include {
		m, err := doublestar.FilepathGlob(g)
		if err != nil {
			continue
		}
		out = append(out, m...)
	}
	return out
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
