package manifestcheck

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// Named templates. The chart hands one block of arguments to several workloads
// through a `{{- define "name" }}` in _helpers.tpl, included where each
// workload's args are — the -otlp-* flags of the three workloads that run the
// agent binary are written ONCE that way, because as three hand-kept copies they
// drifted three times. Read as text, though, an including template shows only
// its include line: every flag the helper passes would be invisible to Flags,
// which would then report "pass" for flags it never saw, on precisely the
// blocks shared most widely, and internal/chartcheck's values-path scan could
// not derive the values feeding them. ExpandIncludes puts a helper's body back
// where helm renders it, so both scans read what the workload actually passes,
// classified by the document that includes it.

// Helper is one named template: its body, with template comments blanked, and
// where the body starts.
type Helper struct {
	Lines []string // the body, one element per source line
	File  string   // base name of the file defining it
	Line  int      // 1-based line number of Lines[0] in File
}

// Line is one line of a template after ExpandIncludes, with the place it came
// from: "<file>:<line>" of its own source, so a line an include brought in
// names the helper's file and line, which is the text to edit.
type Line struct {
	Text  string
	Where string
}

var (
	// defineLine is a line holding a define action and nothing else.
	defineLine = regexp.MustCompile(`^[ \t]*\{\{-?[ \t]*define[ \t]+"([^"]+)"[ \t]*-?\}\}[ \t]*$`)
	// endLine is a line holding an end action and nothing else.
	endLine = regexp.MustCompile(`^[ \t]*\{\{-?[ \t]*end[ \t]*-?\}\}[ \t]*$`)
	// soleInclude is a line holding exactly one include action, which is how a
	// template splices a helper's lines into a block (`| nindent N` or not).
	// An include INSIDE a value (`image: {{ include "kubescrape.image" . }}`)
	// renders one scalar, never lines, and is left alone.
	soleInclude = regexp.MustCompile(`^[ \t]*\{\{-?[ \t]*include[ \t]+"([^"]+)"[^}]*\}\}[ \t]*$`)
	// templateAction is any template action; its first group is the body.
	templateAction = regexp.MustCompile(`\{\{-?\s*(.*?)\s*-?\}\}`)
)

// maxIncludeDepth bounds helper-in-helper expansion, so a helper including
// itself is an error rather than a hang.
const maxIncludeDepth = 8

// opensBlock reports whether an action body opens a block its `end` closes.
func opensBlock(body string) bool {
	for _, kw := range []string{"if ", "with ", "range ", "define ", "block "} {
		if strings.HasPrefix(body, kw) {
			return true
		}
	}
	return false
}

// Helpers reads every named template defined under dirs — in a .tpl file or a
// manifest, since helm accepts a define in either — keyed by name.
//
// It reads a template LINE BY LINE, so a define must sit alone on its line and
// its end on another; a body sharing a line with either, an unterminated
// define and a name defined twice are all errors rather than a guess, since a
// helper read wrong is a set of flags checked wrong.
func Helpers(dirs ...string) (map[string]Helper, error) {
	var files []string
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && (IsManifest(d.Name()) || filepath.Ext(d.Name()) == ".tpl") {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", dir, err)
		}
	}
	slices.Sort(files)
	out := map[string]Helper{}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", f, err)
		}
		if err := readHelpers(filepath.Base(f), stripTemplateComments(string(b)), out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// readHelpers adds the defines in one comment-stripped file to out.
func readHelpers(file, src string, out map[string]Helper) error {
	lines := strings.Split(src, "\n")
	for i := 0; i < len(lines); i++ {
		m := defineLine.FindStringSubmatch(lines[i])
		if m == nil {
			for _, a := range templateAction.FindAllStringSubmatch(lines[i], -1) {
				if strings.HasPrefix(a[1], "define ") {
					return fmt.Errorf("%s:%d: a define that shares its line with other text cannot be read line by line", file, i+1)
				}
			}
			continue
		}
		name := m[1]
		if _, dup := out[name]; dup {
			return fmt.Errorf("%s:%d: %q is defined twice", file, i+1, name)
		}
		depth, j := 1, i+1
		for ; j < len(lines) && depth > 0; j++ {
			for _, a := range templateAction.FindAllStringSubmatch(lines[j], -1) {
				switch {
				case a[1] == "end":
					depth--
				case opensBlock(a[1]):
					depth++
				}
			}
			if depth == 0 && !endLine.MatchString(lines[j]) {
				return fmt.Errorf("%s:%d: the end of %q shares its line with other text, which cannot be read line by line", file, j+1, name)
			}
		}
		if depth != 0 {
			return fmt.Errorf("%s:%d: %q is never closed", file, i+1, name)
		}
		// j is one past the end line.
		out[name] = Helper{Lines: lines[i+1 : j-1], File: file, Line: i + 2}
		i = j - 1
	}
	return nil
}

// ExpandIncludes returns src — one template file, named file — line by line
// with its template comments blanked (helm never renders them) and every line
// that is a sole include of a helper in helpers replaced by that helper's
// body, recursively. An include naming no known helper is kept as it is.
//
// The body is spliced in as written: its indentation is the helper's own, not
// what a `| nindent N` would give it, which is fine for a line scanner (both
// argument grammars allow any indentation) and would not be for YAML.
func ExpandIncludes(file, src string, helpers map[string]Helper) ([]Line, error) {
	var out []Line
	err := expandLines(&out, filepath.Base(file), strings.Split(stripTemplateComments(src), "\n"), 1, helpers, 0)
	return out, err
}

func expandLines(out *[]Line, file string, lines []string, first int, helpers map[string]Helper, depth int) error {
	if depth > maxIncludeDepth {
		return fmt.Errorf("%s:%d: helpers included more than %d deep; is one including itself?", file, first, maxIncludeDepth)
	}
	for i, l := range lines {
		if m := soleInclude.FindStringSubmatch(l); m != nil {
			if h, ok := helpers[m[1]]; ok {
				if err := expandLines(out, h.File, h.Lines, h.Line, helpers, depth+1); err != nil {
					return err
				}
				continue
			}
		}
		*out = append(*out, Line{Text: l, Where: fmt.Sprintf("%s:%d", file, first+i)})
	}
	return nil
}

// joinLines is the text of expanded lines.
func joinLines(lines []Line) string {
	var b strings.Builder
	for i, l := range lines {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(l.Text)
	}
	return b.String()
}
