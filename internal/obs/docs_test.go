package obs

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// Metric names and label names are a PUBLIC interface: they are what users
// write dashboards and alert rules against, and a rule that selects a
// nonexistent series does not error, it silently matches nothing. README once
// documented kubescrape_transform_reloads_total{result} while the code
// registered the label as "outcome", so an alert copied out of the README
// could never fire — for a counter whose whole purpose is reporting that a
// broken transforms file is still serving stale logic.
//
// This pins the direction that matters: everything the docs promise must
// actually exist. The reverse (an undocumented metric) is not an error — a
// name is added before its prose.
func TestDocumentedMetricsExist(t *testing.T) {
	docs, err := ParseMetricDocs("obs.go")
	if err != nil {
		t.Fatalf("ParseMetricDocs: %v", err)
	}
	if len(docs) == 0 {
		t.Fatal("parsed no metric registrations — the extractor is broken, not the docs")
	}
	registered := map[string]bool{}
	labels := map[string]map[string]bool{}
	for _, d := range docs {
		registered[d.Name] = true
		labels[d.Name] = map[string]bool{}
		for _, l := range d.Labels {
			labels[d.Name][l] = true
		}
	}

	nameRE := regexp.MustCompile(`kubescrape_[a-z0-9_]+`)
	// A documented selector, e.g. kubescrape_transform_reloads_total{outcome}
	// or ..._total{outcome="failed"}.
	selectorRE := regexp.MustCompile(`(kubescrape_[a-z0-9_]+)\{([^}]*)\}`)
	labelNameRE := regexp.MustCompile(`[a-z_][a-z0-9_]*`)

	for _, doc := range []string{"../../README.md", "../../docs/CONFIGURATION.md", "../../docs/METRICS.md", "../../CLAUDE.md"} {
		b, err := os.ReadFile(doc)
		if err != nil {
			t.Fatalf("reading %s: %v", doc, err)
		}
		text := string(b)

		for _, name := range nameRE.FindAllString(text, -1) {
			// Gauge families rendered with a _bytes/_total suffix in prose are
			// matched as written; only exact registrations count.
			if !registered[name] {
				t.Errorf("%s documents metric %q, which is not registered in obs.go", doc, name)
			}
		}

		for _, m := range selectorRE.FindAllStringSubmatch(text, -1) {
			name, inner := m[1], m[2]
			if !registered[name] {
				continue // already reported above
			}
			for _, part := range strings.Split(inner, ",") {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				if i := strings.IndexAny(part, "=!~"); i >= 0 {
					part = part[:i]
				}
				part = strings.TrimSpace(part)
				if !labelNameRE.MatchString(part) {
					continue
				}
				if !labels[name][part] {
					t.Errorf("%s documents %s{%s}, but that metric has labels %v",
						doc, name, part, keys(labels[name]))
				}
			}
		}
	}
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
