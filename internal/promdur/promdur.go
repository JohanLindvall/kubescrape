// Package promdur parses prometheus-operator's duration syntax: the CRD
// language (`y`, `w`, `d`, `h`, `m`, `s`, `ms`, largest unit first, e.g.
// "1d12h"), with Go's parser as the fallback so plain "30s"/"1m30s" and the
// fractional forms Prometheus does not admit keep working. Go's
// time.ParseDuration rejects y/w/d outright, so an `interval: 1d` — which the
// API server ACCEPTS, because the CRD's own pattern allows it — used to
// parse-fail and silently drop the target back to the default cadence.
//
// Two consumers read this language and MUST agree: the metadata service's
// monitor merge (internal/scrape) picks the FINER of two explicit intervals,
// and the agent's scraper (internal/agent/promscrape) re-parses whichever
// value won to schedule the target — so a parser disagreement makes the
// service serve a "finer" interval the agent reads differently. The two used
// to be copies (byte-identical regexes held equal by nothing) because the
// agent's lived in the OTLP scraper, which the metadata service must not
// link; a dependency-free leaf package removes the copy without adding the
// link. What deliberately stays at the CALL SITES is each consumer's edge
// rule — promscrape's caller gates non-positive values with its own warning,
// and the merge reads "0" as "not a usable interval" — because those are
// different READINGS of the same parse, not different parses.
//
// This is the sibling of internal/config's Duration, not a competitor: that
// is the one reader for Go-duration config STRINGS and explicitly scopes this
// CRD syntax out — a different input LANGUAGE, owned here.
package promdur

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"time"
)

// Parse parses one prometheus-operator duration value. An explicit "0" (and
// an empty string, which the CRD pattern matches) parses to (0, nil): whether
// a zero is USABLE is the caller's rule, not the parser's. A value in Go's
// syntax but not the CRD's falls back to time.ParseDuration verbatim,
// including its acceptance of negatives — callers that need positivity gate
// on the returned value.
func Parse(s string) (time.Duration, error) {
	if s == "0" {
		return 0, nil
	}
	m := promDurationRE.FindStringSubmatch(s)
	if m == nil {
		// Not the Prometheus shape; let Go try (it also accepts "1h30m",
		// "500ms" and the fractional forms Prometheus does not).
		return time.ParseDuration(s)
	}
	units := [...]time.Duration{
		365 * 24 * time.Hour, // y, as prometheus/common/model defines it
		7 * 24 * time.Hour,   // w
		24 * time.Hour,       // d
		time.Hour,            // h
		time.Minute,          // m
		time.Second,          // s
		time.Millisecond,     // ms
	}
	var out time.Duration
	for i, u := range units {
		g := m[i+1]
		if g == "" {
			continue
		}
		n, err := strconv.ParseInt(g, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("duration %q: %w", s, err)
		}
		// n and u are both non-negative, so both the per-term multiply and the
		// running sum are monotonic: a value that wraps int64 lands on a small
		// or negative duration that a caller's `d <= 0` gate reads as a
		// plausibly-tiny cadence rather than as invalid. The CRD's `[0-9]+`
		// admits any magnitude, so scrapeTimeout "18446744073710ms" wrapped to
		// +448µs — a deadline every scrape exceeds (total metric loss), with no
		// invalid-duration warning. Refuse the overflow at EACH accumulation
		// step (compound forms like "290y290y" overflow the sum, not one term)
		// so callers warn and fall back like any other bad value.
		if n > math.MaxInt64/int64(u) {
			return 0, fmt.Errorf("duration %q: overflows time.Duration", s)
		}
		term := time.Duration(n) * u
		if out > math.MaxInt64-term {
			return 0, fmt.Errorf("duration %q: overflows time.Duration", s)
		}
		out += term
	}
	return out, nil
}

// promDurationRE mirrors prometheus-operator's Duration validation pattern, so
// exactly the values the API server admits are the values we accept.
var promDurationRE = regexp.MustCompile(`^(?:([0-9]+)y)?(?:([0-9]+)w)?(?:([0-9]+)d)?(?:([0-9]+)h)?(?:([0-9]+)m)?(?:([0-9]+)s)?(?:([0-9]+)ms)?$`)
