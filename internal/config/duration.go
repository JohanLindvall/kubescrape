// Package config holds the primitives every YAML config section needs.
//
// The agent's config decodes through sigs.k8s.io/yaml, i.e. encoding/json,
// which accepts only a raw nanosecond integer for a time.Duration — so
// `wait: 10s` fails to decode, and with UnmarshalStrict that rejects the WHOLE
// file. Every duration in this repo's config is therefore a STRING parsed by
// hand after decoding, and by the time there were eight of those parsers they
// no longer agreed: some rejected a negative value, some accepted it and let a
// later bound check produce a different error, one accepted it outright, and
// two shared an error format string but not a policy.
//
// Duration below is the one implementation. The policy it fixes:
//
//   - "" takes the default the caller passes (the section is optional).
//   - a NEGATIVE value is always an error, naming the field and the value.
//     There is no duration in this repo where a negative is meaningful, and
//     "accept it and let the feature misbehave" was never a decision anyone
//     made — it was the absence of a check.
//   - an explicit "0" is legal by default and means what the caller documents
//     it to mean (usually "off"). Callers for which zero is NOT a legal value
//     say so with Positive, which folds the follow-up bound check — and its
//     explanation — into the same error.
//
// Not in scope: internal/promdur, which parses prometheus-operator's `y/w/d`
// CRD syntax for the monitor merge and the scraper. That is a different input
// LANGUAGE (Go durations have no unit above the hour), not a different reading
// of the same one.
package config

import (
	"fmt"
	"time"
)

type durOpts struct {
	positive bool
	why      string
}

// DurationOption tunes how one field is read.
type DurationOption func(*durOpts)

// ZeroDisables states that an explicit "0" is legal for this field and turns
// the feature off. It is the DEFAULT reading and changes nothing; it exists so
// that a call site can say which of the two readings it wants instead of
// leaving it to whoever reads the code next.
func ZeroDisables() DurationOption { return func(o *durOpts) { o.positive = false } }

// Positive requires a value strictly greater than zero. why explains what a
// zero would do (it lands in the error message, because a -check-config
// failure has to point at a line and say why, not just name a feature).
func Positive(why string) DurationOption {
	return func(o *durOpts) {
		o.positive = true
		o.why = why
	}
}

// Duration parses an optional Go-duration config string.
//
// field is the config path as an operator spells it ("tailSampling.decisionWait",
// "logs.sources[].ignoreOlder") — every error names it AND the offending value,
// so a rejected config points at a line.
func Duration(field, value string, def time.Duration, opts ...DurationOption) (time.Duration, error) {
	var o durOpts
	for _, opt := range opts {
		opt(&o)
	}
	d := def
	if value != "" {
		var err error
		if d, err = time.ParseDuration(value); err != nil {
			return 0, fmt.Errorf("%s %q: %w", field, value, err)
		}
	}
	if d < 0 {
		return 0, fmt.Errorf("%s %q is negative", field, quoted(value, d))
	}
	if o.positive && d == 0 {
		if o.why != "" {
			return 0, fmt.Errorf("%s must be greater than zero (%s)", field, o.why)
		}
		return 0, fmt.Errorf("%s must be greater than zero", field)
	}
	return d, nil
}

// quoted renders the offending value: the operator's own spelling when there
// was one, and the default's otherwise (a negative DEFAULT is a programming
// error, and printing "" would hide it).
func quoted(value string, d time.Duration) string {
	if value != "" {
		return value
	}
	return d.String()
}
