package config

import "path"

// Glob validates a path.Match pattern at parse time.
//
// It owns the rationale three call sites used to re-derive independently:
// path.Match reports ErrBadPattern only WHEN MATCHING, and every matcher in
// this repo reads a match error as "no match" — so an unvalidated malformed
// pattern is never an error anywhere downstream, it is a matcher that
// silently selects NOTHING. That failure mode is indistinguishable from "no
// traffic yet": a routing route that never fires sends its tenant's telemetry
// to the default destination with the route's counter sitting at zero, a log
// source's namespace allowlist collects no files, a debug-tap filter streams
// silence. Reject the pattern where it is written instead. Matching against
// the empty string is the cheapest probe that still runs the pattern parser
// over the whole pattern.
func Glob(pattern string) error {
	_, err := path.Match(pattern, "")
	return err
}
