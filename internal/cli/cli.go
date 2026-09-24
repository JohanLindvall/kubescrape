// Package cli holds the small startup helpers both binaries' mains share: the
// process logger built from -log-level, the comma-separated list splitter
// behind several flags, the GOMEMLIMIT derivation, the signal-driven process
// lifetime (ShutdownContext), the listen-address check (CheckListeners) and
// the readiness-gate watch (WatchGates). Each existed once per
// main, byte-identical in behavior, with only the function names drifting —
// the shape that eventually drifts for real.
//
// The kubeconfig precedence builder — whose two copies had already drifted in
// name (buildConfig vs kubeConfig) — is the one that moved OUT, into the
// kubecfg subpackage: it is the only one of these that costs an import of
// k8s.io/client-go, and this package is imported by an agent built without the
// `events` tag, whose entire purpose is not to link that.
//
// # The log format is logfmt, and it is not selectable
//
// There used to be a -log-format flag (text or json). It is gone: one format
// means every consumer — a person with grep, a Loki pipeline, an alert's
// annotation — parses the same bytes, and it means the format's CORRECTNESS is
// a property this package can hold rather than a per-deployment coin flip. See
// logging.go for what "correct" was measured against (this repo ships a logfmt
// PARSER, so the guarantee is a round-trip test, not an assertion).
//
// # The key vocabulary
//
// A logfmt line is only greppable if the same concept is spelled the same way
// everywhere: `error=` must find every failure, `path=` every file. New log
// calls take a key from this list wherever one fits, and add to the list rather
// than inventing a synonym beside an entry. lowercase single words where
// possible, lowerCamelCase for multiword — never snake_case, never a key with a
// space, '=' or a quote in it (logging.go sanitizes those, but a sanitized key
// is a key nobody will grep for).
//
// Identity and objects:
//
//	error        the error. NEVER err, cause, reason or msg
//	path         a filesystem path (a log file, a socket). NEVER file; a
//	             credential file's path is tokenFile
//	dir          a directory
//	url          a full URL that was (or would be) requested
//	endpoint     a configured destination: host:port for gRPC, base URL for HTTP
//	addr         a LISTEN address of this process
//	peer         the remote address of the connection a line is about, as
//	             peerip.ForLog renders it: the bare canonical IP (no port), or
//	             the raw address, clipped, only where it does not parse. NEVER
//	             remoteAddr
//	namespace    a Kubernetes namespace. NEVER ns or podNamespace
//	pod          a pod name (namespace travels in `namespace`)
//	service      a Kubernetes Service name (namespace travels in `namespace`)
//	node         a node name
//	container    a container name
//	uid          a Kubernetes UID. NEVER podUID
//	id           an opaque id that is not a UID (container id, cgroup id)
//	target       a scrape target's identity
//	monitor      a ServiceMonitor/PodMonitor as "namespace/name"
//	kind         the object kind a metric label also carries (servicemonitor, ...)
//	signal       logs, metrics or traces — the same values the metrics label
//	pipeline     a kubescrape pipeline name (logs, cadvisor, journald, ingest, ...)
//	route        a routing route name
//	unit         a systemd unit
//	key          a config/attribute/secret KEY NAME (never its value)
//
// Quantities and time (durations are time.Duration values, so they render
// as 15s / 1m0s and never as a bare number):
//
//	interval     a configured period
//	timeout      a configured per-attempt deadline
//	budget       how much of a deadline this step was given
//	backoff      the delay before the next attempt
//	wait         how long something MAY block: a wait budget or deadline
//	elapsed      how long a finished step took — a blocked lookup, a readiness
//	             wait, a walk, a shutdown. NEVER took or waited
//	grace        a rotation/termination grace window
//	attempts     how many tries were made
//	outage       how long the current run of failures has lasted, beside
//	             failures, how many that run has had (logdedupe.Outage keeps
//	             both: Lasted, Failures)
//	bytes        a size in bytes (maxBytes / limitBytes for a bound)
//	count nouns  plural and bare: records, entries, targets, pods, containers,
//	             routes, shards, waiters, lookups, segments, dropped
//
// Outcomes and hints:
//
//	reason       a classification that matches a metric label value
//	outcome      likewise, where the metric's label is spelled `outcome`
//	flag         the flag an operator would change, spelled with its dash
//	note         a remediation hint, when the message alone cannot carry it
//	version      the build version; built the build timestamp
//	hash         a content hash (transform program, ETag material)
//	tokenFile    the PATH of a credential file — never the credential
//
// Reserved — slog writes these itself and does not dedupe an attribute
// against them, so a second pair would shadow the record's own field for
// whichever consumer keeps that one (TestNoLogCallUsesASlogReservedKey fails
// the build on a call that uses one):
//
//	time level msg
//
// # Never log a secret
//
// No bearer tokens, passwords, connection strings, Authorization headers or
// secret values, and no full request/response bodies. Log the reference
// (tokenFile, key, "namespace/name/key"), a length, or a count.
package cli

import (
	"strings"
)

// SplitList splits a comma-separated flag value, trimming whitespace around
// each entry and dropping empty ones; an empty or all-blank value yields nil.
func SplitList(s string) []string {
	var out []string
	for part := range strings.SplitSeq(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
