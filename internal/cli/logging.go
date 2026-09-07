package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"k8s.io/klog/v2"
)

// NewLogger builds the process logger from the -log-level flag value: any level
// slog.Level.UnmarshalText accepts, logfmt on stderr.
//
// There is no format choice. One format means an operator, a Loki pipeline and
// an alert annotation all parse the same bytes — and it means the format is a
// guarantee this package can hold rather than a per-deployment coin flip.
//
// A MAIN calls SetupLogging, never this: the logger alone leaves the OTHER
// loggers linked into the process (client-go's klog, grpc-go's grpclog) writing
// their own formats into the same stream, which is how the guarantee was
// already half-broken once.
func NewLogger(level string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}
	return slog.New(NewLogfmtHandler(os.Stderr, lvl)), nil
}

// SetupLogging is what a main calls: build the process logger from -log-level
// and route every OTHER logger in the process into it.
//
// The routing is here rather than in each main because the guarantee is
// "everything this process writes is logfmt", and a guarantee held by two
// copies of three statements is one a new binary — or an edit to one main —
// silently breaks. It had already broken once: klog was routed in both mains
// and grpc-go's grpclog in neither, so the collector-misconfiguration path (the
// OTLP exporter's gRPC client, the ingest listeners, the trace tier) wrote
// stdlib log lines with no time=, no level= and no msg= into the middle of the
// stream, at grpc's default severity, needing no env var to switch on.
//
// Order matters only in that all three happen before anything else in run():
// grpclog.SetLoggerV2 writes a package global with no lock and must not race a
// live gRPC client, and klog's records are otherwise glog-formatted.
func SetupLogging(level string) (*slog.Logger, error) {
	log, err := NewLogger(level)
	if err != nil {
		return nil, err
	}
	// The process default, for the few places that log through slog's package
	// functions (and for anything a dependency does the same way).
	slog.SetDefault(log)
	// client-go logs through klog: its lease churn, watch errors and backoffs
	// would otherwise go out as glog lines ("I0829 ... leaderelection.go:250]").
	// Unconditional in both binaries: klog is linked either way.
	klog.SetSlogLogger(log)
	// grpc-go, see grpclog.go.
	SetGRPCLogger(log)
	return log, nil
}

// NewLogfmtHandler is the one handler both binaries log through. Exported so a
// test can render a real log line into a buffer and parse it back — which is
// how the startup summaries are checked (logfmt_test.go, and the cmd tests).
//
// # Why slog.TextHandler, and what had to be added to it
//
// TextHandler is logfmt-SHAPED; nobody had checked that it IS logfmt. This repo
// ships a logfmt parser (github.com/JohanLindvall/logfmt), so the question is
// answerable rather than assumable, and logfmt_test.go answers it over the
// shapes this codebase actually logs. The measured result:
//
//   - VALUES round-trip exactly, every shape tried: spaces, '=', embedded and
//     escaped quotes, newlines and tabs, backslashes and Windows paths, URLs
//     with query strings, durations, empty strings, <nil>, non-ASCII. TextHandler
//     quotes when needed and escapes with Go rules, which is what the reader
//     decodes. Nothing here needed fixing. The ONE exception is a control byte
//     other than \n, \r or \t: TextHandler writes Go's \x00 hex escape — byte for
//     byte what the reference logfmt encoder (go-logfmt, via strconv.Quote)
//     writes — while the reader decodes only \n, \r, \t, \\, \" and \uXXXX, so it
//     hands back "ax00b" for "a\x00b". That asymmetry belongs to the format
//     rather than to this handler, an encoder of our own would be less
//     interoperable than the reference one, and the damage is contained to that
//     one value; it is pinned by test so it stays a known property.
//   - KEYS do not, and that is what this wrapper is for. TextHandler QUOTES a
//     key holding a space or '=' — `"my key"=v` — and the reader then sees the
//     key `"my` with a bare-key value, then `key"=v`: one record silently
//     becomes two wrong pairs, and every later pair on the line is still parsed,
//     so nothing looks broken. A key containing a quote survives with the quotes
//     embedded in the key NAME, which is a key nobody will ever grep for.
//
// So keys are sanitized (safeKey) at every door they arrive through. ReplaceAttr
// covers ordinary attribute keys, wherever they come from (a record's own args,
// With, WithAttrs) — but it is documented as NOT being called for Attrs of kind
// Group, and slog flattens a group into a KEY PREFIX ("g.k=v"), so a group name
// is a key by another name and reaches the record through three doors of its
// own: WithGroup (covered in WithGroup), an inline slog.Group in a call
// (covered in Handle), and a slog.Group passed to With/WithAttrs (covered in
// WithAttrs). The two that take Attrs (Handle, WithAttrs) sanitize at every
// DEPTH, because a group nested under a safe one contributes its own segment of
// the flattened key: both scans recurse, which a top-level-only scan did not, and
// `slog.Group("ok", slog.Group("bad key", …))` rendered `"ok.bad key.x"=v` —
// one attribute silently becoming two wrong pairs, the exact failure this
// wrapper exists to make impossible.
// Sanitizing is deliberately visible: an unsafe byte becomes '_', so a mangled
// key shows up in a grep for the concept rather than corrupting the record.
//
// This is a real dependency on stdlib behaviour, which is exactly why the test
// is the thing that holds it: a future TextHandler that changed its quoting
// rules would fail logfmt_test.go rather than quietly ship a format that is
// logfmt only most of the time.
func NewLogfmtHandler(w io.Writer, level slog.Leveler) slog.Handler {
	return logfmtHandler{slog.NewTextHandler(w, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: safeKeyAttr,
	})}
}

// logfmtHandler is slog.TextHandler plus the key guarantee. Everything except
// key sanitization is the embedded handler's.
type logfmtHandler struct{ slog.Handler }

// WithAttrs sanitizes group names among the attrs a logger carries. ReplaceAttr
// covers the ordinary keys here, but never a group's own name, and these attrs
// are pre-formatted once at With() time and then written into every record the
// derived logger emits — so an unsafe name that slips through corrupts a key on
// every line rather than on one. A new slice is built only when a name actually
// needs rewriting; the caller's slice is never modified.
func (h logfmtHandler) WithAttrs(as []slog.Attr) slog.Handler {
	for _, a := range as {
		if !hasUnsafeGroupName(a) {
			continue
		}
		out := make([]slog.Attr, len(as))
		for i, a := range as {
			out[i] = safeGroupNames(a)
		}
		as = out
		break
	}
	return logfmtHandler{h.Handler.WithAttrs(as)}
}

// WithGroup sanitizes the group NAME: it is prepended to every key under it
// ("g.k=v"), and ReplaceAttr never sees it.
func (h logfmtHandler) WithGroup(name string) slog.Handler {
	return logfmtHandler{h.Handler.WithGroup(safeKey(name))}
}

// Handle sanitizes the group names of a record's own attrs — one of the three
// group doors ReplaceAttr does not cover. Nothing is rebuilt unless a group
// name somewhere in the record is unsafe, so the overwhelmingly common record
// (no groups at all; this repo uses none) pays one Kind check per attribute.
func (h logfmtHandler) Handle(ctx context.Context, r slog.Record) error {
	unsafe := false
	r.Attrs(func(a slog.Attr) bool {
		if hasUnsafeGroupName(a) {
			unsafe = true
			return false
		}
		return true
	})
	if !unsafe {
		return h.Handler.Handle(ctx, r)
	}
	out := slog.NewRecord(r.Time, r.Level, r.Message, r.PC)
	r.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(safeGroupNames(a))
		return true
	})
	return h.Handler.Handle(ctx, out)
}

// hasUnsafeGroupName reports whether a is a group needing a rewrite — its own
// name, or the name of a group nested anywhere beneath it. The recursion is the
// point: slog renders a nested group as "outer.inner.key", so a name at ANY
// depth is a segment of a key on the line, and a scan that looked only at the
// top level left `slog.Group("ok", slog.Group("bad key", …))` to corrupt the
// record. It answers false for a non-group attr, whose key is ReplaceAttr's.
func hasUnsafeGroupName(a slog.Attr) bool {
	if a.Value.Kind() != slog.KindGroup {
		return false
	}
	if safeKey(a.Key) != a.Key {
		return true
	}
	for _, sub := range a.Value.Group() {
		if hasUnsafeGroupName(sub) {
			return true
		}
	}
	return false
}

// safeGroupNames rewrites unsafe group names, recursing into nested groups.
// Non-group attrs are returned untouched — their keys are ReplaceAttr's job.
func safeGroupNames(a slog.Attr) slog.Attr {
	if a.Value.Kind() != slog.KindGroup {
		return a
	}
	src := a.Value.Group()
	dst := make([]slog.Attr, len(src))
	for i, sub := range src {
		dst[i] = safeGroupNames(sub)
	}
	return slog.Attr{Key: safeKey(a.Key), Value: slog.GroupValue(dst...)}
}

// safeKeyAttr is the ReplaceAttr hook: sanitize the key, leave the value alone.
// It runs for the built-in time/level/msg attrs too, where it is a no-op.
func safeKeyAttr(_ []string, a slog.Attr) slog.Attr {
	a.Key = safeKey(a.Key)
	return a
}

// emptyKey is what an empty key becomes. A bare "=v" is not a logfmt pair at
// all, and an attribute with no name is a bug worth seeing rather than hiding.
const emptyKey = "_"

// safeKey replaces every byte that would break a logfmt KEY with '_': anything
// at or below a space (the reader's key stops), '=' (the separator), a double
// quote and DEL. Non-ASCII is left alone — a UTF-8 key parses fine, and
// mangling it would only make it unfindable.
func safeKey(k string) string {
	if k == "" {
		return emptyKey
	}
	if !strings.ContainsFunc(k, unsafeKeyRune) {
		return k
	}
	return strings.Map(func(r rune) rune {
		if unsafeKeyRune(r) {
			return '_'
		}
		return r
	}, k)
}

func unsafeKeyRune(r rune) bool {
	return r <= ' ' || r == '=' || r == '"' || r == 0x7f
}
