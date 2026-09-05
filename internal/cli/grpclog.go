package cli

// grpc-go's own logger, routed into the process logger.
//
// This is the last hole the "one format" guarantee had. grpc-go's DEFAULT
// grpclog writes through a stdlib log.Logger straight to os.Stderr — needing no
// env var, at its default severity — so a line like
//
//	2026/08/29 12:49:08 ERROR: [core] [Channel #1 SubChannel #2] grpc: addrConn.createTransport failed to connect to {collector:4317}. Err: connection refused
//
// lands in the middle of a logfmt stream: no time= key, no level=, no msg=, and
// a Loki pipeline parsing the stream sees a record with no fields at all. The
// path is not exotic — it is the OTLP exporter's client (internal/agent/
// otlpexport), the ingest listeners on :4317 and the trace tier's three
// listeners, i.e. exactly the collector-misconfiguration case that is the
// likeliest thing to go wrong on a first live run, which is also the case an
// operator is most likely to be reading the log for.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/JohanLindvall/kubescrape/internal/clip"

	"google.golang.org/grpc/grpclog"
)

// maxGRPCVerbosity is the highest V level that is worth rendering even at
// -log-level=debug.
//
// grpc's verbose sites are `if logger.V(2) { ... }` guards, and V is the ONLY
// thing standing between the process log and grpc's per-RPC, per-frame chatter:
// the library's own default is verbosity 0 (V(1) already false), and matching
// that at Info is what keeps this routing from turning a quiet steady state
// into a flood. Debug lifts it, because that is what Debug is for — but only to
// the level grpc actually uses, so a future V(9) site cannot make an incident's
// debug capture unreadable.
const maxGRPCVerbosity = 2

// maxGRPCMessageBytes bounds ONE grpc-rendered message in the record it becomes.
//
// grpc hands this adapter an ALREADY-RENDERED string, and several of the strings
// it renders embed bytes that came off the wire — a metadata header name and
// value, an http2 stream error, a marshalled status. None of them is bounded by
// anything this process controls (a gRPC server's default header list size is
// 16 MiB), so the record is as large as the sender chose unless it is cut here.
// A megabyte inside msg= is a second flood in the shape of one line, and it is
// the shape that breaks the "one format" guarantee this file exists for: a log
// pipeline that reads the stream line by line does not stay a stream at that
// size. The cut is generous enough that no grpc message written for an operator
// reaches it.
const maxGRPCMessageBytes = 2 << 10

// clipMessage cuts a rendered grpc message to maxGRPCMessageBytes, on a rune
// boundary (half a rune is a replacement character in whatever reads the line)
// and marked with the original length, so a clipped record can be told from a
// short one and the size of what was clipped is still the diagnosis. The cut
// is internal/clip's — a leaf package, so internal/cli stays importable by both
// mains without a dependency of its own.
func clipMessage(s string) string {
	if len(s) <= maxGRPCMessageBytes {
		return s
	}
	return fmt.Sprintf("%s… (grpc message clipped; %d bytes)", clip.Runes(s, maxGRPCMessageBytes), len(s))
}

// grpcLogger adapts grpc's LoggerV2 onto an *slog.Logger.
//
// The severity mapping is chosen against what the stream ALREADY carried, not
// against the names:
//
//   - Error and Fatal -> Error. These are the two grpc's default logger writes
//     to stderr, so they are the lines the campaign is making logfmt rather
//     than lines it is adding.
//   - Info -> DEBUG, deliberately. grpc logs channel/subchannel/resolver state
//     transitions at Info, several per reconnect per channel; today they are
//     discarded, and promoting them to slog's Info would make every agent's
//     steady state noisier for information nobody reads until an incident.
//     Debug is where "why did it do that" lives (internal/cli's levels).
//   - Warning -> DEBUG as well, which is the one mapping chosen against an
//     ARGUMENT rather than against the class's NAME. See below: this file
//     shipped it as Warn first, and that was a regression.
//
// WHY WARNING IS NOT WARN. Part of grpc's Warning class is PEER-DRIVEN, and the
// worst member of it is internal/transport/http2_server.go's
//
//	Failed to decode metadata header (%q, %q): %v
//
// which renders the header NAME and VALUE VERBATIM for any header any client
// sends, before any application code runs, on listeners this repo documents as
// unauthenticated (the agent's -ingest :4317, the trace tier's application
// ports). At Warn that is three separate violations of rules this repo applies
// everywhere else: an unthrottled
// line per attempt on an unauthenticated path, a record as large as the sender
// chose, and — since a `-bin` header that fails to decode is printed whatever it
// holds — a plausible route for a sender's CREDENTIAL into the process log and
// onward to the collector the log itself is shipped to.
//
// The counter-argument, which is real and which this file used to accept, is
// that Warn "is what every ecosystem grpc adapter does" and that the class
// carries `addrConn.createTransport failed to connect to %s. Err: %v` — the one
// line saying why nothing reaches the collector. It loses on the BASELINE.
// grpc's own default logger sends ONLY the Error class to stderr (grpclog's
// newLoggerV2 leaves warningW at io.Discard unless GRPC_GO_LOG_SEVERITY_LEVEL
// says otherwise), so before this routing existed nothing in the Warning class
// was emitted at all. Debug therefore takes away nothing an operator had, keeps
// the connection-failure line exactly one -log-level=debug away, and removes the
// amplifier completely rather than trying to bound it.
//
// Bounding it was the other candidate and it does not work here: grpc hands over
// an already-rendered string, so a logdedupe.Table keyed on the message is keyed
// on the peer's bytes, ONE shared gate would let a header warning suppress the
// collector one (the defect logdedupe's own doc warns about), and neither cures
// the credential half — a throttle bounds how OFTEN a line is written, never
// WHAT it carries. Clipping still applies (clipMessage, on every level): Debug
// is a level an operator turns ON during an incident, and a header-sized msg=
// would break the stream precisely then.
//
// What is NOT this file's to fix: 16 MiB of headers is a RECEIVE-buffer bound
// before it is a logging one, and it belongs on the servers, whatever this
// adapter does with the message. It is now set there —
// otlpingest.MaxHeaderListSizeOption, on both the ingest listeners and the
// trace tier's internal receiver — which bounds the bytes; the mapping below
// still bounds who sees them.
//
// Fatal does NOT exit here: grpclog.Fatal* calls os.Exit(1) itself after the
// logger returns (its LoggerV2 contract says so in as many words), and an
// os.Exit in here would be a second, racing one that skipped it.
type grpcLogger struct{ log *slog.Logger }

// enabled is the guard every method below takes before it formats anything.
// slog evaluates arguments eagerly, so an unguarded fmt.Sprintf in Infof would
// be paid on every channel state change at the DEFAULT level, for a record the
// handler then throws away. It guards the Warning trio for a sharper reason
// still: those arguments are the peer's bytes, so at the default level the
// header that arrives is never rendered at all.
func (g grpcLogger) enabled(l slog.Level) bool {
	return g.log.Enabled(context.Background(), l)
}

func (g grpcLogger) Info(args ...any) {
	if g.enabled(slog.LevelDebug) {
		g.log.Debug(clipMessage(fmt.Sprint(args...)))
	}
}

func (g grpcLogger) Infoln(args ...any) {
	if g.enabled(slog.LevelDebug) {
		g.log.Debug(clipMessage(sprintln(args...)))
	}
}

func (g grpcLogger) Infof(format string, args ...any) {
	if g.enabled(slog.LevelDebug) {
		g.log.Debug(clipMessage(fmt.Sprintf(format, args...)))
	}
}

func (g grpcLogger) Warning(args ...any) {
	if g.enabled(slog.LevelDebug) {
		g.log.Debug(clipMessage(fmt.Sprint(args...)))
	}
}

func (g grpcLogger) Warningln(args ...any) {
	if g.enabled(slog.LevelDebug) {
		g.log.Debug(clipMessage(sprintln(args...)))
	}
}

func (g grpcLogger) Warningf(format string, args ...any) {
	if g.enabled(slog.LevelDebug) {
		g.log.Debug(clipMessage(fmt.Sprintf(format, args...)))
	}
}

func (g grpcLogger) Error(args ...any) {
	if g.enabled(slog.LevelError) {
		g.log.Error(clipMessage(fmt.Sprint(args...)))
	}
}

func (g grpcLogger) Errorln(args ...any) {
	if g.enabled(slog.LevelError) {
		g.log.Error(clipMessage(sprintln(args...)))
	}
}

func (g grpcLogger) Errorf(format string, args ...any) {
	if g.enabled(slog.LevelError) {
		g.log.Error(clipMessage(fmt.Sprintf(format, args...)))
	}
}

// The Fatal trio is the process' last line before grpclog exits it, so it is
// written unconditionally: a level filter must not be what swallows the reason
// a pod died. It is still clipped — the reason fits in the budget many times
// over, and the record has to survive the stream that carries it.
func (g grpcLogger) Fatal(args ...any) { g.log.Error(clipMessage(fmt.Sprint(args...))) }

func (g grpcLogger) Fatalln(args ...any) { g.log.Error(clipMessage(sprintln(args...))) }

func (g grpcLogger) Fatalf(format string, args ...any) {
	g.log.Error(clipMessage(fmt.Sprintf(format, args...)))
}

// V answers grpc's `if logger.V(n)` guards from the process log level, which is
// what keeps the verbose sites from being formatted at all when nothing would
// print them. Level 0 is ordinary Info material (mapped to Debug above, so the
// question is whether Debug prints); anything above it is verbose chatter and
// is capped at maxGRPCVerbosity.
func (g grpcLogger) V(l int) bool {
	if l > maxGRPCVerbosity {
		return false
	}
	return g.enabled(slog.LevelDebug)
}

// sprintln is fmt.Sprintln without the trailing newline: grpc's *ln methods
// carry Println's spacing rules, and a message ending in a newline would be
// escaped into the record as `msg="...\n"` — parseable, and noise in every
// grep.
func sprintln(args ...any) string {
	return strings.TrimSuffix(fmt.Sprintln(args...), "\n")
}

// SetGRPCLogger routes grpc-go's package-level logger into log.
//
// grpclog.SetLoggerV2 writes a package global with no lock and documents that
// it must be called before any gRPC function; both mains call it through
// SetupLogging, at the top of run(), before any client, listener or dial
// exists.
func SetGRPCLogger(log *slog.Logger) {
	grpclog.SetLoggerV2(grpcLogger{log})
}
