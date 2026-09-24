package cli

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/JohanLindvall/logfmt"
	"google.golang.org/grpc/grpclog"
)

// The last hole the "one format" guarantee had: grpc-go's grpclog. Its DEFAULT
// logger writes stdlib log lines straight to os.Stderr at its default severity
// — no env var needed — so on the collector-misconfiguration path (the OTLP
// exporter's client, the ingest listeners, the trace tier's three) the stream
// carried records with no time=, no level= and no msg= at all. These tests run
// the REAL wiring, through the package global grpclog.SetLoggerV2 writes, so
// they fail if SetupLogging stops routing it.
//
// captureStderr (logfmt_test.go) is the harness they share with the klog and
// stdlib-log routing tests.

// The line an operator reads on a first live run when the collector address is
// wrong. Before the routing it was
//
//	2026/08/29 12:49:08 ERROR: [core] ... connection refused
//
// which has no fields at all.
func TestGRPCLogsGoThroughTheProcessLoggerAsLogfmt(t *testing.T) {
	line := captureStderr(t, func() {
		if _, err := SetupLogging("info"); err != nil {
			t.Fatal(err)
		}
		grpclog.Errorf("[core] [Channel #1 SubChannel #2] grpc: addrConn.createTransport failed to connect to %s. Err: %v",
			"{collector.monitoring:4317}", errors.New("connection refused"))
	})
	if len(line) == 0 {
		t.Fatal("grpclog wrote nothing to the routed stderr: it is still writing through its own default logger")
	}
	got := pairs(t, line)
	if len(got) != 3 {
		t.Fatalf("record has %d pairs, want time/level/msg: %q", len(got), line)
	}
	if got[1] != [2]string{"level", "ERROR"} {
		t.Errorf("level pair = %v, want level=ERROR", got[1])
	}
	if k, v := got[2][0], got[2][1]; k != "msg" ||
		!strings.Contains(v, "addrConn.createTransport failed") || !strings.Contains(v, "connection refused") {
		t.Errorf("message pair = %v; the formatted grpc message must survive intact", got[2])
	}
}

// The severity mapping, against what the stream ALREADY carried rather than
// against the class names. grpc's Info is per-channel state chatter that its
// default logger discards, so it maps to DEBUG — promoting it would make every
// agent's steady state noisier for something nobody reads until an incident.
// Warning maps to DEBUG for the same baseline reason plus a sharper one: grpc's
// default logger discards that class too, and part of it is peer-driven (see
// TestGRPCPeerDrivenWarningsAreNotWarn). Only the Error class was ever on
// stderr, so only it stays at a level the default prints.
func TestGRPCSeveritiesMapOntoTheProcessLevels(t *testing.T) {
	for _, tc := range []struct {
		level string
		want  string // levels, in call order
	}{
		{"info", "ERROR"},
		{"debug", "DEBUG,DEBUG,ERROR"},
	} {
		t.Run(tc.level, func(t *testing.T) {
			out := captureStderr(t, func() {
				if _, err := SetupLogging(tc.level); err != nil {
					t.Fatal(err)
				}
				grpclog.Infof("[core] Channel created")
				grpclog.Warningf("[core] grpc: addrConn.createTransport failed")
				grpclog.Errorln("[transport]", "connection error")
			})
			var levels []string
			for line := range bytes.SplitSeq(out, []byte("\n")) {
				if len(line) == 0 {
					continue
				}
				v, _ := logfmt.Get(line, "level")
				levels = append(levels, string(v))
			}
			if got := strings.Join(levels, ","); got != tc.want {
				t.Errorf("at -log-level=%s grpc levels = %q, want %q", tc.level, got, tc.want)
			}
		})
	}
}

// An *ln message must not carry Println's trailing newline into the record: it
// would be escaped into msg="...\n" — parseable, and noise in every grep.
func TestGRPCLineMethodsDoNotCarryATrailingNewline(t *testing.T) {
	line := captureStderr(t, func() {
		if _, err := SetupLogging("info"); err != nil {
			t.Fatal(err)
		}
		grpclog.Errorln("[transport]", "connection error:", "desc = transport is closing")
	})
	v, ok := logfmt.Get(line, "msg")
	if !ok {
		t.Fatalf("no msg pair in %q", line)
	}
	if got := string(logfmt.AppendUnescape(nil, v)); got != "[transport] connection error: desc = transport is closing" {
		t.Errorf("msg = %q, want Println spacing with no trailing newline", got)
	}
}

// spyArg reports whether it was rendered. fmt renders a Stringer only when it
// actually formats the verb, so this is how "the argument was evaluated" is
// observed.
type spyArg struct{ rendered *bool }

func (s spyArg) String() string {
	*s.rendered = true
	return "rendered"
}

// slog evaluates arguments eagerly, so a Debug whose arguments cost more than a
// field read has to be guarded — grpc's Infof is exactly that call, made per
// channel state change, and it maps to Debug. Unguarded, every agent would pay
// the Sprintf at the DEFAULT level for a record the handler throws away.
func TestGRPCVerboseArgumentsAreNotFormattedWhenNothingWouldPrintThem(t *testing.T) {
	var rendered bool
	arg := spyArg{&rendered}
	out := captureStderr(t, func() {
		if _, err := SetupLogging("info"); err != nil {
			t.Fatal(err)
		}
		grpclog.Infof("channel state %v", arg)
		grpclog.Info("channel state ", arg)
		grpclog.Infoln("channel state", arg)
	})
	if rendered {
		t.Error("grpc's Info arguments were formatted at -log-level=info, where the record is discarded")
	}
	if len(out) != 0 {
		t.Errorf("grpc Info reached the stream at -log-level=info: %q", out)
	}
	// And at debug it is genuinely delivered — the guard must gate the cost,
	// not the feature.
	rendered = false
	out = captureStderr(t, func() {
		if _, err := SetupLogging("debug"); err != nil {
			t.Fatal(err)
		}
		grpclog.Infof("channel state %v", arg)
	})
	if !rendered {
		t.Error("grpc's Info arguments were not formatted at -log-level=debug")
	}
	if v, _ := logfmt.Get(out, "msg"); string(v) != "channel state rendered" {
		t.Errorf("msg = %q at -log-level=debug", v)
	}
}

// V is the only thing between the stream and grpc's per-RPC chatter: its
// verbose sites are `if logger.V(n)` guards. The default must answer no, debug
// must answer yes up to the level grpc actually uses, and a hypothetical V(9)
// site must not be able to make a debug capture unreadable.
func TestGRPCVerbosityGateIsQuietByDefaultAndBoundedAtDebug(t *testing.T) {
	var buf bytes.Buffer
	at := func(l slog.Level) grpcLogger {
		return grpcLogger{slog.New(NewLogfmtHandler(&buf, l))}
	}
	if at(slog.LevelInfo).V(1) {
		t.Error("V(1) is true at -log-level=info: grpc's verbose sites would format on every RPC")
	}
	if !at(slog.LevelDebug).V(1) || !at(slog.LevelDebug).V(maxGRPCVerbosity) {
		t.Errorf("V(1..%d) is false at -log-level=debug: the verbose sites an incident needs are gated off", maxGRPCVerbosity)
	}
	if at(slog.LevelDebug).V(maxGRPCVerbosity + 1) {
		t.Errorf("V(%d) is true at -log-level=debug: nothing bounds a future high-verbosity site", maxGRPCVerbosity+1)
	}
}

// grpc's Warning class is PEER-DRIVEN in part, and the members that are cost
// nothing to trigger: internal/transport/http2_server.go renders "Failed to
// decode metadata header (%q, %q)" — the header NAME and VALUE verbatim — for
// any header any client sends, before any application code runs, on listeners
// this repo documents as unauthenticated and whose grpc-go default header list
// size is 16 MiB.
//
// So the class must not reach a level the default prints: at -log-level=info
// the record must not be written AND the peer's bytes must not even be
// formatted, which is the eager-argument rule applied to somebody else's input.
// grpc's own default logger discards this class, so nothing an operator had is
// lost — the line is one -log-level=debug away.
func TestGRPCPeerDrivenWarningsAreNotWarn(t *testing.T) {
	var rendered bool
	arg := spyArg{&rendered}
	out := captureStderr(t, func() {
		if _, err := SetupLogging("info"); err != nil {
			t.Fatal(err)
		}
		grpclog.Warningf("Failed to decode metadata header (%q, %q): %v", "x-junk-bin", arg, "illegal base64")
		grpclog.Warning("Encountered http2.StreamError: ", arg)
		grpclog.Warningln("Encountered http2.StreamError:", arg)
	})
	if rendered {
		t.Error("a peer's header bytes were formatted at -log-level=info: an unauthenticated sender pays for the Sprintf")
	}
	if len(out) != 0 {
		t.Errorf("grpc's peer-driven Warning class reached the stream at -log-level=info: %q", out)
	}
	// And at debug it is genuinely delivered: the mapping moves the level, it
	// does not remove the line an operator turns debug on to read.
	out = captureStderr(t, func() {
		if _, err := SetupLogging("debug"); err != nil {
			t.Fatal(err)
		}
		grpclog.Warningf("[core] grpc: addrConn.createTransport failed to connect to %s", "{collector:4317}")
	})
	v, ok := logfmt.Get(out, "level")
	if !ok || string(v) != "DEBUG" {
		t.Errorf("level = %q at -log-level=debug, want the connection line at DEBUG", v)
	}
	if v, _ := logfmt.Get(out, "msg"); !bytes.Contains(v, []byte("addrConn.createTransport failed")) {
		t.Errorf("msg = %q: the connection-failure line must survive the level change", v)
	}
}

// grpc hands this adapter an already-rendered string, and several of the
// strings it renders embed bytes that came off the wire with nothing in this
// process bounding them. Debug is a level an operator turns ON during an
// incident, which is exactly when a 16 MiB msg= would stop the stream being a
// stream — so the record is clipped whatever the level.
//
// This one routes through the grpclog global into a BUFFER rather than through
// captureStderr's pipe: an unclipped megabyte fills the pipe's 64 KiB and
// blocks the writer forever, since captureStderr only reads after f returns —
// so a regression here would HANG the package's tests instead of failing them.
func TestGRPCMessagesAreClippedIntoTheRecord(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	setGRPCLogger(slog.New(NewLogfmtHandler(&buf, slog.LevelDebug)))
	t.Cleanup(func() { setGRPCLogger(old) })

	// A header value the size the default header list bound permits, with a
	// distinctive tail so the assertion can prove the tail did NOT ship.
	huge := strings.Repeat("A", 1<<20) + "TAILOFTHEPEERSBYTES"
	grpclog.Warningf("Failed to decode metadata header (%q, %q): %v", "x-junk-bin", huge, "illegal base64")

	out := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	if len(out) > 4<<10 {
		t.Errorf("record is %d bytes: a peer chose the size of a log line", len(out))
	}
	v, ok := logfmt.Get(out, "msg")
	if !ok {
		t.Fatalf("no msg pair in %q", out[:min(len(out), 200)])
	}
	msg := logfmt.AppendUnescape(nil, v)
	if bytes.Contains(msg, []byte("TAILOFTHEPEERSBYTES")) {
		t.Error("the peer's bytes reached the record whole: the message was not clipped")
	}
	if !bytes.Contains(msg, []byte("clipped")) {
		t.Errorf("a clipped message does not say so, so it cannot be told from a short one: %q", msg[:min(len(msg), 120)])
	}
	// The head is what carries the diagnosis, so it must still be there.
	if !bytes.Contains(msg, []byte("Failed to decode metadata header")) {
		t.Error("clipping ate the head of the message, which is the half that says what happened")
	}
}

// The clip cuts on a RUNE boundary: half a rune is a replacement character in
// whatever reads the line, and the message goes into a logfmt value.
func TestGRPCMessageClipCutsOnARuneBoundary(t *testing.T) {
	// Multi-byte runes straddling the ceiling from every offset.
	for pad := range 4 {
		s := strings.Repeat("x", pad) + strings.Repeat("é", maxGRPCMessageBytes)
		got := clipMessage(s)
		if !utf8.ValidString(got) {
			t.Fatalf("clipMessage cut a rune in half at pad=%d", pad)
		}
		if len(got) >= len(s) {
			t.Fatalf("clipMessage did not cut an over-budget message at pad=%d: %d bytes in, %d out", pad, len(s), len(got))
		}
	}
	if got := clipMessage("short"); got != "short" {
		t.Errorf("clipMessage(short) = %q, want it untouched", got)
	}
}
