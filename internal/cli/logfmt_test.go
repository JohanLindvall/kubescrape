package cli

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/JohanLindvall/logfmt"
	"google.golang.org/grpc/grpclog"
)

// The guarantee behind dropping -log-format: the one format that is left IS
// logfmt, checked with this repo's own reader rather than asserted from the
// handler's shape. See NewLogfmtHandler for what the investigation found.
//
// Round-tripping means: the reader recovers exactly the pairs that were logged,
// in order, with values decoded back to the bytes that went in. A line that
// merely PARSES is not enough — a quoted key parses beautifully and yields two
// wrong pairs.

// logLine renders one record through the production handler and returns it
// without its trailing newline.
func logLine(t *testing.T, msg string, args ...any) []byte {
	t.Helper()
	var buf bytes.Buffer
	slog.New(NewLogfmtHandler(&buf, slog.LevelDebug)).Info(msg, args...)
	line := buf.Bytes()
	if n := bytes.Count(line, []byte("\n")); n != 1 || !bytes.HasSuffix(line, []byte("\n")) {
		t.Fatalf("record spans %d newlines, so it is not one logfmt record: %q", n, line)
	}
	return bytes.TrimSuffix(line, []byte("\n"))
}

// pairs reads a rendered line back, decoding each value exactly as a consumer
// would: quoted values are unescaped, unquoted ones are not (a backslash means
// nothing outside quotes — logfmt's own trap, and the reason AppendValue takes
// the quoted bit from the parser instead of guessing).
func pairs(t *testing.T, line []byte) [][2]string {
	t.Helper()
	if err := logfmt.Validate(line); err != nil {
		t.Fatalf("logfmt.Validate(%q) = %v", line, err)
	}
	var out [][2]string
	// The quoted bit is per OCCURRENCE, not per key (a record can carry two
	// pairs with one key), so GetQuoted cannot supply it. Iterate delivers
	// pairs in order, so walking a cursor forward through the raw line finds
	// each value's opening byte.
	cursor := 0
	err := logfmt.Iterate(line, func(k, v []byte) bool {
		val := string(v)
		if start := valueStart(line, cursor, k); start >= 0 {
			if quoted := line[start] == '"'; quoted {
				if logfmt.NeedsUnescape(v) {
					val = string(logfmt.AppendUnescape(nil, v))
				}
				cursor = start + len(v) + 2
			} else {
				cursor = start + len(v)
			}
		}
		out = append(out, [2]string{string(k), val})
		return true
	})
	if err != nil {
		t.Fatalf("logfmt.Iterate(%q) = %v", line, err)
	}
	return out
}

// valueStart returns the offset of the value that follows "key=" at or after
// from, or -1 when the line does not spell the pair that way (a bare key, which
// only a malformed record produces).
func valueStart(line []byte, from int, key []byte) int {
	needle := append(append([]byte(nil), key...), '=')
	i := bytes.Index(line[from:], needle)
	if i < 0 || from+i+len(needle) >= len(line) {
		return -1
	}
	return from + i + len(needle)
}

// TestEveryLoggedValueShapeRoundTripsAsLogfmt is the corpus: one attribute per
// case, over the value shapes this codebase actually logs.
func TestEveryLoggedValueShapeRoundTripsAsLogfmt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		key  string
		val  any
		want string // what the reader must hand back
	}{
		{"plain", "pod", "kubescrape-agent-abcde", "kubescrape-agent-abcde"},
		{"spaces", "error", errors.New("connection refused to host"), "connection refused to host"},
		{"equals", "error", errors.New(`flag provided but not defined: -log-format=json`), "flag provided but not defined: -log-format=json"},
		{"quotes", "error", errors.New(`bad value "abc" for -x`), `bad value "abc" for -x`},
		{"escaped quotes", "error", errors.New(`he said \"hi\"`), `he said \"hi\"`},
		{"url", "url", "http://kubescrape.monitoring:8080/v1/self?wait=1s", "http://kubescrape.monitoring:8080/v1/self?wait=1s"},
		{"path", "path", "/var/log/containers/app_ns_c-0123.log", "/var/log/containers/app_ns_c-0123.log"},
		{"backslash path", "path", `C:\Users\bob`, `C:\Users\bob`},
		{"trailing backslash", "path", `/tmp/x\`, `/tmp/x\`},
		{"duration", "interval", 15 * time.Second, "15s"},
		{"zero duration", "backoff", time.Duration(0), "0s"},
		{"multiline", "error", errors.New("first line\nsecond\tline"), "first line\nsecond\tline"},
		{"empty", "addr", "", ""},
		{"zero int", "records", 0, "0"},
		{"false", "enrich", false, "false"},
		{"nil error", "error", error(nil), "<nil>"},
		{"logfmt inside", "note", `route=a namespace="b c"`, `route=a namespace="b c"`},
		{"unicode", "node", "nöde-1 ✓", "nöde-1 ✓"},
		{"only quotes", "value", `""`, `""`},
		{"leading space", "value", "  x", "  x"},
		{"float", "probability", 0.1, "0.1"},
		{"stringer", "level", slog.LevelWarn, "WARN"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			line := logLine(t, "a message with spaces", c.key, c.val)
			got := pairs(t, line)
			if len(got) != 4 {
				t.Fatalf("got %d pairs, want time/level/msg + the attribute: %q -> %v", len(got), line, got)
			}
			for i, want := range []string{"time", "level", "msg"} {
				if got[i][0] != want {
					t.Errorf("pair %d key = %q, want %q", i, got[i][0], want)
				}
			}
			if got[2][1] != "a message with spaces" {
				t.Errorf("msg = %q, want the message verbatim", got[2][1])
			}
			if got[3][0] != c.key {
				t.Errorf("attribute key = %q, want %q (line %q)", got[3][0], c.key, line)
			}
			if got[3][1] != c.want {
				t.Errorf("attribute value = %q, want %q (line %q)", got[3][1], c.want, line)
			}
		})
	}
}

// TestUnsafeKeysAreSanitizedRatherThanCorruptingTheRecord pins the one thing
// TextHandler alone gets wrong. Without safeKey, `"my key"=v` parses as the key
// `"my` with a bare-key value plus a second pair `key"=v`: one attribute
// silently becomes two wrong ones, and nothing downstream can tell.
// The ONE shape that does not round-trip byte-for-byte, pinned here so it is a
// known property rather than a surprise: a control byte other than \n, \r or \t.
//
// TextHandler renders it with Go's hex escape (\x00), exactly as the reference
// logfmt ENCODER does (go-logfmt quotes with strconv.Quote, which emits the same
// bytes) — and this repo's READER decodes only \n, \r, \t, \\, \" and \uXXXX, so
// an unknown escape comes back with its backslash consumed: "a\x00b" reads as
// "ax00b". The asymmetry is the format's, not the handler's, and writing our own
// encoder to dodge it would make us LESS interoperable than the reference one.
//
// What matters is that the damage is contained: the record stays one valid
// logfmt line, the key is intact, and every other pair on it is untouched. The
// only values that can carry such bytes are ones this process should not be
// logging whole anyway (a log line, a response body — see the "never log a
// secret" rule, which is the same rule about the same values).
func TestControlBytesRenderAsEscapesTheReaderDoesNotDecode(t *testing.T) {
	t.Parallel()
	line := logLine(t, "m", "body", "a\x00b\x1bc", "path", "/tmp/x")
	got := pairs(t, line)
	if len(got) != 5 {
		t.Fatalf("got %d pairs, want 5: %q -> %v", len(got), line, got)
	}
	if got[3][0] != "body" || got[3][1] != "ax00bx1bc" {
		t.Errorf("body = %q=%q, want body=ax00bx1bc (the known rendering)", got[3][0], got[3][1])
	}
	if got[4] != [2]string{"path", "/tmp/x"} {
		t.Errorf("the pair AFTER the control bytes is %v, want path=/tmp/x — the damage must stay contained", got[4])
	}
}

func TestUnsafeKeysAreSanitizedRatherThanCorruptingTheRecord(t *testing.T) {
	t.Parallel()
	cases := []struct{ key, want string }{
		{"my key", "my_key"},
		{"my=key", "my_key"},
		{`my"key`, "my_key"},
		{"my\tkey", "my_key"},
		{"my\nkey", "my_key"},
		{"", "_"},
		{"lowerCamelCase", "lowerCamelCase"},
		{"dotted.key", "dotted.key"},
		{"-flag-shaped", "-flag-shaped"},
		{"nöde", "nöde"},
	}
	for _, c := range cases {
		line := logLine(t, "m", c.key, "v")
		got := pairs(t, line)
		if len(got) != 4 {
			t.Fatalf("key %q produced %d pairs, want 4: %q -> %v", c.key, len(got), line, got)
		}
		if got[3][0] != c.want || got[3][1] != "v" {
			t.Errorf("key %q rendered as %q=%q, want %q=v (line %q)", c.key, got[3][0], got[3][1], c.want, line)
		}
	}
}

// Group names are the door ReplaceAttr does not cover, so Handle and WithGroup
// cover it. The repo uses no groups today; this is what keeps the guarantee
// true if one appears.
func TestGroupNamesAreSanitizedToo(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(NewLogfmtHandler(&buf, slog.LevelDebug))

	log.Info("m", slog.Group("bad name", "k", "v"))
	line := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	if got := pairs(t, line); len(got) != 4 || got[3][0] != "bad_name.k" {
		t.Errorf("inline group: got %v, want the group name sanitized (line %q)", got, line)
	}

	buf.Reset()
	log.WithGroup("bad name").With("a", "b").Info("m", "k", "v")
	line = bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	got := pairs(t, line)
	if len(got) != 5 || got[3][0] != "bad_name.a" || got[4][0] != "bad_name.k" {
		t.Errorf("WithGroup: got %v, want sanitized group prefixes (line %q)", got, line)
	}

	buf.Reset()
	log.Info("m", slog.Group("outer x", slog.Group("inner y", "k", "v")))
	line = bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	if got := pairs(t, line); len(got) != 4 || got[3][0] != "outer_x.inner_y.k" {
		t.Errorf("nested group: got %v, want both names sanitized (line %q)", got, line)
	}
}

// WithAttrs is pre-formatted at With() time rather than per record, so it takes
// a different path through TextHandler and needs its own case.
func TestWithAttrsRoundTrips(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	slog.New(NewLogfmtHandler(&buf, slog.LevelDebug)).
		With("node", "n 1", "bad key", "x").
		Info("m", "error", errors.New("a=b c"))
	line := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	want := [][2]string{{"msg", "m"}, {"node", "n 1"}, {"bad_key", "x"}, {"error", "a=b c"}}
	got := pairs(t, line)[2:] // skip time/level
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v (line %q)", got, want, line)
	}
}

// A dangling argument is a bug in the CALL, not in the format: slog turns it
// into !BADKEY, which is a legal — and very greppable — logfmt key. Pinned so
// that a mis-paired log call stays diagnosable instead of breaking the line.
func TestDanglingArgumentStaysParseable(t *testing.T) {
	t.Parallel()
	line := logLine(t, "m", "orphan value with spaces")
	got := pairs(t, line)
	if len(got) != 4 || got[3][0] != "!BADKEY" || got[3][1] != "orphan value with spaces" {
		t.Errorf("got %v, want !BADKEY carrying the orphan (line %q)", got, line)
	}
}

// An attribute named msg/time/level does not break the format — it appends a
// SECOND pair with that key, and the reader's duplicate rule (first non-empty
// wins) then hands consumers the record's own field. Both pairs are on the
// line, which is why the vocabulary in cli.go reserves those three names: this
// test documents the shadowing rather than blessing it.
func TestBuiltinKeyCollisionShadowsRatherThanCorrupts(t *testing.T) {
	t.Parallel()
	line := logLine(t, "the real message", "msg", "shadow")
	got := pairs(t, line)
	if len(got) != 4 || got[2] != [2]string{"msg", "the real message"} || got[3] != [2]string{"msg", "shadow"} {
		t.Fatalf("got %v, want both msg pairs in order (line %q)", got, line)
	}
	if v, ok := logfmt.Get(line, "msg"); !ok || string(v) != "the real message" {
		t.Errorf("Get(msg) = %q, want the record's own message to win", v)
	}
}

// Levels are what -log-level selects on, and an operator greps them; pin the
// spellings and that the level gate actually gates.
func TestLevelsRenderAsTheirNames(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := slog.New(NewLogfmtHandler(&buf, slog.LevelInfo))
	log.Debug("d")
	log.Info("i")
	log.Warn("w")
	log.Error("e")
	var levels []string
	for _, line := range bytes.Split(bytes.TrimSuffix(buf.Bytes(), []byte("\n")), []byte("\n")) {
		v, _ := logfmt.Get(line, "level")
		levels = append(levels, string(v))
	}
	if got := strings.Join(levels, ","); got != "INFO,WARN,ERROR" {
		t.Errorf("levels = %q, want INFO,WARN,ERROR (debug suppressed at -log-level=info)", got)
	}
}

func TestNewLoggerRejectsAnUnknownLevel(t *testing.T) {
	t.Parallel()
	if _, err := NewLogger("chatty"); err == nil {
		t.Fatal("NewLogger(chatty) = nil error, want a refusal naming the value")
	} else if !strings.Contains(err.Error(), `"chatty"`) {
		t.Errorf("error %q does not name the value the operator typed", err)
	}
	for _, lvl := range []string{"debug", "info", "warn", "error", "INFO", "WARN"} {
		if _, err := NewLogger(lvl); err != nil {
			t.Errorf("NewLogger(%q) = %v", lvl, err)
		}
	}
}

// The last hole the "one format" guarantee had: grpc-go's grpclog. Its DEFAULT
// logger writes stdlib log lines straight to os.Stderr at its default severity
// — no env var needed — so on the collector-misconfiguration path (the OTLP
// exporter's client, the ingest listeners, the trace tier's three) the stream
// carried records with no time=, no level= and no msg= at all. These tests run
// the REAL wiring, through the package global grpclog.SetLoggerV2 writes, so
// they fail if SetupLogging stops routing it.

// captureStderr swaps os.Stderr for a pipe, runs f, and returns what was
// written. SetupLogging binds os.Stderr at construction, so the swap has to
// happen before it is called — which is also why nothing here can be a
// t.Parallel test.
func captureStderr(t *testing.T, f func()) []byte {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old, oldDefault := os.Stderr, slog.Default()
	os.Stderr = w
	t.Cleanup(func() {
		os.Stderr = old
		slog.SetDefault(oldDefault)
		// klog and grpclog have no getter, so their globals cannot be restored
		// — re-point them at a logger writing to the real stderr, or every
		// later test in this binary logs into a closed pipe.
		SetGRPCLogger(oldDefault)
	})
	f()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSuffix(out, []byte("\n"))
}

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
			for _, line := range bytes.Split(out, []byte("\n")) {
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
	SetGRPCLogger(slog.New(NewLogfmtHandler(&buf, slog.LevelDebug)))
	t.Cleanup(func() { SetGRPCLogger(old) })

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
