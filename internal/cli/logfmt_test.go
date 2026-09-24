package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"testing/slogtest"
	"time"

	"github.com/JohanLindvall/logfmt"
	"k8s.io/klog/v2"
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

// TestUnsafeKeysAreSanitizedRatherThanCorruptingTheRecord pins the one thing
// TextHandler alone gets wrong. Without safeKey, `"my key"=v` parses as the key
// `"my` with a bare-key value plus a second pair `key"=v`: one attribute
// silently becomes two wrong ones, and nothing downstream can tell.
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
		// Beyond ASCII, TextHandler quotes what is a Unicode space, what is not
		// printable, and invalid UTF-8 — and a quoted key comes back from the
		// reader with the quotes and escapes inside its NAME. Printable
		// non-ASCII ("nöde" above) must stay untouched.
		{"a\u00a0b", "a_b"}, // no-break space
		{"a\u2028b", "a_b"}, // line separator
		{"a\u200bb", "a_b"}, // zero-width space: not printable
		{"a\xffb", "a_b"},   // invalid UTF-8
		{"a\ufffdb", "a_b"}, // a literal replacement character, which TextHandler quotes too
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

// A group name is a key by another name — slog flattens it into the key prefix
// — so it has to be sanitized at every DEPTH and through every door, not just
// the top level of a record. Two doors were open: Handle's scan looked only at
// a record's top-level attrs, and WithAttrs looked at nothing at all (its
// ordinary keys go through ReplaceAttr, which slog documents as never being
// called for an Attr of kind Group). Both rendered a quoted key — `"ok.bad
// key.x"=v` and `"bad key.y"=w` — which the reader below then splits into two
// wrong pairs, which is precisely the corruption this whole file exists to
// prove impossible.
func TestGroupNamesAreSanitizedAtEveryDepthAndEveryDoor(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		log  func(*slog.Logger)
		want string
	}{
		{
			// A safe group name hiding an unsafe one: the door Handle's
			// top-level scan walked straight past.
			name: "nested under a safe group",
			log:  func(l *slog.Logger) { l.Info("m", slog.Group("ok", slog.Group("bad key", "x", "v"))) },
			want: "ok.bad_key.x",
		},
		{
			name: "group passed to With",
			log:  func(l *slog.Logger) { l.With(slog.Group("bad key", "y", "w")).Info("m") },
			want: "bad_key.y",
		},
		{
			name: "group nested under a safe one passed to With",
			log: func(l *slog.Logger) {
				l.With(slog.Group("ok", slog.Group("bad key", "z", "q"))).Info("m")
			},
			want: "ok.bad_key.z",
		},
		{
			// WithGroup and an inline group interleave, and the whole
			// flattened prefix must be safe.
			name: "WithGroup then a nested inline group",
			log: func(l *slog.Logger) {
				l.WithGroup("bad one").Info("m", slog.Group("ok", slog.Group("bad two", "k", "v")))
			},
			want: "bad_one.ok.bad_two.k",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			tc.log(slog.New(NewLogfmtHandler(&buf, slog.LevelDebug)))
			line := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
			got := pairs(t, line)
			// pairs validates and re-reads the line the way a consumer does,
			// so a corrupted key shows up here as an extra pair, not as a
			// funny-looking one.
			if len(got) != 4 || got[3][0] != tc.want {
				t.Errorf("got %v, want one attribute keyed %q (line %q)", got, tc.want, line)
			}
		})
	}
}

// groupValuer is a slog.LogValuer that resolves to a group — the shape klog's
// ObjectRef has, and client-go's records reach this handler through klog.
// calls counts LogValue invocations.
type groupValuer struct {
	name  string
	calls *atomic.Int32
}

func (g groupValuer) LogValue() slog.Value {
	if g.calls != nil {
		g.calls.Add(1)
	}
	return slog.GroupValue(slog.Group(g.name, slog.String("x", "1")))
}

// The fourth door: a LogValuer that resolves to a group. TextHandler resolves
// it and then skips ReplaceAttr because the result is a group, so neither the
// attr's own key nor any group name inside the resolved value was ever judged —
// `"bad top.bad key.x"=1` shipped, and the reader split it into two wrong pairs.
// Every door a LogValuer can arrive through is covered: a record's own args, a
// logger's With, nested under a safe group, and inlined under an empty key.
func TestLogValuerGroupKeysAreSanitized(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		log  func(*slog.Logger)
		want string
	}{
		{
			name: "under a safe key",
			log:  func(l *slog.Logger) { l.Info("m", slog.Any("ok", groupValuer{name: "bad key"})) },
			want: "ok.bad_key.x",
		},
		{
			name: "under an unsafe key",
			log:  func(l *slog.Logger) { l.Info("m", slog.Any("bad top", groupValuer{name: "bad key"})) },
			want: "bad_top.bad_key.x",
		},
		{
			name: "passed to With",
			log:  func(l *slog.Logger) { l.With(slog.Any("bad top", groupValuer{name: "bad key"})).Info("m") },
			want: "bad_top.bad_key.x",
		},
		{
			name: "nested under a safe group",
			log: func(l *slog.Logger) {
				l.Info("m", slog.Group("ok", slog.Any("v", groupValuer{name: "bad inner"})))
			},
			want: "ok.v.bad_inner.x",
		},
		{
			// An empty key inlines the resolved group, exactly as slog does for
			// a literal group — it does not become a group named "_".
			name: "under an empty key",
			log:  func(l *slog.Logger) { l.Info("m", slog.Any("", groupValuer{name: "bad key"})) },
			want: "bad_key.x",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			tc.log(slog.New(NewLogfmtHandler(&buf, slog.LevelDebug)))
			line := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
			got := pairs(t, line)
			if len(got) != 4 || got[3] != [2]string{tc.want, "1"} {
				t.Errorf("got %v, want one attribute %s=1 (line %q)", got, tc.want, line)
			}
		})
	}
}

// The rebuild RESOLVES a LogValuer and hands TextHandler the resolved value, so
// LogValue runs once per record — resolving it in the scan to decide whether a
// rebuild was needed would have run it twice, on every client-go record that
// carries a klog.KObj.
func TestLogValuerIsResolvedOnce(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	var buf bytes.Buffer
	log := slog.New(NewLogfmtHandler(&buf, slog.LevelDebug))
	log.Info("m", slog.Any("obj", groupValuer{name: "pod", calls: &calls}))
	if n := calls.Load(); n != 1 {
		t.Errorf("LogValue ran %d times for one record, want 1", n)
	}
	calls.Store(0)
	log.With(slog.Any("obj", groupValuer{name: "pod", calls: &calls})).Info("m")
	if n := calls.Load(); n != 1 {
		t.Errorf("LogValue ran %d times for one With and one record, want 1", n)
	}
}

// Two slog.Handler rules the sanitizer used to break by treating an empty key
// as one more unsafe key: the ZERO Attr must be ignored (it rendered `_=<nil>`,
// because TextHandler elides it only when the key is still empty after
// ReplaceAttr), and a group with an empty key must be INLINED (it rendered
// `_.a=1`). An empty key on a real value is a different thing and still becomes
// "_" — TestUnsafeKeysAreSanitizedRatherThanCorruptingTheRecord pins that.
//
// slogtest (below) covers the inline half but only checks that the key "" is
// absent, which `_=<nil>` satisfies — so the zero Attr is asserted here.
func TestEmptyKeysKeepTheirSlogMeaning(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		log  func(*slog.Logger)
		want [][2]string // after time/level/msg
	}{
		{"zero attr", func(l *slog.Logger) { l.Info("m", slog.Attr{}) }, nil},
		{"zero attr via LogAttrs", func(l *slog.Logger) {
			l.LogAttrs(context.Background(), slog.LevelInfo, "m", slog.Attr{}, slog.String("k", "v"))
		}, [][2]string{{"k", "v"}}},
		{"zero attr via With", func(l *slog.Logger) { l.With(slog.Attr{}).Info("m") }, nil},
		{"inline group", func(l *slog.Logger) { l.Info("m", slog.Group("", slog.String("a", "1"))) }, [][2]string{{"a", "1"}}},
		{"inline group via With", func(l *slog.Logger) { l.With(slog.Group("", slog.String("b", "2"))).Info("m") }, [][2]string{{"b", "2"}}},
		{"inline group holding an unsafe one", func(l *slog.Logger) {
			l.Info("m", slog.Group("", slog.Group("bad key", slog.String("c", "3"))))
		}, [][2]string{{"bad_key.c", "3"}}},
		{"empty WithGroup on the handler", func(l *slog.Logger) {
			slog.New(l.Handler().WithGroup("")).Info("m", "d", "4")
		}, [][2]string{{"d", "4"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			tc.log(slog.New(NewLogfmtHandler(&buf, slog.LevelDebug)))
			line := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
			got := pairs(t, line)
			if len(got) < 3 {
				t.Fatalf("got %v, want at least time/level/msg (line %q)", got, line)
			}
			if fmt.Sprint(got[3:]) != fmt.Sprint(tc.want) {
				t.Errorf("attributes = %v, want %v (line %q)", got[3:], tc.want, line)
			}
		})
	}
}

// The handler is a slog.Handler, so the standard library's own conformance
// suite applies to it. Each line is read back with this repo's logfmt reader and
// nested on '.', which is how slog flattens a group into a key.
func TestHandlerConformsToSlogtest(t *testing.T) {
	var buf *bytes.Buffer
	slogtest.Run(t, func(*testing.T) slog.Handler {
		buf = new(bytes.Buffer)
		return NewLogfmtHandler(buf, slog.LevelDebug)
	}, func(t *testing.T) map[string]any {
		m := map[string]any{}
		for _, kv := range pairs(t, bytes.TrimSuffix(buf.Bytes(), []byte("\n"))) {
			path := strings.Split(kv[0], ".")
			cur := m
			for _, seg := range path[:len(path)-1] {
				next, ok := cur[seg].(map[string]any)
				if !ok {
					next = map[string]any{}
					cur[seg] = next
				}
				cur = next
			}
			cur[path[len(path)-1]] = kv[1]
		}
		return m
	})
}

// The sanitizing rewrite is a COPY: slog.Handler's contract is that WithAttrs
// neither retains nor modifies the caller's slice, and the caller here is
// ordinary logging code that may well be reusing the attrs it passed.
func TestWithAttrsDoesNotRewriteTheCallersAttrs(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	as := []slog.Attr{slog.String("node", "n1"), slog.Group("bad key", slog.String("y", "w"))}
	_ = NewLogfmtHandler(&buf, slog.LevelDebug).WithAttrs(as)
	if as[1].Key != "bad key" {
		t.Errorf("WithAttrs sanitized the caller's own slice in place: %q", as[1].Key)
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
// SECOND pair with that key. This repo's logfmt reader keeps the first
// non-empty pair, so logfmt.Get hands back the record's own field; a consumer
// that builds a map from the pairs typically keeps the LAST and reads the
// attribute instead. Both pairs are on the line, which is why the vocabulary
// in cli.go reserves those three names and TestNoLogCallUsesASlogReservedKey
// refuses a call that uses one: this test documents the shadowing rather than
// blessing it.
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
	for line := range bytes.SplitSeq(bytes.TrimSuffix(buf.Bytes(), []byte("\n")), []byte("\n")) {
		v, _ := logfmt.Get(line, "level")
		levels = append(levels, string(v))
	}
	if got := strings.Join(levels, ","); got != "INFO,WARN,ERROR" {
		t.Errorf("levels = %q, want INFO,WARN,ERROR (debug suppressed at -log-level=info)", got)
	}
}

func TestUnknownLogLevelIsRejected(t *testing.T) {
	t.Parallel()
	if _, err := newLogger("chatty"); err == nil {
		t.Fatal("newLogger(chatty) = nil error, want a refusal naming the value")
	} else if !strings.Contains(err.Error(), `"chatty"`) {
		t.Errorf("error %q does not name the value the operator typed", err)
	}
	for _, lvl := range []string{"debug", "info", "warn", "error", "INFO", "WARN"} {
		if _, err := newLogger(lvl); err != nil {
			t.Errorf("newLogger(%q) = %v", lvl, err)
		}
	}
}

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
	oldLogOut, oldLogFlags := log.Writer(), log.Flags()
	// SetLogLoggerLevel has no getter either; setting it returns the old value.
	oldBridge := slog.SetLogLoggerLevel(slog.LevelInfo)
	slog.SetLogLoggerLevel(oldBridge)
	os.Stderr = w
	t.Cleanup(func() {
		os.Stderr = old
		slog.SetDefault(oldDefault)
		// SetDefault re-points the stdlib log package only for a NON-default
		// handler, so restoring the original default leaves it writing into
		// the pipe; put it back by hand, with the bridge level SetupLogging set.
		log.SetOutput(oldLogOut)
		log.SetFlags(oldLogFlags)
		slog.SetLogLoggerLevel(oldBridge)
		// klog and grpclog have no getter, so their globals cannot be restored
		// — re-point them at a logger writing to the real stderr, or every
		// later test in this binary logs into a closed pipe.
		klog.SetSlogLogger(oldDefault)
		setGRPCLogger(oldDefault)
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

// client-go logs through klog, klog through logr, and logr's slog bridge keys an
// error `err` — so the reflector's watch errors, runtime.HandleError and leader
// election rendered `err=boom`, and a grep for `error=`, which the vocabulary
// says finds every failure, missed exactly the failures of the API-server
// plumbing. This runs the REAL wiring: SetupLogging, then klog.
func TestKlogIsRoutedAsLogfmtWithTheErrorKey(t *testing.T) {
	line := captureStderr(t, func() {
		if _, err := SetupLogging("info"); err != nil {
			t.Fatal(err)
		}
		klog.ErrorS(errors.New("boom"), "watch failed", "resource", "pods")
		klog.Flush()
	})
	if len(line) == 0 {
		t.Fatal("klog wrote nothing to the routed stderr: it is not going through the process logger")
	}
	got := pairs(t, line)
	want := map[string]string{"level": "ERROR", "msg": "watch failed", "error": "boom", "resource": "pods"}
	seen := map[string]string{}
	for _, p := range got {
		if p[0] == "err" {
			t.Errorf("klog's error rendered as err= rather than the vocabulary's error=: %q", line)
		}
		seen[p[0]] = p[1]
	}
	for k, v := range want {
		if seen[k] != v {
			t.Errorf("%s = %q, want %q (line %q)", k, seen[k], v, line)
		}
	}
}

// Only a TOP-LEVEL err is the dependency's error key; "g.err" is a key nothing
// in the vocabulary claims, and renaming it would be guessing.
func TestOnlyATopLevelErrKeyIsRenamed(t *testing.T) {
	t.Parallel()
	got := pairs(t, logLine(t, "m", "err", "top", slog.Group("g", "err", "nested")))
	if got[3] != [2]string{"error", "top"} || got[4] != [2]string{"g.err", "nested"} {
		t.Errorf("got %v, want error=top then g.err=nested", got)
	}
}

// slog.SetDefault routes the stdlib log package into the process logger, at
// the bridge's default level — INFO. What arrives there is net/http's own
// reports (nothing here sets http.Server.ErrorLog): the "http: panic serving"
// stack that is the only record of a handler panic net/http recovered, a
// superfluous WriteHeader, an Accept error, a peer's unsolicited response. They
// are handled surprises, i.e. WARN — not steady-state INFO.
func TestStdlibLogLinesArriveAtWarn(t *testing.T) {
	line := captureStderr(t, func() {
		if _, err := SetupLogging("info"); err != nil {
			t.Fatal(err)
		}
		log.Printf("http: panic serving 10.0.0.1:34512: %v", "boom")
	})
	got := pairs(t, line)
	if len(got) != 3 {
		t.Fatalf("record has %d pairs, want time/level/msg: %q", len(got), line)
	}
	if got[1] != [2]string{"level", "WARN"} {
		t.Errorf("level pair = %v, want level=WARN (line %q)", got[1], line)
	}
	if got[2][0] != "msg" || !strings.Contains(got[2][1], "http: panic serving") {
		t.Errorf("message pair = %v; the stdlib message must survive intact", got[2])
	}
}
