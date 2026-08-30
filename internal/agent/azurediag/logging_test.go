package azurediag

// What the operator can SEE when the Event Hubs consumer degrades. The three
// cases that were invisible: a credential that does not work (kgo retries it
// internally and forever), data that does not decode (committed past, so it is
// gone by the time anyone looks) and a per-hub fetch error that repeats on
// every poll.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

func capturedLog() (*slog.Logger, func() string) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	return slog.New(h), buf.String
}

// An undecodable message is skipped AND COMMITTED PAST, so it is not retried
// and cannot be inspected later. The counter says how much; only the line says
// what the decoder made of it.
func TestUndecodableDataIsLoggedWithoutItsBody(t *testing.T) {
	log, dump := capturedLog()
	r := New(Config{Logger: log, Exporter: nil})
	before := obs.AzureDecodeErrors.Value()

	// A secret-looking body, to pin that nothing copies it into the log: a
	// diagnostic-settings record is customer data.
	body := []byte(`{"records":[{"category":"x","properties":{"password":"hunter2"}}`)
	// A truncated envelope may yield the elements it did parse; what matters is
	// that the SYNTAX error is counted and reported rather than swallowed.
	r.decode([][]byte{body})
	if obs.AzureDecodeErrors.Value() <= before {
		t.Error("kubescrape_azure_decode_errors_total did not move")
	}
	out := dump()
	if !strings.Contains(out, "could not be decoded") {
		t.Errorf("no warning for undecodable data:\n%s", out)
	}
	if !strings.Contains(out, "bytes=") {
		t.Errorf("the warning does not say how much was skipped:\n%s", out)
	}
	if strings.Contains(out, "hunter2") {
		t.Fatalf("the record's body reached the log:\n%s", out)
	}
}

// The same producer writes the same broken shape forever, so the line is
// throttled to the condition rather than the message.
func TestUndecodableDataWarningIsThrottled(t *testing.T) {
	log, dump := capturedLog()
	r := New(Config{Logger: log})
	for i := 0; i < 5; i++ {
		r.decode([][]byte{[]byte("not json")})
	}
	if n := strings.Count(dump(), "level=WARN"); n != 1 {
		t.Errorf("want one throttled warning, got %d:\n%s", n, dump())
	}
}

// A token that cannot be acquired makes every new Kafka connection fail SASL,
// which kgo retries internally: without this line the consumer just goes quiet.
func TestTokenFailureIsLoggedAndNeverCarriesTheToken(t *testing.T) {
	log, dump := capturedLog()
	now := time.Unix(1000, 0)
	calls := 0
	ts := &tokenSource{
		log: log, what: "imds",
		now: func() time.Time { return now },
		fetch: func(context.Context) (string, time.Duration, error) {
			calls++
			if calls == 1 {
				return "super-secret-access-token", time.Hour, nil
			}
			return "", 0, errors.New("imds: HTTP 400: identity not found")
		},
	}
	if _, err := ts.get(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Inside the refresh margin, still valid: the stale token is served, and
	// THAT is the thing to report — the pipeline is fine until the expiry.
	now = now.Add(time.Hour - time.Minute)
	if _, err := ts.get(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Past expiry: no usable token at all.
	now = now.Add(2 * time.Minute)
	if _, err := ts.get(context.Background()); err == nil {
		t.Fatal("want an error past expiry")
	}

	out := dump()
	for _, want := range []string{
		"acquired an entra token",     // the first acquisition is lifecycle
		"serving the last good token", // the fallback
		"cannot authenticate",         // no usable token
		"source=imds",                 // which protocol failed
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Contains(out, "super-secret-access-token") {
		t.Fatalf("the bearer token reached the log:\n%s", out)
	}
}

// A per-topic fetch error repeats on every poll for as long as the ACL gap
// exists. The FATAL shape is different: it rebuilds the client, which is a
// transition worth a line every time.
func TestFetchErrorWarningIsThrottledPerTopic(t *testing.T) {
	log, dump := capturedLog()
	tab := logdedupe.New(fetchWarnKeys, fetchWarnEvery)
	f := errFetch("insights-logs-audit", 0, kerr.TopicAuthorizationFailed)
	for i := 0; i < 4; i++ {
		if _, _, err := pollResult(f, log, tab); err != nil {
			t.Fatal(err)
		}
	}
	out := dump()
	if n := strings.Count(out, "event hubs fetch error"); n != 1 {
		t.Errorf("want one throttled warning per topic, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, "topic=insights-logs-audit") {
		t.Errorf("the warning does not name the hub:\n%s", out)
	}

	// A DIFFERENT topic is a different condition and gets its own line.
	if _, _, err := pollResult(errFetch("insights-logs-other", 0, kerr.TopicAuthorizationFailed), log, tab); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(dump(), "topic=insights-logs-other") {
		t.Errorf("a second failing hub was suppressed by the first:\n%s", dump())
	}
}

// A cluster-wide failure closes and rebuilds the consumer, and says so — the
// log must not make that look like the retried per-topic case.
func TestFatalFetchErrorIsLoggedAsSuch(t *testing.T) {
	log, dump := capturedLog()
	f := errFetch("", -1, kerr.SaslAuthenticationFailed)
	if _, _, err := pollResult(f, log, logdedupe.New(fetchWarnKeys, fetchWarnEvery)); err == nil {
		t.Fatal("want the poll to fail")
	}
	if !strings.Contains(dump(), "closed and rebuilt") {
		t.Errorf("the client rebuild is not reported:\n%s", dump())
	}
}

// kgo's own logging is what makes a dial or SASL failure visible at all, since
// kgo retries those internally. Warn-level records must survive at Info, and
// kgo's chatter must be asked for only when Debug is on.
func TestKgoLoggerLevels(t *testing.T) {
	log, dump := capturedLog()
	kl := kgoLoggerFor(log)
	if kl.Level() != kgo.LogLevelDebug {
		t.Errorf("Level() = %v under a Debug logger, want Debug", kl.Level())
	}
	kl.Log(kgo.LogLevelError, "unable to dial", "addr", "ns.servicebus.windows.net:9093", "err", "i/o timeout")
	out := dump()
	if !strings.Contains(out, "level=WARN") || !strings.Contains(out, "unable to dial") {
		t.Errorf("a kgo error did not reach the log as a warning:\n%s", out)
	}
	if !strings.Contains(out, "addr=ns.servicebus.windows.net:9093") {
		t.Errorf("kgo's own fields were dropped:\n%s", out)
	}

	quiet := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if lv := kgoLoggerFor(quiet).Level(); lv != kgo.LogLevelWarn {
		t.Errorf("Level() = %v under an Info logger, want Warn (kgo must not format its chatter)", lv)
	}
}

// kgo produces its Error and Warn records PER CONNECTION ATTEMPT, and it
// retries forever: "unable to initialize sasl" is one line per failed
// connection, "read from broker errored, killing connection" one per read. The
// adapter exists to make a wrong credential visible, so it must not turn it
// into a flood that buries itself — while keeping the conditions independent,
// because the dial failure looping every second is not the SASL failure that
// explains it.
func TestKgoWarningsAreThrottledPerMessage(t *testing.T) {
	log, dump := capturedLog()
	kl := kgoLoggerFor(log)

	for range 6 {
		kl.Log(kgo.LogLevelError, "unable to initialize sasl", "broker", "ns.servicebus.windows.net:9093",
			"err", "SASL authentication failed")
	}
	if n := strings.Count(dump(), "unable to initialize sasl"); n != 1 {
		t.Errorf("six identical kgo errors produced %d lines, want 1:\n%s", n, dump())
	}
	// The first line of the window still carries this attempt's own detail.
	if !strings.Contains(dump(), "broker=ns.servicebus.windows.net:9093") {
		t.Errorf("the surviving line lost kgo's fields:\n%s", dump())
	}

	// A DIFFERENT condition must not be suppressed by the first one's window.
	for range 3 {
		kl.Log(kgo.LogLevelWarn, "read from broker errored, killing connection", "err", "EOF")
	}
	if n := strings.Count(dump(), "read from broker errored"); n != 1 {
		t.Errorf("a second kgo condition produced %d lines, want 1:\n%s", n, dump())
	}
}

// The Debug arm is asked for — Level() only returns Debug when the slog level
// does — and is read during an incident, where a gap is worse than volume.
func TestKgoDebugArmIsNotThrottled(t *testing.T) {
	log, dump := capturedLog()
	kl := kgoLoggerFor(log)
	for range 4 {
		kl.Log(kgo.LogLevelInfo, "metadata update triggered", "why", "opportunistic load")
	}
	if n := strings.Count(dump(), "metadata update triggered"); n != 4 {
		t.Errorf("kgo's Debug arm was throttled: %d lines for 4 records:\n%s", n, dump())
	}
}

// Two clients in one namespace (one per credential, sources.go) must each keep
// their own gate: one hub's broker trouble silencing another's is exactly the
// shared-gate mistake logdedupe exists to prevent.
func TestKgoThrottlesArePerAdapter(t *testing.T) {
	log, dump := capturedLog()
	a, b := kgoLoggerFor(log), kgoLoggerFor(log)
	a.Log(kgo.LogLevelError, "unable to dial", "addr", "one:9093")
	b.Log(kgo.LogLevelError, "unable to dial", "addr", "two:9093")
	if n := strings.Count(dump(), "unable to dial"); n != 2 {
		t.Errorf("two clients share one throttle: %d lines, want 2:\n%s", n, dump())
	}
}
