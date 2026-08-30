package azurediag

// franz-go's own logging, routed through slog.
//
// WHY THIS EXISTS. kgo retries connection, TLS and SASL failures internally and
// forever: PollFetches simply keeps blocking, so a wrong password, an expired
// federated token, a network policy that blocks 9093 or a namespace host that
// does not resolve produce NOTHING — no fetch error, no poll return, no
// counter. The readiness gate stays closed (which is the alarm), but nothing
// anywhere says WHY, and the first live run of this pipeline is precisely when
// the answer is "the credential is wrong". Without a logger kgo discards every
// one of those observations.
//
// LEVELS. kgo is chatty by design: it logs metadata refreshes, connection
// opens and group rebalances at Info and Debug. Info here would break the
// repo's "Info stays quiet in steady state" rule outright, so kgo's Error and
// Warn become slog Warn — the consumer keeps running, kgo's retry IS the
// handling, so nothing here is an Error in this repo's sense — and kgo's Info
// and Debug become slog Debug, which is where "why did it do that" belongs.
//
// SECRETS. kgo logs broker addresses, mechanism names, error strings and
// timings; it does not log SASL credentials (audited against franz-go v1.21.6:
// the SASL path logs the mechanism NAME, the broker, an "authenticate" bool and
// a step number, never the client write that carries the password or the
// bearer token). What this adapter must never do is
// widen that: the fields are forwarded verbatim, so nothing constructs a value
// out of the connection string or the bearer token, and `Level()` is what keeps
// the volume down rather than any filtering that could accidentally pass one on.

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
)

// THROTTLING. kgo's Error and Warn are per CONNECTION ATTEMPT on exactly the
// failures this adapter exists to surface: "unable to initialize sasl" once per
// failed connection, "read from broker errored, killing connection" once per
// read. A wrong password is therefore not one line, it is a line per retry for
// as long as the pipeline is deployed — the flood buries itself, and the
// singleton Deployment's whole log becomes it.
//
// A keyed Table rather than a keyless Throttle, because kgo's message TEXT is a
// fixed string per condition and the conditions are independent: a dial failure
// looping every second must not suppress the SASL failure that explains it. The
// varying half (broker address, error string, retry number) rides the
// attributes, so it is the message that keys the gate and the first line of
// each window still carries this attempt's own detail.
const (
	kgoWarnWindow = time.Minute
	// kgo has on the order of a hundred distinct log strings; the cap is here
	// so a future one cannot grow the table without bound, not because it is
	// expected to bind.
	kgoWarnKeys = 64
)

// kgoLogger adapts kgo.Logger onto a slog.Logger.
type kgoLogger struct {
	log *slog.Logger
	// warns gates the Warn arm per kgo message. Per ADAPTER, so a namespace
	// consuming several hubs through several clients does not have one client's
	// broker trouble silence another's (sources.go builds one Reader, and one
	// kgo client, per credential).
	warns *logdedupe.Table
}

// Level tells kgo how much to produce. It is asked per record, so honouring the
// slog level here is what makes kgo's Debug chatter cost nothing when Debug is
// off — kgo formats nothing it is not going to hand over.
func (k kgoLogger) Level() kgo.LogLevel {
	if k.log.Enabled(context.Background(), slog.LevelDebug) {
		return kgo.LogLevelDebug
	}
	return kgo.LogLevelWarn
}

// Log forwards one kgo record. kgo passes alternating key/value pairs, exactly
// slog's own shape, so they are handed over untouched — a key kgo spells its
// own way is still greppable, and the shared handler sanitizes anything that
// would break logfmt.
func (k kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	lvl := slog.LevelDebug
	if level == kgo.LogLevelError || level == kgo.LogLevelWarn {
		lvl = slog.LevelWarn
		allow, saturated := k.warns.Allow(msg)
		if saturated {
			// Once, ever: the list an operator is reading is truncated. The
			// package's rule is that saturation suppresses and never clears.
			k.log.Warn("event hubs client: too many distinct client warnings; further NEW ones are suppressed",
				"keys", kgoWarnKeys, "interval", kgoWarnWindow)
		}
		if !allow {
			return
		}
	}
	// The Debug arm is deliberately NOT throttled: it is asked for (Level()
	// only returns Debug when the slog level does), it is read during an
	// incident, and a gap in it is worse than volume.
	//
	// The kgo level rides as an attribute rather than being folded away: its
	// Error and Warn both land on slog Warn (see the file comment), and an
	// operator reading a broker problem wants to know which kgo called it.
	args := append([]any{"kgoLevel", level.String()}, keyvals...)
	k.log.Log(context.Background(), lvl, "event hubs client: "+msg, args...)
}

// kgoLoggerFor builds the adapter, defaulting a nil logger.
func kgoLoggerFor(log *slog.Logger) kgo.Logger {
	if log == nil {
		log = slog.Default()
	}
	return kgoLogger{log: log, warns: logdedupe.New(kgoWarnKeys, kgoWarnWindow)}
}

// describeMechanism names an auth mechanism for a startup line WITHOUT touching
// its material: kgo's own Name() is the mechanism string ("PLAIN",
// "OAUTHBEARER"), which is the whole of what may be said about a credential.
func describeMechanism(cfg *KafkaConfig) string {
	if cfg.Mechanism == nil {
		return "none"
	}
	return cfg.Mechanism.Name()
}
