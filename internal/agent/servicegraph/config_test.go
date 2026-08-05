package servicegraph

import (
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

// TestConfigDecodesFromYAML is the test the whole section is worth: the agent
// config is YAML decoded through sigs.k8s.io/yaml (YAML -> JSON ->
// encoding/json), which accepts only a raw nanosecond integer for a
// time.Duration. The documented `wait: 10s` / `staleAfter: 15m` spellings —
// what README, docs/CONFIGURATION.md and the chart's values all show — must
// therefore decode, and because the agent config is UnmarshalStrict'ed a
// failure here rejects the ENTIRE file: both the DaemonSet and the shard
// StatefulSet fail to start.
func TestConfigDecodesFromYAML(t *testing.T) {
	const doc = `
wait: 10s
maxItems: 10000
maxCardinality: 20000
staleAfter: 15m
histogramBuckets: [0.1, 0.2, 0.4]
exemplars: true
dimensions: [http.route]
virtualNodePeerAttributes: [peer.service, db.name, db.system]
`
	var cfg Config
	if err := yaml.UnmarshalStrict([]byte(doc), &cfg); err != nil {
		t.Fatalf("the documented serviceGraph YAML does not decode: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate rejected the documented config: %v", err)
	}
	wait, err := cfg.wait()
	if err != nil || wait != 10*time.Second {
		t.Errorf("wait = %v, %v; want 10s", wait, err)
	}
	stale, err := cfg.staleAfter()
	if err != nil || stale != 15*time.Minute {
		t.Errorf("staleAfter = %v, %v; want 15m", stale, err)
	}
}

// The two duration fields' full contract: empty takes the default, "0" DISABLES
// staleAfter (three doc sites say so, and the Registry has a <= 0 branch that is
// dead code otherwise), an unparseable value is a Validate error naming the
// field and the value, and a negative one is refused.
func TestConfigDurationSemantics(t *testing.T) {
	if got, err := (Config{}).wait(); err != nil || got != DefaultWait {
		t.Errorf("empty wait = %v, %v; want %v", got, err, DefaultWait)
	}
	if got, err := (Config{}).staleAfter(); err != nil || got != DefaultStaleAfter {
		t.Errorf("empty staleAfter = %v, %v; want %v", got, err, DefaultStaleAfter)
	}
	if got, err := (Config{StaleAfter: "0"}).staleAfter(); err != nil || got != 0 {
		t.Errorf(`staleAfter "0" = %v, %v; want 0 (eviction disabled)`, got, err)
	}
	if got, err := (Config{StaleAfter: "0s"}).staleAfter(); err != nil || got != 0 {
		t.Errorf(`staleAfter "0s" = %v, %v; want 0 (eviction disabled)`, got, err)
	}
	// A zero wait is not a pairing window at all (every half-edge would expire
	// before its partner could arrive) — and it is REFUSED (config.Positive),
	// not silently read as the default: an explicit "0" substituting 10s with
	// no diagnostic was a third zero-policy beside the config taxonomy's two.
	// The default still rides along so the constructor can fall back; Validate
	// is what reports it.
	if got, err := (Config{Wait: "0"}).wait(); err == nil || got != DefaultWait {
		t.Errorf(`wait "0" = %v, %v; want an error carrying the default %v`, got, err, DefaultWait)
	}

	for _, tc := range []struct {
		name string
		cfg  Config
		want []string
	}{
		{"unparseable wait", Config{Wait: "ten seconds"}, []string{"wait", "ten seconds"}},
		{"unparseable staleAfter", Config{StaleAfter: "quarter hour"}, []string{"staleAfter", "quarter hour"}},
		{"negative wait", Config{Wait: "-1s"}, []string{"wait"}},
		{"negative staleAfter", Config{StaleAfter: "-1m"}, []string{"staleAfter"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %+v", tc.cfg)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not name %q", err, w)
				}
			}
		})
	}

	// An unparseable value never refuses to aggregate — Validate reports it and
	// the constructors fall back to the default, exactly as spanmetrics does.
	if got := NewProcessor(Config{Wait: "nonsense"}, discardLog()).Wait(); got != DefaultWait {
		t.Errorf("a bad wait fell back to %v, want %v", got, DefaultWait)
	}
	if got := NewRegistry(Config{StaleAfter: "nonsense"}).store.StaleAfter(); got != DefaultStaleAfter {
		t.Errorf("a bad staleAfter fell back to %v, want %v", got, DefaultStaleAfter)
	}
}

// staleAfter: "0" must genuinely reach the Registry's disable branch — the
// branch existed from the start and was unreachable, because the old
// time.Duration field mapped 0 to the 15m default in withDefaults.
func TestStaleAfterZeroDisablesEviction(t *testing.T) {
	r := NewRegistry(Config{StaleAfter: "0"})
	if got := r.store.StaleAfter(); got != 0 {
		t.Fatalf("staleAfter = %v, want 0", got)
	}
	now := t0
	fixedClock(r, &now)
	r.Record(edge("checkout", "orders"))

	exp := &capExporter{}
	export(t, r, exp) // renders, then marks the values delivered
	// Far past any plausible staleAfter, with the values delivered: eviction
	// would fire here if it were enabled.
	now = now.Add(24 * time.Hour)
	export(t, r, exp)
	if r.store.Len() != 1 {
		t.Fatalf("series = %d after 24h with staleAfter disabled, want 1", r.store.Len())
	}
}
