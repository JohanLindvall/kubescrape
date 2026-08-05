package main

import (
	"strings"
	"testing"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/agent/tailbuffer"
	"github.com/JohanLindvall/kubescrape/internal/agent/tailsample"
	"github.com/JohanLindvall/kubescrape/internal/agent/tracesample"
)

// withServiceGraph turns the trace tier on for the duration of a test: the
// combination below only matters where traces are actually received, and
// startServiceGraph already says so for every other workload.
func withServiceGraph(t *testing.T) {
	t.Helper()
	old := *serviceGraphOn
	*serviceGraphOn = true
	t.Cleanup(func() { *serviceGraphOn = old })
}

func tailOnly() *tailbuffer.Config {
	return &tailbuffer.Config{Config: tailsample.Config{Policies: []tailsample.PolicyConfig{
		{Name: "all", Type: tailsample.TypeAlwaysSample},
	}}}
}

func warnText(cfg agentConfig) string { return strings.Join(configWarnings(cfg), "\n") }

// The footgun: the head sampler's guard rails are decided PER SPAN, so below a
// whole-trace tail sampler they hand it fragments. keepErrors defaults to on, so
// the plainest possible head-sampler config trips it — which is exactly why this
// warns instead of refusing, and why it must name the field.
func TestHeadSamplerGuardRailsBelowTailSamplingWarn(t *testing.T) {
	withServiceGraph(t)
	got := warnText(agentConfig{
		TailSampling:  tailOnly(),
		TraceSampling: &tracesample.Config{Probability: 0.1}, // keepErrors defaults true
	})
	if !strings.Contains(got, "keepErrors (defaulted on)") {
		t.Fatalf("the defaulted guard rail must be named: %q", got)
	}
	if !strings.Contains(got, "PER SPAN") || !strings.Contains(got, "statusCode") {
		t.Fatalf("the warning must say what is wrong and what to do instead: %q", got)
	}

	yes := true
	got = warnText(agentConfig{
		TailSampling:  tailOnly(),
		TraceSampling: &tracesample.Config{Probability: 0.1, KeepErrors: &yes, KeepSlowerThan: "2s"},
	})
	if !strings.Contains(got, "keepErrors and keepSlowerThan") {
		t.Fatalf("both explicit guard rails must be named: %q", got)
	}
}

// The SUPPORTED combination: probability nests exactly with a tail probabilistic
// policy, and maxSpansPerSecond is an overload valve. Neither is warned about,
// or the warning becomes noise an operator learns to skip past.
func TestSupportedHeadTailCombinationIsSilent(t *testing.T) {
	withServiceGraph(t)
	no := false
	if got := warnText(agentConfig{
		TailSampling: tailOnly(),
		TraceSampling: &tracesample.Config{
			Probability: 0.1, KeepErrors: &no, MaxSpansPerSecond: 5000,
		},
	}); got != "" {
		t.Fatalf("probability + maxSpansPerSecond below a tail sampler is supported and must be silent, got: %q", got)
	}
}

// Either section alone is fine.
func TestEitherSamplerAloneIsSilent(t *testing.T) {
	withServiceGraph(t)
	if got := warnText(agentConfig{TailSampling: tailOnly()}); got != "" {
		t.Fatalf("tailSampling alone warned: %q", got)
	}
	if got := warnText(agentConfig{TraceSampling: &tracesample.Config{Probability: 0.1}}); got != "" {
		t.Fatalf("traceSampling alone warned: %q", got)
	}
}

// A bound its consumer normalises rather than refuses is invisible otherwise:
// the scraper substitutes its own timeout and the tailer lifts the token bucket
// to the smallest one that can grant, so the process runs — doing something
// other than what the flags read like, with nothing saying so. -check-config
// and every start emit these from the same list.
func TestNormalisedFlagValuesWarn(t *testing.T) {
	defer func(to time.Duration, limit, burst float64) {
		*scrapeTimeout, *logsRateLimit, *logsRateBurst = to, limit, burst
	}(*scrapeTimeout, *logsRateLimit, *logsRateBurst)

	for _, tc := range []struct {
		name         string
		timeout      time.Duration
		limit, burst float64
		want         string
	}{
		{"zero scrape timeout", 0, 0, 0, "-scrape-timeout=0s"},
		{"negative scrape timeout", -time.Second, 0, 0, "-scrape-timeout=-1s"},
		{"explicit burst below one token", 15 * time.Second, 100, 0.5, "-logs-rate-burst=0.5"},
		{"derived burst below one token", 15 * time.Second, 0.2, 0, "bucket of 0.4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*scrapeTimeout, *logsRateLimit, *logsRateBurst = tc.timeout, tc.limit, tc.burst
			if got := warnText(agentConfig{}); !strings.Contains(got, tc.want) {
				t.Fatalf("warning %q does not name %s", got, tc.want)
			}
		})
	}

	// The usable shapes stay silent, or the warning becomes noise: the
	// defaults, and a rate whose derived bucket already holds a token.
	*scrapeTimeout, *logsRateLimit, *logsRateBurst = 15*time.Second, 0, 0
	if got := warnText(agentConfig{}); got != "" {
		t.Fatalf("the defaults warned: %q", got)
	}
	*logsRateLimit, *logsRateBurst = 100, 0
	if got := warnText(agentConfig{}); got != "" {
		t.Fatalf("a usable rate limit warned: %q", got)
	}
}

// Off the trace tier both sections are ignored outright (startServiceGraph says
// so once, per section), so repeating the composition warning on every node
// would be noise about config that does nothing.
func TestNoCompositionWarningOffTheTraceTier(t *testing.T) {
	old := *serviceGraphOn
	*serviceGraphOn = false
	t.Cleanup(func() { *serviceGraphOn = old })
	if got := warnText(agentConfig{
		TailSampling:  tailOnly(),
		TraceSampling: &tracesample.Config{Probability: 0.1},
	}); got != "" {
		t.Fatalf("a DaemonSet agent warned about sections it ignores: %q", got)
	}
}
