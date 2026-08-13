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

// warnText joins the warnings for cfg with the process in the shipped
// manifests' shape for the FLAGS these tests do not exercise: every manifest
// renders -positions-file and -kubelet-endpoint, and each has a warning of its
// own (persistence off; kubelet scrapes that are never scheduled) that would
// otherwise land in every "this composition must be silent" assertion here.
// Both have their own tests — TestPositionsWarningCoversJournald and
// TestKubeletScrapeWithoutEndpointWarns.
func warnText(cfg agentConfig) string {
	pos, endpoint := *positionsFile, *kubeletEndpoint
	*positionsFile = "/var/lib/kubescrape/positions.json"
	*kubeletEndpoint = "https://10.0.0.1:10250"
	defer func() { *positionsFile, *kubeletEndpoint = pos, endpoint }()
	return strings.Join(configWarnings(cfg), "\n")
}

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

// The one bound still NORMALISED rather than refused: a token bucket DERIVED
// from a sub-0.5 -logs-rate-limit. The operator typed the rate and gets the
// rate — only the bucket they did not type moves — so it is named rather than
// refused, and everything TYPED is refused instead
// (TestValidateConfigRefusesTypedFlagNonsense). A silent lift would leave the
// flags reading like one thing and the process doing another.
func TestDerivedRateBurstWarns(t *testing.T) {
	defer func(limit, burst float64) {
		*logsRateLimit, *logsRateBurst = limit, burst
	}(*logsRateLimit, *logsRateBurst)

	*logsRateLimit, *logsRateBurst = 0.2, 0
	got := warnText(agentConfig{})
	for _, want := range []string{"-logs-rate-limit=0.2", "bucket of 0.4", "raises the bucket to 1"} {
		if !strings.Contains(got, want) {
			t.Fatalf("warning %q does not name %s", got, want)
		}
	}

	// Silent shapes, or the warning becomes noise an operator learns to skip:
	// rate limiting off, a rate whose derived bucket already holds a token, and
	// a bucket the operator chose (no derivation happens at all).
	for _, tc := range []struct {
		name         string
		limit, burst float64
	}{
		{"rate limiting off", 0, 0},
		{"usable derived bucket", 100, 0},
		{"explicit bucket", 0.2, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			*logsRateLimit, *logsRateBurst = tc.limit, tc.burst
			if got := warnText(agentConfig{}); got != "" {
				t.Fatalf("warned: %q", got)
			}
		})
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

// withPeerIP sets the peer-IP fallback and the self-metadata knobs it silently
// depends on, restoring all three.
func withPeerIP(t *testing.T, peerIP, selfAttrs bool, refresh time.Duration) {
	t.Helper()
	oldP, oldS, oldR := *ingestPeerIP, *selfAttrsOn, *selfAttrsRefresh
	*ingestPeerIP, *selfAttrsOn, *selfAttrsRefresh = peerIP, selfAttrs, refresh
	t.Cleanup(func() { *ingestPeerIP, *selfAttrsOn, *selfAttrsRefresh = oldP, oldS, oldR })
}

// The tier's veto on a peer-IP attribution resolving to its OWN workload reads
// the pod -self-attributes resolved. With that lookup off there is no pod, and
// peerIsOurOwnWorkload's honest "we do not know yet" becomes permanent: a
// rewritten source address labels application spans with a kubescrape pod, and
// the counter documented as the signal for that (peer_ip_rejected) stays flat
// either way, so nothing distinguishes it from success.
func TestPeerIPFallbackWithoutSelfAttributesWarns(t *testing.T) {
	withServiceGraph(t)

	for _, tc := range []struct {
		name      string
		selfAttrs bool
		refresh   time.Duration
		wantFlag  string
	}{
		{"self-attributes off", false, time.Minute, "-self-attributes=false"},
		{"refresh disables the lookup", true, 0, "-self-attributes-refresh=0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withPeerIP(t, true, tc.selfAttrs, tc.refresh)
			got := warnText(agentConfig{})
			if !strings.Contains(got, "-ingest-peer-ip-fallback") || !strings.Contains(got, tc.wantFlag) {
				t.Fatalf("warning must name both flags, got: %q", got)
			}
			if !strings.Contains(got, "peer_ip_rejected") {
				t.Fatalf("warning must name the counter that stays flat, got: %q", got)
			}
		})
	}
}

// The shapes that must stay silent: the veto works (self-attributes on with a
// positive refresh), the fallback is off, or this is not the tier at all — the
// DaemonSet's ingest receiver takes a hop from its own node and has no such
// workload to confuse itself with.
func TestPeerIPFallbackWarningIsScoped(t *testing.T) {
	withServiceGraph(t)
	withPeerIP(t, true, true, time.Minute)
	if got := warnText(agentConfig{}); got != "" {
		t.Fatalf("a working veto warned: %q", got)
	}
	withPeerIP(t, false, false, 0)
	if got := warnText(agentConfig{}); got != "" {
		t.Fatalf("no peer-IP fallback, nothing to warn about: %q", got)
	}

	old := *serviceGraphOn
	*serviceGraphOn = false
	t.Cleanup(func() { *serviceGraphOn = old })
	withPeerIP(t, true, false, 0)
	if got := warnText(agentConfig{}); got != "" {
		t.Fatalf("a DaemonSet agent warned about the tier's veto: %q", got)
	}
}

// `probability: 0` reads as "ship nothing" and ships everything: Probability is
// a plain float64, so 0 is indistinguishable from unset, Enabled() is false, the
// sampler is never wired and no line, warning or counter says so. The operator's
// only symptom is the egress bill — which is the one shape this repo refuses to
// leave silent.
func TestInertTraceSamplingSectionWarns(t *testing.T) {
	withServiceGraph(t)

	got := warnText(agentConfig{TraceSampling: &tracesample.Config{Probability: 0}})
	if !strings.Contains(got, "samples nothing") || !strings.Contains(got, "keeps EVERY trace") {
		t.Fatalf("an inert traceSampling section was not named: %q", got)
	}
	// It must say what a sampling value looks like, or the operator repeats the
	// mistake with 0.0 and then with 50.
	if !strings.Contains(got, "BETWEEN 0 and 1") {
		t.Fatalf("the warning does not say what to write instead: %q", got)
	}

	// The guard rails do not make it live: they only rescue spans a probability
	// decision dropped, and there is no such decision here.
	no := false
	if got := warnText(agentConfig{TraceSampling: &tracesample.Config{KeepErrors: &no, KeepSlowerThan: "2s"}}); !strings.Contains(got, "samples nothing") {
		t.Fatalf("a section of nothing but guard rails is still inert: %q", got)
	}

	// Silent shapes. A rate cap alone IS sampling (an overload valve), a
	// fraction is the whole point, `probability: 1` is an honest explicit
	// keep-everything, and off the tier the section is ignored wholesale — with
	// startServiceGraph saying so once, per section.
	for _, tc := range []struct {
		name string
		cfg  agentConfig
		tier bool
	}{
		{"rate cap alone", agentConfig{TraceSampling: &tracesample.Config{MaxSpansPerSecond: 5000}}, true},
		{"a real fraction", agentConfig{TraceSampling: &tracesample.Config{Probability: 0.1}}, true},
		{"an explicit keep-everything", agentConfig{TraceSampling: &tracesample.Config{Probability: 1}}, true},
		{"no section at all", agentConfig{}, true},
		{"inert, off the tier", agentConfig{TraceSampling: &tracesample.Config{}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := *serviceGraphOn
			*serviceGraphOn = tc.tier
			t.Cleanup(func() { *serviceGraphOn = old })
			if got := warnText(tc.cfg); got != "" {
				t.Fatalf("warned: %q", got)
			}
		})
	}
}
