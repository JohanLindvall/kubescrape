package servicegraph

import (
	"strings"
	"testing"
)

// The dry run must judge a shard set exactly as a start does.
//
// ReshardConfig.Validate is shape-only and cannot see a DERIVED destination, so
// -check-config used to accept a section whose per-shard exporter configs
// otlpexport.New refuses — TLS material beside a plaintext gRPC hop, a
// scheme-less explicit endpoint under protocol: http — printing the ring as if
// it were fine and then aborting every pod of the tier's StatefulSet at startup.
//
// The property under test is the EQUIVALENCE with NewResharder, not the presence
// of an error: a dry run that is stricter than a start is just as wrong as one
// that is laxer, and this repo has already CrashLooped a fleet with the strict
// kind (otlpexport.ExportConfig.ValidateAgainst's own war story). Every fixture
// here is pure shape — no CA bundle is READ, since a file check is the one thing
// ValidateAgainst deliberately cannot do and would diverge on legitimately.
func TestValidateAgainstMatchesNewResharder(t *testing.T) {
	base := otlpexportConfigForTest()
	yes := true

	for _, tc := range []struct {
		name    string
		cfg     ReshardConfig
		wantErr string // substring; "" = both must accept
	}{
		{
			name: "template, plaintext gRPC in-cluster",
			cfg:  ReshardConfig{StatefulSet: "sg", Replicas: 3, Namespace: "monitoring"},
		},
		{
			// The chart's shape with a CA bundle mounted but the hop left
			// plaintext: the TLS is a no-op, which is a security control that
			// silently does nothing.
			name:    "caFile on a plaintext gRPC hop",
			cfg:     ReshardConfig{StatefulSet: "sg", Replicas: 2, Namespace: "monitoring", Insecure: &yes, CAFile: "/etc/kubescrape/ca.pem"},
			wantErr: "the gRPC connection is plaintext",
		},
		{
			name:    "explicit endpoints without a scheme under protocol: http",
			cfg:     ReshardConfig{Protocol: "http", Endpoints: []string{"sg-0.sg.monitoring.svc:4319", "sg-1.sg.monitoring.svc:4319"}},
			wantErr: "needs an http:// or https:// scheme",
		},
		{
			name: "explicit endpoints WITH a scheme under protocol: http",
			cfg:  ReshardConfig{Protocol: "http", Endpoints: []string{"http://sg-0.sg.monitoring.svc:4319"}},
		},
		{
			// A template ring addresses THIS pod too, and NewResharder builds no
			// client for it. Refusing here would fail a config that starts.
			name: "the only shard is ourselves, and its client is never built",
			cfg: ReshardConfig{StatefulSet: "sg", Replicas: 1, Namespace: "monitoring", Self: "sg-0",
				Insecure: &yes, CAFile: "/etc/kubescrape/ca.pem"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dry := tc.cfg.ValidateAgainst(base)
			r, start := NewResharder(tc.cfg, base, discardLog())
			if r != nil {
				defer func() { _ = r.Close() }()
			}
			if (dry != nil) != (start != nil) {
				t.Fatalf("the dry run and the start disagree: ValidateAgainst = %v, NewResharder = %v", dry, start)
			}
			if tc.wantErr == "" {
				if dry != nil {
					t.Fatalf("rejected a shard set a real start accepts: %v", dry)
				}
				return
			}
			if dry == nil {
				t.Fatalf("-check-config accepted a shard set that aborts the trace tier at startup (want an error naming %q)", tc.wantErr)
			}
			if !strings.Contains(dry.Error(), tc.wantErr) {
				t.Fatalf("dry-run error %q does not name the mistake (%q)", dry, tc.wantErr)
			}
			// One mistake, one sentence: the operator reading the CrashLoop must
			// be able to match it to what CI told them.
			if dry.Error() != start.Error() {
				t.Fatalf("dry run says %q, the start says %q", dry, start)
			}
			// And it names the shard, since a ring is many destinations.
			if !strings.Contains(dry.Error(), "service-graph shard ") {
				t.Fatalf("error %q does not say which shard", dry)
			}
		})
	}
}

// The dry run must not resolve a namespace: $POD_NAMESPACE is a POD fact and
// -check-config commonly runs in CI, where failing for an unresolvable namespace
// would refuse a config that starts perfectly in the pod that reads it. The
// substitute fills one DNS label and cannot change a verdict.
func TestValidateAgainstNeedsNoNamespace(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "")
	cfg := ReshardConfig{StatefulSet: "sg", Replicas: 2} // no namespace anywhere
	if err := cfg.ValidateAgainst(otlpexportConfigForTest()); err != nil {
		t.Fatalf("the dry run demanded a namespace it has no business resolving: %v", err)
	}

	// It still renders a complete destination, or the shape rules would be
	// judging an empty endpoint.
	got := cfg.targets(dryRunNamespace)
	if len(got) != 2 || !strings.Contains(got[0].endpoint, dryRunNamespace) {
		t.Fatalf("targets = %+v, want the placeholder namespace in a complete hostname", got)
	}
	// And a broken shape is still caught with the namespace unknown.
	yes := true
	cfg.Insecure, cfg.CAFile = &yes, "/etc/kubescrape/ca.pem"
	if err := cfg.ValidateAgainst(otlpexportConfigForTest()); err == nil {
		t.Fatal("an unresolved namespace made the dry run blind to a plaintext TLS contradiction")
	}
}
