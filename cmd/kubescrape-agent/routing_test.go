package main

import (
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
)

// A route with no endpoint inherits the flag base — which is not a
// destination at all when every signal is overridden in export:, since
// BuildExporter never builds the default chain. Inheriting it there sent a
// tenant's telemetry to whatever the endpoint flag happened to default to.
func TestRouteWithoutEndpointRejectedWhenTheBaseIsUnused(t *testing.T) {
	full := &otlpexport.ExportConfig{
		Logs:    &otlpexport.ExportOverride{Endpoint: "https://loki:443", Protocol: "http"},
		Metrics: &otlpexport.ExportOverride{Endpoint: "https://mimir:443", Protocol: "http"},
		Traces:  &otlpexport.ExportOverride{Endpoint: "https://tempo:443", Protocol: "http"},
	}
	headerOnly := []route.Route{{Name: "tenant-a", Namespaces: []string{"a-*"}, Headers: map[string]string{"X-Scope-OrgID": "a"}}}

	cfg := agentConfig{Export: full, Routing: &route.Config{Routes: headerOnly}}
	if err := validateConfig(cfg, ""); err == nil {
		t.Fatal("accepted a header-only route inheriting a base the deployment never dials")
	}

	// With the default chain in play, inheriting the base is the point.
	partial := &otlpexport.ExportConfig{Logs: full.Logs}
	cfg = agentConfig{Export: partial, Routing: &route.Config{Routes: headerOnly}}
	if err := validateConfig(cfg, ""); err != nil {
		t.Fatalf("rejected a header-only route where the base IS the fallback destination: %v", err)
	}

	// All three signals overridden, but one override sets only HEADERS — it
	// inherits the base endpoint through signalConfig(), so the base is still a
	// destination and an endpoint-less route is legitimate. Testing struct
	// presence instead of endpoints failed this config at startup.
	inherits := &otlpexport.ExportConfig{
		Logs:    full.Logs,
		Metrics: full.Metrics,
		Traces:  &otlpexport.ExportOverride{Headers: map[string]string{"X-Scope-OrgID": "traces"}},
	}
	cfg = agentConfig{Export: inherits, Routing: &route.Config{Routes: headerOnly}}
	if err := validateConfig(cfg, ""); err != nil {
		t.Fatalf("rejected a route inheriting a base that a header-only override still dials: %v", err)
	}
}

// routeExportConfig's own-endpoint arm now builds on
// otlpexport.Config.TransportOnly (through otlpexport's one destination
// derivation, RouteConfig) instead of hand-dropping each credential.
// The rework must be BIT-IDENTICAL to the old derivation — this test IS that
// old derivation, compared field-for-field against the new one for every
// shape the old code distinguished, with TWO deliberate divergences spelled
// here: an UNSET route `insecure` (nil) keeps the merged base's value instead
// of a bool's zero, because every route written before the field existed
// dialled plaintext through -otlp-insecure and must keep doing so across the
// upgrade; and an own-endpoint route no longer inherits
// -otlp-tls-insecure-skip-verify — that carryover sent the route's own
// credentials to an unverified peer, and the route opts in with its own
// insecureSkipVerify instead (TestRouteOwnEndpointDoesNotInheritSkipVerify).
// A third shape is deliberately NOT here: a route REPEATING the base endpoint
// is the base destination now, credentials included
// (TestRouteRepeatingTheBaseEndpointIsTheBaseDestination), so no case below
// names base-collector:4317.
// The base is constructed with EVERY destination-scoped flag and
// section field set, so a field the new derivation drops (or newly inherits)
// cannot hide behind a zero value.
func TestRouteExportConfigMatchesTheOldDerivation(t *testing.T) {
	oldDerivation := func(exp *otlpexport.ExportConfig, rt route.Route) otlpexport.Config {
		rcfg := exp.ApplyBase(baseExportConfig())
		if len(rt.Headers) > 0 {
			merged := make(map[string]string, len(rcfg.Headers)+len(rt.Headers))
			maps.Copy(merged, rcfg.Headers)
			maps.Copy(merged, rt.Headers)
			rcfg.Headers = merged
		}
		if rt.Endpoint != "" {
			rcfg.Endpoint = rt.Endpoint
			rcfg.BearerTokenFile = rt.BearerTokenFile
			rcfg.ClientCertFile = rt.ClientCertFile
			rcfg.ClientKeyFile = rt.ClientKeyFile
			rcfg.CAFile = rt.CAFile
			if rt.Insecure != nil {
				rcfg.Insecure = *rt.Insecure
			}
			// The second divergence: the flag's skip-verify is not carried.
			rcfg.InsecureSkipVerify = false
			if rt.InsecureSkipVerify != nil {
				rcfg.InsecureSkipVerify = *rt.InsecureSkipVerify
			}
		}
		return rcfg
	}

	// A base with every destination-scoped field populated: bearer, CA,
	// skip-verify from the flags; headers and the mTLS client pair from the
	// export section.
	oldEP, oldBearer, oldCA, oldSkip := *otlpEndpoint, *otlpBearer, *otlpCAFile, *otlpSkipTLS
	defer func() { *otlpEndpoint, *otlpBearer, *otlpCAFile, *otlpSkipTLS = oldEP, oldBearer, oldCA, oldSkip }()
	*otlpEndpoint = "base-collector:4317"
	*otlpBearer = "/base/bearer-token"
	*otlpCAFile = "/base/ca.pem"
	*otlpSkipTLS = true

	exp := &otlpexport.ExportConfig{
		Headers:        map[string]string{"X-Scope-OrgID": "base", "X-Base": "1"},
		ClientCertFile: "/base/client.pem",
		ClientKeyFile:  "/base/client-key.pem",
	}

	insecureTrue, insecureFalse := true, false
	for _, tc := range []struct {
		name string
		rt   route.Route
	}{
		{"base-only route", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
		}},
		{"own-endpoint route with credentials", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
			Endpoint:        "https://tenant.example.com:443",
			BearerTokenFile: "/route/bearer-token",
			ClientCertFile:  "/route/client.pem",
			ClientKeyFile:   "/route/client-key.pem",
			CAFile:          "/route/ca.pem",
			Insecure:        &insecureTrue,
		}},
		{"own-endpoint route with an explicit insecure:false", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
			Endpoint: "tenant.example.com:4317",
			Insecure: &insecureFalse,
		}},
		{"own-endpoint route opting into skip-verify", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
			Endpoint:           "https://tenant.example.com:443",
			InsecureSkipVerify: &insecureTrue,
		}},
		{"own-endpoint route without credentials", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
			Endpoint: "https://tenant.example.com:443",
		}},
		{"headers merge onto an own-endpoint route", route.Route{
			Name: "t", Namespaces: []string{"t-*"},
			Endpoint: "https://tenant.example.com:443",
			Headers:  map[string]string{"X-Scope-OrgID": "route", "X-Route": "1"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := routeExportConfig(exp, tc.rt)
			if err != nil {
				t.Fatalf("routeExportConfig: %v", err)
			}
			if want := oldDerivation(exp, tc.rt); !reflect.DeepEqual(got, want) {
				t.Errorf("derivation drifted from the old one:\n got: %+v\nwant: %+v", got, want)
			}
		})
	}
}

// A route config written before the `insecure` field existed reached its
// plaintext in-cluster collector through the flag base's -otlp-insecure
// (default true). The field's introduction must not flip those routes to TLS
// on upgrade — the exports fail as endless transient Unavailable, which
// -check-config cannot see — so unset (nil) inherits the merged base and an
// explicit value wins in either direction.
func TestRouteInsecureUnsetInheritsTheFlagBase(t *testing.T) {
	rt := route.Route{Name: "t", Namespaces: []string{"t-*"}, Endpoint: "collector.team-a:4317"}

	got, err := routeExportConfig(nil, rt)
	if err != nil {
		t.Fatalf("routeExportConfig: %v", err)
	}
	if !got.Insecure {
		t.Fatal("an insecure-less own-endpoint route flipped to TLS: pre-field configs dial plaintext through the base")
	}

	// An explicit false is the operator choosing TLS, base notwithstanding.
	no := false
	rt.Insecure = &no
	if got, err = routeExportConfig(nil, rt); err != nil || got.Insecure {
		t.Fatalf("insecure:false must mean TLS whatever the base (insecure=%v, err=%v)", got.Insecure, err)
	}

	// And with a TLS base, unset still inherits while explicit true wins.
	oldInsecure := *otlpInsecure
	defer func() { *otlpInsecure = oldInsecure }()
	*otlpInsecure = false
	rt.Insecure = nil
	if got, err = routeExportConfig(nil, rt); err != nil || got.Insecure {
		t.Fatalf("unset insecure must inherit a TLS base (insecure=%v, err=%v)", got.Insecure, err)
	}
	yes := true
	rt.Insecure = &yes
	if got, err = routeExportConfig(nil, rt); err != nil || !got.Insecure {
		t.Fatalf("insecure:true must mean plaintext whatever the base (insecure=%v, err=%v)", got.Insecure, err)
	}
}

// An own-endpoint route does not inherit -otlp-tls-insecure-skip-verify. The
// flag is a trust decision about the deployment's OWN collector; carrying it to
// a route that names a different host sent the route's bearer token and client
// certificate to a peer whose certificate was never checked — the carryover the
// export section's per-signal overrides already drop. The route opts in with
// its own field, an endpoint-less route (the default destination) still
// inherits it, and the drop is SAID, by -check-config and every start alike.
func TestRouteOwnEndpointDoesNotInheritSkipVerify(t *testing.T) {
	oldSkip := *otlpSkipTLS
	defer func() { *otlpSkipTLS = oldSkip }()
	*otlpSkipTLS = true

	rt := route.Route{
		Name: "saas", Namespaces: []string{"t-*"},
		Endpoint: "saas.example:443", BearerTokenFile: "/tok",
	}
	got, err := routeExportConfig(nil, rt)
	if err != nil {
		t.Fatalf("routeExportConfig: %v", err)
	}
	if got.InsecureSkipVerify {
		t.Fatal("an own-endpoint route inherited -otlp-tls-insecure-skip-verify: its own bearer token goes to an unverified peer")
	}
	if got.BearerTokenFile != "/tok" {
		t.Fatalf("the route's own credential was lost: %+v", got)
	}
	if w := warnText(agentConfig{Routing: &route.Config{Routes: []route.Route{rt}}}); !strings.Contains(w, `routing route "saas"`) ||
		!strings.Contains(w, "-otlp-tls-insecure-skip-verify") {
		t.Fatalf("the dropped skip-verify must be reported by configWarnings, got: %q", w)
	}

	yes := true
	rt.InsecureSkipVerify = &yes
	if got, err = routeExportConfig(nil, rt); err != nil || !got.InsecureSkipVerify {
		t.Fatalf("an explicit route insecureSkipVerify:true must apply (skip=%v, err=%v)", got.InsecureSkipVerify, err)
	}
	if w := warnText(agentConfig{Routing: &route.Config{Routes: []route.Route{rt}}}); strings.Contains(w, "-otlp-tls-insecure-skip-verify") {
		t.Fatalf("a route that set its own insecureSkipVerify was still reported as dropping the flag: %q", w)
	}

	// The default destination reached with extra headers still inherits it.
	headerOnly := route.Route{Name: "hdr", Namespaces: []string{"t-*"}, Headers: map[string]string{"X-Scope-OrgID": "t"}}
	if got, err = routeExportConfig(nil, headerOnly); err != nil || !got.InsecureSkipVerify {
		t.Fatalf("an endpoint-less route must inherit the flag (skip=%v, err=%v)", got.InsecureSkipVerify, err)
	}
}

// A route whose endpoint REPEATS -otlp-endpoint names the base destination, so
// it keeps the base's credentials — the rule an export.<signal> override
// repeating the base follows (otlpexport's one derivation, RouteConfig). The
// own-endpoint rebuild used to fire on any non-empty endpoint, so such a route
// silently lost the collector's bearer token and CA on the very host they were
// issued for, and configWarnings reported a "drop" that protected nothing. The
// route's own fields still win where it sets them.
func TestRouteRepeatingTheBaseEndpointIsTheBaseDestination(t *testing.T) {
	oldEP, oldBearer, oldCA, oldSkip := *otlpEndpoint, *otlpBearer, *otlpCAFile, *otlpSkipTLS
	defer func() { *otlpEndpoint, *otlpBearer, *otlpCAFile, *otlpSkipTLS = oldEP, oldBearer, oldCA, oldSkip }()
	*otlpEndpoint = "base-collector:4317"
	*otlpBearer = "/base/bearer-token"
	*otlpCAFile = "/base/ca.pem"
	*otlpSkipTLS = true
	exp := &otlpexport.ExportConfig{ClientCertFile: "/base/client.pem", ClientKeyFile: "/base/client-key.pem"}

	rt := route.Route{
		Name: "tenant-b", Namespaces: []string{"b-*"},
		Endpoint: "base-collector:4317",
		Headers:  map[string]string{"X-Scope-OrgID": "b"},
	}
	got, err := routeExportConfig(exp, rt)
	if err != nil {
		t.Fatalf("routeExportConfig: %v", err)
	}
	want := exp.ApplyBase(baseExportConfig())
	want.Headers = map[string]string{"X-Scope-OrgID": "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("a route repeating the base endpoint is not the base destination:\n got: %+v\nwant: %+v", got, want)
	}
	if w := warnText(agentConfig{Export: exp, Routing: &route.Config{Routes: []route.Route{rt}}}); strings.Contains(w, `routing route "tenant-b"`) {
		t.Fatalf("nothing is dropped for a route repeating the base endpoint, yet it was warned about: %q", w)
	}

	// The route's own fields still win.
	no := false
	rt.BearerTokenFile, rt.CAFile, rt.InsecureSkipVerify = "/route/token", "/route/ca.pem", &no
	if got, err = routeExportConfig(exp, rt); err != nil {
		t.Fatalf("routeExportConfig: %v", err)
	}
	if got.BearerTokenFile != "/route/token" || got.CAFile != "/route/ca.pem" || got.InsecureSkipVerify {
		t.Fatalf("the route's own fields must win over the base: %+v", got)
	}
	if got.ClientCertFile != "/base/client.pem" {
		t.Fatalf("an unset route client pair must keep the base destination's: %+v", got)
	}
}

// Three export overrides that merely REPEAT -otlp-endpoint all dial the flag
// base, so the base IS a destination and an endpoint-less route inheriting it is
// legitimate. A test of endpoint presence alone (rather than
// ExportConfig.BaseEndpointUnused's OwnEndpoint) refused this config at startup
// ("nothing dials -otlp-endpoint") although every per-signal client dialled
// exactly that.
func TestRouteWithoutEndpointAcceptedWhenOverridesRepeatTheBase(t *testing.T) {
	oldEP := *otlpEndpoint
	defer func() { *otlpEndpoint = oldEP }()
	*otlpEndpoint = "base-collector:4317"
	repeat := &otlpexport.ExportConfig{
		Logs:    &otlpexport.ExportOverride{Endpoint: "base-collector:4317", Headers: map[string]string{"X-Signal": "logs"}},
		Metrics: &otlpexport.ExportOverride{Endpoint: "base-collector:4317", Headers: map[string]string{"X-Signal": "metrics"}},
		Traces:  &otlpexport.ExportOverride{Endpoint: "base-collector:4317", Headers: map[string]string{"X-Signal": "traces"}},
	}
	headerOnly := []route.Route{{Name: "tenant-a", Namespaces: []string{"a-*"}, Headers: map[string]string{"X-Scope-OrgID": "a"}}}
	if err := validateConfig(agentConfig{Export: repeat, Routing: &route.Config{Routes: headerOnly}}, ""); err != nil {
		t.Fatalf("refused an endpoint-less route although every override dials the base: %v", err)
	}
}

// An endpoint-less route IS the default destination, and the fields it sets win
// there exactly as an export.<signal> override without an endpoint applies its
// own: one derivation (otlpexport's RouteConfig) serves both. The route side
// used to be a second copy that ignored an endpoint-less route's credentials
// altogether — so `endpoint: <-otlp-endpoint>` plus a bearerTokenFile presented
// the route's token while the same route with the endpoint left out presented
// the collector's, for one destination.
func TestEndpointlessRouteAppliesItsOwnCredentials(t *testing.T) {
	oldEP, oldBearer, oldCA, oldSkip := *otlpEndpoint, *otlpBearer, *otlpCAFile, *otlpSkipTLS
	defer func() { *otlpEndpoint, *otlpBearer, *otlpCAFile, *otlpSkipTLS = oldEP, oldBearer, oldCA, oldSkip }()
	*otlpEndpoint = "base-collector:4317"
	*otlpBearer = "/base/bearer-token"
	*otlpCAFile = "/base/ca.pem"
	*otlpSkipTLS = true

	no := false
	endpointless := route.Route{
		Name: "tenant-c", Namespaces: []string{"c-*"},
		Headers:         map[string]string{"X-Scope-OrgID": "c"},
		BearerTokenFile: "/route/token", CAFile: "/route/ca.pem", InsecureSkipVerify: &no,
	}
	repeating := endpointless
	repeating.Endpoint = *otlpEndpoint
	for _, rt := range []route.Route{endpointless, repeating} {
		got, err := routeExportConfig(nil, rt)
		if err != nil {
			t.Fatalf("routeExportConfig(endpoint %q): %v", rt.Endpoint, err)
		}
		if got.Endpoint != "base-collector:4317" {
			t.Errorf("endpoint %q: the default destination is the base's, got %q", rt.Endpoint, got.Endpoint)
		}
		if got.BearerTokenFile != "/route/token" || got.CAFile != "/route/ca.pem" || got.InsecureSkipVerify {
			t.Errorf("endpoint %q: the route's own credentials must win over the base it reaches: %+v", rt.Endpoint, got)
		}
	}
}

// A HALF client pair on a route is refused by name wherever the route points.
// The route's derivation used to take its pair only when the certificate was
// set, so a lone key on a route reaching the default destination was dropped
// silently — the route validated, and the base's pair (or none) was presented
// in its place. The flag base here carries no TLS material of its own, so the
// only thing that can refuse these routes is the pair.
func TestRouteWithHalfAClientPairIsRefused(t *testing.T) {
	oldEP := *otlpEndpoint
	defer func() { *otlpEndpoint = oldEP }()
	*otlpEndpoint = "base-collector:4317"
	no := false
	for _, rt := range []route.Route{
		{Name: "key-only", Namespaces: []string{"k-*"}, ClientKeyFile: "/route/client-key.pem", Insecure: &no},
		{Name: "key-only-repeat", Namespaces: []string{"k-*"}, Endpoint: "base-collector:4317", ClientKeyFile: "/route/client-key.pem", Insecure: &no},
		{Name: "key-only-own", Namespaces: []string{"k-*"}, Endpoint: "tenant.example.com:4317", ClientKeyFile: "/route/client-key.pem", Insecure: &no},
	} {
		_, err := validateRoutes(nil, []route.Route{rt})
		if err == nil || !strings.Contains(err.Error(), `"`+rt.Name+`"`) || !strings.Contains(err.Error(), "clientCertFile and clientKeyFile") {
			t.Errorf("route %q carries half a client pair and was not refused for it by name: %v", rt.Name, err)
		}
	}
}
