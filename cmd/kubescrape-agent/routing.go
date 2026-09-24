package main

// The export destinations the routing and export sections derive: one
// derivation, run once by compileConfig and consumed by the real start, so a
// config the dry run accepts is a config that starts.

import (
	"fmt"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
	"github.com/JohanLindvall/kubescrape/internal/config"
)

// validateRoutes checks the whole routing section and derives every route's
// export config, in route order. Called ONCE, by compileConfig: the dry run
// takes its verdict and run()'s route loop dials the configs it returned
// (compiledConfig.routes), so a new refusal cannot land in one and not the
// other.
//
// Each route is validated like any other destination (Config.Validate) — the
// dry run used to check the name, the namespaces and the patterns and stop, so
// a scheme-less route endpoint, or TLS material inherited onto a plaintext
// route, passed -check-config and CrashLooped the agent on start.
//
// A repeated NAME is refused: route("name") resolves to the FIRST route with
// that name, and every per-route series (kubescrape_routed_payload_parts_total
// and kubescrape_routed_failures_total, both labelled route) and every
// failure line is keyed by it, so two destinations sharing a name could not be
// told apart anywhere an operator looks — and the second could never be
// chosen by a script at all.
func validateRoutes(exp *otlpexport.ExportConfig, routes []route.Route) ([]otlpexport.Config, error) {
	cfgs := make([]otlpexport.Config, 0, len(routes))
	seen := make(map[string]int, len(routes))
	for i, rt := range routes {
		rcfg, err := validateRoute(exp, i, rt)
		if err != nil {
			return nil, err
		}
		if j, dup := seen[rt.Name]; dup {
			return nil, fmt.Errorf("routing route %d: name %q is already used by route %d", i, rt.Name, j)
		}
		seen[rt.Name] = i
		if err := rcfg.Validate(); err != nil {
			return nil, fmt.Errorf("routing route %q: %w", rt.Name, err)
		}
		cfgs = append(cfgs, rcfg)
	}
	return cfgs, nil
}

// validateRoute checks one routing route's shape — name and namespaces
// present, every namespace pattern parseable — and derives its export config
// through the shared derivation. Reached only through validateRoutes; i is the
// route's index, used only when it has no name to report.
//
// The pattern check fails startup because a malformed glob reads as silent
// no-match at runtime — the route never fires and its tenant's telemetry goes
// to the default destination, indistinguishable from "no traffic yet"
// (config.Glob carries the full rationale).
func validateRoute(exp *otlpexport.ExportConfig, i int, rt route.Route) (otlpexport.Config, error) {
	if rt.Name == "" || len(rt.Namespaces) == 0 {
		return otlpexport.Config{}, fmt.Errorf("routing route %d: name and namespaces are required", i)
	}
	for _, pat := range rt.Namespaces {
		if err := config.Glob(pat); err != nil {
			return otlpexport.Config{}, fmt.Errorf("routing route %q: invalid namespace pattern %q: %w", rt.Name, pat, err)
		}
	}
	// The SAME derivation on both paths, so a config the dry run accepts is a
	// config that starts.
	return routeExportConfig(exp, rt)
}

// routeExportConfig derives one route destination's client config: the flag
// base, plus the export section's base additions (headers, client cert), plus
// the route's own endpoint, headers and credentials (which win per key).
//
// ONE derivation, shared by validateConfig and the real start, for the same
// reason validateConfig itself is shared — a dry run that builds something
// else proves nothing about what will start. And the derivation itself is
// otlpexport's ExportConfig.RouteConfig, the one an export.<signal> override
// goes through too: this used to be a second copy, and the two drifted on
// which endpoint counts as another host, on whether skip-verify crossed to it
// and on what an endpoint-less route did with its own credentials. What a
// route does and does not inherit is argued there.
//
// A route with no endpoint of its own inherits the flag base, and that is an
// ERROR when the base is not a destination this deployment uses: with all
// three signals overridden in export:, BuildExporter never constructs the
// default chain, so the flag endpoint is whatever it happened to default to
// (the stock otel-collector.monitoring address). Inheriting it silently sent
// a tenant's telemetry to a collector nobody configured.
func routeExportConfig(exp *otlpexport.ExportConfig, rt route.Route) (otlpexport.Config, error) {
	flagBase := baseExportConfig()
	if rt.Endpoint == "" && exp.BaseEndpointUnused(flagBase) {
		return otlpexport.Config{}, fmt.Errorf("routing route %q: no endpoint, and the flag base is not a destination here (export: gives every signal its own endpoint, so nothing dials -otlp-endpoint) — give the route its own endpoint", rt.Name)
	}
	o := rt.ExportOverride()
	return exp.RouteConfig(&o, flagBase), nil
}
