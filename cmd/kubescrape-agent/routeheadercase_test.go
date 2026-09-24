package main

import (
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/otlpexport"
	"github.com/JohanLindvall/kubescrape/internal/agent/route"
)

// A route's headers replace the export section's per header NAME, whatever
// its case: both transports fold header names, so a route spelling
// `x-scope-orgid` beside a base `X-Scope-OrgID` used to ship one header with
// two values — gRPC sent both tenants, HTTP one at random per export. Both
// route shapes go through otlpexport.MergeHeaders; this pins that they do.
func TestRouteHeaderOverrideIsCaseInsensitive(t *testing.T) {
	exp := &otlpexport.ExportConfig{Headers: map[string]string{"X-Scope-OrgID": "default-tenant"}}
	for _, rt := range []route.Route{
		{Name: "header-only", Namespaces: []string{"t-*"}, Headers: map[string]string{"x-scope-orgid": "team-a"}},
		{Name: "own-endpoint", Namespaces: []string{"t-*"}, Endpoint: "tenant.example:4317", Headers: map[string]string{"x-scope-orgid": "team-a"}},
	} {
		got, err := routeExportConfig(exp, rt)
		if err != nil {
			t.Fatalf("%s: routeExportConfig: %v", rt.Name, err)
		}
		if len(got.Headers) != 1 || got.Headers["x-scope-orgid"] != "team-a" {
			t.Errorf("%s: headers = %v, want the route's spelling REPLACING the section's", rt.Name, got.Headers)
		}
		if err := got.Validate(); err != nil {
			t.Errorf("%s: the derived destination does not validate: %v", rt.Name, err)
		}
	}
}
