package servicemonitors

import (
	"encoding/json"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
)

// The package doc calls ignoredFields + specLimits.ignored the AUTHORITATIVE
// list of what kubescrape does not interpret. That claim is only worth
// something if it is enforced, and it cannot be: a field that is not PARSED
// cannot be reported, so ten real CRD fields (scrapeProtocols, the
// nativeHistogram pair, the three ProxyConfig siblings of proxyUrl, …) were
// silent — set on a monitor, dropped without a word and without a
// kubescrape_monitor_fields_ignored_total bump.
//
// What CAN be enforced is the half that lives in this package: every field the
// spec shapes parse is either INTERPRETED or REPORTED. A new field added to
// endpointSpec or specLimits fails here until it is classified, and every field
// classified as reported is exercised — set alone on an otherwise empty
// endpoint, it must appear in the report.
//
// (Adding the CRD field itself is still a human step. This is what makes the
// step visible rather than a comment nobody re-reads.)
var (
	// interpretedEndpointFields are honoured by toEndpoint.
	interpretedEndpointFields = []string{
		"port", "targetPort", "path", "scheme", "interval", "scrapeTimeout",
		"tlsConfig", "tlsConfig.insecureSkipVerify", "tlsConfig.ca", "tlsConfig.cert",
		"tlsConfig.keySecret", "tlsConfig.serverName",
		"basicAuth", "authorization", "bearerTokenSecret", "metricRelabelings",
	}
	// reportedEndpointFields are parsed only to be reported, each paired with a
	// value that makes the report fire.
	reportedEndpointFields = map[string]any{
		"oauth2":                   json.RawMessage(`{"clientId":{}}`),
		"bearerTokenFile":          "/var/run/token",
		"followRedirects":          ptr(true),
		"enableHttp2":              ptr(true),
		"honorTimestamps":          ptr(true),
		"trackTimestampsStaleness": ptr(true),
		"proxyUrl":                 "http://proxy:3128",
		"noProxy":                  "10.0.0.0/8",
		"proxyFromEnvironment":     ptr(true),
		"proxyConnectHeader":       json.RawMessage(`{"Proxy-Authorization":[{"name":"s","key":"k"}]}`),
		"params":                   json.RawMessage(`{"module":["http_2xx"]}`),
		"honorLabels":              ptr(true),
		"relabelings":              json.RawMessage(`[{"action":"replace"}]`),
		// The one whose DEFAULT (true) agrees with scrape.Scrapeable: only an
		// explicit false is a partial application.
		"filterRunning":        ptr(false),
		"portNumber":           ptr(int32(9090)),
		"tlsConfig.minVersion": "TLS13",
		"tlsConfig.maxVersion": "TLS13",
		"tlsConfig.caFile":     "/etc/prometheus/secrets/ca.crt",
		"tlsConfig.certFile":   "/etc/prometheus/secrets/tls.crt",
		"tlsConfig.keyFile":    "/etc/prometheus/secrets/tls.key",
		"tlsConfig.ca.configMap": map[string]string{ // reported through secretOrCM
			"name": "cm", "key": "ca.crt",
		},
		"tlsConfig.cert.configMap": map[string]string{"name": "cm", "key": "tls.crt"},
	}
	// reportedSpecFields are the monitor-level guard rails, same contract.
	reportedSpecFields = map[string]any{
		"sampleLimit":                    ptr(uint64(10000)),
		"targetLimit":                    ptr(uint64(5)),
		"labelLimit":                     ptr(uint64(30)),
		"labelNameLengthLimit":           ptr(uint64(64)),
		"labelValueLengthLimit":          ptr(uint64(128)),
		"keepDroppedTargets":             ptr(uint64(100)),
		"jobLabel":                       "app",
		"targetLabels":                   []string{"team"},
		"podTargetLabels":                []string{"team"},
		"bodySizeLimit":                  "50MB",
		"attachMetadata":                 &map[string]any{"node": true},
		"scrapeClass":                    "tls",
		"scrapeProtocols":                []string{"PrometheusText0.0.4"},
		"fallbackScrapeProtocol":         "PrometheusText0.0.4",
		"selectorMechanism":              "RelabelConfig",
		"nativeHistogramBucketLimit":     ptr(uint64(160)),
		"nativeHistogramMinBucketFactor": json.RawMessage(`"1.1"`),
		"convertClassicHistogramsToNHCB": ptr(true),
		"scrapeClassicHistograms":        ptr(true),
	}
)

func ptr[T any](v T) *T { return &v }

func TestEveryParsedEndpointFieldIsInterpretedOrReported(t *testing.T) {
	classified := append(slices.Clone(interpretedEndpointFields), keys(reportedEndpointFields)...)
	assertClassified(t, "endpointSpec", jsonFields(reflect.TypeOf(endpointSpec{}), ""), classified)

	for name, value := range reportedEndpointFields {
		t.Run(name, func(t *testing.T) {
			var ep endpointSpec
			setField(t, reflect.ValueOf(&ep).Elem(), name, value)
			if got := ep.ignoredFields(); !slices.Contains(got, name) {
				t.Errorf("%s set alone is not reported: ignoredFields = %v", name, got)
			}
		})
	}
	// And an interpreted field must not report ITSELF as ignored.
	for _, name := range interpretedEndpointFields {
		if strings.Contains(name, ".") {
			continue // the nested arms need their parent struct; covered above
		}
		var ep endpointSpec
		switch name {
		case "targetPort":
			ep.TargetPort = ptr(intstr.FromInt32(9090))
		case "tlsConfig", "basicAuth", "authorization", "bearerTokenSecret", "metricRelabelings":
			continue // struct arms: their own sub-fields are classified above
		default:
			setField(t, reflect.ValueOf(&ep).Elem(), name, "x")
		}
		if got := ep.ignoredFields(); slices.Contains(got, name) {
			t.Errorf("%s is interpreted but reported as ignored: %v", name, got)
		}
	}
}

func TestEveryParsedSpecFieldIsReported(t *testing.T) {
	assertClassified(t, "specLimits", jsonFields(reflect.TypeOf(specLimits{}), ""), keys(reportedSpecFields))

	for name, value := range reportedSpecFields {
		t.Run(name, func(t *testing.T) {
			var s specLimits
			setField(t, reflect.ValueOf(&s).Elem(), name, value)
			if got := s.ignored(); !slices.Contains(got, name) {
				t.Errorf("%s set alone is not reported: ignored = %v", name, got)
			}
		})
	}
}

// assertClassified compares the fields a spec shape PARSES with the fields the
// package claims to have classified.
func assertClassified(t *testing.T, what string, parsed, classified []string) {
	t.Helper()
	sort.Strings(parsed)
	sort.Strings(classified)
	for _, f := range parsed {
		if !slices.Contains(classified, f) {
			t.Errorf("%s parses %q but neither interprets nor reports it: "+
				"a parsed-but-unclassified field is dropped in silence", what, f)
		}
	}
	for _, f := range classified {
		if !slices.Contains(parsed, f) {
			t.Errorf("%s no longer parses %q, which this test still classifies", what, f)
		}
	}
}

// jsonFields lists a spec struct's json tag names, descending into the nested
// anonymous structs whose arms are reported under a dotted name (tlsConfig) and
// through the inline embedding (smSpec's specLimits).
func jsonFields(t reflect.Type, prefix string) []string {
	var out []string
	for i := range t.NumField() {
		f := t.Field(i)
		tag, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if tag == "" || tag == "-" {
			continue
		}
		name := prefix + tag
		out = append(out, name)
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		// Only tlsConfig's arms are reported individually; basicAuth,
		// authorization and metricRelabelings are interpreted as a whole.
		if ft.Kind() == reflect.Struct && tag == "tlsConfig" {
			out = append(out, jsonFields(ft, name+".")...)
			// secretOrCM's configMap arm is what gets reported, under
			// "<field>.configMap"; the secret arm is interpreted.
			out = append(out, name+".ca.configMap", name+".cert.configMap")
		}
	}
	return out
}

// setField assigns value to the struct field carrying the given json tag name,
// descending one level for a dotted name.
func setField(t *testing.T, v reflect.Value, name string, value any) {
	t.Helper()
	head, rest, nested := strings.Cut(name, ".")
	for i := range v.NumField() {
		tag, _, _ := strings.Cut(v.Type().Field(i).Tag.Get("json"), ",")
		if tag != head {
			continue
		}
		f := v.Field(i)
		if nested {
			if f.Kind() == reflect.Pointer {
				f.Set(reflect.New(f.Type().Elem()))
				f = f.Elem()
			}
			setNested(t, f, rest, value)
			return
		}
		f.Set(reflect.ValueOf(value))
		return
	}
	t.Fatalf("no field with json tag %q on %s", head, v.Type())
}

// setNested handles tlsConfig's arms, including the secretOrCM configMap arm
// whose "field.configMap" name is two levels down.
func setNested(t *testing.T, v reflect.Value, name string, value any) {
	t.Helper()
	head, _, isConfigMap := strings.Cut(name, ".")
	for i := range v.NumField() {
		tag, _, _ := strings.Cut(v.Type().Field(i).Tag.Get("json"), ",")
		if tag != head {
			continue
		}
		f := v.Field(i)
		if !isConfigMap {
			f.Set(reflect.ValueOf(value))
			return
		}
		cm := value.(map[string]string)
		f.Set(reflect.ValueOf(&secretOrCM{ConfigMap: &struct {
			Name string `json:"name"`
			Key  string `json:"key"`
		}{Name: cm["name"], Key: cm["key"]}}))
		return
	}
	t.Fatalf("no nested field with json tag %q on %s", head, v.Type())
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
