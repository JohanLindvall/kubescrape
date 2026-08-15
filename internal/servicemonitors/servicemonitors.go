// Package servicemonitors indexes Prometheus-Operator ServiceMonitor and
// PodMonitor custom resources so their targets can be served alongside
// annotation-discovered ones. Only pod-backed Services are supported: targets
// resolve through the selected Services' pod selectors, which keeps scraping
// node-local.
//
// A documented SUBSET of the CRD is interpreted: endpoint port/targetPort/
// path/scheme, per-endpoint interval/scrapeTimeout, basicAuth, authorization,
// bearerTokenSecret, secret-backed tlsConfig (ca/cert/keySecret/serverName and
// insecureSkipVerify), and the keep/drop subset of metricRelabelings.
// Everything else parsed here exists to be REPORTED as uninterpreted through
// Endpoint.Ignored — see IgnoredFields — because a narrower implementation is
// a choice and a silently partially-applied CR is not.
//
// This file also owns Endpoint.secretRefs, which is a security boundary: it is
// the ONE list of the endpoint fields carrying secret references, and both the
// monitor-namespacing and the /v1/scrape-auth allowlist derive from it. Adding
// a secret-bearing field means adding it there.
package servicemonitors

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// GVR is the ServiceMonitor resource.
var GVR = schema.GroupVersionResource{
	Group:    "monitoring.coreos.com",
	Version:  "v1",
	Resource: "servicemonitors",
}

// Endpoint is one scrape endpoint declaration of a monitor.
type Endpoint struct {
	// Port is the Service port name (ServiceMonitor) or container port name
	// (PodMonitor).
	Port string
	// TargetPort overrides the pod port directly (number or container port
	// name); nil defers to the service port's targetPort.
	TargetPort *intstr.IntOrString
	Path       string
	Scheme     string
	// InsecureSkipVerify comes from the endpoint's tlsConfig; agents scrape
	// https targets without verifying the certificate when set.
	InsecureSkipVerify bool
	// BearerSecret references the endpoint's bearerTokenSecret as
	// "namespace/name/key" (namespace = the monitor's). Served to agents by
	// the metadata service only when -scrape-auth-secrets is enabled.
	BearerSecret string
	// MetricRelabelings holds the keep/drop subset of the endpoint's
	// metricRelabelings; other actions are ignored (documented).
	MetricRelabelings []RelabelRule
	// BasicAuthUser/Pass, AuthType/AuthCredentials and the TLS* refs carry the
	// endpoint's remaining auth material as "namespace/name/key" secret
	// references (namespace = the monitor's), resolved by agents through the
	// same -scrape-auth-secrets channel as BearerSecret. kube-prometheus-stack's
	// own control-plane monitors (etcd, scheduler, controller-manager) use
	// client certificates, and anything behind a private CA was previously
	// scrapeable only by turning verification off entirely.
	BasicAuthUser   string
	BasicAuthPass   string
	AuthType        string // "authorization.type", default Bearer
	AuthCredentials string
	TLSCA           string
	TLSCert         string
	TLSKey          string
	TLSServerName   string
	// Interval and ScrapeTimeout are the endpoint's own cadence (Go duration
	// strings, empty = the agent's -scrape-interval/-scrape-timeout). Honouring
	// them matters on migration: a kube-prometheus-stack shop routinely has
	// monitors at 10s (ingress, mesh) and 5m (expensive exporters), and
	// collapsing both onto one global interval silently coarsens the first and
	// multiplies the sample bill of the second.
	Interval      string
	ScrapeTimeout string
	// Ignored names the endpoint fields kubescrape parsed but does NOT
	// interpret. They are reported once per monitor so a partially-applied CR
	// is visible: "narrower than prometheus-operator" is a documented choice,
	// "silently does something different" is not.
	Ignored []string
}

// secretRefs returns POINTERS to every field of this endpoint that carries a
// secret reference. It is the ONE list of them, and it is a security boundary:
//
//   - Both parsers namespace these fields with the MONITOR's namespace, which
//     is what confines a monitor to secrets in its own namespace.
//   - AuthSecretRefs harvests the same fields into the allowlist
//     /v1/scrape-auth will serve, which is what keeps -scrape-auth-secrets from
//     being a general secret-read API.
//
// Those three loops used to be written out by hand — twice for namespacing
// (ServiceMonitor and PodMonitor, verbatim copies) and once for harvesting.
// They agreed, but adding an eighth secret-bearing field and updating two of
// three fails only at RUNTIME and only for the targets that use it: a ref
// namespaced but not allowlisted 404s, a ref allowlisted but not namespaced can
// never match, and either way the target scrapes unauthenticated and reports
// up=0. Returning pointers from one method makes a new field a COMPILE-VISIBLE
// omission at exactly one site.
//
// Deliberately NOT here: non-secret fields (TLSServerName, AuthType — no
// material, no allowlist entry) and tlsConfig's configMap arm, which is
// reported as ignored rather than resolved because the agent reads secret keys
// through one channel only.
func (e *Endpoint) secretRefs() []*string {
	return []*string{
		&e.BearerSecret,
		&e.BasicAuthUser,
		&e.BasicAuthPass,
		&e.AuthCredentials,
		&e.TLSCA,
		&e.TLSCert,
		&e.TLSKey,
	}
}

// namespaceSecretRefs prefixes every set secret reference with ns, turning the
// endpoint's "name/key" refs into the "namespace/name/key" form the rest of the
// system uses. Both parsers call it; nothing else may.
func (e *Endpoint) namespaceSecretRefs(ns string) {
	for _, p := range e.secretRefs() {
		if *p != "" {
			*p = ns + "/" + *p
		}
	}
}

// RelabelRule is the keep/drop subset of a Prometheus relabel_config,
// evaluated per sample against sourceLabels joined by ";" (Prometheus
// semantics; "__name__" refers to the metric name).
//
// It IS kubemeta.RelabelRule — the wire type that rides on ScrapeTargets and
// that the agent compiles — not a structurally-identical local copy for
// internal/scrape to bridge field-by-field: one type cannot drift from itself.
type RelabelRule = kubemeta.RelabelRule

// endpointSpec is the shared endpoint shape of ServiceMonitor endpoints and
// PodMonitor podMetricsEndpoints.
type endpointSpec struct {
	Port       string              `json:"port"`
	TargetPort *intstr.IntOrString `json:"targetPort"`
	// PortNumber is a PodMonitor-only endpoint field: a container port given as
	// a NUMBER, alongside `port` (a name) and the deprecated `targetPort`.
	// Reported as uninterpreted rather than honoured — an endpoint naming only
	// portNumber otherwise resolves to no targets at all, with no warning and
	// no kubescrape_monitor_fields_ignored_total bump, which is the silent
	// partial application the Ignored machinery exists to prevent.
	PortNumber    *int32 `json:"portNumber"`
	Path          string `json:"path"`
	Scheme        string `json:"scheme"`
	Interval      string `json:"interval"`
	ScrapeTimeout string `json:"scrapeTimeout"`
	TLSConfig     *struct {
		InsecureSkipVerify bool        `json:"insecureSkipVerify"`
		CA                 *secretOrCM `json:"ca"`
		Cert               *secretOrCM `json:"cert"`
		KeySecret          *secretRef  `json:"keySecret"`
		ServerName         string      `json:"serverName"`
		// Parsed only to be REPORTED. A minVersion/maxVersion is a security
		// FLOOR an operator set deliberately, and the agent builds its per-
		// target client from the resolved CA/cert/serverName without it — so
		// honouring neither the field nor the reporting machinery meant a
		// monitor that pinned TLS 1.3 was scraped over whatever the Go default
		// negotiated, with nothing anywhere saying so. That is the one outcome
		// Endpoint.Ignored exists to make impossible.
		MinVersion string `json:"minVersion"`
		MaxVersion string `json:"maxVersion"`
		// Parsed only to be REPORTED as uninterpreted. These are the
		// file-path arms of prometheus-operator's TLSConfig, used by every
		// kube-prometheus-stack control-plane monitor (etcd, kube-scheduler,
		// kube-controller-manager). The agent reads credentials through the
		// service's /v1/scrape-auth channel and has no access to files on the
		// Prometheus pod, so they cannot be honoured — but leaving them
		// unparsed made an https target silently fall back to the system
		// trust store and fail every scrape with up=0 as the only signal.
		CAFile   string `json:"caFile"`
		CertFile string `json:"certFile"`
		KeyFile  string `json:"keyFile"`
	} `json:"tlsConfig"`
	BasicAuth *struct {
		Username *secretRef `json:"username"`
		Password *secretRef `json:"password"`
	} `json:"basicAuth"`
	Authorization *struct {
		Type        string     `json:"type"`
		Credentials *secretRef `json:"credentials"`
	} `json:"authorization"`
	// Parsed only to be REPORTED as uninterpreted (see Endpoint.Ignored).
	OAuth2 json.RawMessage `json:"oauth2"`
	// prometheus-operator's ProxyConfig, all four fields of it. proxyUrl alone
	// was parsed, so the sibling clauses of the same struct were silently
	// dropped — and proxyConnectHeader carries SecretKeySelectors, i.e. secret
	// material named by a monitor and reported nowhere. Harmless only because
	// none of the four is honoured; inconsistent reporting of one struct is
	// exactly the partial application Ignored exists to make visible.
	ProxyURL             string          `json:"proxyUrl"`
	NoProxy              string          `json:"noProxy"`
	ProxyFromEnvironment *bool           `json:"proxyFromEnvironment"`
	ProxyConnectHeader   json.RawMessage `json:"proxyConnectHeader"`
	// Parsed only to be REPORTED as uninterpreted.
	BearerTokenFile string `json:"bearerTokenFile"`
	// filterRunning is an ENDPOINT field on BOTH kinds — ServiceMonitor
	// `Endpoint` and PodMonitor `PodMetricsEndpoint` (verified against the
	// shipped CRDs, v0.68 through v0.84) — which is exactly why it belongs on
	// the shared endpointSpec. It lived in specLimits, which the CRD has no
	// filterRunning on at all, so the branch reporting it was unreachable: not
	// because inline embedding fails to decode (it decodes fine), but because
	// the API server PRUNES an unknown spec-level property, so the value never
	// arrives. The one field singled out below as most likely to surprise was
	// therefore the one silently dropped.
	//
	// Only a FALSE value differs from what kubescrape does: scrape.Scrapeable
	// already excludes finished and terminating pods, which is filterRunning's
	// default behaviour. `filterRunning: false` asks for the OPPOSITE.
	FilterRunning            *bool           `json:"filterRunning"`
	FollowRedirects          *bool           `json:"followRedirects"`
	EnableHTTP2              *bool           `json:"enableHttp2"`
	HonorTimestamps          *bool           `json:"honorTimestamps"`
	TrackTimestampsStaleness *bool           `json:"trackTimestampsStaleness"`
	Params                   json.RawMessage `json:"params"`
	HonorLabels              *bool           `json:"honorLabels"`
	Relabelings              json.RawMessage `json:"relabelings"`
	BearerTokenSecret        *secretRef      `json:"bearerTokenSecret"`
	MetricRelabelings        []struct {
		Action       string   `json:"action"`
		SourceLabels []string `json:"sourceLabels"`
		Regex        string   `json:"regex"`
		// Parsed to be REPORTED, and to SUPPRESS the rule rather than apply
		// it wrongly: the agent joins sourceLabels with a hardcoded ';', so
		// honouring a rule that asked for a different separator would build a
		// different string than the user's regex was written against. For a
		// keep rule that inverts the intent — it matches nothing and drops
		// everything the user meant to keep.
		Separator string `json:"separator"`
	} `json:"metricRelabelings"`
}

// secretRef is a SecretKeySelector.
type secretRef struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// ref renders "name/key" (the namespace is prefixed by the caller); empty when
// incomplete or when either part carries a '/'.
//
// The slash rule is what makes the three-segment join UNAMBIGUOUS, and that is
// a security property, not tidiness: the rendered string becomes an
// /v1/scrape-auth allowlist key, and the handler checks that key against three
// separately-chosen URL path segments. SecretKeySelector.Name and .Key are
// plain strings in the CRD with no validation of their own, so a tenant able to
// create a monitor in namespace `tenant` could mint the entry
// "tenant/victim/creds/token" and satisfy it with
// GET /v1/scrape-auth/tenant%2Fvictim/creds/token — Go's ServeMux unescapes
// %2F inside a single wildcard segment — reaching SecretReader.Get with
// namespace "tenant/victim". The shipped client-go reader rejects that
// namespace before it sends anything, but SecretReader is a pluggable
// interface and one implemented over a lister keyed by "ns/name" would perform
// the read. Refusing here makes the ambiguity inexpressible; handleScrapeAuth
// re-validates the request's own segments for the same reason.
//
// A refused ref reads as absent (the target scrapes unauthenticated, exactly as
// for an incomplete one) — never as some other secret.
func (r *secretRef) ref() string {
	if r == nil || r.Name == "" || r.Key == "" {
		return ""
	}
	if strings.Contains(r.Name, "/") || strings.Contains(r.Key, "/") {
		return ""
	}
	return r.Name + "/" + r.Key
}

// secretOrCM is prometheus-operator's SecretOrConfigMap. Only the secret arm is
// resolvable: the agent reads secret keys through the metadata service's
// -scrape-auth-secrets channel, and adding a parallel configMap channel is not
// worth it while every CA can be stored in a secret.
type secretOrCM struct {
	Secret    *secretRef `json:"secret"`
	ConfigMap *struct {
		Name string `json:"name"`
		Key  string `json:"key"`
	} `json:"configMap"`
}

func (s *secretOrCM) ref() string {
	if s == nil {
		return ""
	}
	return s.Secret.ref()
}

// usesConfigMap reports the unsupported arm, so it is reported as ignored.
func (s *secretOrCM) usesConfigMap() bool {
	return s != nil && s.ConfigMap != nil && s.ConfigMap.Name != ""
}

// noPortIgnored is the Ignored entry for an endpoint that names neither port
// nor targetPort. Spelled with the "(unset)" suffix so a reader of the log line
// sees that this one is about a field's ABSENCE.
const noPortIgnored = "port(unset)"

// ignoredFields lists the endpoint fields that are set but not interpreted.
func (ep endpointSpec) ignoredFields() []string {
	var out []string
	add := func(name string, set bool) {
		if set {
			out = append(out, name)
		}
	}
	add("oauth2", len(ep.OAuth2) > 0)
	add("bearerTokenFile", ep.BearerTokenFile != "")
	add("followRedirects", ep.FollowRedirects != nil)
	add("enableHttp2", ep.EnableHTTP2 != nil)
	add("honorTimestamps", ep.HonorTimestamps != nil)
	add("trackTimestampsStaleness", ep.TrackTimestampsStaleness != nil)
	add("proxyUrl", ep.ProxyURL != "")
	add("noProxy", ep.NoProxy != "")
	add("proxyFromEnvironment", ep.ProxyFromEnvironment != nil)
	add("proxyConnectHeader", len(ep.ProxyConnectHeader) > 0)
	add("params", len(ep.Params) > 0)
	add("honorLabels", ep.HonorLabels != nil && *ep.HonorLabels)
	add("relabelings", len(ep.Relabelings) > 0)
	add("filterRunning", ep.FilterRunning != nil && !*ep.FilterRunning)
	add("portNumber", ep.PortNumber != nil)
	// The one entry that reports an ABSENCE, because the absence has the same
	// consequence every other entry here has: the endpoint resolves to no
	// targets at all (scrape.MonitorTargets and PodMonitorTargets both refuse
	// it — an empty port must not match a Service's unnamed port by "" == ""
	// and fabricate a phantom target). Every other path to zero targets is
	// data-dependent and cannot be judged at parse time; this one is a property
	// of the CR, so it is reported like any other clause we do not act on.
	// Suppressed when portNumber IS set: that entry already names the cause,
	// and claiming the endpoint names no port would be false.
	add(noPortIgnored, ep.Port == "" && ep.TargetPort == nil && ep.PortNumber == nil)
	if ep.TLSConfig != nil {
		// Only the configMap arm is unsupported; secret-backed CA/cert are
		// interpreted below.
		add("tlsConfig.ca.configMap", ep.TLSConfig.CA.usesConfigMap())
		add("tlsConfig.cert.configMap", ep.TLSConfig.Cert.usesConfigMap())
		// The file-path arms cannot be honoured (the agent has no access to
		// the Prometheus pod's filesystem) and their absence is not benign:
		// the target falls back to the system trust store and every scrape
		// fails verification. Say so.
		add("tlsConfig.caFile", ep.TLSConfig.CAFile != "")
		add("tlsConfig.certFile", ep.TLSConfig.CertFile != "")
		add("tlsConfig.keyFile", ep.TLSConfig.KeyFile != "")
		add("tlsConfig.minVersion", ep.TLSConfig.MinVersion != "")
		add("tlsConfig.maxVersion", ep.TLSConfig.MaxVersion != "")
	}
	for _, r := range ep.MetricRelabelings {
		if !isKeepDrop(r.Action) {
			out = append(out, "metricRelabelings.action="+r.Action)
			continue
		}
		if r.Separator != "" && r.Separator != ";" {
			out = append(out, "metricRelabelings.separator="+r.Separator)
		}
	}
	return out
}

// toEndpoint converts the spec shape (BearerSecret namespace filled by the
// caller).
func (ep endpointSpec) toEndpoint() Endpoint {
	out := Endpoint{
		Port: ep.Port, TargetPort: ep.TargetPort, Path: ep.Path, Scheme: ep.Scheme,
		Interval: ep.Interval, ScrapeTimeout: ep.ScrapeTimeout,
		Ignored: ep.ignoredFields(),
	}
	if ep.TLSConfig != nil {
		out.InsecureSkipVerify = ep.TLSConfig.InsecureSkipVerify
		out.TLSServerName = ep.TLSConfig.ServerName
		out.TLSCA = ep.TLSConfig.CA.ref()
		out.TLSCert = ep.TLSConfig.Cert.ref()
		out.TLSKey = ep.TLSConfig.KeySecret.ref()
	}
	if ep.BasicAuth != nil {
		out.BasicAuthUser = ep.BasicAuth.Username.ref()
		out.BasicAuthPass = ep.BasicAuth.Password.ref()
	}
	if ep.Authorization != nil {
		out.AuthType = ep.Authorization.Type
		out.AuthCredentials = ep.Authorization.Credentials.ref()
	}
	// secretRef.ref owns the incomplete-ref-is-empty rule; bearerTokenSecret
	// used to re-spell it inline beside six fields that already went through it.
	out.BearerSecret = ep.BearerTokenSecret.ref()
	for _, r := range ep.MetricRelabelings {
		if !isKeepDrop(r.Action) {
			continue
		}
		// A custom separator is reported (below) and the rule SKIPPED: the
		// agent joins sourceLabels with ';', so applying the rule anyway
		// would test the user's regex against a string it was never written
		// for — silently inverting a keep into a drop-everything.
		if r.Separator != "" && r.Separator != ";" {
			continue
		}
		out.MetricRelabelings = append(out.MetricRelabelings, RelabelRule{
			// Normalized, so everything downstream compares one spelling.
			Action:       strings.ToLower(r.Action),
			SourceLabels: r.SourceLabels,
			Regex:        r.Regex,
		})
	}
	return out
}

// isKeepDrop reports whether a relabel action is one of the two this repo
// interprets, in EITHER of the spellings the CRD accepts.
//
// prometheus-operator's RelabelConfig.action enum lists both cases explicitly —
// `replace;Replace;keep;Keep;drop;Drop;hashmod;HashMod;…` — and the operator
// lowercases the value when it generates scrape config, so `action: Drop` is a
// perfectly ordinary, CRD-valid, Prometheus-honoured rule. Comparing against
// the lowercase literals alone therefore DISCARDED it: the rule was reported as
// an unsupported action and the series the user asked to drop were exported
// instead — the opposite of what the CR says, with the only signal a generic
// "field ignored" line naming an action that is in fact supported.
func isKeepDrop(action string) bool {
	switch strings.ToLower(action) {
	case "keep", "drop":
		return true
	}
	return false
}

// Monitor is one parsed ServiceMonitor.
type Monitor struct {
	Namespace string
	Name      string
	// resourceVersion is the object this record was parsed from; see
	// upsertMonitor. Not part of the model and never served.
	resourceVersion string
	// Selector selects Services by their labels.
	Selector labels.Selector
	// NamespaceAny selects Services in all namespaces; otherwise Namespaces
	// (defaulting to the monitor's own) applies.
	NamespaceAny bool
	Namespaces   []string
	Endpoints    []Endpoint
}

// ServiceNamespaces returns the namespaces the monitor selects Services in;
// nil means all.
func (m *Monitor) ServiceNamespaces() []string {
	return namespaceSelector{Any: m.NamespaceAny, MatchNames: m.Namespaces}.resolve(m.Namespace)
}

// namespaceSelector is the CRD's namespaceSelector clause, shared verbatim by
// the ServiceMonitor and PodMonitor specs (it was an anonymous struct in each).
type namespaceSelector struct {
	Any        bool     `json:"any"`
	MatchNames []string `json:"matchNames"`
}

// resolve returns the namespaces the selector covers for a monitor living in
// ownNS: nil for "all namespaces", the explicit matchNames when given, else
// the monitor's own namespace (the CRD default). ServiceNamespaces and
// PodNamespaces both answer through this — the rule existed twice, verbatim.
func (s namespaceSelector) resolve(ownNS string) []string {
	if s.Any {
		return nil
	}
	if len(s.MatchNames) > 0 {
		return s.MatchNames
	}
	return []string{ownNS}
}

// specLimits are the monitor-LEVEL guard rails, shared by ServiceMonitor and
// PodMonitor. They are parsed only to be REPORTED as uninterpreted: they were
// dropped at parse time, so a user who set sampleLimit specifically to fence
// off a cardinality bomb got no protection and no warning — and sampleLimit is
// the very example IgnoredFields' own doc comment cites.
type specLimits struct {
	SampleLimit           *uint64  `json:"sampleLimit"`
	TargetLimit           *uint64  `json:"targetLimit"`
	LabelLimit            *uint64  `json:"labelLimit"`
	LabelNameLengthLimit  *uint64  `json:"labelNameLengthLimit"`
	LabelValueLengthLimit *uint64  `json:"labelValueLengthLimit"`
	KeepDroppedTargets    *uint64  `json:"keepDroppedTargets"`
	JobLabel              string   `json:"jobLabel"`
	TargetLabels          []string `json:"targetLabels"`
	PodTargetLabels       []string `json:"podTargetLabels"`
	// Set but not interpreted, and previously not even PARSED — so they
	// produced no warning and no MonitorFieldsIgnored bump, breaching the
	// no-silent-partial-application contract the Ignored machinery exists for.
	//
	// filterRunning is NOT here: the CRD puts it on the ENDPOINT (on both
	// kinds), so it lives on endpointSpec. It sat in this struct for a while,
	// where the API server's pruning of unknown spec properties meant it never
	// arrived.
	BodySizeLimit  string          `json:"bodySizeLimit"`
	AttachMetadata *map[string]any `json:"attachMetadata"`
	ScrapeClass    string          `json:"scrapeClass"`
	// The exposition-format and native-histogram clauses, parsed for the same
	// reason as the guard rails above them.
	//
	// scrapeProtocols/fallbackScrapeProtocol are the loudest of these: the agent
	// negotiates its own Accept header (OpenMetrics when exemplars are on,
	// protobuf when native histograms are), so an operator who pinned
	// PrometheusText0.0.4 because a target's OpenMetrics output is broken gets
	// the opposite of what the CR says. The nativeHistogram* pair sits in the
	// CRD beside sampleLimit/targetLimit/labelLimit and guards the same thing —
	// cardinality — and all three of those ARE reported.
	ScrapeProtocols                []string        `json:"scrapeProtocols"`
	FallbackScrapeProtocol         string          `json:"fallbackScrapeProtocol"`
	SelectorMechanism              string          `json:"selectorMechanism"`
	NativeHistogramBucketLimit     *uint64         `json:"nativeHistogramBucketLimit"`
	NativeHistogramMinBucketFactor json.RawMessage `json:"nativeHistogramMinBucketFactor"`
	ConvertClassicHistogramsToNHCB *bool           `json:"convertClassicHistogramsToNHCB"`
	ScrapeClassicHistograms        *bool           `json:"scrapeClassicHistograms"`
}

// ignored lists the monitor-level fields that are set but not interpreted.
func (s specLimits) ignored() []string {
	var out []string
	add := func(name string, set bool) {
		if set {
			out = append(out, name)
		}
	}
	add("sampleLimit", s.SampleLimit != nil)
	add("targetLimit", s.TargetLimit != nil)
	add("labelLimit", s.LabelLimit != nil)
	add("labelNameLengthLimit", s.LabelNameLengthLimit != nil)
	add("labelValueLengthLimit", s.LabelValueLengthLimit != nil)
	add("keepDroppedTargets", s.KeepDroppedTargets != nil)
	add("jobLabel", s.JobLabel != "")
	add("targetLabels", len(s.TargetLabels) > 0)
	add("podTargetLabels", len(s.PodTargetLabels) > 0)
	add("bodySizeLimit", s.BodySizeLimit != "")
	add("attachMetadata", s.AttachMetadata != nil)
	add("scrapeClass", s.ScrapeClass != "")
	add("scrapeProtocols", len(s.ScrapeProtocols) > 0)
	add("fallbackScrapeProtocol", s.FallbackScrapeProtocol != "")
	add("selectorMechanism", s.SelectorMechanism != "")
	add("nativeHistogramBucketLimit", s.NativeHistogramBucketLimit != nil)
	add("nativeHistogramMinBucketFactor", len(s.NativeHistogramMinBucketFactor) > 0)
	add("convertClassicHistogramsToNHCB", s.ConvertClassicHistogramsToNHCB != nil)
	add("scrapeClassicHistograms", s.ScrapeClassicHistograms != nil)
	return out
}

// smSpec mirrors the ServiceMonitor spec fields we interpret.
type smSpec struct {
	Selector          metav1.LabelSelector `json:"selector"`
	NamespaceSelector namespaceSelector    `json:"namespaceSelector"`
	Endpoints         []endpointSpec       `json:"endpoints"`
	specLimits        `json:",inline"`
}

func (s *smSpec) labelSelector() *metav1.LabelSelector { return &s.Selector }
func (s *smSpec) nsSelector() namespaceSelector        { return s.NamespaceSelector }
func (s *smSpec) endpointSpecs() []endpointSpec        { return s.Endpoints }
func (s *smSpec) monitorIgnored() []string             { return s.ignored() }

// monitorSpec is what the shared parse skeleton needs from a kind's decoded
// spec shape: its label selector, its namespace selector, its endpoint list
// (the two kinds spell the JSON key differently) and its monitor-level
// ignored-fields report. A new monitor arm implements these four accessors
// and gets the WHOLE skeleton — including the per-endpoint security step in
// parseMonitorSpec — for free.
type monitorSpec interface {
	labelSelector() *metav1.LabelSelector
	nsSelector() namespaceSelector
	endpointSpecs() []endpointSpec
	monitorIgnored() []string
}

// monitorBase is the kind-independent half a parse produces.
type monitorBase struct {
	Namespace    string
	Name         string
	Selector     labels.Selector
	NamespaceAny bool
	Namespaces   []string
	Endpoints    []Endpoint
	// ResourceVersion of the object parsed, carried so the index can tell a
	// re-delivery that changes nothing from a real update (see upsertMonitor).
	ResourceVersion string
}

// parseMonitorSpec is the ONE parse skeleton of both monitor kinds: the
// no-spec error, the unstructured decode into the kind's spec shape, the
// selector conversion, and — per endpoint — the monitor-level ignored-fields
// append plus namespaceSecretRefs with the MONITOR's namespace.
//
// The last step is a security boundary, which is why it lives here rather than
// in each kind's parser: namespacing every secret ref with the monitor's own
// namespace is what confines a monitor to its own secrets and so bounds what
// /v1/scrape-auth will serve (see Endpoint.secretRefs). The two hand-written
// copies of this loop agreed; a THIRD arm that forgot the call would have
// compiled fine and shipped an unnamespaced — unmatchable, so silently
// unauthenticated — credential ref. An arm cannot skip it now without also
// rewriting the decode, the selector conversion and the error strings.
func parseMonitorSpec(u *unstructured.Unstructured, kind string, spec monitorSpec) (monitorBase, error) {
	var b monitorBase
	specRaw, ok := u.Object["spec"].(map[string]any)
	if !ok {
		return b, fmt.Errorf("%s %s/%s: no spec", kind, u.GetNamespace(), u.GetName())
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(specRaw, spec); err != nil {
		return b, fmt.Errorf("%s %s/%s: %w", kind, u.GetNamespace(), u.GetName(), err)
	}
	sel, err := metav1.LabelSelectorAsSelector(spec.labelSelector())
	if err != nil {
		return b, fmt.Errorf("%s %s/%s selector: %w", kind, u.GetNamespace(), u.GetName(), err)
	}
	nss := spec.nsSelector()
	b = monitorBase{
		Namespace:       u.GetNamespace(),
		Name:            u.GetName(),
		Selector:        sel,
		NamespaceAny:    nss.Any,
		Namespaces:      nss.MatchNames,
		ResourceVersion: u.GetResourceVersion(),
	}
	specIgnored := spec.monitorIgnored()
	for _, ep := range spec.endpointSpecs() {
		e := ep.toEndpoint()
		// Monitor-level ignored fields ride on every endpoint; IgnoredFields
		// dedupes across endpoints, so they are reported exactly once.
		e.Ignored = append(e.Ignored, specIgnored...)
		// Every secret reference is namespaced with the MONITOR's namespace: a
		// monitor may only name secrets in its own namespace, which is what
		// bounds what /v1/scrape-auth will serve. The FIELD LIST lives on
		// Endpoint (secretRefs), shared with both parsers and with
		// AuthSecretRefs — see its doc for why it must be exactly one list.
		e.namespaceSecretRefs(b.Namespace)
		b.Endpoints = append(b.Endpoints, e)
	}
	return b, nil
}

// Parse converts an unstructured ServiceMonitor.
func Parse(u *unstructured.Unstructured) (*Monitor, error) {
	b, err := parseMonitorSpec(u, "servicemonitor", &smSpec{})
	if err != nil {
		return nil, err
	}
	return &Monitor{
		Namespace:       b.Namespace,
		Name:            b.Name,
		resourceVersion: b.ResourceVersion,
		Selector:        b.Selector,
		NamespaceAny:    b.NamespaceAny,
		Namespaces:      b.Namespaces,
		Endpoints:       b.Endpoints,
	}, nil
}

// Index is the thread-safe monitor store fed by the informer.
type Index struct {
	mu          sync.RWMutex
	monitors    map[string]*Monitor
	podMonitors map[string]*PodMonitor
	// The resourceVersion of the object last REJECTED under each key, per kind,
	// so upsertMonitor can tell a newly broken monitor from the same broken
	// monitor re-delivered by a resync. It holds nothing the index serves — a
	// rejected monitor is exactly the one that is not indexed — and it is
	// bounded by the broken monitors that exist: an entry is dropped when the
	// key parses again (upsertMonitor) and when the object goes away
	// (deleteMonitor, which clears it BEFORE its own not-indexed early return,
	// since a rejected key is never in the monitor map).
	//
	// Per kind and not one shared map: the key is "namespace/name", which a
	// ServiceMonitor and a PodMonitor may both carry.
	rejectedMonitors    map[string]string
	rejectedPodMonitors map[string]string
	// gen changes on every mutation, so a consumer that derives something
	// expensive from the whole index (the server's monitor→services cross
	// product) can hold it until the index actually changes instead of until a
	// timer lapses. Atomic and read without the lock: a stale read only costs
	// one extra rebuild.
	gen atomic.Uint64

	// The AuthSecretRefs memo, on that same token. Its own mutex, never mu: the
	// build takes mu for reading, and a read path that took the index's WRITE
	// lock would serialise every /v1/scrape-auth request against the informer.
	// authBuilds counts full harvests, for tests.
	authMu     sync.Mutex
	authRefs   AuthRefs
	authGen    uint64
	authValid  bool
	authBuilds atomic.Int64
}

// Generation changes whenever the indexed monitors change. It is a change
// TOKEN, not a count: compare it with a previously observed value, never
// interpret the difference.
func (ix *Index) Generation() uint64 { return ix.gen.Load() }

// NewIndex creates an empty index.
func NewIndex() *Index {
	return &Index{
		monitors:            make(map[string]*Monitor),
		podMonitors:         make(map[string]*PodMonitor),
		rejectedMonitors:    make(map[string]string),
		rejectedPodMonitors: make(map[string]string),
	}
}

// upsertMonitor is the ONE invalid-update-removes policy behind Upsert and
// UpsertPodMonitor: a monitor UPDATED to an unparseable spec is removed rather
// than kept, because silently serving the previous version forever would
// diverge from what the manifest declares (prometheus-operator likewise
// generates no config for an invalid monitor) — and the stale endpoints carry
// the secret refs AuthSecretRefs allowlists, so keeping them would leave
// /v1/scrape-auth willing to serve a Secret the live spec no longer names.
//
// The parse already happened, outside the lock; one write-lock hold then
// either stores the monitor or deletes the key. The two arms used different
// lock choreography for the same observable behavior (the ServiceMonitor arm
// re-locked via Delete on the error path); the single-hold form is kept for
// both — each branch is still exactly one atomic map transition, so a
// concurrent reader sees the same states as before.
//
// It is also where the CHANGE TOKEN is moved, and only a real change may move
// it. An informer resync re-delivers every monitor byte-identical, and the
// token is what holds the server's monitor→Service cross product together
// (buildMonitoredServices: 19.8 ms and 9.67 MB at 50 monitors x 2,000
// Services) — bumping it for a re-delivery meant a `-resync`-configured
// service rebuilt that cross product on essentially every agent poll. Three
// deliveries leave the TOKEN alone: the same resourceVersion stored under the
// same key, a failed parse for a key that is already absent, and (in Delete) a
// key that was not there. An EMPTY resourceVersion counts as changed — only
// hand-built objects have one, and for those the version says nothing about the
// content.
//
// It REPORTS that decision as well as acting on it. The change token settles
// what the index does; the CALLER has reporting of its own that is just as
// event-shaped — the metadata service logs a monitor's uninterpreted fields and
// counts kubescrape_monitor_fields_ignored_total, and it logs and counts a
// monitor it could not parse — and a re-delivery re-fired all of it. With
// `-resync 10m`, fifty monitors carrying a `relabelings` or a `sampleLimit`
// (ordinary in a prometheus-operator install) meant fifty WARN lines every ten
// minutes forever and a counter whose RATE tracked the resync period rather
// than anything an operator changed, which is not a thing an alert can be
// written against. The sibling refusal in the same handler chain was demoted to
// Debug for exactly this reason, and said so.
//
// So the reported bool is "this delivery is NEWS", which is what an event
// report needs, and the ERROR path is included in that — it is the branch the
// gate is easiest to get wrong. `!had` cannot stand in for "already reported":
// a monitor that never parsed is not indexed either, so the FIRST sighting of a
// broken monitor and the thousandth resync of it are the same map lookup, and
// gating on the index alone would silence exactly the report an operator needs
// (an applied monitor doing nothing) while still firing forever for the one it
// does not. The rejected-version tables are what separate them: news is a
// monitor DROPPED from the index, a key never rejected before, or a rejection
// at a resourceVersion different from the one last rejected — with an empty
// resourceVersion news every time, the same rule the success path applies and
// for the same reason.
func upsertMonitor[M any, P interface {
	*M
	version() string
}](ix *Index, monitors map[string]P, rejected map[string]string, key, resourceVersion string, m P, err error) (news bool, _ error) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if err != nil {
		_, had := monitors[key]
		if had {
			// The invalid-update-removes policy: this one really did change
			// what is served, so the token moves and the report is news
			// whatever was rejected here before.
			delete(monitors, key)
			ix.gen.Add(1)
		}
		previous, seen := rejected[key]
		rejected[key] = resourceVersion
		return had || !seen || resourceVersion == "" || previous != resourceVersion, err
	}
	// It parses now, so a later failure is news again.
	delete(rejected, key)
	if cur, ok := monitors[key]; ok && m.version() != "" && cur.version() == m.version() {
		return false, nil // re-delivery of the object already indexed
	}
	monitors[key] = m
	ix.gen.Add(1)
	return true, nil
}

// version reports the resourceVersion a record was parsed from. It is the
// constraint upsertMonitor takes, so a third monitor kind cannot be added
// without one — the alternative is a kind whose re-deliveries silently
// invalidate the server's memo again.
func (m *Monitor) version() string { return m.resourceVersion }

// Upsert parses and stores a ServiceMonitor (see upsertMonitor for the
// invalid-update-removes policy).
func (ix *Index) Upsert(u *unstructured.Unstructured) error {
	_, err := ix.UpsertChanged(u)
	return err
}

// UpsertChanged is Upsert, additionally reporting whether the delivery was
// NEWS — false for the byte-identical re-delivery an informer resync makes of
// every object it holds, including a re-delivery of an object that does not
// parse. A caller whose logging or metrics describe an EVENT rather than a
// state gates them on it; see upsertMonitor for what the distinction costs when
// it is not made, and for why the error path cannot be gated on the index alone.
func (ix *Index) UpsertChanged(u *unstructured.Unstructured) (bool, error) {
	m, err := Parse(u)
	return upsertMonitor(ix, ix.monitors, ix.rejectedMonitors,
		u.GetNamespace()+"/"+u.GetName(), u.GetResourceVersion(), m, err)
}

// Delete removes a monitor.
func (ix *Index) Delete(namespace, name string) {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	deleteMonitor(ix, ix.monitors, ix.rejectedMonitors, namespace+"/"+name)
}

// deleteMonitor removes a key and moves the change token only if it was there.
// A delete for a key the index never held (a monitor that failed to parse, a
// DeletedFinalStateUnknown replay) changes nothing, and the token's whole job
// is to say when something changed. Caller holds the write lock.
//
// The rejection record goes FIRST and unconditionally, before that early
// return: a rejected key is precisely the one that is not in the monitor map,
// so clearing it afterwards would never run — leaving the entry until the
// process exits, and making a re-created monitor that is broken the same way
// report nothing.
func deleteMonitor[M any](ix *Index, monitors map[string]*M, rejected map[string]string, key string) {
	delete(rejected, key)
	if _, had := monitors[key]; !had {
		return
	}
	delete(monitors, key)
	ix.gen.Add(1)
}

// sortedMonitors collects a monitor map's values ordered by (namespace, name):
// map iteration order must not decide which monitor a URL-deduped target is
// attributed to. The caller holds at least the read lock. It sorts on the two
// FIELDS, never on the "ns/name" map key: '/' (0x2F) sorts after '-' (0x2D),
// so the composite string would order "a-x/…" before "a/…" while the field
// sort orders them the other way.
func sortedMonitors[M any](monitors map[string]*M, key func(*M) (namespace, name string)) []*M {
	out := make([]*M, 0, len(monitors))
	for _, m := range monitors {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		ni, nmi := key(out[i])
		nj, nmj := key(out[j])
		if ni != nj {
			return ni < nj
		}
		return nmi < nmj
	})
	return out
}

// All returns the current monitors, ordered by namespace/name: map iteration
// order must not decide which monitor a URL-deduped target is attributed to
// (the same determinism the server enforces for services).
func (ix *Index) All() []*Monitor {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	return sortedMonitors(ix.monitors, func(m *Monitor) (string, string) { return m.Namespace, m.Name })
}

// AuthRefs is the allowlist AuthSecretRefs answers with: a READ-ONLY view of
// the memoised "namespace/name/key" set.
//
// A bare map is the obvious return type and was the previous one. It is the
// wrong one HERE, because what is returned is not a value the caller owns: it
// is the memo itself, shared by every concurrent /v1/scrape-auth request until
// the next monitor change, and it is the boundary that keeps
// -scrape-auth-secrets from being a general secret-read API. One entry written
// into it widens what the service is willing to read, cluster-wide, with
// nothing anywhere to notice. Handing out a COPY instead would put an
// allocation back on the route the memo exists to keep free — a maps.Clone of
// the 400 refs BenchmarkAuthSecretRefs builds is 13,656 B and 4 allocations per
// request, against 0 for this view — so the safety is structural rather than a
// copy or a comment: the set has no exported field and no method that can add
// to it, so a caller outside this package CANNOT widen it. It is not trusted
// not to.
type AuthRefs struct{ refs map[string]struct{} }

// Has reports whether ref — the "namespace/name/key" join the scrape-auth
// route builds from its three path segments — is allowlisted.
func (a AuthRefs) Has(ref string) bool {
	_, ok := a.refs[ref]
	return ok
}

// Len is the number of allowlisted references.
func (a AuthRefs) Len() int { return len(a.refs) }

// AuthSecretRefs returns the set of "namespace/name/key" bearerTokenSecret
// references across all indexed ServiceMonitor and PodMonitor endpoints. The
// scrape-auth endpoint serves ONLY these, so a direct HTTP caller cannot use
// it to read arbitrary cluster secrets — only the tokens a monitor actually
// references.
//
// THE RESULT IS THE SHARED MEMO, not a copy — see AuthRefs for why that is a
// type and not a comment. (server.monitoredServices carries the same shared
// contract by convention; its consumer is inside this repo's own request path,
// and what it holds is not a security boundary.)
//
// Memoised on the change token, because this is the allowlist check on the ONE
// route holding cluster-wide `secrets: get` and every agent re-asks each
// credential once a minute: rebuilding it per request was O(monitors ×
// endpoints) of pure garbage under the index's read lock, which the informer's
// writes contend with. Measured (BenchmarkAuthSecretRefs, 200 ServiceMonitors +
// 200 PodMonitors of one secret-bearing endpoint each): 27,112 B and 15
// allocations per request, scaling with the REFS — an endpoint that also names
// a tlsConfig ca/cert/keySecret is four of them — against 0 allocations once
// the answer is held.
func (x *Index) AuthSecretRefs() AuthRefs {
	// Read the token BEFORE the build, exactly as server.monitoredServices
	// does: a mutation landing during the harvest is then recorded as unbuilt
	// and rebuilds on the next call, rather than being stamped as
	// already-included and lost until some unrelated change moves the token.
	// Getting this backwards on THIS map would leave a removed monitor's secret
	// reachable.
	gen := x.Generation()
	x.authMu.Lock()
	defer x.authMu.Unlock()
	if !x.authValid || x.authGen != gen {
		x.authRefs = x.buildAuthSecretRefs()
		x.authGen, x.authValid = gen, true
	}
	return x.authRefs
}

// buildAuthSecretRefs harvests the allowlist from scratch.
func (x *Index) buildAuthSecretRefs() AuthRefs {
	x.authBuilds.Add(1)
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := map[string]struct{}{}
	add := func(eps []Endpoint) {
		for i := range eps {
			// Every secret an endpoint references, so the metadata service
			// serves exactly the keys some monitor actually names — the
			// allowlist that keeps -scrape-auth-secrets from being a general
			// secret-read API. The FIELD LIST is Endpoint.secretRefs, the same
			// one both parsers namespace with: an allowlist that could disagree
			// with the namespacing is a 404 on one side or an unmatchable ref on
			// the other, and both scrape unauthenticated.
			//
			// Indexed, not ranged by value: secretRefs takes the address of the
			// endpoint's fields, and a loop copy's addresses are the copy's.
			for _, ref := range eps[i].secretRefs() {
				if *ref != "" {
					out[*ref] = struct{}{}
				}
			}
		}
	}
	for _, m := range x.monitors {
		add(m.Endpoints)
	}
	for _, m := range x.podMonitors {
		add(m.Endpoints)
	}
	return AuthRefs{refs: out}
}

// IgnoredFields returns the distinct endpoint fields present on these
// endpoints that kubescrape does not interpret, sorted.
//
// kubescrape deliberately implements a SUBSET of the ServiceMonitor and
// PodMonitor spec, which is fine — but a partially-applied CR must not be
// silent. An operator who applies a monitor with `relabelings` renaming a
// label, or a `sampleLimit` guarding against a cardinality bomb, otherwise
// sees targets appear and never learns those clauses did nothing.
func IgnoredFields(eps []Endpoint) []string {
	seen := map[string]bool{}
	var out []string
	for _, ep := range eps {
		for _, f := range ep.Ignored {
			if !seen[f] {
				seen[f] = true
				out = append(out, f)
			}
		}
	}
	sort.Strings(out)
	return out
}

// Endpoints returns a stored ServiceMonitor's endpoints (nil when absent).
func (ix *Index) Endpoints(namespace, name string) []Endpoint {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if m := ix.monitors[namespace+"/"+name]; m != nil {
		return m.Endpoints
	}
	return nil
}

// PodEndpoints returns a stored PodMonitor's endpoints (nil when absent).
func (ix *Index) PodEndpoints(namespace, name string) []Endpoint {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	if m := ix.podMonitors[namespace+"/"+name]; m != nil {
		return m.Endpoints
	}
	return nil
}
