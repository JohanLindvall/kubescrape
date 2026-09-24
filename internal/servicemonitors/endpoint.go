package servicemonitors

// The Endpoint model, the CRD's endpoint decode shape (endpointSpec) and the
// conversion between them, including the secret-reference list
// (Endpoint.secretRefs) both parsers and the /v1/scrape-auth allowlist use.

import (
	"encoding/json"
	"strings"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
	"k8s.io/apimachinery/pkg/util/intstr"
)

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
	// ScrapeAuth is the endpoint's auth/TLS group, the very struct a target
	// carries (so scrape stamps it with one assignment): InsecureSkipVerify
	// from tlsConfig, AuthSecret from bearerTokenSecret, the basicAuth and
	// authorization refs, and tlsConfig's secret-backed ca/cert/keySecret plus
	// its serverName. Every reference is "namespace/name/key" (namespace = the
	// monitor's), resolved by agents through the -scrape-auth-secrets channel.
	// kube-prometheus-stack's own control-plane monitors (etcd, scheduler,
	// controller-manager) use client certificates, and anything behind a
	// private CA was previously scrapeable only by turning verification off
	// entirely.
	kubemeta.ScrapeAuth
	// MetricRelabelings holds the keep/drop subset of the endpoint's
	// metricRelabelings; other actions are ignored (documented).
	MetricRelabelings []RelabelRule
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
	// Refused names the field(s) whose size took this endpoint past
	// enforceFieldBounds' ceilings, and is empty for every ordinary endpoint.
	// A refused endpoint yields NO targets: the string that was over the
	// ceiling is the URL to scrape, the name to verify a certificate against or
	// the credential to present, and none of those has a safe shorter form —
	// see enforceFieldBounds for why a refusal beats both a truncation and a
	// fallback to the default. internal/scrape reads it at both endpoint
	// resolvers and /v1/explain reports it (scrape.MonitorEndpointNote).
	Refused string
}

// RefusalReasons returns the Ignored entries behind Refused — each names the
// field and WHY it refused the endpoint (`path(oversize)`,
// `metricRelabelings.regex(invalid)`, `metricRelabelings.action(dependent)`) —
// for /v1/explain, which would otherwise have to guess the reason from the bare
// field names Refused carries. Empty when the endpoint is not refused.
func (e Endpoint) RefusalReasons() []string {
	if e.Refused == "" {
		return nil
	}
	var out []string
	for _, f := range e.Ignored {
		if strings.HasSuffix(f, oversizeSuffix) || strings.HasSuffix(f, invalidSuffix) ||
			strings.HasSuffix(f, dependentSuffix) {
			out = append(out, f)
		}
	}
	return out
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
		&e.AuthSecret,
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
		// honouring a rule of TWO OR MORE sourceLabels that asked for a
		// different separator would build a different string than the user's
		// regex was written against. For a keep rule that inverts the intent —
		// it matches nothing and drops everything the user meant to keep. With
		// zero or one sourceLabels no separator is ever written, by the agent
		// or by Prometheus, so such a rule is applied exactly as written.
		Separator string `json:"separator"`
		// Parsed only for the label-MUTATING actions kubescrape does not apply
		// (replace, hashmod, lowercase, uppercase): it names the label such a
		// rule would have written, which a LATER keep/drop may read. See
		// relabelWrites.
		TargetLabel string `json:"targetLabel"`
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

// ignoredFields lists the endpoint fields that are set but not interpreted,
// ending with relabelIgnored — the relabel chain's own report, supplied by the
// caller so that this stays the ONE place the endpoint's report is assembled
// while the chain is still walked only once. toEndpoint walks it (relabelChain)
// and hands out both halves of that one verdict: the report here, the rules and
// the refusal to the endpoint. The walk parses every regex it admits and
// compiles every labeldrop/labelkeep it tracks, so a second walk would not be
// free.
func (ep endpointSpec) ignoredFields(relabelIgnored []string) []string {
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
	// The relabel chain's own report comes from the ONE walk that also decides
	// which rules are applied (relabelChain): the two verdicts must agree, and
	// they were parallel loops here and in toEndpoint until the size bounds
	// gave them something non-trivial to disagree about.
	return append(out, relabelIgnored...)
}

// toEndpoint converts the spec shape (the secret refs' namespace filled by the
// caller).
func (ep endpointSpec) toEndpoint() Endpoint {
	// ONE walk of the chain, both halves of whose verdict are used below: the
	// report onto Ignored, the rules and the refusal onto the endpoint.
	rules, relabelIgnored, refusedByChain := ep.relabelChain()
	out := Endpoint{
		Port: ep.Port, TargetPort: ep.TargetPort, Path: ep.Path,
		Scheme:   canonicalScheme(ep.Scheme),
		Interval: ep.Interval, ScrapeTimeout: ep.ScrapeTimeout,
		Ignored: ep.ignoredFields(relabelIgnored),
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
	out.AuthSecret = ep.BearerTokenSecret.ref()
	// The chain's report half is already on out.Ignored, but a chain refusal is
	// not a report, it is a REFUSAL, and it is carried into enforceFieldBounds
	// so one function decides what a refused endpoint looks like.
	out.MetricRelabelings = rules
	var preRefused []string
	if refusedByChain != "" {
		preRefused = []string{refusedByChain}
	}
	// Last, because it reads the fields every branch above fills in — including
	// the RENDERED secret references rather than their CRD halves, which is the
	// form that reaches a target.
	out.enforceFieldBounds(preRefused...)
	return out
}

// canonicalScheme folds an endpoint's `scheme` to the lower-case spelling
// everything downstream compares against. The CRD's enum is
// `http;https;HTTP;HTTPS`, the upper-case spelling is the one its own field
// documentation shows ("Supported values are HTTP and HTTPS"), and
// prometheus-operator lower-cases it when it renders scrape config. Carried
// verbatim, `HTTPS` fell through scrape's `!= "https"` default to plain http —
// and the agent attaches the endpoint's bearer/authorization/basic credential
// without looking at the scheme, so a monitor that asked for TLS sent its
// secret in cleartext with its tlsConfig unused and nothing reporting it. The
// fold happens at this ONE parse door, so both kinds and every reader
// downstream (targets, explain, merge) see one spelling; internal/scrape folds
// again at its monitor doors, which keeps a hand-built Endpoint honest too.
//
// Only the two recognised values are rewritten, and never by copying: any
// other value is kept verbatim (scrape maps it to http, as before), so a long
// tenant string is neither lower-cased into a second copy nor walked.
func canonicalScheme(scheme string) string {
	switch {
	case len(scheme) == len("https") && strings.EqualFold(scheme, "https"):
		return "https"
	case len(scheme) == len("http") && strings.EqualFold(scheme, "http"):
		return "http"
	}
	return scheme
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
