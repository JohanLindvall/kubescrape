package servicemonitors

// The ceilings on the tenant-supplied endpoint strings scrape copies onto every
// target, and enforceFieldBounds, which refuses an endpoint over one.

import (
	"strings"
)

// Ceilings on the tenant-supplied endpoint STRINGS that scrape copies onto
// EVERY target the endpoint resolves to (stampEndpoint, makeTarget). They are
// the metricRelabelings ceiling's siblings and they exist for the identical
// reason — the chain was bounded and every other string on the same derivation
// was left alone, which is this package's own lesson ("a bound on ENTRIES is not
// a bound on BYTES") applied to one field and not to its neighbours.
//
// Measured through the real derivation (MonitorTargets + json.Marshal): ONE
// endpoint with `path: /<1 MiB of a>` and no metricRelabelings at all — so
// neither relabel ceiling is approached, and the CR is well inside etcd's
// object limit — yields ONE target of 2,097,625 bytes, because the path is
// copied into both t.URL and t.Path. /v1/nodes/{node}/targets embeds one such
// target per matched pod and is re-derived and re-marshalled on every agent
// poll (writeCached must build the body to hash the ETag, so a 304 does not
// save it): ~220 MiB per request on a 110-pod node, once per scrape cycle per
// node, in the singleton the chart requests 128Mi for with no memory limit.
// The tenant needs edit rights in ONE namespace and a `selector: {}` +
// `namespaceSelector.any: true` monitor, which the default -monitor-namespaces
// honours. scrape.MaxPortsPerPod cannot see it: it bounds the target COUNT
// against a per-target cost it models as the ~2 KiB pod document.
//
// Every ceiling is far above what the field can legitimately hold — a scrape
// path is a few tens of bytes, an SNI name cannot exceed 253 (RFC 1035), a
// duration is under twenty, an `authorization.type` is "Bearer", and a rendered
// "name/key" secret reference cannot exceed 507 even with two maximal DNS-1123
// subdomains — so no real monitor can reach one.
const (
	maxEndpointPathBytes       = 2 << 10
	maxEndpointServerNameBytes = 256
	maxEndpointAuthTypeBytes   = 64
	maxEndpointDurationBytes   = 64
	maxEndpointSecretRefBytes  = 768
)

// cappedSuffix marks a report entry for a LIST kubescrape does interpret but
// has kept only a prefix of. It is the counterpart of oversizeSuffix, which
// marks a refusal: a capped list still yields targets, an oversize field does
// not. Spelled once so the two ceilings that use it — the relabel chain and the
// endpoint list — cannot drift into two words for one outcome.
const cappedSuffix = "(capped)"

// oversizeSuffix marks a report entry for a field kubescrape DOES interpret but
// REFUSED for its size, the spelling relabelOversizeIgnored already uses.
const oversizeSuffix = "(oversize)"

// boundedField is one tenant-supplied endpoint string and its ceiling.
type boundedField struct {
	name  string
	value *string
	max   int
}

// boundedFields returns a POINTER to every endpoint string that scrape stamps
// onto a target, with the ceiling it is held to. It is the sibling of
// secretRefs and it is one list for the same reason: a new string field on
// kubemeta.ScrapeTarget that is fed from an endpoint and forgotten here is
// unbounded, and it fails nowhere until somebody sends a megabyte through it.
// TestEveryTenantSuppliedEndpointStringIsBounded walks every Endpoint string against
// this list, so the omission is a TEST failure rather than a finding.
//
// Deliberately NOT here:
//
//   - Scheme, which defaultSchemePath maps to one of two constants, so its
//     size cannot reach a target however long the CR spells it.
//   - Port and TargetPort, which are resolution INPUTS (a port name is looked
//     up and the resolved int32 is what a target carries); an absurd one
//     resolves to nothing and is retained once in the index, not per target.
//   - MetricRelabelings, bounded by the relabel chain's own ceilings
//     (maxRelabelRules and its siblings, relabel.go).
func (e *Endpoint) boundedFields() []boundedField {
	return []boundedField{
		{"path", &e.Path, maxEndpointPathBytes},
		{"interval", &e.Interval, maxEndpointDurationBytes},
		{"scrapeTimeout", &e.ScrapeTimeout, maxEndpointDurationBytes},
		{"tlsConfig.serverName", &e.TLSServerName, maxEndpointServerNameBytes},
		{"authorization.type", &e.AuthType, maxEndpointAuthTypeBytes},
		{"authorization.credentials", &e.AuthCredentials, maxEndpointSecretRefBytes},
		{"basicAuth.username", &e.BasicAuthUser, maxEndpointSecretRefBytes},
		{"basicAuth.password", &e.BasicAuthPass, maxEndpointSecretRefBytes},
		{"bearerTokenSecret", &e.AuthSecret, maxEndpointSecretRefBytes},
		{"tlsConfig.ca", &e.TLSCA, maxEndpointSecretRefBytes},
		{"tlsConfig.cert", &e.TLSCert, maxEndpointSecretRefBytes},
		{"tlsConfig.keySecret", &e.TLSKey, maxEndpointSecretRefBytes},
	}
}

// enforceFieldBounds holds every string above to its ceiling and REFUSES the
// endpoint — no targets at all — when one is over.
//
// Refused, never TRUNCATED and never dropped-to-the-default, and the three
// outcomes are genuinely different:
//
//   - A truncated path scrapes a DIFFERENT URL than the CR names, silently and
//     forever. So does dropping the path, which defaults it to /metrics — and
//     that one is worse still, because /metrics is very often a URL the pod
//     ALREADY has a target for, so the refused endpoint's relabel chain, auth
//     material and cadence would merge (scrape.MergeMonitorEndpoint) onto
//     somebody else's working target: a tenant's drop rules applied to a scrape
//     they never named.
//   - Dropping a serverName or a credential ref quietly downgrades a security
//     decision the operator made — verification against a name they chose, or
//     a scrape that authenticates.
//
// So the endpoint yields nothing, which is the one outcome that cannot be
// mistaken for the CR being honoured, and it says so three ways: the refusal
// rides Endpoint.Ignored (hence kubescrape_monitor_fields_ignored_total and the
// per-upsert warning, exactly as relabelCappedIgnored does), Endpoint.Refused
// names the fields for /v1/explain, and the values are dropped so the index
// retains none of them. The MONITOR is never rejected — that would take every
// target its other endpoints contribute with it, the same trade the relabel
// ceiling makes.
//
// Port/TargetPort are cleared too, so an endpoint that is refused here resolves
// to nothing through the port door as well: a caller that has not learned to
// read Refused must not end up scraping the default path unauthenticated, which
// is precisely the silent misapplication above.
//
// preRefused carries the field names of refusals decided BEFORE this call and
// already reported in Ignored — today the relabel chain's, which refuses the
// endpoint for an oversized rule, an uncompilable regex or a keep/drop that
// depends on an unapplied mutating rule (relabelChain; see its ceilings in
// relabel.go for why those refuse the endpoint while the aggregate ceilings
// keep a prefix). They are passed in
// rather than re-derived here so that ONE function decides what a refused
// endpoint looks like, and appended to Refused only — the Ignored entry is the
// walk's to write, and writing it twice would double-count
// kubescrape_monitor_fields_ignored_total.
func (e *Endpoint) enforceFieldBounds(preRefused ...string) {
	refused := preRefused
	for _, f := range e.boundedFields() {
		if len(*f.value) > f.max {
			refused = append(refused, f.name)
			e.Ignored = append(e.Ignored, f.name+oversizeSuffix)
		}
	}
	if len(refused) == 0 {
		return
	}
	e.Refused = strings.Join(refused, ",")
	for _, f := range e.boundedFields() {
		*f.value = ""
	}
	// The chain goes too: this endpoint yields no target, so a retained chain
	// is bytes the index holds for the life of the CR and filters nothing.
	e.MetricRelabelings = nil
	e.Port, e.TargetPort = "", nil
}
