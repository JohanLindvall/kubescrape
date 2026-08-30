package scrape

// Two monitors resolving to ONE URL on one pod are served as ONE target that
// honours BOTH endpoint declarations. kubescrape scrapes each URL once by
// design (one scrape load, one exported series identity with no monitor
// component), where prometheus-operator generates a job per (monitor,
// endpoint) and scrapes twice — so the second monitor's configuration must
// merge into the first's target rather than being dropped with it. The one
// residual divergence: the merged relabel chain is the UNION of the monitors'
// keep/drop chains applied to the single scrape (a series any monitor asked to
// drop is dropped for everyone), where the operator's two jobs each apply only
// their own.

import (
	"slices"
	"time"

	"github.com/JohanLindvall/kubescrape/internal/promdur"
	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// mergeClass classifies one servicemonitors.Endpoint field for the merge:
// either MergeMonitorEndpoint must honour it when a second monitor's endpoint
// collides on a URL, or ignoring it must be safe by construction.
type mergeClass int

const (
	// inertClass fields need no merge logic: Port/TargetPort/Path/Scheme only
	// derive the URL — the collision key, which two endpoints meeting here
	// already agree on — and Ignored is reporting metadata no target carries.
	// The bare gate must read an endpoint carrying ONLY inert fields as bare.
	inertClass mergeClass = iota
	// relabelClass: the endpoint's chain concatenates after the holder's.
	relabelClass
	// cadenceClass: interval/scrapeTimeout resolve through mergeCadence.
	cadenceClass
	// authClass: the auth/TLS group, compared and adopted whole through
	// authMaterial (endpointAuth/targetAuth/stampAuth below).
	authClass
)

// endpointMergeClass is the AUTHORITATIVE classification of every Endpoint
// field for the merge — the servicemonitors.Endpoint.secretRefs pattern: one
// declared list where there were four hand-maintained ones (the bare gate and
// the three authMaterial functions). The gate and the adopt logic stay
// hand-written field compares (the bare path is relied on to cost field
// compares and no allocation — see targetDedup.monitorHolder), so
// merge_guard_test.go is what holds them to this map: an Endpoint field
// missing here fails the sweep, and a field classified mergeable that the
// gate cannot see — or that authMaterial does not carry through the adopt
// round trip — fails the behavioural half. Without that, a newly interpreted
// field carried ALONE reads as BARE at the gate and is silently dropped on
// every URL two monitors share: no adoption, no MonitorTargetShadowed count,
// up=0 the only symptom.
var endpointMergeClass = map[string]mergeClass{
	"Port":       inertClass,
	"TargetPort": inertClass,
	"Path":       inertClass,
	"Scheme":     inertClass,
	"Ignored":    inertClass,
	// A REFUSED endpoint resolves to no URL at all (monitorEndpoint /
	// podMonitorEndpoint refuse it), so it can never be one of the two
	// endpoints a merge holds — inert by unreachability, not by indifference.
	"Refused": inertClass,

	"MetricRelabelings": relabelClass,

	"Interval":      cadenceClass,
	"ScrapeTimeout": cadenceClass,

	"InsecureSkipVerify": authClass,
	"BearerSecret":       authClass,
	"BasicAuthUser":      authClass,
	"BasicAuthPass":      authClass,
	"AuthType":           authClass,
	"AuthCredentials":    authClass,
	"TLSCA":              authClass,
	"TLSCert":            authClass,
	"TLSKey":             authClass,
	"TLSServerName":      authClass,
}

// MergeReport is what one fold of an endpoint into a served target could not
// honour silently: everything a caller must count, log or explain. It is a
// struct rather than a fourth positional bool because every bound this merge
// grows adds one — and at a call site that reads `_, _, capped, _ :=` the
// wrong blank is a refusal reported as nothing at all, which is precisely the
// class of defect the caps exist to make impossible.
type MergeReport struct {
	// AuthAdopted reports that THIS endpoint's auth/TLS group was adopted
	// whole (the holder had none): the serving credential now belongs to this
	// monitor, not the URL-holding one, and the caller records that so a later
	// conflict warning names the monitor whose material is actually served.
	AuthAdopted bool
	// AuthConflict reports the one group a single scrape cannot honour twice:
	// both sides declare auth/TLS material and it differs, so the holder's is
	// served and the caller counts the loss.
	AuthConflict bool
	// RelabelCapped reports that only a PREFIX of this endpoint's
	// metricRelabelings was folded in — the merged chain reached
	// MaxRelabelChainRules/MaxRelabelChainBytes and the rest filter nothing.
	RelabelCapped bool
	// ContributorsCapped reports that this endpoint's CONFIGURATION merged but
	// its monitor's NAME was refused from the target's contributor list, which
	// is at MaxContributorsPerTarget. Attribution is lost, not configuration —
	// see addContributor for why that is the half worth refusing.
	ContributorsCapped bool
	// Bytes is how much this fold GREW the served target's own document, in
	// TargetOwnBytes' accounting — the merged relabel chain, the contributor
	// name, an adopted auth group, a finer cadence.
	//
	// It exists because the merge arm is the one place the per-pod byte budget
	// (MaxTargetBytesPerPod, charged in server.targetDedup.add) used to charge
	// NOTHING, on the grounds that a merge copies no pod document. That is true
	// of the pod document and incomplete about the target: the merged chain is
	// bounded at MaxRelabelChainBytes and the contributor list at
	// MaxContributorsPerTarget x ~317 bytes, so a fully-merged target can grow
	// by ~26 KiB, and MaxPortsPerPod of them by ~400 KiB — bounded and
	// deterministic, needing >=32 monitors on one URL, but strictly on top of a
	// budget whose whole claim is that it charges the WHOLE target document.
	// Reporting it here lets the caller charge it, so the claim is true rather
	// than nearly true; it refuses no merge (refusing one would drop relabel
	// rules a monitor asked for, changing what is EXPORTED to bound a
	// response), it only spends the pod's budget so a later NEW url is refused
	// sooner.
	//
	// Measured only when something can have changed: the bare-endpoint gate
	// returns before the first measurement, so the common cluster-wide-monitor
	// shape stays the field compares and no walk that monitorHolder's comment
	// promises.
	Bytes int
}

// MergeMonitorEndpoint folds a monitor endpoint into the monitor-derived
// target already holding its URL, reporting whether the endpoint's auth/TLS
// material CONFLICTS with the holder's (both declare it, differently) — the
// one group a single scrape cannot honour twice; the holder's is served and
// the caller counts the loss. Everything else merges deterministically:
//
//   - metricRelabelings: the endpoint's chain is concatenated AFTER the
//     holder's, in the caller's (deterministic) encounter order. keep/drop
//     chains are order-insensitive for the union semantics and idempotent
//     under duplication, so a longer chain is always safe; a chain identical
//     to the holder's whole current chain is skipped (the same declaration
//     made twice — two monitors that agree).
//   - interval/scrapeTimeout: an explicit interval beats an empty one; two
//     explicit intervals resolve to the FINER (the coarser monitor is still
//     scraped at least as often as it asked), and the timeout travels with
//     whichever interval won — the agent's clamp-to-interval applies after.
//   - auth/TLS (bearer/basicAuth/authorization refs, the tlsConfig material,
//     insecureSkipVerify, serverName): adopted whole when exactly one side
//     carries any; identical declarations keep; differing ones conflict.
//
// A BARE endpoint (nothing mergeable) returns immediately: that is the common
// cluster-wide-monitor shape, and the caller relies on this costing field
// compares and no allocation (see targetDedup.monitorHolder in
// internal/server).
//
// What could not be honoured silently comes back in the MergeReport above.
//
// # The caller contract: each endpoint is offered to a URL AT MOST ONCE
//
// This is a FOLD, not an idempotent operation — the second offer of a chain
// appends it a second time, the second offer of a conflicting auth group
// reports a second conflict — and it cannot be made idempotent from in here:
// the only state it has is the served target, and "was THIS endpoint already
// folded" is not a question the served target can answer. Nothing on the wire
// records fold boundaries, so every test derived from the accumulated chain is
// a guess. The contiguous-run guess was tried and is wrong in both directions:
// with one Service, monitors declaring [A,B] and [A] served monitors=[] (the
// contributor list EMPTY, where the model documents it as absent only when
// Monitor alone describes the target), and a third monitor whose chain happens
// to be a run of the merged one vanished from the list entirely.
//
// So the exactness lives at the caller, which knows the identity the merge
// cannot see — the endpoint DECLARATION — and offers each (URL, endpoint) once
// per pod: internal/server's monitorOffers. It has to, because that caller
// walks the monitor endpoints once per matched SERVICE, so a pod behind two
// Services reaches this function twice with the same declaration and the union
// the API promises would otherwise be served doubled at two Services and
// tripled at three (a single Service is exactly the union, which is why
// nothing caught it for so long). internal/server's
// TestMergedChainIsTheUnionAcrossMatchingServices drives the real loop.
func MergeMonitorEndpoint(t *kubemeta.ScrapeTarget, monitor string, ep *servicemonitors.Endpoint) MergeReport {
	var rep MergeReport
	epAuth := endpointAuth(ep)
	// The bare gate: one arm per mergeable class of endpointMergeClass —
	// relabel, cadence, and the whole auth group in a single authMaterial
	// compare. A mergeable field no arm reads would make an endpoint carrying
	// only it bare, silently dropping the declaration; merge_guard_test.go
	// holds every classified field to an arm.
	if len(ep.MetricRelabelings) == 0 && ep.Interval == "" && ep.ScrapeTimeout == "" && epAuth == (authMaterial{}) {
		return rep
	}
	// In the byte budget's OWN accounting, so what the merge arm charges and
	// what the new-URL arm charges cannot drift apart (see MergeReport.Bytes).
	beforeBytes := TargetOwnBytes(t)
	contributed := false
	// stampEndpoint built t.MetricRelabelings as a fresh copy, so appending
	// never writes into the indexed monitor's own slice.
	if len(ep.MetricRelabelings) > 0 && !relabelChainsEqual(t.MetricRelabelings, ep.MetricRelabelings) {
		added, capped := appendRelabelChain(t, ep.MetricRelabelings)
		if added {
			contributed = true
		}
		rep.RelabelCapped = capped
	}
	if mergeCadence(t, ep) {
		contributed = true
	}
	if epAuth != (authMaterial{}) {
		switch targetAuth(t) {
		case authMaterial{}:
			stampAuth(t, epAuth)
			contributed = true
			rep.AuthAdopted = true
		case epAuth:
			// The same material declared twice: served as-is, nothing lost.
		default:
			rep.AuthConflict = true // the holder's material is served
		}
	}
	if contributed {
		rep.ContributorsCapped = addContributor(t, monitor)
	}
	rep.Bytes = TargetOwnBytes(t) - beforeBytes
	return rep
}

// MaxRelabelChainRules and MaxRelabelChainBytes bound the metricRelabelings a
// single SERVED target may carry once every monitor that resolved to its URL
// has folded in.
//
// servicemonitors bounds ONE endpoint's chain at parse time (maxRelabelRules /
// maxRelabelChainBytes there, both strictly under these, so a target stamped
// from a single endpoint can never arrive here already over). This bounds the
// SUM, and it is a separate bound because the merge CONCATENATES and nothing
// bounds how many monitors exist: a tenant with namespace edit rights creates
// N ServiceMonitors, each individually legal, each `selector: {}` +
// `namespaceSelector.any: true`, all colliding on one URL per pod — and the
// per-endpoint cap multiplies by N in the node document every agent fetches
// each scrape cycle. The same lesson the parse-time cap records: a bound on
// one contributor is not a bound on the total.
//
// Also, and independently: the agent walks the MERGED chain per sample.
//
// The excess is refused, the prefix served, and the caller reports it — see
// the servicemonitors bounds for why refusing the excess beats refusing the
// monitor.
const (
	MaxRelabelChainRules = 128
	MaxRelabelChainBytes = 16 << 10
)

// MaxContributorsPerTarget bounds the contributor list a single SERVED target
// carries (kubemeta.ScrapeTarget.Monitors: the URL holder plus every monitor
// whose declaration merged into it).
//
// It is the SIBLING of the relabel ceiling above and it is needed for exactly
// the same reason, against a cheaper attack: a contribution costs a finer
// `interval` and nothing else, so a tenant with edit rights in one namespace
// creates N ServiceMonitors — each `selector: {}` + `namespaceSelector.any:
// true`, each carrying no metricRelabelings at all — that all resolve to one
// URL on one pod, and NEITHER the per-endpoint parse bound nor the merged-chain
// bound is reached. The list is appended per contribution and scanned per
// append, so it was O(n²) in CPU and O(n) on the wire PER POD: measured through
// the real derivation, 2,000 such monitors put 2,000 names into ONE target and
// turned a ONE-POD node's targets document into 124,793 bytes with 40-character
// monitor names (~0.6 MB at the name length Kubernetes permits) in ~0.07 s —
// bounded, the same document is 2,777 bytes. Multiply by the pods on the node,
// re-derived and re-marshalled on every agent poll, in the singleton the chart
// requests 128Mi for with no memory limit. MaxPortsPerPod cannot
// see it — this adds no target — and it is the same lesson twice over: a bound
// on ONE contributor is not a bound on the total.
//
// What is refused is ATTRIBUTION, never CONFIGURATION: the endpoint's relabel
// rules, cadence and auth have already merged when this binds, so the scrape is
// unchanged and only the name of a monitor that is one of many stops being
// listed. That is the right half to refuse — refusing the merge instead would
// change what is scraped to bound a diagnostic — but it is still a loss no
// consumer of the document could otherwise detect, so it is counted, warned and
// named by /v1/explain like every other refusal here.
//
// The value is a ceiling on the pathological, not a budget for the normal: real
// collisions are two or three monitors, and 32 names of the length Kubernetes
// permits (a namespace and a name, 317 bytes) is ~10 KiB of worst-case wire per
// target — the same order as MaxRelabelChainBytes.
const MaxContributorsPerTarget = 32

// appendRelabelChain folds a chain into the target under the ceiling above,
// reporting whether anything was appended and whether anything was refused.
//
// The held size is recomputed per merge rather than carried on the target: the
// wire model (kubemeta.ScrapeTarget) is served to agents and must not grow a
// bookkeeping field, chains are small by construction of the bound itself, and
// a merge happens once per (URL, endpoint) per pod per derivation — not per
// sample, where the cost would matter.
func appendRelabelChain(t *kubemeta.ScrapeTarget, add []kubemeta.RelabelRule) (added, capped bool) {
	held := 0
	for i := range t.MetricRelabelings {
		held += relabelRuleBytes(&t.MetricRelabelings[i])
	}
	for i := range add {
		if len(t.MetricRelabelings) >= MaxRelabelChainRules {
			return added, true
		}
		n := relabelRuleBytes(&add[i])
		if held+n > MaxRelabelChainBytes {
			return added, true
		}
		held += n
		t.MetricRelabelings = append(t.MetricRelabelings, add[i])
		added = true
	}
	return added, false
}

// relabelLabelBytes is what ONE sourceLabels entry costs beyond its own
// characters: its JSON framing in the marshalled node-targets document (two
// quotes and a comma) and its slice slot and per-sample visit in the agent's
// relabelFilter. It is charged for the reason its twin in servicemonitors is
// (servicemonitors.relabelLabelBytes, which carries the measurement): a list of
// EMPTY strings walks straight past an accounting that charges only characters,
// so a rule costed at 2 bytes marshals to 1.5 MB in every target. The two doors
// must stay the same shape, which is why the constant exists on both sides.
const relabelLabelBytes = 3

// relabelRuleBytes is one rule's cost in the two places the ceiling defends:
// the marshalled node-targets document, and the per-sample walk in the agent's
// relabelFilter. The action is one of two constants and is not charged.
func relabelRuleBytes(r *kubemeta.RelabelRule) int {
	n := len(r.Regex)
	for _, l := range r.SourceLabels {
		n += len(l) + relabelLabelBytes
	}
	return n
}

// mergeCadence resolves the interval/scrapeTimeout pair, reporting whether the
// endpoint's cadence was adopted. The timeout has no meaning apart from the
// interval it was written against (the agent clamps it to that interval), so
// the two always travel together — including an empty timeout replacing a set
// one when its interval wins.
func mergeCadence(t *kubemeta.ScrapeTarget, ep *servicemonitors.Endpoint) bool {
	switch {
	case ep.Interval == "" && t.Interval == "":
		// Neither sets an interval. A lone timeout is still cadence: adopt it
		// when only the endpoint carries one; when both do, the holder's keeps
		// (there is no interval to decide by, and the choice must be
		// deterministic).
		if t.ScrapeTimeout == "" && ep.ScrapeTimeout != "" {
			t.ScrapeTimeout = ep.ScrapeTimeout
			return true
		}
		return false
	case ep.Interval == "":
		return false // the holder's explicit interval beats the empty one
	case t.Interval == "":
		t.Interval, t.ScrapeTimeout = ep.Interval, ep.ScrapeTimeout
		return true
	}
	// Both explicit and spelled the SAME: the holder keeps, which is what the
	// comparison below decides anyway (equal durations are not finer) — and
	// that is by far the ordinary collision, two charts' monitors both saying
	// `interval: 30s` on one endpoint. Short-circuited because promDuration is
	// a REGEXP: an alloc profile of a colliding-monitor derivation found
	// regexp.FindStringSubmatch at 48.6% of every object it allocated, two
	// parses per merge, to re-derive an answer the strings already give. It is
	// equivalent on the unparseable case too (both sides fail, holder keeps).
	if t.Interval == ep.Interval {
		return false
	}
	// Otherwise the finer wins. Incomparable (either side unparseable — the
	// agent warns once and falls back at scrape time) keeps the holder's,
	// deterministically.
	held, hok := promDuration(t.Interval)
	asked, aok := promDuration(ep.Interval)
	if !hok || !aok || asked >= held {
		return false
	}
	t.Interval, t.ScrapeTimeout = ep.Interval, ep.ScrapeTimeout
	return true
}

// authMaterial is the auth/TLS group of an endpoint — the fields that select
// WHAT credential and trust a scrape presents. It is one group, compared and
// adopted whole: mixing one monitor's client cert with another's CA (or
// serverName, or skip-verify) would build a TLS client neither CR describes.
// Its fields correspond 1:1 to endpointMergeClass's authClass entries, and
// merge_guard_test.go pins the endpointAuth → stampAuth → targetAuth round
// trip, so a field cannot enter one of the three functions and miss another.
type authMaterial struct {
	insecureSkipVerify bool
	bearer             string
	basicAuthUser      string
	basicAuthPass      string
	authType           string
	authCredentials    string
	tlsCA              string
	tlsCert            string
	tlsKey             string
	tlsServerName      string
}

func endpointAuth(ep *servicemonitors.Endpoint) authMaterial {
	return authMaterial{
		insecureSkipVerify: ep.InsecureSkipVerify,
		bearer:             ep.BearerSecret,
		basicAuthUser:      ep.BasicAuthUser,
		basicAuthPass:      ep.BasicAuthPass,
		authType:           ep.AuthType,
		authCredentials:    ep.AuthCredentials,
		tlsCA:              ep.TLSCA,
		tlsCert:            ep.TLSCert,
		tlsKey:             ep.TLSKey,
		tlsServerName:      ep.TLSServerName,
	}
}

func targetAuth(t *kubemeta.ScrapeTarget) authMaterial {
	return authMaterial{
		insecureSkipVerify: t.InsecureSkipVerify,
		bearer:             t.AuthSecret,
		basicAuthUser:      t.BasicAuthUser,
		basicAuthPass:      t.BasicAuthPass,
		authType:           t.AuthType,
		authCredentials:    t.AuthCredentials,
		tlsCA:              t.TLSCA,
		tlsCert:            t.TLSCert,
		tlsKey:             t.TLSKey,
		tlsServerName:      t.TLSServerName,
	}
}

func stampAuth(t *kubemeta.ScrapeTarget, a authMaterial) {
	t.InsecureSkipVerify = a.insecureSkipVerify
	t.AuthSecret = a.bearer
	t.BasicAuthUser = a.basicAuthUser
	t.BasicAuthPass = a.basicAuthPass
	t.AuthType = a.authType
	t.AuthCredentials = a.authCredentials
	t.TLSCA = a.tlsCA
	t.TLSCert = a.tlsCert
	t.TLSKey = a.tlsKey
	t.TLSServerName = a.tlsServerName
}

// addContributor records a monitor whose configuration the target now carries,
// reporting whether the list was at MaxContributorsPerTarget and the name was
// therefore refused.
// Initialised lazily with the holder so the field stays ABSENT — and the wire
// shape unchanged — for the overwhelmingly common single-monitor target; the
// caller's encounter order keeps the list (and with it the response body and
// its ETag) deterministic. A monitor contributing through two endpoints is
// listed once.
//
// The HOLDER contributing through a second of its OWN endpoints is not a
// second monitor: the caller walks every endpoint of one monitor against one
// pod, so two endpoints of one CR resolving to one URL (the same port spelled
// twice, or one carrying an interval the other lacks) reach the merge with
// monitor == t.Monitor. Seeding the list there shipped `"monitors":["ns/x"]`
// on a target only ONE monitor describes — which the model documents as absent
// in that case, and which a consumer reads as "more than one monitor resolved
// to this URL".
func addContributor(t *kubemeta.ScrapeTarget, monitor string) (capped bool) {
	if len(t.Monitors) == 0 {
		if monitor == t.Monitor {
			return false
		}
		t.Monitors = append(t.Monitors, t.Monitor)
	}
	// The membership test runs BEFORE the ceiling, so a monitor already listed
	// is never reported as refused however full the list is — it contributed,
	// and it is on the wire. Over a list this short the scan is cheaper than
	// the map that would replace it, and the ceiling is what keeps it short:
	// unbounded, it was the O(n²) half of this function.
	if slices.Contains(t.Monitors, monitor) {
		return false
	}
	if len(t.Monitors) >= MaxContributorsPerTarget {
		return true
	}
	t.Monitors = append(t.Monitors, monitor)
	return false
}

// relabelChainsEqual is the identical-declaration test: two monitors asking
// for exactly the same chain get it once, and the second is not a contributor
// (nothing of its declaration was lost — see the "bare and identical merge
// silently" rule). It is deliberately NOT a subsequence or substring test: a
// monitor whose rules merely appear inside the accumulated chain made its own
// declaration and must be listed as a contributor, whatever the other monitors
// happen to have asked for.
func relabelChainsEqual(a, b []kubemeta.RelabelRule) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Action != b[i].Action || a[i].Regex != b[i].Regex ||
			!slices.Equal(a[i].SourceLabels, b[i].SourceLabels) {
			return false
		}
	}
	return true
}

// promDuration parses a monitor's interval for the ONE comparison the merge
// makes (which of two explicit intervals is finer), through internal/promdur —
// the same parser the agent's promscrape schedules by, so the interval this
// merge picks as "finer" is finer under the reading that will actually scrape.
// The edge rule that is OURS, not the parser's: "0" (and anything else
// non-positive) is not a usable interval, so ok is false for it exactly as for
// an overflowing or unparseable value, which the merge reads as incomparable
// (the holder keeps); the agent independently warns on such a value at scrape
// time.
func promDuration(s string) (time.Duration, bool) {
	d, err := promdur.Parse(s)
	return d, err == nil && d > 0
}
