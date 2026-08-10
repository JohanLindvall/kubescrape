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
// authAdopted reports that THIS endpoint's auth/TLS group was adopted whole
// (the holder had none): the serving credential now belongs to this monitor,
// not the URL-holding one, and the caller records that so a later conflict
// warning names the monitor whose material is actually served.
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
func MergeMonitorEndpoint(t *kubemeta.ScrapeTarget, monitor string, ep *servicemonitors.Endpoint) (authAdopted, authConflict bool) {
	epAuth := endpointAuth(ep)
	// The bare gate: one arm per mergeable class of endpointMergeClass —
	// relabel, cadence, and the whole auth group in a single authMaterial
	// compare. A mergeable field no arm reads would make an endpoint carrying
	// only it bare, silently dropping the declaration; merge_guard_test.go
	// holds every classified field to an arm.
	if len(ep.MetricRelabelings) == 0 && ep.Interval == "" && ep.ScrapeTimeout == "" && epAuth == (authMaterial{}) {
		return false, false
	}
	contributed := false
	// stampEndpoint built t.MetricRelabelings as a fresh copy, so appending
	// never writes into the indexed monitor's own slice.
	if len(ep.MetricRelabelings) > 0 && !relabelChainsEqual(t.MetricRelabelings, ep.MetricRelabelings) {
		t.MetricRelabelings = append(t.MetricRelabelings, ep.MetricRelabelings...)
		contributed = true
	}
	if mergeCadence(t, ep) {
		contributed = true
	}
	if epAuth != (authMaterial{}) {
		switch targetAuth(t) {
		case authMaterial{}:
			stampAuth(t, epAuth)
			contributed = true
			authAdopted = true
		case epAuth:
			// The same material declared twice: served as-is, nothing lost.
		default:
			authConflict = true // the holder's material is served
		}
	}
	if contributed {
		addContributor(t, monitor)
	}
	return authAdopted, authConflict
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
	// Both explicit: the finer wins. Equal or incomparable (either side
	// unparseable — the agent warns once and falls back at scrape time) keeps
	// the holder's, deterministically.
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

// addContributor records a monitor whose configuration the target now carries.
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
func addContributor(t *kubemeta.ScrapeTarget, monitor string) {
	if len(t.Monitors) == 0 {
		if monitor == t.Monitor {
			return
		}
		t.Monitors = append(t.Monitors, t.Monitor)
	}
	if !slices.Contains(t.Monitors, monitor) {
		t.Monitors = append(t.Monitors, monitor)
	}
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
