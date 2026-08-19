package otlpingest

// Reserved-attribute stripping at first receipt.
//
// kubescrape reserves a few attribute keys as its own plumbing, and their
// consumers are PRESENCE-ONLY — neither can tell kubescrape's own mark from a
// sender's. The namespace router honors its script marker on a RESOURCE
// before any namespace glob, so a wire-supplied copy selects any configured
// route and that route's tenant headers; the transform engine's post-script
// prune deletes any marked ELEMENT whenever a script for its signal is
// active, and counts the deletion as an operator-intended
// kubescrape_transform_dropped_total. On listeners nothing authenticates, the
// wire copy must therefore die at first receipt — the application-facing hop,
// the same seam where enrichment and peer attribution happen. The trace
// tier's INTERNAL receiver is deliberately not on this path: what arrives
// there is kubescrape-to-kubescrape and was sanitized when an application
// pushed it.
//
// The SECOND half is the sender's own identity claim (ReservedAttrs.Identity,
// built by Enricher.SenderIdentityStrip, which owns the argument). It rides the
// same walk and is otherwise a different animal: those keys are ones honest
// senders set, so the removal is counted and logged apart from the plumbing
// markers — see ReservedAttrs.
//
// WHICH keys are reserved is the caller's knowledge (ServerConfig.
// ReservedAttrs, wired in cmd/kubescrape-agent), exactly as RejectTraces
// keeps the loop marker's: this package must not know route's or transform's
// spellings.

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/obs"
)

// ReservedAttrs names the attribute keys an application-facing receiver strips
// from every accepted payload at first receipt (both transports, all three
// signals). The zero value strips nothing, which is what any receiver outside
// the two application-facing ones wants.
//
// Resource/Element and Identity are two DIFFERENT EVENTS and are reported
// separately, because they mean opposite things about the sender: a plumbing
// marker on the wire is a key nothing but kubescrape has a reason to set, so
// any rate at all is worth finding, while an identity claim is what every
// conformant SDK ships (an OpenTelemetry-Operator-instrumented pod sets five of
// them) and its removal is a routine, expected correction. Counting them into
// one series — and warning about both in one line that says "reserved for
// kubescrape's own plumbing" — gave a healthy cluster a permanently climbing
// counter, a stream of Warns accusing honest senders, and an alert on the
// metric's own help text that can never clear.
type ReservedAttrs struct {
	// Resource keys are kubescrape's own plumbing markers on a resource — the
	// router reads its script marker there and nowhere else. Counted
	// kubescrape_ingest_reserved_stripped_total{key}, warned throttled per key.
	Resource []string
	// Element keys are kubescrape's plumbing markers on an ELEMENT: stripped
	// from log-record attributes, span attributes, metric Metadata and every
	// data point's attributes across all five metric types — everywhere the
	// transform prune reads its drop marker. Same counter and warn as Resource.
	Element []string
	// Identity keys are the sender's own identity CLAIM on a resource
	// (Enricher.SenderIdentityStrip owns which). Counted
	// kubescrape_ingest_identity_stripped_total{key} and logged at DEBUG,
	// throttled per key: this fires for well-behaved senders by design.
	Identity []string
}

func (r ReservedAttrs) empty() bool {
	return len(r.Resource) == 0 && len(r.Element) == 0 && len(r.Identity) == 0
}

// senderControlledIdentity are attrs.ReservedIdentity keys an application-facing
// receiver deliberately LEAVES to the sender, even though a pod annotation may
// not set them.
//
// The argument is the one that already exempts service.name, applied honestly:
// the OTLP service triple (service.namespace, service.name, service.instance.id)
// is how a sender names ITSELF to the backend, and service.name — the one that
// picks which workload's series a sample joins — is sender-controlled by design.
// A sender that wants to masquerade as another workload's series therefore only
// has to declare that workload's service.name; deleting the two dimensions
// beside it buys nothing an attacker cannot walk around, and costs an honest
// sender its instance dimension — half of the Prometheus/Tempo job+instance
// pair. Neither key steers kubescrape's OWN behaviour: routing keys on
// k8s.namespace.name and nothing reads these. Stripping them would leave a
// plain-SDK sender (the common shape on the trace tier: spans from every
// namespace, no k8s attributes, peer-IP fallback off) with no instance identity
// at all.
//
// This list is shared with Enricher.resolvedWins, which is the whole point:
// the exemption has to mean the same thing on the RESOLVED path, and the
// far more common one. It briefly did not — the strip preserved the two keys
// and the resolved-wins overwrite then destroyed them for every sender the
// lookup succeeded on, excused here as "mergeAttrs corrects them anyway". There
// is nothing to correct: neither key steers any kubescrape behaviour, which is
// this exemption's own premise.
var senderControlledIdentity = []string{"service.namespace", "service.instance.id"}

// SenderIdentityStrip returns the resource keys an application-facing receiver
// should strip at first receipt as a sender's identity CLAIM, for THIS
// enricher's configuration — ReservedAttrs.Identity, counted and logged apart
// from the caller's plumbing markers because it fires for honest senders.
//
// Why a sender's identity claim must die at the door: internal/agent/route keys
// TENANCY on k8s.namespace.name, so a pod that declares someone else's
// namespace has its records exported to that tenant's endpoint under that
// tenant's headers — the same attack attrs.ReservedIdentity spells out for the
// pod-annotation surface, which refuses it and counts a metric, reached here
// through a listener with no credentials at all. The k8s.pod.*/k8s.node.name/
// container.* siblings go with it because they are exactly the keys a resolved
// lookup overwrites (mergeAttrs), so on a resolvable resource the strip changes
// nothing, and on an unresolvable one a claim nobody can check would otherwise
// name someone else's pod on every series and every log-derived metric bound to
// the resource. Enrichment CANNOT stand in for this: a resource with no
// resolvable id — every sender that is not a pod on this node, and every push
// with the peer-IP fallback off — has no correction available, and there the
// sender's declaration was the only namespace there was.
//
// What is deliberately NOT stripped:
//
//   - service.name, which attrs.ReservedIdentity leaves out on purpose: it is
//     DESCRIPTIVE, and naming its own service is the whole point of a sender.
//
//   - service.namespace and service.instance.id, for the reason recorded at
//     senderControlledIdentity: the same argument that exempts service.name.
//     Enricher.resolvedWins exempts the same two, so a resolved sender keeps
//     what an unresolved one keeps.
//
//   - the LOOKUP KEYS (this enricher's -ingest-container-id-keys /
//     -ingest-pod-uid-keys), because stripping them would disable attribution
//     entirely: the strip runs BEFORE enrichment (sanitizeLogs -> EnrichLogs),
//     so a receiver that removed them would resolve nothing at all. That is
//     the trap in "just strip every identity key".
//
//     THE RESIDUAL THIS LEAVES, stated as the trade it is rather than as a
//     harmlessness claim: the exact tenancy crossing the strip exists to stop
//     is still reachable through the lookup keys, one hop over. The metadata
//     service's /v1/pods/{ns}/{name} is UNAUTHENTICATED by design and serves
//     each container's id and the pod UID, and the store's container index is
//     CLUSTER-wide rather than node-scoped. So a pod that can reach both
//     services reads a victim's container.id, pushes it with no
//     k8s.namespace.name of its own, and mergeAttrs writes the victim's
//     namespace onto the resource — which route.match then keys tenancy on,
//     sending the payload to that tenant's endpoint and X-Scope-OrgID. The
//     strip removes nothing relevant on that path, because the forged value
//     never appears on the wire: it is DERIVED, correctly, from a stolen id.
//
//     It is documented rather than closed because the obvious closure does
//     not work. Refusing a resolved object on another NODE (Config.NodeInfo)
//     would cost legitimate attribution — the datapoint/split path exists so
//     a sender can describe objects across the cluster, the trace tier
//     receives from every node by design, and a DaemonSet receiver fronted by
//     a Service round-robins away from the sender's own node — and it would
//     not even close the hole, since a co-located victim (the common case in
//     a cluster that spreads namespaces across nodes) is still reachable.
//     Narrowing the victim set while silently un-attributing honest senders is
//     a worse bargain than the residual. What DOES close it is authenticating
//     the listener or the metadata service, neither of which is on offer here.
//
// It is derived from attrs.ReservedIdentityKeys() rather than spelled out, so a
// key that becomes resolved identity there is covered here by default; the two
// exclusion lists are what an exemption has to be argued into.
//
// The cost, stated so it is a decision and not a surprise: a sender that this
// receiver cannot resolve LOSES its own honestly-declared k8s.pod.name and
// friends, because nothing here can tell an honest declaration from a forged
// one. That is the trade the unauthenticated listener forces; the alternative
// is a routing key any pod may choose.
func (e *Enricher) SenderIdentityStrip() []string {
	all := attrs.ReservedIdentityKeys()
	out := make([]string, 0, len(all))
	for _, k := range all {
		if slices.Contains(e.containerIDKeys, k) || slices.Contains(e.podUIDKeys, k) {
			continue
		}
		if slices.Contains(senderControlledIdentity, k) {
			continue
		}
		out = append(out, k)
	}
	return out
}

// reservedWarnEvery is the per-key re-warn cadence: the condition is a state
// (a sender that ships a reserved key ships it on every push), and the useful
// information is one line naming the key.
const reservedWarnEvery = time.Minute

// stripReserved removes each reserved PLUMBING key present in m, counting and
// (per key, throttled) warning every removal. keys is the operator-wired list,
// so a clean map costs one probe per configured key and allocates nothing — the
// element walk runs per record/point/span and must stay free.
//
// Warn is the right level here and only here: these keys are kubescrape's own
// markers, which no SDK and no conformant sender has any reason to emit, so a
// single occurrence is a fact an operator wants told.
func (s *Server) stripReserved(m pcommon.Map, keys []string) {
	for _, k := range keys {
		if !m.Remove(k) {
			continue
		}
		obs.IngestReservedStripped.WithLabelValues(k).Inc()
		if allow, _ := s.reservedWarns.Allow(k); allow {
			s.log.Warn("ingest: a sender shipped an attribute reserved for kubescrape's own plumbing; stripped at receipt so wire data cannot steer routing or masquerade as an operator-intended drop",
				"key", k)
		}
	}
}

// stripIdentity removes each sender-identity key present in m — the same walk
// as stripReserved, reported as the DIFFERENT event it is.
//
// DEBUG, not Warn: every conformant SDK names its own pod, and a workload
// instrumented by the OpenTelemetry Operator ships five of these keys on every
// single push, so a Warn here accuses the whole fleet of honest senders once a
// minute per key forever. The throttle is kept anyway — a debug-enabled agent
// must not turn one chatty sender into a line per push — and the counter, not
// the log, is the observable.
//
// The LEVEL is checked before the throttle, which is the opposite order from
// stripReserved and deliberately so. logdedupe.Table.Allow is a process-wide
// mutex plus a map lookup, and it is affordable there because a plumbing
// marker is a key no conformant sender sets, so the removal — and the probe —
// essentially never happens. Here it is the reverse by construction: an
// OpenTelemetry-Operator-instrumented fleet strips ~6 keys off every resource
// of every push, so consulting the throttle first would be a lock/unlock pair
// per key per resource per push spent deciding not to write a line slog would
// discard anyway. The counter bump stays unconditional — it is the observable,
// and it must not depend on the log level.
func (s *Server) stripIdentity(m pcommon.Map, keys []string) {
	for _, k := range keys {
		if !m.Remove(k) {
			continue
		}
		obs.IngestIdentityStripped.WithLabelValues(k).Inc()
		if !s.log.Enabled(context.Background(), slog.LevelDebug) {
			continue
		}
		if allow, _ := s.reservedWarns.Allow(k); allow {
			s.log.Debug("ingest: a sender declared its own Kubernetes identity; stripped at receipt because this listener authenticates nothing and routing keys tenancy on the namespace (enrichment sets these for a resource it can resolve)",
				"key", k)
		}
	}
}

// sanitizeLogs strips the reserved plumbing keys and the sender's identity
// claim from a pushed logs payload.
func (s *Server) sanitizeLogs(ld plog.Logs) {
	ra := s.cfg.ReservedAttrs
	if ra.empty() {
		return
	}
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		s.stripReserved(rl.Resource().Attributes(), ra.Resource)
		s.stripIdentity(rl.Resource().Attributes(), ra.Identity)
		if len(ra.Element) == 0 {
			continue // the per-record walk is only worth taking for a key it can find
		}
		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			lrs := sls.At(j).LogRecords()
			for k := 0; k < lrs.Len(); k++ {
				s.stripReserved(lrs.At(k).Attributes(), ra.Element)
			}
		}
	}
}

// sanitizeTraces strips the reserved plumbing keys and the sender's identity
// claim from a pushed traces payload. It runs AFTER the RejectTraces guard — a refused payload needs no
// sanitizing — and before enrichment, like its siblings.
func (s *Server) sanitizeTraces(td ptrace.Traces) {
	ra := s.cfg.ReservedAttrs
	if ra.empty() {
		return
	}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		s.stripReserved(rs.Resource().Attributes(), ra.Resource)
		s.stripIdentity(rs.Resource().Attributes(), ra.Identity)
		if len(ra.Element) == 0 {
			continue
		}
		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			sps := sss.At(j).Spans()
			for k := 0; k < sps.Len(); k++ {
				s.stripReserved(sps.At(k).Attributes(), ra.Element)
			}
		}
	}
}

// sanitizeMetrics strips the reserved plumbing keys and the sender's identity
// claim from a pushed metrics payload: resource attributes, each metric's Metadata and every data point's
// attributes (the three places a marker could ride a metric).
func (s *Server) sanitizeMetrics(md pmetric.Metrics) {
	ra := s.cfg.ReservedAttrs
	if ra.empty() {
		return
	}
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		s.stripReserved(rm.Resource().Attributes(), ra.Resource)
		s.stripIdentity(rm.Resource().Attributes(), ra.Identity)
		if len(ra.Element) == 0 {
			continue
		}
		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				s.sanitizeMetricElements(ms.At(k), ra.Element)
			}
		}
	}
}

// sanitizeMetricElements strips Element keys from one metric's Metadata and
// from every data point's attributes — the transform prune reads its marker
// in both places (a marked metric prunes whole, a marked point alone).
func (s *Server) sanitizeMetricElements(m pmetric.Metric, keys []string) {
	s.stripReserved(m.Metadata(), keys)
	switch m.Type() {
	case pmetric.MetricTypeGauge:
		dps := m.Gauge().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			s.stripReserved(dps.At(i).Attributes(), keys)
		}
	case pmetric.MetricTypeSum:
		dps := m.Sum().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			s.stripReserved(dps.At(i).Attributes(), keys)
		}
	case pmetric.MetricTypeHistogram:
		dps := m.Histogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			s.stripReserved(dps.At(i).Attributes(), keys)
		}
	case pmetric.MetricTypeExponentialHistogram:
		dps := m.ExponentialHistogram().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			s.stripReserved(dps.At(i).Attributes(), keys)
		}
	case pmetric.MetricTypeSummary:
		dps := m.Summary().DataPoints()
		for i := 0; i < dps.Len(); i++ {
			s.stripReserved(dps.At(i).Attributes(), keys)
		}
	}
}
