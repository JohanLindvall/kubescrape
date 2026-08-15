package scrape

import (
	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Two SERVED targets that export the same series identity are two scrapes with
// ONE identity. This file is what stops that from happening silently.
//
// The agent exports a target under (service.namespace, service.name,
// service.instance.id = host:port) plus url.full, and the OTLP→Prometheus
// translation makes labels of the first three and none of the last —
// promscrape's fillTargetResource says so in as many words. `job` is
// service.namespace/service.name and `instance` is service.instance.id, so a
// target's EXPORTED IDENTITY is exactly
//
//	(pod.Namespace, attrs.ServiceName(pod), target.Address)
//
// and nothing else on the resource can tell two of them apart. So `/metrics`
// and `/actuator/prometheus` on one container port arrive as ONE (job,
// instance): every metric name the two endpoints share becomes a single series
// carrying both their values, and `up`, scrape_duration_seconds and
// scrape_samples_scraped — one point per target, with no data-point attributes
// to tell them apart — arrive twice with the same identity, the same timestamp
// and disagreeing values, which a backend reads as a conflict rather than as
// two targets.
//
// The URL dedup does not catch it, and should not: a target's identity to the
// DEDUP is its URL, because the URL is what gets fetched. It is the EXPORTED
// identity that is narrower than the derivation. The three ways out are not
// equal:
//
//   - Dedup by host:port and merge. Not available. scrape.MergeMonitorEndpoint
//     folds two DECLARATIONS OF ONE URL into the single GET that was going to
//     happen anyway, and loses nothing; two paths are two GETs. "Merging" them
//     can only mean dropping one, and a dropped target is invisible in the
//     data — indistinguishable from an application that stopped exporting
//     those series. /metrics beside /metrics/cadvisor, or an org-wide
//     `prometheus.io/scrape` beside a Spring Boot /actuator/prometheus: both
//     declarations were written on purpose.
//
//   - Widen the exported identity so the path is part of it. This is the real
//     fix, and it belongs to the AGENT: service.instance.id is stamped by
//     promscrape.fillTargetResource. It is wire-visible — it re-baselines
//     `instance` for exactly the colliding targets — and prometheus-operator's
//     own answer, a `job` per (monitor, endpoint), is not open to kubescrape,
//     whose job is the WORKLOAD's on purpose so that a pod's metrics join its
//     logs and its cadvisor series.
//
//   - Serve both and make the collision impossible to miss. That is what this
//     does, and it is what an operator is best served by while the identity is
//     still host:port. Nothing anyone configured is thrown away, and the part
//     that was genuinely broken is fixed: no counter, no log line and no
//     /v1/explain field mentioned any of this, so the only symptom was `up`
//     flapping between 0 and 1 at one timestamp on one pod — a signal that
//     names neither the pod nor the two paths.
//
// # The scope is the NODE, and it was the pod
//
// The pod-scoped predecessor missed the WORST shape while its own doc asserted
// the shape could not exist: "two hostNetwork pods share the node's address
// legitimately, and they are two workloads — they cannot even share a port, so
// no such pair can collide." Both halves are false, and this repo already said
// so in two other places.
//
//   - They CAN share a port in the TARGET. A target's port comes from the
//     `prometheus.io/port` ANNOTATION, never from an observed bind: two
//     hostNetwork pods annotated with the same port yield the same URL whether
//     or not both processes could listen on it. nodeTargets' sort carries a
//     pod-UID tiebreak for exactly this case ("identical URL, empty Monitor,
//     Source pod, but different pod documents"), and the agent's scheduleKey
//     comment says it SCRAPES BOTH.
//
//   - They are not necessarily two workloads. Two hostNetwork REPLICAS of one
//     ReplicaSet share a Deployment, so attrs.ServiceName derives one
//     service.name for both — one job, one instance, two targets.
//
// That pair is strictly worse than two paths on one port: url.full is identical
// too, so not even the one resource attribute a human could have read tells
// them apart.
//
// # A collision is the same instance AND the same job
//
// Two GENUINELY DIFFERENT workloads on hostNetwork, annotated with the same
// port, share the instance and not the job: up{job="ns/a",instance="10.0.0.5:9100"}
// and up{job="ns/b",instance="10.0.0.5:9100"} are two series. Nothing overwrites
// anything, and this file is deliberately silent about them:
//
//   - The warning would be false in its own words. It says the two targets'
//     samples collide. Theirs do not.
//   - Their real fault — one URL fetched twice, and at most one of the two
//     annotations can describe a process that is actually listening — is
//     VISIBLE: two jobs reporting byte-identical values for one endpoint, each
//     naming its own pod. Being invisible is the entire reason the (job,
//     instance) case is worth a log line.
//   - It is the shape with real false positives. Two hostNetwork DaemonSets
//     annotated for one well-known port is a thing a cluster may hold on
//     purpose, and a warning that fires for it is one operators learn to
//     filter — which is the same as not warning at all.
//
// service.name comes from attrs.ServiceName rather than from a copy of its
// rule: it IS the job the agent will export, kindTable and all, and a second
// derivation that drifts would report collisions that are not and miss ones
// that are. It is the first import of an internal/agent package by the service
// side, and it costs the service nothing: every package attrs pulls in — pdata's
// pcommon, featuregate, json-iterator, go-version — is already linked into
// cmd/kubescrape for the OTLP self-metrics push, so the whole dependency is
// +552 bytes on the unstripped binary (measured). A `resourceAttributes`
// template that overrides service.name on the agent is outside what the service
// can see, and outside what it claims.

// CollidingTarget names one member of an InstanceCollision: enough to find the
// declaration that produced it without copying a 592-byte target.
//
// Pod is part of that and not decoration. A collision group can span pods (two
// hostNetwork replicas of one workload), and those two members agree on URL,
// Source and Monitor — everything else here — so without the pod the report
// prints one line twice and names neither pod.
type CollidingTarget struct {
	URL     string `json:"url"`
	Source  string `json:"source"`
	Monitor string `json:"monitor,omitempty"`
	Pod     string `json:"pod"` // "namespace/name"
}

// InstanceCollision is one exported series identity that more than one SERVED
// target resolves to — targets the URL dedup keeps apart and (job, instance)
// cannot. Targets holds them in served order, always two or more.
type InstanceCollision struct {
	// Job is the Prometheus job the colliding targets share:
	// service.namespace/service.name, i.e. "namespace/workload".
	Job string `json:"job"`
	// Address is the shared host:port — the Prometheus instance.
	Address string            `json:"address"`
	Targets []CollidingTarget `json:"targets"`
}

// instanceKey is a target's exported identity, as a comparable struct rather
// than a joined string: keying costs no allocation.
type instanceKey struct {
	namespace string // service.namespace
	service   string // service.name
	address   string // service.instance.id
}

// instanceSlot is what the scan remembers per identity: where its first target
// is, and — once a second one turns up — which group holds them.
type instanceSlot struct {
	first int
	group int // index into the result, or -1 while the identity is still unique
}

// InstanceScan finds the served targets that share one exported identity.
//
// The zero value is ready. A caller that scans more than once should keep ONE
// and reuse it: the map is the scan's whole cost, and reusing it is what makes
// the answer "nothing collides" — which is the answer for nearly every node —
// allocation-free. Not safe for concurrent use; one per request, like
// targetDedup's maps.
//
// A map rather than the pairwise scan the pod-scoped predecessor used, because
// the scope change moved n from "the targets of one pod" (one or two,
// essentially always) to "the targets of a node", and this runs per targets
// request, per agent, per scrape cycle. See BenchmarkInstanceScan for both
// shapes at both sizes.
type InstanceScan struct {
	seen map[instanceKey]instanceSlot
}

// Collisions reports the identity groups of a SERVED target list that hold more
// than one target, in first-appearance order, each group in served order.
//
// The scope is the caller's: the targets path passes a whole node's list, so it
// sees the cross-pod hostNetwork case; /v1/explain passes one pod's, which is
// all the targets that endpoint derives.
func (s *InstanceScan) Collisions(targets []kubemeta.ScrapeTarget) []InstanceCollision {
	if len(targets) < 2 {
		return nil // nothing to collide with, and the map is never touched
	}
	if s.seen == nil {
		s.seen = make(map[instanceKey]instanceSlot, len(targets))
	} else {
		clear(s.seen)
	}
	var out []InstanceCollision
	for i := range targets {
		k := identityOf(&targets[i])
		slot, dup := s.seen[k]
		if !dup {
			s.seen[k] = instanceSlot{first: i, group: -1}
			continue
		}
		if slot.group < 0 {
			// Only the SECOND target of an identity opens a group, so a triple
			// is reported once rather than once per member.
			slot.group = len(out)
			s.seen[k] = slot
			out = append(out, InstanceCollision{
				Job:     k.namespace + "/" + k.service,
				Address: k.address,
				Targets: []CollidingTarget{describeColliding(&targets[slot.first])},
			})
		}
		g := &out[slot.group]
		g.Targets = append(g.Targets, describeColliding(&targets[i]))
	}
	return out
}

// identityOf is the (job, instance) a target will be exported under. The Pod is
// passed by value because that is attrs.ServiceName's signature; it reads
// Name and Owners and keeps neither.
func identityOf(t *kubemeta.ScrapeTarget) instanceKey {
	return instanceKey{
		namespace: t.Pod.Namespace,
		service:   attrs.ServiceName(t.Pod),
		address:   t.Address,
	}
}

func describeColliding(t *kubemeta.ScrapeTarget) CollidingTarget {
	return CollidingTarget{
		URL:     t.URL,
		Source:  t.Source,
		Monitor: t.Monitor,
		Pod:     t.Pod.Namespace + "/" + t.Pod.Name,
	}
}
