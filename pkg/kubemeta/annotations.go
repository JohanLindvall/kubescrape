package kubemeta

import (
	"slices"
	"strconv"
	"strings"
)

// droppedAnnotations are annotation keys stripped from every object this API
// serves — pods, owners and namespaces alike.
//
// The metadata routes are unauthenticated by design ("it carries no secret
// material and agents poll it constantly"), and that claim only holds if the
// annotations riding along carry none either. An applied-object copy breaks it
// outright: it is the whole spec verbatim, so anything a user inlined — an env
// var with a token, a connection string, a webhook URL — is served to any
// caller that can reach the port, and lands in every log record's resource
// attributes downstream.
//
// Such a copy is also pure bloat as telemetry metadata: it duplicates the spec
// the rest of the response already models, and on a CronJob or Deployment it is
// routinely the largest field in the payload.
//
// The entries are the deploy tools that write one: kubectl apply (and every
// provider following it — Terraform, Pulumi, Argo CD's default tracking) and
// kapp, which writes its own copy on every object it deploys. Sibling keys that
// merely FINGERPRINT the applied object (kapp's `-diff-md5`, Flux's checksums)
// carry no spec content and stay.
var droppedAnnotations = map[string]bool{
	"kubectl.kubernetes.io/last-applied-configuration": true,
	"kapp.k14s.io/original":                            true,
}

// StripDroppedAnnotations removes the refused keys from m IN PLACE, reporting
// whether it removed any. It is for the informer TRANSFORMS, which own an
// object before it enters the cache: FilterAnnotations copies, and copying is
// exactly what a transform must not do — its job is to keep the bytes out of
// the cache in the first place.
//
// Without it a last-applied-configuration copy is resident for the process
// lifetime on every cached object and can never be read, because every read
// path funnels through CopyMeta/FilterAnnotations. On a kubectl- or
// kapp-managed cluster that is megabytes of permanently unreadable heap.
func StripDroppedAnnotations(m map[string]string) bool {
	dropped := false
	for k := range droppedAnnotations {
		if _, ok := m[k]; ok {
			delete(m, k)
			dropped = true
		}
	}
	// OmittedAnnotation is this API's OWN word about what it refused to serve,
	// so a copy arriving from the cluster is a forgery — a tenant claiming a
	// budget bound that never did, or (worse) claiming one did not. It is
	// stripped at the same door as the deploy blobs for the same reason theirs
	// are: the transform owns the object before the cache does.
	if _, ok := m[OmittedAnnotation]; ok {
		delete(m, OmittedAnnotation)
		dropped = true
	}
	return dropped
}

// CopyMeta deep-copies an object's labels verbatim and passes its annotations
// through FilterAnnotations, returning nil for either when empty so the model
// fields stay omitempty. Every labels+annotations pair the unauthenticated API
// serves — pods, owners, namespaces/nodes, Services — goes through here: the
// pairing is the invariant, because when the two were copied by separate
// per-package helpers, Services (the fourth annotation-bearing object) got the
// verbatim copy for BOTH and served kubectl's last-applied-configuration on
// every service- and monitor-derived scrape target.
func CopyMeta(labels, annotations map[string]string) (map[string]string, map[string]string) {
	return cloneMap(labels), FilterAnnotations(annotations)
}

// cloneMap copies m, nil for empty (omitempty stays omitted).
func cloneMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// FilterAnnotations copies m without the annotations this API refuses to
// serve — the deploy-tool denylist above, the reserved OmittedAnnotation key,
// and whatever the two annotation ceilings below refuse. It returns nil for an
// empty result so the field stays omitempty.
//
// The denylist is deliberately a fixed, tiny one rather than a config knob:
// every entry is a deploy-tool convention that no consumer of this API wants,
// and a per-deployment allowlist would make "is this endpoint safe to expose"
// depend on configuration nobody reviews.
//
// The COMMON path is ONE walk and exactly the output map, as it always was: the
// ceilings are checked from lengths, never from bytes, so a 200 KiB value costs
// one comparison. Only an object that trips a ceiling — no real one does —
// reaches budgetAnnotations, which throws this map away and rebuilds it in a
// deterministic order. Paying an allocation on the abuse path is the right side
// to pay it on; a pre-pass to avoid it would cost a second walk of every
// ordinary object instead.
func FilterAnnotations(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	total := 0
	for k, v := range m {
		if refusedAnnotation(k) {
			continue
		}
		if len(v) > MaxAnnotationValueBytes {
			return budgetAnnotations(m)
		}
		if total += len(k) + len(v); total > MaxAnnotationBytes {
			return budgetAnnotations(m)
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// The two ceilings on ONE object's annotations, and why this API needs them
// when the API server's own 256 KiB per-object limit already exists.
//
// A pod DOCUMENT is not one object's annotations. It carries the pod's own,
// the pod's NAMESPACE's, and one set per resolved ownerReference — and
// Kubernetes bounds neither the ownerReferences count nor how many pods may
// name one fat owner. Measured through the real derivation: a tenant with edit
// rights in ONE namespace creates 100 ReplicaSets each carrying a 200 KiB
// annotation and points every pod's ownerReferences at all of them, and each
// pod document becomes ~25 MB — served once per scrapeable pod by
// /v1/nodes/{node}/targets, re-derived and re-marshalled on every agent poll,
// in the singleton the chart requests 128Mi for with no memory limit. The
// owner COUNT is bounded by owners.MaxOwners; these two bound the BYTES each
// of those objects contributes, so a pod document is a constant this service
// chose rather than a product of numbers a tenant picks.
//
// MaxAnnotationValueBytes refuses a single oversized VALUE, whole. Truncating
// one instead was considered and rejected: annotations are load-bearing for
// attribution (attrs templates read them), and a silently shortened value is a
// worse failure than a missing one — a template rendering half a value looks
// like it worked. 8 KiB is far above the real ones this filter leaves behind
// once the applied-object copies are dropped: the fattest in the field are an
// istio sidecar status or a CNI network-status at ~1-2 KiB.
//
// MaxAnnotationBytes bounds the SUM, because the per-value ceiling alone does
// not: 32 values of 8 KiB is the API server's whole 256 KiB again. 16 KiB is
// ~4x the largest post-filter annotation set seen on a real pod.
//
// Both refusals are REPORTED — the surviving map carries OmittedAnnotation,
// which names what went (see budgetAnnotations), so /v1/pods, /v1/explain and
// every downstream consumer read a document that says it is short rather than
// a shorter document.
const (
	MaxAnnotationValueBytes = 8 << 10
	MaxAnnotationBytes      = 16 << 10
)

// OmittedAnnotation is the key the filter adds to an object whose annotations
// it refused part of. It is a MAP ENTRY rather than a new field on each of the
// four carriers (Pod, Owner, ObjectMeta, Service) because it then rides every
// route that serves any of them — /v1/pods, /v1/containers, /v1/explain,
// /v1/nodes/{node}/targets — with no wire-contract change and no chance of one
// route staying silent. A cluster-supplied copy is stripped at the door
// (StripDroppedAnnotations, refusedAnnotation), so only this service can set
// it.
const OmittedAnnotation = "kubescrape.io/annotations-omitted"

// maxOmittedNamedBytes bounds how much of OmittedAnnotation's value is spent
// naming keys. The note is served inside a document whose size is the whole
// point of the ceilings above, so it cannot itself be proportional to the
// abuse: past this many bytes of names the rest become a count.
//
// The note is added AFTER the budget rather than charged against it — an
// object's served annotations are therefore at most MaxAnnotationBytes plus
// this plus the fixed prose, still a constant — because charging it could
// evict a real annotation to make room for the sentence saying an annotation
// was evicted.
const maxOmittedNamedBytes = 512

// preservedAnnotationPrefixes are admitted BEFORE anything else when the total
// budget binds. They are the prefixes this project's own derivations read —
// internal/scrape's prometheus.io/scrape|port|path|scheme, and the agent's
// kubescrape.io/logs — and a budget that starved them would silently stop
// scraping a pod, or silently change how its logs are collected, because of an
// unrelated blob some controller wrote onto the same object.
//
// PREFIXES rather than the exact key list, deliberately: pkg/ must not import
// internal/, so an exact list here would be a second spelling of constants
// that live in internal/scrape and internal/agent/tailer — the drift this repo
// keeps paying for. A prefix cannot drift.
//
// They are admitted first, not EXEMPT: a pod whose own prometheus.io/*
// annotations exceed the whole budget still loses some, which is self-harm on
// that one pod rather than a lever on anyone else. What the ordering buys is
// the realistic case — a fat but honest pod whose prometheus.io/scrape must
// survive 16 KiB of someone else's annotations.
var preservedAnnotationPrefixes = []string{"prometheus.io/", "kubescrape.io/"}

func preservedAnnotation(k string) bool {
	for _, p := range preservedAnnotationPrefixes {
		if strings.HasPrefix(k, p) {
			return true
		}
	}
	return false
}

// refusedAnnotation reports the keys that never reach a served document
// whatever their size: the deploy-tool blobs, and this API's own reserved
// note key.
func refusedAnnotation(k string) bool {
	return droppedAnnotations[k] || k == OmittedAnnotation
}

// budgetAnnotations is FilterAnnotations' slow path: the object is over one of
// the two ceilings, so admission becomes ordered and what is refused is named.
//
// The order is preserved-prefix-first, then lexicographic, and it is
// DETERMINISTIC on purpose. Map order is not: an arbitrary subset would make
// the served document differ between two requests describing the same object,
// which mints a fresh ETag on every agent poll and defeats the 304 path on the
// one route that re-sends every pod on the node each scrape cycle.
//
// A key over the budget is SKIPPED rather than ending the walk, so a small
// annotation behind a large one is still served.
func budgetAnnotations(m map[string]string) map[string]string {
	keys := make([]string, 0, len(m))
	omitted := make([]string, 0, 4)
	for k, v := range m {
		switch {
		case refusedAnnotation(k):
		case len(v) > MaxAnnotationValueBytes:
			omitted = append(omitted, k)
		default:
			keys = append(keys, k)
		}
	}
	slices.SortFunc(keys, func(a, b string) int {
		if pa, pb := preservedAnnotation(a), preservedAnnotation(b); pa != pb {
			if pa {
				return -1
			}
			return 1
		}
		return strings.Compare(a, b)
	})
	out := make(map[string]string, len(keys)+1)
	spent := 0
	for _, k := range keys {
		if cost := len(k) + len(m[k]); spent+cost <= MaxAnnotationBytes {
			spent += cost
			out[k] = m[k]
			continue
		}
		omitted = append(omitted, k)
	}
	slices.Sort(omitted)
	out[OmittedAnnotation] = omittedNote(omitted)
	return out
}

// omittedNote renders OmittedAnnotation's value: the count, the two ceilings
// that can have produced it, and as many of the refused keys as
// maxOmittedNamedBytes affords.
func omittedNote(keys []string) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(len(keys)))
	b.WriteString(" annotation(s) omitted by kubescrape: a value over ")
	b.WriteString(strconv.Itoa(MaxAnnotationValueBytes))
	b.WriteString(" bytes, or this object's ")
	b.WriteString(strconv.Itoa(MaxAnnotationBytes))
	b.WriteString("-byte annotation budget was spent")
	named, spent := 0, 0
	for _, k := range keys {
		if spent+len(k) > maxOmittedNamedBytes {
			break
		}
		if named == 0 {
			b.WriteString("; omitted: ")
		} else {
			b.WriteString(", ")
		}
		b.WriteString(k)
		spent += len(k) + 2
		named++
	}
	if rest := len(keys) - named; rest > 0 {
		if named == 0 {
			b.WriteString("; omitted: ")
		} else {
			b.WriteString(", ")
		}
		b.WriteString("+")
		b.WriteString(strconv.Itoa(rest))
		b.WriteString(" more")
	}
	return b.String()
}

// AnnotationsOmitted reports whether a served annotation map is SHORT — the
// filter refused something for size. It is how the callers that know the
// object's KIND count the refusal: pkg/ cannot import internal/obs, and each
// of the three doors (the pod conversion, the owner/namespace resolver, the
// Service index) runs once per informer event or once per request-scoped
// resolution, never per served document.
func AnnotationsOmitted(m map[string]string) bool {
	_, ok := m[OmittedAnnotation]
	return ok
}
