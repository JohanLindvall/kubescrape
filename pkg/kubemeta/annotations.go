package kubemeta

import (
	"maps"
	"slices"
	"strconv"
	"strings"
)

// refusedAnnotations are the annotation keys that never reach a served
// document, whatever their size, on any object this API serves — pods, owners,
// namespaces and Services alike. It is the ONE list of them and both doors read
// it: the read filter (FilterAnnotations and its slow path budgetAnnotations)
// and the informer transform (StripDroppedAnnotations). It used to be spelled
// twice, as a deploy-tool map plus the reserved note keys, once per door — and a
// key added to one door and not the other is either refused on read while
// staying resident in every cached object, or stripped from the cache while a
// hand-built object still serves it.
//
// There are two reasons a key is here, and each entry says which:
//
//   - A DEPLOY TOOL'S COPY OF THE APPLIED OBJECT. The metadata routes are
//     unauthenticated by design ("it carries no secret material and agents poll
//     it constantly"), and that claim only holds if the annotations riding
//     along carry none either. An applied-object copy breaks it outright: it is
//     the whole spec verbatim, so anything a user inlined — an env var with a
//     token, a connection string, a webhook URL — is served to any caller that
//     can reach the port, and lands in every log record's resource attributes
//     downstream. Such a copy is also pure bloat as telemetry metadata: it
//     duplicates the spec the rest of the response already models, and on a
//     CronJob or Deployment it is routinely the largest field in the payload.
//     The entries are the deploy tools that write one: kubectl apply (and every
//     provider following it — Terraform, Pulumi, Argo CD's default tracking)
//     and kapp, which writes its own copy on every object it deploys. Sibling
//     keys that merely FINGERPRINT the applied object (kapp's `-diff-md5`,
//     Flux's checksums) carry no spec content and stay.
//   - THIS API'S OWN NOTE KEYS (OmittedAnnotation, LabelsOmittedAnnotation):
//     its words about what it refused to serve, which only this service may
//     set. A copy arriving from the cluster is a forgery — a tenant claiming a
//     budget bound that never did, or (worse) claiming one did not.
var refusedAnnotations = map[string]bool{
	// Deploy-tool copies of the applied object.
	"kubectl.kubernetes.io/last-applied-configuration": true,
	"kapp.k14s.io/original":                            true,
	// This API's reserved note keys.
	OmittedAnnotation:       true,
	LabelsOmittedAnnotation: true,
}

// StripDroppedAnnotations removes the refused keys (refusedAnnotations) from m
// IN PLACE, reporting whether it removed any. It is for the informer
// TRANSFORMS, which own an object before it enters the cache: FilterAnnotations
// copies, and copying is exactly what a transform must not do — its job is to
// keep the bytes out of the cache in the first place.
//
// Without it a last-applied-configuration copy is resident for the process
// lifetime on every cached object and can never be read, because every read
// path funnels through CopyMeta/FilterAnnotations. On a kubectl- or
// kapp-managed cluster that is megabytes of permanently unreadable heap. A
// forged note key is stripped at the same door for the same reason: the
// transform owns the object before the cache does.
func StripDroppedAnnotations(m map[string]string) bool {
	dropped := false
	for k := range refusedAnnotations {
		if _, ok := m[k]; ok {
			delete(m, k)
			dropped = true
		}
	}
	return dropped
}

// CopyOwnerMeta is CopyMeta for an OWNER — a ReplicaSet, Deployment, Job,
// CronJob, StatefulSet or DaemonSet named by a pod's ownerReferences — whose
// LABELS are bounded too (MaxOwnerLabelBytes). The labels CopyMeta leaves
// verbatim are verbatim because they are selection input; an owner's are not:
// no Service, PodMonitor or scrape derivation selects on them, only attrs
// templates read them. And they ride every pod document once per resolved
// reference, up to MaxOwners of them, while the API server bounds an object's
// label COUNT only by its ~1.5 MiB ceiling — so a tenant who creates eight
// label-fat ReplicaSets and points its pods at them put ~11 MiB in each pod
// document, and six such pods on a node pushed the node-targets response past
// the agent's 64 MiB read cap, blinding target discovery for every pod on it.
//
// Refused labels are omitted whole in a deterministic order (lexicographic, so
// a fat owner serves the same subset on every poll and its ETag holds) and
// NAMED in the owner's own annotations under LabelsOmittedAnnotation — a label
// cannot carry the note, its value being limited to 63 bytes.
func CopyOwnerMeta(labels, annotations map[string]string) (map[string]string, map[string]string) {
	outLabels, omitted := budgetOwnerLabels(labels)
	outAnnotations := FilterAnnotations(annotations)
	if len(omitted) > 0 {
		if outAnnotations == nil {
			outAnnotations = make(map[string]string, 1)
		}
		outAnnotations[LabelsOmittedAnnotation] = omittedLabelsNote(omitted)
	}
	return outLabels, outAnnotations
}

// MaxOwnerLabelBytes bounds the key+value bytes of ONE owner's served labels.
// 16 KiB, the annotation set's budget: a real Deployment carries a few hundred
// bytes of labels, so nothing legitimate is near it, and eight owners at the
// ceiling are 128 KiB of labels in a pod document — a constant this service
// chose rather than a product of numbers a tenant picks.
const MaxOwnerLabelBytes = 16 << 10

// LabelsOmittedAnnotation is the key CopyOwnerMeta adds to an owner whose
// labels it refused part of, naming the refused keys. Reserved like
// OmittedAnnotation: a cluster-supplied copy is stripped at the door.
const LabelsOmittedAnnotation = "kubescrape.io/labels-omitted"

// budgetOwnerLabels copies labels within MaxOwnerLabelBytes. The common path is
// one walk and the output map, as a plain copy would be, with the ceiling checked
// from lengths inside it; only an owner over the budget throws that map away
// and pays for the sort that makes its admitted subset deterministic.
func budgetOwnerLabels(labels map[string]string) (map[string]string, []string) {
	if len(labels) == 0 {
		return nil, nil
	}
	fast := make(map[string]string, len(labels))
	total := 0
	for k, v := range labels {
		if total += len(k) + len(v); total > MaxOwnerLabelBytes {
			return budgetOwnerLabelsSorted(labels)
		}
		fast[k] = v
	}
	return fast, nil
}

// budgetOwnerLabelsSorted is budgetOwnerLabels' slow path: admission in
// lexicographic order, the refused keys returned for the note.
func budgetOwnerLabelsSorted(labels map[string]string) (map[string]string, []string) {
	keys := slices.Sorted(maps.Keys(labels))
	out := make(map[string]string)
	var omitted []string
	spent := 0
	for _, k := range keys {
		// A label over what is left is SKIPPED rather than ending the walk, so
		// a small label behind a large one is still served.
		if cost := len(k) + len(labels[k]); spent+cost <= MaxOwnerLabelBytes {
			spent += cost
			out[k] = labels[k]
			continue
		}
		omitted = append(omitted, k)
	}
	return out, omitted
}

// omittedLabelsNote renders LabelsOmittedAnnotation's value: the count, the
// ceiling, and as many of the refused keys as maxOmittedNamedBytes affords.
func omittedLabelsNote(keys []string) string {
	var b strings.Builder
	b.WriteString(strconv.Itoa(len(keys)))
	b.WriteString(" label(s) omitted by kubescrape: this owner's ")
	b.WriteString(strconv.Itoa(MaxOwnerLabelBytes))
	b.WriteString("-byte label budget was spent")
	appendNamedKeys(&b, keys)
	return b.String()
}

// LabelsOmitted reports whether an owner's served labels are SHORT — how the
// owner resolver counts the refusal, pkg/ being unable to import internal/obs.
func LabelsOmitted(annotations map[string]string) bool {
	_, ok := annotations[LabelsOmittedAnnotation]
	return ok
}

// CopyMeta deep-copies an object's labels verbatim and passes its annotations
// through FilterAnnotations, returning nil for either when empty so the model
// fields stay omitempty. Every labels+annotations pair the unauthenticated API
// serves — pods, namespaces/nodes, Services, and owners through CopyOwnerMeta,
// which is this plus a bound on the labels — goes through here: the
// pairing is the invariant, because when the two were copied by separate
// per-package helpers, Services (the fourth annotation-bearing object) got the
// verbatim copy for BOTH and served kubectl's last-applied-configuration on
// every service- and monitor-derived scrape target.
func CopyMeta(labels, annotations map[string]string) (map[string]string, map[string]string) {
	return cloneMap(labels), FilterAnnotations(annotations)
}

// cloneMap copies m, nil for empty (omitempty stays omitted). maps.Clone
// rather than a make-and-range loop: the runtime copies the table wholesale,
// about twice as fast with the same allocations, and the sources are
// JSON-decoded informer maps, so the capacity it preserves is their size.
func cloneMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	return maps.Clone(m)
}

// FilterAnnotations copies m without the annotations this API refuses to
// serve — the refusedAnnotations list above (the deploy-tool copies and the
// reserved note keys) and whatever the two annotation ceilings below refuse. It returns nil for an
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
		if refusedAnnotations[k] {
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
// route staying silent. A cluster-supplied copy is stripped at both doors
// (refusedAnnotations), so only this service can set it.
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
//
// The note names the refused keys in that SAME order — preserved-prefix keys
// first, each group lexicographic — so a consumer looking for one of the
// derivations' own keys finds it inside the note's maxOmittedNamedBytes of
// names however many unrelated keys were refused beside it (AnnotationWasOmitted
// depends on it: the agent's tailer uses it to tell "no kubescrape.io/logs
// annotation" from "an annotation this filter refused").
//
// The order is built by PARTITIONING rather than by one comparator sort: the
// preserved keys and the rest are collected into the two ends of one slice and
// each end is sorted on its own, which is the same order without a prefix test
// per comparison — and each half of the note's list is a MERGE of lists
// already sorted, not a second sort. This path is reached per owner per
// resolution on the unauthenticated routes, and a tenant chooses how many keys
// it walks (a review's probe on 7,759 keys put the comparator sort plus the
// re-sort at roughly three times this). The output is byte-identical to the
// comparator's, which TestBudgetedAnnotationsMatchTheirDefinition pins. None
// of this removes the per-request O(n) walk itself: a fat owner is still
// re-filtered on every resolution, which only a per-object memo would avoid.
func budgetAnnotations(m map[string]string) map[string]string {
	keys := make([]string, len(m))
	np, nr := 0, len(m)
	var bigPreserved, bigRest []string
	for k, v := range m {
		switch {
		case refusedAnnotations[k]:
		case len(v) > MaxAnnotationValueBytes:
			if preservedAnnotation(k) {
				bigPreserved = append(bigPreserved, k)
			} else {
				bigRest = append(bigRest, k)
			}
		case preservedAnnotation(k):
			keys[np] = k
			np++
		default:
			nr--
			keys[nr] = k
		}
	}
	preserved, rest := keys[:np], keys[nr:]
	slices.Sort(preserved)
	slices.Sort(rest)
	slices.Sort(bigPreserved)
	slices.Sort(bigRest)
	out := make(map[string]string, len(preserved)+len(rest)+1)
	preserved, spent := admitAnnotations(m, preserved, out, 0)
	rest, _ = admitAnnotations(m, rest, out, spent)
	named := make([]string, 0, len(bigPreserved)+len(preserved)+len(bigRest)+len(rest))
	named = appendMerged(named, bigPreserved, preserved)
	named = appendMerged(named, bigRest, rest)
	out[OmittedAnnotation] = omittedNote(named)
	return out
}

// admitAnnotations admits keys into out, in order, while they fit what is left
// of MaxAnnotationBytes after spent, and returns the keys it refused —
// compacted into keys' own prefix, so still in keys' order — with the new
// spend.
func admitAnnotations(m map[string]string, keys []string, out map[string]string, spent int) ([]string, int) {
	refused := keys[:0]
	for _, k := range keys {
		if cost := len(k) + len(m[k]); spent+cost <= MaxAnnotationBytes {
			spent += cost
			out[k] = m[k]
			continue
		}
		refused = append(refused, k)
	}
	return refused, spent
}

// appendMerged appends the sorted merge of two sorted, mutually disjoint key
// lists (they come from one map) to dst.
func appendMerged(dst, a, b []string) []string {
	for len(a) > 0 && len(b) > 0 {
		if a[0] < b[0] {
			dst, a = append(dst, a[0]), a[1:]
		} else {
			dst, b = append(dst, b[0]), b[1:]
		}
	}
	dst = append(dst, a...)
	return append(dst, b...)
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
	appendNamedKeys(&b, keys)
	return b.String()
}

// omittedListPrefix and omittedListSep frame the key list in both notes; they
// are constants because AnnotationWasOmitted parses what appendNamedKeys
// writes.
const (
	omittedListPrefix = "; omitted: "
	omittedListSep    = ", "
)

// appendNamedKeys appends "; omitted: a, b, +N more" — as many keys as
// maxOmittedNamedBytes affords, and the rest as a count — to a note.
func appendNamedKeys(b *strings.Builder, keys []string) {
	named, spent := 0, 0
	for _, k := range keys {
		if spent+len(k) > maxOmittedNamedBytes {
			break
		}
		if named == 0 {
			b.WriteString(omittedListPrefix)
		} else {
			b.WriteString(omittedListSep)
		}
		b.WriteString(k)
		spent += len(k) + len(omittedListSep)
		named++
	}
	if rest := len(keys) - named; rest > 0 {
		if named == 0 {
			b.WriteString(omittedListPrefix)
		} else {
			b.WriteString(omittedListSep)
		}
		b.WriteString("+")
		b.WriteString(strconv.Itoa(rest))
		b.WriteString(" more")
	}
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

// AnnotationWasOmitted reports whether the filter refused key from the object
// whose served annotations are m: whether m's OmittedAnnotation note NAMES it.
// It is how a consumer tells "the object has no such annotation" from "it has
// one this API would not serve" — the two read identically as an absent key,
// and a consumer that falls back to a default on the second silently ignores
// what the object's author wrote (the agent's tailer, on an oversized
// kubescrape.io/logs, would otherwise drop a pod's opt-out without a word).
//
// It is exact for a named key and false for one the note only COUNTS ("+N
// more"). The note names refused keys preserved-prefix first (see
// budgetAnnotations), so a prometheus.io/* or kubescrape.io/* key is named
// unless more than maxOmittedNamedBytes of such keys sorting before it were
// refused from the SAME object — which only that object's own author can
// cause. Annotation keys cannot contain ", " or ";", so the parse cannot be
// confused by a key; the note itself cannot be forged, a cluster-supplied
// copy being stripped at the door.
func AnnotationWasOmitted(m map[string]string, key string) bool {
	note, ok := m[OmittedAnnotation]
	if !ok {
		return false
	}
	_, names, ok := strings.Cut(note, omittedListPrefix)
	if !ok {
		return false
	}
	for name := range strings.SplitSeq(names, omittedListSep) {
		if name == key {
			return true
		}
	}
	return false
}
