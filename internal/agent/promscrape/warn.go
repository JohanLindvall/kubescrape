package promscrape

// The once-per-process CONFIGURATION complaints (warnOnce) and the identity they
// are keyed by (warnTarget). A scrape FAILURE re-warns instead, through
// failures.go's allowRepeatingWarn: an operator fixes a failure out of band, so
// the line has to come back, while a configuration mistake has nothing new to
// say until a CR is edited. The two tables differ on purpose.

import (
	"github.com/JohanLindvall/kubescrape/internal/clip"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// maxWarnKeys bounds the dedupe table. The keys are configuration-derived, so a
// healthy cluster holds a handful of them; the cap only catches a pathological
// generator (thousands of monitors, each with its own typo).
//
// Reaching it SUPPRESSES further keys and says so once — see internal/logdedupe
// for why clearing instead is worse than the unbounded map it replaced.
const maxWarnKeys = 1024

// warnOnce logs a per-key message at most once per process, for per-target
// complaints that would otherwise repeat every cycle forever. A zero re-warn
// window is what makes it "once": the operator has to edit a CR, and until they
// do there is nothing new to say.
//
// THE KEY RULE, and it is a security boundary rather than a style preference:
// **a key is built from IDENTITY, never from content a target supplies.** The
// window is zero, so nothing in this table ever expires; the cap therefore
// SUPPRESSES (internal/logdedupe's rule, and the right one — clearing is worse)
// and the suppression is permanent for the process. A key carrying bytes off a
// scraped body, or a free-form field a workload can rewrite, hands whoever
// controls those bytes a way to mint maxWarnKeys distinct keys and shut every
// FUTURE warning in this package — the kubelet's RBAC refusal, a monitor's
// uncompilable regex — for the life of the agent. Identity (warnTarget, the
// kubelet endpoint, a pipeline name) is bounded by the cluster's own objects;
// content is not. Content still RIDES on the line, through clipForLog.
//
// Bounded by the cluster's objects means bounded PER INSTANT, and this table
// lives for the process, so warnTarget additionally avoids naming anything that
// churns faster than the agent restarts — see its bare-pod arm. The residual,
// stated rather than papered over: an owner object that is itself per-run (an
// Argo Workflow, which the owner resolver cannot follow and appends bare) is
// still named, because the operator's fix IS on that object and nothing stable
// stands behind it. It takes maxWarnKeys distinct such workloads, each also
// serving a malformed exposition or an unusable scrapeTimeout, to saturate.
func (s *Scraper) warnOnce(key, msg string, args ...any) {
	allow, saturated := s.warned.Allow(key)
	if saturated {
		s.log.Warn("scrape warning dedupe table is full; further distinct warnings are suppressed",
			"keys", maxWarnKeys)
	}
	if allow {
		s.log.Warn(msg, args...)
	}
}

// maxLoggedValueBytes bounds one target-supplied value on a log line. A metric
// family name or a monitor field arrives from outside this process and is
// bounded only by the body/CR size, and a megabyte of it in a log record is a
// second flood in the shape of one line.
const maxLoggedValueBytes = 96

// clipForLog renders a target-supplied value for a log attribute: bounded, and
// cut on a rune boundary (internal/clip) so a clipped UTF-8 sequence does not
// become a replacement character in whatever reads the line. It is the
// counterpart of the key rule above — the value cannot be part of the KEY, so
// this is how it still reaches the operator.
func clipForLog(v string) string { return clip.Ellipsis(v, maxLoggedValueBytes) }

// warnTarget identifies a target for warnOnce by the CONFIGURATION that
// produced it, never by its URL.
//
// The URL embeds the pod IP, so keying on it was wrong twice over: the table
// grew one entry per pod incarnation for the process' whole life, and the
// warning re-fired on every pod restart — defeating the "once" it exists for,
// with the noisiest clusters (frequent restarts) getting the most noise. What
// the operator actually has to fix is a field on a monitor CR, a Service
// annotation or a workload's pod annotation, and all three outlive the pods
// they produce targets for.
func warnTarget(t kubemeta.ScrapeTarget) string {
	if t.Monitor != "" {
		return t.Source + ":" + t.Monitor // "ns/name" of the ServiceMonitor/PodMonitor
	}
	if t.Service != nil {
		return t.Source + ":" + t.Service.Namespace + "/" + t.Service.Name
	}
	// Pod annotations. The chain is direct-owner-first, so the LAST owner is
	// the workload root: a Deployment rather than its per-revision ReplicaSet,
	// so a rollout does not mint a new key.
	if n := len(t.Pod.Owners); n > 0 {
		o := t.Pod.Owners[n-1]
		return t.Source + ":" + t.Pod.Namespace + "/" + o.Kind + "/" + o.Name
	}
	// A BARE pod is keyed by its namespace alone, deliberately without its
	// name. It is the one arm with no object outliving the pod behind it, so
	// the name is the churn the table cannot survive: a cluster that keeps
	// creating owner-less annotated pods (kubectl run, debug pods carrying
	// prometheus.io/scrape=true) mints one permanent key per incarnation, and
	// the window is zero, so past maxWarnKeys the table refuses every NEW key
	// for the life of the agent — including the kubelet's RBAC refusal, which
	// is the warning an operator can least afford to lose. The cost is
	// granularity among owner-less pods of one namespace, which is small: the
	// line still names the target, and the counters are the ongoing signal.
	return t.Source + ":" + t.Pod.Namespace
}
