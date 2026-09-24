package tailer

// Per-workload log configuration via a pod annotation: the workload declares
// its own multiline behavior, drop/sample rules, extra attributes or a
// service-name override — no agent config change, no restart. The annotation
// arrives for free through the metadata resolution every containerd file
// already performs; it is parsed once per file at resolve time.

import (
	"encoding/json"
	"fmt"

	"github.com/JohanLindvall/kubescrape/internal/agent/attrs"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/obs"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// LogAnnotation is the pod annotation carrying per-workload log config, one
// JSON object:
//
//	kubescrape.io/logs: |
//	  {"exclude": false, "multiline": true, "serviceName": "checkout",
//	   "attributes": {"team": "payments"},
//	   "rules": [{"action": "drop", "matchRegexp": ["level=debug"]}]}
const LogAnnotation = "kubescrape.io/logs"

// podLogConfig is the parsed annotation.
type podLogConfig struct {
	// Exclude skips this pod's log files entirely (like an excluded
	// namespace, but self-service).
	Exclude bool `json:"exclude,omitempty"`
	// Multiline overrides the source's stack-trace joining for this pod.
	Multiline *bool `json:"multiline,omitempty"`
	// ServiceName overrides the derived service.name resource attribute.
	ServiceName string `json:"serviceName,omitempty"`
	// Attributes are additional resource attributes (overwriting — the
	// workload is authoritative about itself).
	Attributes map[string]string `json:"attributes,omitempty"`
	// Rules are keep/drop/sample rules evaluated BEFORE the global logs.rules
	// (each chain is first-match-wins on its own; a pod-rule drop is final,
	// a pod-rule keep still passes through the global chain).
	Rules []logline.LineRule `json:"rules,omitempty"`
}

// Bounds on what one pod may ask the agent to do per line. Unlike the agent's
// own `logs.rules`, which an OPERATOR writes once for the node, these arrive
// from a namespace-scoped annotation any workload author controls — and they
// are evaluated on the SINGLE sweep goroutine that serves every log file on the
// node, against every record, including the synthetic `__line__` key (the whole
// raw body, up to MaxEntryBytes). A pod that ships fifty nested-quantifier
// regexes therefore does not slow itself down; it stalls log collection for the
// whole node. The bounds are far above any legitimate self-service use.
const (
	maxPodRules         = 16
	maxPodSelectors     = 16
	maxPodPatternLength = 512
)

// parsePodLogConfig parses the annotation value and compiles its rules.
func parsePodLogConfig(raw string) (*podLogConfig, *logline.LineFilter, error) {
	var cfg podLogConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, nil, fmt.Errorf("parsing %s annotation: %w", LogAnnotation, err)
	}
	if n := len(cfg.Rules); n > maxPodRules {
		return nil, nil, fmt.Errorf("%s: %d rules exceeds the per-pod maximum of %d", LogAnnotation, n, maxPodRules)
	}
	for i, r := range cfg.Rules {
		if n := len(r.Match) + len(r.MatchRegexp); n > maxPodSelectors {
			return nil, nil, fmt.Errorf("%s: rule %d has %d selectors, exceeding the per-rule maximum of %d",
				LogAnnotation, i, n, maxPodSelectors)
		}
		for _, p := range r.MatchRegexp {
			if len(p) > maxPodPatternLength {
				return nil, nil, fmt.Errorf("%s: rule %d has a %d-byte regex, exceeding the maximum of %d",
					LogAnnotation, i, len(p), maxPodPatternLength)
			}
		}
	}
	var rules *logline.LineFilter
	if len(cfg.Rules) > 0 {
		var err error
		if rules, err = logline.NewLineFilter(cfg.Rules); err != nil {
			return nil, nil, fmt.Errorf("compiling %s rules: %w", LogAnnotation, err)
		}
	}
	return &cfg, rules, nil
}

// rejectPodConfig records a pod log annotation the file cannot honour. Counted
// and kept on the file for /debug/tailer (podConfigError): the Warn line lands
// on ONE node while the operator who edited the annotation is elsewhere, and
// "I changed it and nothing happened" otherwise has no signal at all.
func (t *Tailer) rejectPodConfig(f *file, err error) {
	f.podConfigErr = err.Error()
	obs.LogPodConfigInvalid.Inc()
	t.log.Warn("ignoring unusable pod log annotation", "path", f.path, "annotation", LogAnnotation, "error", err)
}

// applyPodConfig applies the pod's annotation to a freshly resolved file.
// A malformed annotation — or one the metadata service omitted for size — must
// not lose logs: it is warned about (once — this runs once per file) and
// ignored, everything else about the file proceeds.
func (t *Tailer) applyPodConfig(f *file, annotations map[string]string) {
	raw, ok := annotations[LogAnnotation]
	if !ok || raw == "" {
		if kubemeta.AnnotationWasOmitted(annotations, LogAnnotation) {
			// The metadata service REFUSED the annotation for size and served
			// the pod without it. An absent key reads exactly like "no log
			// config", so falling through here collected the pod with the
			// source defaults — dropping its opt-out, its drop rules and its
			// attributes — while the only evidence sat on the metadata service.
			// It takes the malformed path instead: logs are still collected,
			// and the refusal is counted, warned and on /debug/tailer.
			t.rejectPodConfig(f, fmt.Errorf("the metadata service omitted the %s annotation for size "+
				"(a value over %d bytes, or the pod's %d-byte annotation budget spent; the pod's %s annotation names it)",
				LogAnnotation, kubemeta.MaxAnnotationValueBytes, kubemeta.MaxAnnotationBytes, kubemeta.OmittedAnnotation))
		}
		return
	}
	cfg, rules, err := parsePodLogConfig(raw)
	if err != nil {
		t.rejectPodConfig(f, err)
		return
	}
	f.podConfigErr = ""
	if cfg.Exclude {
		f.excluded = true
		t.log.Info("pod opted out of log collection", "path", f.path)
		return
	}
	f.podRules = rules
	if cfg.Multiline != nil && *cfg.Multiline != f.source.multiline {
		// The pipeline was built at discovery from the source default, before
		// this annotation was read (metadata resolves on the first sweep,
		// after newPipeline). Rebuild it now so the override takes effect on
		// the file's INITIAL pipeline — nothing has been fed yet (reads are
		// gated on resolution), so the rebuild only clears empty stream state.
		// Without this the override was ignored until the next rotation, and
		// forever for a file that never rotates.
		f.multiline = cfg.Multiline
		t.newPipeline(f)
	}
	// The vetting happens HERE, once per file, and only the RESULT is kept:
	// buildResource re-applies these whenever it re-renders the resource (a
	// node relabel), and re-vetting there would re-count obs.LogPodAttrsRefused
	// and re-warn once per refresh, forever.
	f.podService = cfg.ServiceName
	f.podAttrs = nil
	for k, v := range cfg.Attributes {
		if reservedAttr(k) {
			// The workload is authoritative about its own DESCRIPTION, never
			// about its IDENTITY or kubescrape's control-plane markers — see
			// reservedAttr.
			obs.LogPodAttrsRefused.WithLabelValues(k).Inc()
			t.log.Warn("refusing a reserved pod-annotation attribute",
				"path", f.path, "key", k, "value", v, "annotation", LogAnnotation)
			continue
		}
		if f.podAttrs == nil {
			f.podAttrs = make(map[string]string, len(cfg.Attributes))
		}
		f.podAttrs[k] = v
	}
	f.applyPodResource(t.cfg.Attrs)
}

// applyPodResource stamps the pod annotation's vetted resource overrides onto
// the file's current resource. The workload is authoritative about itself, so
// these land last among the WRITERS — after the builder, the source statics and
// the path captures — on every render of the resource, not just the first.
//
// The operator's resourceAttributes enable/disable filter still has the final
// word, exactly as it does over the builder's output and a plain source's
// statics (attrs.Builder.FilterResource): an annotation is TENANT-authored, and
// stamping after the filter let anyone who can annotate a pod export resource
// keys the operator's `disable` list drops, or that the `enable` allowlist —
// the documented way to bound what reaches the backend — excludes. Nil-safe
// builder; a file with no pod overrides pays nothing.
func (f *file) applyPodResource(b *attrs.Builder) {
	if f.podService == "" && len(f.podAttrs) == 0 {
		return
	}
	a := f.resource.Attributes()
	if f.podService != "" {
		a.PutStr("service.name", f.podService)
	}
	// Distinct keys, so the map's iteration order cannot change the result.
	for k, v := range f.podAttrs {
		a.PutStr(k, v)
	}
	b.FilterResource(f.resource)
}

// reservedAttr reports whether a pod annotation may NOT set this resource
// attribute. Two disjoint sets, both security boundaries, both single-homed in
// internal/agent/attrs so a new key lands in one place:
//
//   - attrs.ReservedIdentity — resolved Kubernetes identity (k8s.namespace.name,
//     k8s.pod.*, container.*, …): forging it steers a pod's telemetry to another
//     tenant's route via the namespace glob, or forges series identity.
//   - attrs.ReservedPlumbing — kubescrape's own control-plane markers
//     (route.ScriptMarker, transform.DropMarker): the router honours the route
//     marker BEFORE the namespace globs, so a pod-annotation copy steers this
//     pod's logs to any route and its tenant headers — the DIRECT form of the
//     same attack ReservedIdentity blocks indirectly, and the one the ingest
//     listeners already strip (otlpingest.ReservedAttrs). This surface was the
//     one that lacked the strip.
func reservedAttr(k string) bool {
	return attrs.ReservedIdentity(k) || attrs.ReservedPlumbing(k)
}
