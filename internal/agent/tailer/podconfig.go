package tailer

// Per-workload log configuration via a pod annotation: the workload declares
// its own multiline behavior, drop/sample rules, extra attributes or a
// service-name override — no agent config change, no restart. The annotation
// arrives for free through the metadata resolution every containerd file
// already performs; it is parsed once per file at resolve time.

import (
	"encoding/json"
	"fmt"

	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/obs"
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

// applyPodConfig applies the pod's annotation to a freshly resolved file.
// A malformed annotation must not lose logs: it is warned about (once — this
// runs once per file) and ignored, everything else about the file proceeds.
func (t *Tailer) applyPodConfig(f *file, annotations map[string]string) {
	raw, ok := annotations[LogAnnotation]
	if !ok || raw == "" {
		return
	}
	cfg, rules, err := parsePodLogConfig(raw)
	if err != nil {
		t.log.Warn("ignoring malformed pod log annotation", "path", f.path, "error", err)
		return
	}
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
		// gated on resolution), so reset() only clears empty stream state.
		// Without this the override was ignored until the next rotation, and
		// forever for a file that never rotates.
		f.multiline = cfg.Multiline
		t.newPipeline(f)
	}
	attrsMap := f.resource.Attributes()
	if cfg.ServiceName != "" {
		attrsMap.PutStr("service.name", cfg.ServiceName)
	}
	for k, v := range cfg.Attributes {
		if reservedIdentityAttr(k) {
			// The workload is authoritative about its own DESCRIPTION, never
			// about its IDENTITY — see reservedIdentityAttrs.
			obs.LogPodAttrsRefused.WithLabelValues(k).Inc()
			t.log.Warn("refusing a pod-annotation attribute that names resolved Kubernetes identity",
				"path", f.path, "key", k, "value", v, "annotation", LogAnnotation)
			continue
		}
		attrsMap.PutStr(k, v)
	}
}

// reservedIdentityAttrs are the resource attributes a pod annotation may NOT
// set: the ones the agent RESOLVED from the API server, as opposed to the ones
// the workload legitimately declares about itself.
//
// The distinction is a security boundary, not tidiness. `k8s.namespace.name` is
// what internal/agent/route keys tenancy on (route.Router.match reads it
// straight off this resource), so an unfiltered write let any user who can
// annotate a pod in their OWN namespace — an ordinary namespace-scoped action —
// send that pod's logs to a different tenant's endpoint under that tenant's
// X-Scope-OrgID, and simultaneously remove them from their own. The same write
// forged `service.name`/`service.instance.id`/`k8s.pod.name` in the backend and
// on every log-derived metric series bound to this resource.
//
// The operator's own key filter cannot stand in for this: `attrs.Filter` runs
// INSIDE `Attrs.Build`, which resolveMetadata calls BEFORE applyPodConfig, so a
// `resourceAttributes.disable` entry is applied and then overwritten.
//
// `service.name` is deliberately NOT here — overriding it is the annotation's
// documented purpose (and has its own `serviceName` field). It is descriptive:
// it renames the workload's own series, it does not move them to another owner.
var reservedIdentityAttrs = map[string]struct{}{
	"k8s.namespace.name":  {},
	"k8s.pod.name":        {},
	"k8s.pod.uid":         {},
	"k8s.pod.ip":          {},
	"k8s.container.name":  {},
	"k8s.node.name":       {},
	"container.id":        {},
	"container.name":      {},
	"service.namespace":   {},
	"service.instance.id": {},
}

func reservedIdentityAttr(k string) bool {
	_, bad := reservedIdentityAttrs[k]
	return bad
}
