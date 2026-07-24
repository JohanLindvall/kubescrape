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

// parsePodLogConfig parses the annotation value and compiles its rules.
func parsePodLogConfig(raw string) (*podLogConfig, *logline.LineFilter, error) {
	var cfg podLogConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, nil, fmt.Errorf("parsing %s annotation: %w", LogAnnotation, err)
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
	f.multiline = cfg.Multiline
	f.podRules = rules
	attrsMap := f.resource.Attributes()
	if cfg.ServiceName != "" {
		attrsMap.PutStr("service.name", cfg.ServiceName)
	}
	for k, v := range cfg.Attributes {
		attrsMap.PutStr(k, v)
	}
}
