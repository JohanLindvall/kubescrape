// Package cli holds the small startup helpers both binaries' mains share: the
// process logger built from -log-level/-log-format, the kubeconfig precedence
// builder, and the comma-separated list splitter behind several flags. Each
// existed once per main, byte-identical in behavior, with only the function
// names drifting (buildConfig vs kubeConfig) — the shape that eventually
// drifts for real.
package cli

import (
	"fmt"
	"log/slog"
	"os"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewLogger builds the process logger from the -log-level/-log-format flag
// values: any level slog.Level.UnmarshalText accepts, format text or json,
// output on stderr.
func NewLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("log level %q: %w", level, err)
	}
	opts := &slog.HandlerOptions{Level: lvl}
	switch format {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("log format %q (want text or json)", format)
	}
}

// KubeConfig prefers an explicit kubeconfig path, then in-cluster config, then
// the default kubeconfig loading rules ($KUBECONFIG, ~/.kube/config).
func KubeConfig(path string) (*rest.Config, error) {
	if path == "" {
		if cfg, err := rest.InClusterConfig(); err == nil {
			return cfg, nil
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = path
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
}

// SplitList splits a comma-separated flag value, trimming whitespace around
// each entry and dropping empty ones; an empty or all-blank value yields nil.
func SplitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
