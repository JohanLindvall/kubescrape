// Package kubecfg builds a *rest.Config with this repo's kubeconfig
// precedence: an explicit path, then in-cluster credentials, then the default
// loading rules.
//
// It is a SUBPACKAGE of internal/cli rather than a function in it, and the
// reason is linkage, not taste. clientcmd drags k8s.io/client-go and the API
// machinery in with it, and internal/cli is imported by both binaries for
// their logger, their shared flag blocks and their memory limit — so a
// KubeConfig sitting beside those pinned the whole Kubernetes client into an
// agent built WITHOUT the `events` tag, which is the one thing that tag exists
// to remove (2.71 MiB of the shipped binary, on top of the 28.65 MiB the tag
// itself removes). One implementation still, one import away.
package kubecfg

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

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
