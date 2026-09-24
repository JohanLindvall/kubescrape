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
	"errors"
	"fmt"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// KubeConfig prefers an explicit kubeconfig path, then in-cluster config, then
// the default kubeconfig loading rules ($KUBECONFIG, ~/.kube/config).
//
// When the in-cluster attempt fails for a reason OTHER than "not in a cluster"
// and the fallback finds nothing either, the error carries both. The in-cluster
// one is the diagnosis: a pod with KUBERNETES_SERVICE_HOST set but no readable
// ServiceAccount token (automountServiceAccountToken: false, a hand-written
// manifest) otherwise failed startup with clientcmd's "no configuration has
// been provided, try setting KUBERNETES_MASTER environment variable" — a
// variable nothing here reads — while the missing token file was named
// nowhere.
func KubeConfig(path string) (*rest.Config, error) {
	var inClusterErr error
	if path == "" {
		cfg, err := rest.InClusterConfig()
		if err == nil {
			return cfg, nil
		}
		inClusterErr = err
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.ExplicitPath = path
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil && inClusterErr != nil && !errors.Is(inClusterErr, rest.ErrNotInCluster) {
		return nil, fmt.Errorf("in-cluster config: %w (and the kubeconfig fallback found none: %w)", inClusterErr, err)
	}
	return cfg, err
}
