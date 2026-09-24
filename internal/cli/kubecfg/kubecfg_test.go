package kubecfg

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
)

// A pod whose service env vars are set but whose ServiceAccount token is not
// mounted (automountServiceAccountToken: false) used to fail with clientcmd's
// advice to set KUBERNETES_MASTER — which nothing here reads — because the
// in-cluster error was discarded. The error must name the real cause.
//
// KUBECONFIG is pointed at a file that does not exist: the default loading
// rules read it at call time, whereas ~/.kube/config is captured at package
// init, so setting HOME would NOT keep a developer's own kubeconfig out.
func TestInClusterFailureIsReportedWhenTheFallbackFindsNothing(t *testing.T) {
	if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
		t.Skip("running in a pod with a ServiceAccount token mounted: the in-cluster config would succeed")
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "none"))

	_, err := KubeConfig("")
	if err == nil {
		t.Fatal("KubeConfig succeeded with no token and no kubeconfig")
	}
	if !strings.Contains(err.Error(), "in-cluster config") || !strings.Contains(err.Error(), "serviceaccount/token") {
		t.Errorf("error %q does not name the in-cluster failure (the missing token file)", err)
	}
}

// Outside a cluster the in-cluster attempt failing is expected, not a cause:
// the error stays the kubeconfig one alone.
func TestNotInClusterLeavesTheKubeconfigErrorAlone(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "none"))

	_, err := KubeConfig("")
	if err == nil {
		t.Fatal("KubeConfig succeeded with no cluster and no kubeconfig")
	}
	if strings.Contains(err.Error(), "in-cluster") || strings.Contains(err.Error(), rest.ErrNotInCluster.Error()) {
		t.Errorf("error %q blames the in-cluster path for a process that is not in a cluster", err)
	}
}
