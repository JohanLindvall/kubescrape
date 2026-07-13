package main

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	discoveryfake "k8s.io/client-go/discovery/fake"
	coretesting "k8s.io/client-go/testing"
)

// The -servicemonitors pre-check must verify the servicemonitors resource
// itself, not just the group/version: a cluster with only other
// monitoring.coreos.com/v1 CRDs (e.g. PrometheusRule) serves the group, but a
// servicemonitor informer there can never sync and would wedge readiness.
func TestCheckServiceMonitorCRD(t *testing.T) {
	disc := func(resources ...metav1.APIResource) *discoveryfake.FakeDiscovery {
		fake := &coretesting.Fake{}
		if resources != nil {
			fake.Resources = []*metav1.APIResourceList{{
				GroupVersion: "monitoring.coreos.com/v1",
				APIResources: resources,
			}}
		}
		return &discoveryfake.FakeDiscovery{Fake: fake}
	}

	if err := checkServiceMonitorCRD(disc(metav1.APIResource{Name: "servicemonitors"})); err != nil {
		t.Errorf("CRD present: %v", err)
	}
	if err := checkServiceMonitorCRD(disc(metav1.APIResource{Name: "prometheusrules"})); err == nil {
		t.Error("group present without servicemonitors must error")
	}
	if err := checkServiceMonitorCRD(disc()); err == nil {
		t.Error("absent group must error")
	}
}
