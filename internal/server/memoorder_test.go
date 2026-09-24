package server

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// The monitor→Service memo and the PodMonitor snapshot are validated by change
// tokens a caller samples BEFORE it takes the memo's lock, so a caller can
// arrive holding a sample OLDER than the stamp: it sampled just before a change,
// then queued behind the build that included it. Both tokens only ever advance,
// so such a memo already includes everything that caller could have seen — but
// an EQUALITY test rebuilt it and stamped the older sample, which then forced
// the next current caller to rebuild AGAIN: two extra full cross products
// (20-89 ms each at scale, under a lock every node-targets and explain request
// waits on) for every request straddling a change.
func TestMonitorMemosAreNotRebuiltForACallerOlderThanTheStamp(t *testing.T) {
	s := repeatFixture(t, 1, 1, stubResolver{})
	if err := s.monitors.UpsertPodMonitor(podMonitorObj("a")); err != nil {
		t.Fatal(err)
	}

	// The late caller's sample, taken before the change below.
	oldMon, oldSvc := s.monitors.Generation(), s.services.Generation()
	s.monitoredServices()
	s.allPodMonitors()

	// The change, and the current caller that builds and stamps it.
	s.services.Upsert(&corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "svc2", Namespace: "prod", UID: "svc2-uid"},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(9090)}},
		},
	})
	if err := s.monitors.UpsertPodMonitor(podMonitorObj("b")); err != nil {
		t.Fatal(err)
	}
	s.monitoredServices()
	s.allPodMonitors()
	mon, pm := s.monBuilds.Load(), s.pmBuilds.Load()
	stampMon, stampSvc, stampPM := s.monGen, s.svcGen, s.pmGen

	// The late caller gets the lock now.
	s.monitoredServicesAt(oldMon, oldSvc)
	s.allPodMonitorsAt(oldMon)
	if got := s.monBuilds.Load(); got != mon {
		t.Errorf("a caller older than the stamp rebuilt the monitor→Service memo (%d builds, want %d)", got, mon)
	}
	if got := s.pmBuilds.Load(); got != pm {
		t.Errorf("a caller older than the stamp re-rendered the PodMonitor snapshot (%d, want %d)", got, pm)
	}
	if s.monGen != stampMon || s.svcGen != stampSvc || s.pmGen != stampPM {
		t.Errorf("the stamps moved backwards: (%d, %d, %d), want (%d, %d, %d)",
			s.monGen, s.svcGen, s.pmGen, stampMon, stampSvc, stampPM)
	}

	// And so the next CURRENT caller is served from the memo too.
	s.monitoredServices()
	s.allPodMonitors()
	if got := s.monBuilds.Load(); got != mon {
		t.Errorf("the next current caller rebuilt the monitor→Service memo (%d builds, want %d)", got, mon)
	}
	if got := s.pmBuilds.Load(); got != pm {
		t.Errorf("the next current caller re-rendered the PodMonitor snapshot (%d, want %d)", got, pm)
	}

	// A change PAST the stamp is still picked up at once.
	if err := s.monitors.UpsertPodMonitor(podMonitorObj("c")); err != nil {
		t.Fatal(err)
	}
	if got := len(s.allPodMonitors()); got != 3 {
		t.Errorf("PodMonitors = %d after a third was added, want 3", got)
	}
	s.monitoredServices()
	if got := s.monBuilds.Load(); got != mon+1 {
		t.Errorf("a monitor change did not rebuild the monitor→Service memo (%d builds, want %d)", got, mon+1)
	}
}
