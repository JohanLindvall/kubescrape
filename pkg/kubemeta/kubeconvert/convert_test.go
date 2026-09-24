package kubeconvert

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/JohanLindvall/kubescrape/internal/testrace"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

func TestFromPod(t *testing.T) {
	pod, byID := FromPod(testCorePod())

	if pod.Name != "pod1" || pod.Namespace != "ns1" || pod.UID != "uid-1" ||
		pod.NodeName != "node1" || pod.PodIP != "10.0.0.5" || pod.HostIP != "192.168.1.10" ||
		pod.Phase != "Running" {
		t.Fatalf("pod = %+v", pod)
	}
	if pod.StartedAt == nil || pod.Labels["app"] != "web" || pod.Annotations["prometheus.io/scrape"] != "true" {
		t.Fatalf("pod meta = %+v", pod)
	}

	// Containers in spec order: init, app, debug — with type, state and ports.
	if len(pod.Containers) != 3 {
		t.Fatalf("containers = %d: %+v", len(pod.Containers), pod.Containers)
	}
	initC, app, debug := pod.Containers[0], pod.Containers[1], pod.Containers[2]
	if initC.Type != "init" || initC.State != "terminated" || initC.ID != "initid" ||
		initC.ExitCode == nil || *initC.ExitCode != 0 || initC.FinishedAt == nil {
		t.Fatalf("init = %+v", initC)
	}
	if app.Type != "container" || app.State != "running" || app.ID != "appid2" ||
		!app.Ready || app.RestartCount != 1 || app.StartedAt == nil ||
		app.RuntimeID != "containerd://appid2" || app.ImageID != "sha256:img" {
		t.Fatalf("app = %+v", app)
	}
	if len(app.Ports) != 1 || app.Ports[0].Name != "http" || app.Ports[0].Port != 8080 || app.Ports[0].Protocol != "TCP" {
		t.Fatalf("app ports = %+v", app.Ports)
	}
	if debug.Type != "ephemeral" || debug.State != "waiting" || debug.WaitingReason != "PodInitializing" || debug.ID != "" {
		t.Fatalf("debug = %+v", debug)
	}

	// The ID index carries the live incarnations AND the restarted container's
	// previous incarnation from lastState.
	if len(byID) != 3 {
		t.Fatalf("byID = %v", byID)
	}
	if c, ok := byID["appid2"]; !ok || c.State != "running" {
		t.Fatalf("byID[appid2] = %+v (%v)", c, ok)
	}
	prev, ok := byID["appid1"]
	if !ok || prev.State != "terminated" || prev.Ready ||
		prev.ExitCode == nil || *prev.ExitCode != 137 ||
		prev.RuntimeID != "containerd://appid1" ||
		prev.StartedAt == nil || prev.FinishedAt == nil {
		t.Fatalf("previous incarnation = %+v (%v)", prev, ok)
	}
	if _, ok := byID["initid"]; !ok {
		t.Fatal("init container missing from the ID index")
	}
}

// FromPod must share no memory with the informer object: mutating the source
// afterwards must not change the returned model.
func TestFromPodDeepCopies(t *testing.T) {
	src := testCorePod()
	pod, byID := FromPod(src)

	src.Labels["app"] = "MUTATED"
	src.Annotations["prometheus.io/scrape"] = "MUTATED"
	src.Spec.Containers[0].Ports[0].ContainerPort = 9999
	src.Status.ContainerStatuses[0].ContainerID = "containerd://MUTATED"

	if pod.Labels["app"] != "web" {
		t.Error("labels alias the informer object")
	}
	if pod.Annotations["prometheus.io/scrape"] != "true" {
		t.Error("annotations alias the informer object")
	}
	if pod.Containers[1].Ports[0].Port != 8080 {
		t.Error("ports alias the informer object")
	}
	if byID["appid2"].RuntimeID != "containerd://appid2" {
		t.Error("container status aliases the informer object")
	}
}

// A pod with no statuses yet (just scheduled) still yields containers from the
// spec, with no IDs indexed.
func TestFromPodNoStatuses(t *testing.T) {
	pod, byID := FromPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "u"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app:1"}}},
	})
	if len(pod.Containers) != 1 || pod.Containers[0].State != "" || pod.Containers[0].ID != "" {
		t.Fatalf("containers = %+v", pod.Containers)
	}
	if len(byID) != 0 {
		t.Fatalf("byID = %v", byID)
	}
	if pod.Labels != nil || pod.Annotations != nil {
		t.Fatalf("empty maps must stay nil: %+v", pod)
	}
}

// Readiness and the deletion timestamp are carried on the model: the phase
// says neither. A draining pod stays Running for its whole grace period (the
// only signal it is going away), and a Running pod may be failing every
// readiness probe.
func TestFromPodReadyAndDeletionTimestamp(t *testing.T) {
	deleted := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	base := func(conds []corev1.PodCondition, deletionTS *metav1.Time) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "u", DeletionTimestamp: deletionTS},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: conds},
		}
	}

	ready := []corev1.PodCondition{
		{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	}
	notReady := []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}

	if pod, _ := FromPod(base(ready, nil)); !pod.Ready || pod.DeletionTimestamp != nil {
		t.Errorf("live ready pod: ready=%v deletionTimestamp=%v", pod.Ready, pod.DeletionTimestamp)
	}
	if pod, _ := FromPod(base(notReady, nil)); pod.Ready {
		t.Error("PodReady=False must not report Ready")
	}
	// No conditions at all (freshly scheduled): not ready, not terminating.
	if pod, _ := FromPod(base(nil, nil)); pod.Ready || pod.DeletionTimestamp != nil {
		t.Errorf("condition-less pod = %+v", pod)
	}

	pod, _ := FromPod(base(ready, &metav1.Time{Time: deleted}))
	if pod.DeletionTimestamp == nil || !pod.DeletionTimestamp.Equal(deleted) {
		t.Fatalf("deletionTimestamp = %v, want %v", pod.DeletionTimestamp, deleted)
	}
	// Still Running and Ready — which is exactly why the timestamp is needed.
	if pod.Phase != "Running" || !pod.Ready || pod.DeletedAt != nil {
		t.Fatalf("terminating pod = %+v (DeletedAt is the tombstone marker, not this)", pod)
	}
}

// A CrashLoopBackOff pod is the shape that breaks previousIncarnation's "a
// restarted container gets a NEW runtime ID" premise: between restarts the
// kubelet keeps status.containerID EQUAL to lastState.terminated.containerID
// (observed live on real clusters). One runtime ID must have exactly ONE state
// in the response — the live one — or GET /v1/containers/{id} contradicts the
// pod document embedded in the same body.
func TestFromPodCrashLoopBackOffSameContainerID(t *testing.T) {
	started := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	finished := started.Add(2 * time.Second)
	pod, byID := FromPod(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns", UID: "u"},
		Spec: corev1.PodSpec{
			NodeName:   "node1",
			Containers: []corev1.Container{{Name: "app", Image: "app:2"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "app",
				ContainerID:  "containerd://sameid",
				ImageID:      "sha256:img2",
				RestartCount: 5,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "CrashLoopBackOff",
				}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ContainerID: "containerd://sameid",
					ExitCode:    1,
					StartedAt:   metav1.Time{Time: started},
					FinishedAt:  metav1.Time{Time: finished},
				}},
			}},
		},
	})

	if len(byID) != 1 {
		t.Fatalf("byID = %v, want exactly one entry: lastState names the SAME runtime ID as the live status", byID)
	}
	got, ok := byID["sameid"]
	if !ok {
		t.Fatalf("byID missing the container ID: %v", byID)
	}
	// The live status wins: waiting/CrashLoopBackOff, no exit code.
	if got.State != "waiting" || got.WaitingReason != "CrashLoopBackOff" || got.ExitCode != nil {
		t.Errorf("byID[sameid] = state %q reason %q exitCode %v, want the live waiting/CrashLoopBackOff view",
			got.State, got.WaitingReason, got.ExitCode)
	}
	// ...and it agrees with the pod document served in the same response.
	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %+v", pod.Containers)
	}
	cur := pod.Containers[0]
	if got.State != cur.State || got.WaitingReason != cur.WaitingReason ||
		got.RestartCount != cur.RestartCount || got.Image != cur.Image ||
		(got.ExitCode == nil) != (cur.ExitCode == nil) {
		t.Errorf("byID entry %+v disagrees with the pod document's container %+v", got, cur)
	}
}

func testCorePod() *corev1.Pod {
	created := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	started := created.Add(5 * time.Second)
	cStarted := created.Add(8 * time.Second)
	prevStart := created.Add(-time.Hour)
	prevEnd := created.Add(-30 * time.Minute)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pod1",
			Namespace:         "ns1",
			UID:               "uid-1",
			Labels:            map[string]string{"app": "web"},
			Annotations:       map[string]string{"prometheus.io/scrape": "true"},
			CreationTimestamp: metav1.Time{Time: created},
		},
		Spec: corev1.PodSpec{
			NodeName:       "node1",
			InitContainers: []corev1.Container{{Name: "init", Image: "init:1"}},
			Containers: []corev1.Container{{
				Name:  "app",
				Image: "app:2",
				Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP}},
			}},
			EphemeralContainers: []corev1.EphemeralContainer{{
				EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: "debug", Image: "busybox:1"},
			}},
		},
		Status: corev1.PodStatus{
			Phase:     corev1.PodRunning,
			PodIP:     "10.0.0.5",
			HostIP:    "192.168.1.10",
			StartTime: &metav1.Time{Time: started},
			InitContainerStatuses: []corev1.ContainerStatus{{
				Name:        "init",
				ContainerID: "containerd://initid",
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   0,
					StartedAt:  metav1.Time{Time: created},
					FinishedAt: metav1.Time{Time: started},
				}},
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:         "app",
				ContainerID:  "containerd://appid2",
				ImageID:      "sha256:img",
				RestartCount: 1,
				Ready:        true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{
					StartedAt: metav1.Time{Time: cStarted},
				}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ContainerID: "containerd://appid1",
					ExitCode:    137,
					StartedAt:   metav1.Time{Time: prevStart},
					FinishedAt:  metav1.Time{Time: prevEnd},
				}},
			}},
			EphemeralContainerStatuses: []corev1.ContainerStatus{{
				Name:  "debug",
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "PodInitializing"}},
			}},
		},
	}
}

// previousIncarnation runs on the informer callback path, once per restarted
// container per pod upsert. It clears StartedAt/FinishedAt/ExitCode and refills
// them from lastState, so a clone that copies them first allocates up to three
// values per call purely to discard them — invisible in behaviour and paid on
// every update of every crash-looping pod in the cluster.
//
// The budget is what the result genuinely needs: its own Ports array plus the
// three pointers fillTerminated sets.
func TestPreviousIncarnationAllocationBudget(t *testing.T) {
	if testrace.Enabled {
		t.Skip("-race perturbs allocation counts")
	}
	const budget = 4
	now := time.Unix(1_700_000_000, 0)
	lastState := corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ContainerID: "containerd://appid1", ExitCode: 137,
		StartedAt:  metav1.Time{Time: now.Add(-time.Hour)},
		FinishedAt: metav1.Time{Time: now.Add(-time.Minute)},
	}}
	for _, tc := range []struct {
		name  string
		state corev1.ContainerState
	}{
		{"running", corev1.ContainerState{Running: &corev1.ContainerStateRunning{
			StartedAt: metav1.Time{Time: now},
		}}},
		// A crash-looping container caught between restarts: the CURRENT state
		// carries all three pointers, so all three were cloned and dropped.
		{"terminated", corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ContainerID: "containerd://appid2", ExitCode: 1,
			StartedAt: metav1.Time{Time: now}, FinishedAt: metav1.Time{Time: now},
		}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &corev1.ContainerStatus{
				Name: "app", ContainerID: "containerd://appid2", RestartCount: 1,
				State: tc.state, LastTerminationState: lastState,
			}
			c := convertContainer("app", "img",
				[]corev1.ContainerPort{{Name: "http", ContainerPort: 8080}}, "container", st)
			got := testing.AllocsPerRun(200, func() {
				prev, _ := previousIncarnation(c, st)
				prevSink = prev
			})
			if got > budget {
				t.Errorf("previousIncarnation allocates %v times, want at most %d", got, budget)
			}
		})
	}
}

// prevSink defeats the dead-store elimination that would let the budget above
// hold vacuously.
var prevSink kubemeta.Container

// The previous incarnation is served as HISTORY under the crashed container's
// own ID, and the struct copy it is built from carried three fields that
// describe the LIVE container instead — the harm the same-ID guard's comment
// already names, applied in that branch and not in this one.
//
// The image is the sharp one: spec.containers[].image is mutable in place, so
// `kubectl set image` restarts the container, leaves the crashed incarnation's
// ID in lastState, and GET /v1/containers/{oldID} answered with the NEW image
// and the new digest — which the tailer then stamps on every record of that
// incarnation's still-present log file, and the cadvisor router on its series.
// ContainerStateTerminated carries no image, so the honest answer is none.
// RestartCount was wrong by exactly one on every restarted container, image
// change or not: the kubelet increments it when the NEW container starts.
func TestPreviousIncarnationDoesNotCarryTheLiveContainersImageOrRestartCount(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: "u"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			// The image was changed in place; the crashed incarnation ran the
			// OLD one, which nothing in the API object records any more.
			{Name: "app", Image: "app:v2"},
		}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", ContainerID: "containerd://new", ImageID: "sha256:v2",
				RestartCount: 3, Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ContainerID: "containerd://old", ExitCode: 1,
				}},
			}},
		},
	}
	_, byID := FromPod(pod)
	prev, ok := byID["old"]
	if !ok {
		t.Fatalf("the previous incarnation is not indexed: %v", byID)
	}
	if prev.Image != "" || prev.ImageID != "" {
		t.Errorf("the previous incarnation reports image %q / imageID %q — the LIVE container's, on a record presented as history",
			prev.Image, prev.ImageID)
	}
	if prev.RestartCount != 2 {
		t.Errorf("previous RestartCount = %d, want 2: the status's 3 counts the live container's start", prev.RestartCount)
	}
	// The live container is untouched by any of it.
	cur := byID["new"]
	if cur.Image != "app:v2" || cur.ImageID != "sha256:v2" || cur.RestartCount != 3 {
		t.Errorf("the live container lost its own image/restartCount: %+v", cur)
	}
}

// A container replaced without the restart counter moving must not produce a
// negative count. (Reachable in principle — a lastState beside restartCount 0 —
// and a negative count is not a thing the model can mean.)
func TestPreviousIncarnationRestartCountIsNeverNegative(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: "u"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app:v1"}}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", ContainerID: "containerd://new", RestartCount: 0,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ContainerID: "containerd://old", ExitCode: 0,
				}},
			}},
		},
	}
	_, byID := FromPod(pod)
	if got := byID["old"].RestartCount; got != 0 {
		t.Errorf("previous RestartCount = %d, want 0", got)
	}
}

// previousIncarnation BUILDS the history record rather than copying the live
// container and clearing it, so a field added to kubemeta.Container defaults to
// absent on a record presented as history instead of silently inheriting the
// LIVE container's value — which is how Image, ImageID and RestartCount leaked
// for as long as the record was a copy-and-clear.
//
// Every field of the live container is filled with a non-zero sentinel that
// lastState does not share, so the result may equal the live value only on the
// SPEC-derived fields that are meant to carry over. A new field fails here
// until it is either answered from lastState or added to carried below with a
// reason; a field of a kind sentinel cannot fill fails loudly rather than being
// skipped.
func TestPreviousIncarnationCarriesOnlyTheSpecFields(t *testing.T) {
	carried := map[string]bool{"Name": true, "Type": true, "Ports": true}
	var live kubemeta.Container
	lv := reflect.ValueOf(&live).Elem()
	for i := range lv.NumField() {
		fillSentinel(t, lv.Field(i), lv.Type().Field(i).Name)
	}
	live.ID = "liveid" // previousIncarnation refuses a lastState naming the live ID
	now := time.Unix(1_600_000_000, 0)
	st := &corev1.ContainerStatus{
		Name: live.Name, RestartCount: 3,
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ContainerID: "containerd://previd", ExitCode: 137,
			StartedAt:  metav1.Time{Time: now.Add(-time.Hour)},
			FinishedAt: metav1.Time{Time: now},
		}},
	}
	prev, ok := previousIncarnation(live, st)
	if !ok {
		t.Fatal("a distinct lastState was not modelled as a previous incarnation")
	}
	pv := reflect.ValueOf(prev)
	for i := range lv.NumField() {
		name := lv.Type().Field(i).Name
		same := reflect.DeepEqual(lv.Field(i).Interface(), pv.Field(i).Interface())
		switch {
		case carried[name] && !same:
			t.Errorf("%s is spec-derived and must carry over: live %v, previous %v",
				name, lv.Field(i).Interface(), pv.Field(i).Interface())
		case !carried[name] && same:
			t.Errorf("%s on the previous incarnation is the LIVE container's value %v: answer it from lastState or leave it absent",
				name, pv.Field(i).Interface())
		}
	}
	if len(prev.Ports) > 0 && &prev.Ports[0] == &live.Ports[0] {
		t.Error("the previous incarnation shares the live container's Ports array")
	}
}

// fillSentinel sets v to a non-zero value derived from name.
func fillSentinel(t *testing.T, v reflect.Value, name string) {
	t.Helper()
	switch v.Kind() {
	case reflect.String:
		v.SetString("live-" + name)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(41)
	case reflect.Slice:
		s := reflect.MakeSlice(v.Type(), 1, 1)
		fillSentinel(t, s.Index(0), name)
		v.Set(s)
	case reflect.Pointer:
		p := reflect.New(v.Type().Elem())
		fillSentinel(t, p.Elem(), name)
		v.Set(p)
	case reflect.Struct:
		if v.Type() == reflect.TypeFor[time.Time]() {
			v.Set(reflect.ValueOf(time.Unix(1_000_000_000, 0)))
			return
		}
		for i := range v.NumField() {
			fillSentinel(t, v.Field(i), name+"."+v.Type().Field(i).Name)
		}
	default:
		t.Fatalf("%s: no sentinel for kind %s; teach fillSentinel", name, v.Kind())
	}
}
