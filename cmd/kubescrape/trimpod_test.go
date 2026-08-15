package main

// The pod informer's transform drops most of the pod SPEC and most of the
// STATUS to keep the cache small. That is only safe while the dropped parts
// are exactly the parts nothing reads — and "nothing reads them" is a property
// of kubeconvert.FromPod (plus the three ObjectMeta fields store.UpsertPod
// takes off the same cached object), which can change. So assert it the strong
// way: convert the ORIGINAL and the TRIMMED pod and require the results to be
// identical. A future FromPod that starts reading, say, container resources
// fails here rather than in production, where it would surface as a silently
// empty field.

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/JohanLindvall/kubescrape/pkg/kubemeta/kubeconvert"
)

// fatPod is a pod carrying every heavy spec field trimPod removes, plus the
// few it must preserve.
func fatPod() *corev1.Pod {
	probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{
		HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt32(8080)},
	}}
	heavy := func(name string) corev1.Container {
		return corev1.Container{
			Name:       name,
			Image:      "registry.example.com/" + name + ":v1.2.3",
			Command:    []string{"/bin/app", "--serve"},
			Args:       []string{"--flag=value"},
			WorkingDir: "/srv",
			Ports: []corev1.ContainerPort{
				{Name: "metrics", ContainerPort: 9090, Protocol: corev1.ProtocolTCP},
				{Name: "http", ContainerPort: 8080, Protocol: corev1.ProtocolTCP},
			},
			Env: []corev1.EnvVar{
				{Name: "SECRET", Value: "very-long-value-that-should-not-be-cached"},
				{Name: "REF", ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
			},
			EnvFrom: []corev1.EnvFromSource{{
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: "cm"}}}},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
				Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
			},
			VolumeMounts:   []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
			VolumeDevices:  []corev1.VolumeDevice{{Name: "blk", DevicePath: "/dev/xvda"}},
			ResizePolicy:   []corev1.ContainerResizePolicy{{ResourceName: corev1.ResourceCPU, RestartPolicy: corev1.NotRequired}},
			LivenessProbe:  probe,
			ReadinessProbe: probe,
			StartupProbe:   probe,
			Lifecycle: &corev1.Lifecycle{
				PreStop: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"sleep", "5"}}}},
			SecurityContext: &corev1.SecurityContext{RunAsUser: ptr(int64(1000))},
		}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "web-abc123", UID: types.UID("uid-1"),
			ResourceVersion: "100042",
			Labels:          map[string]string{"app": "web"},
			Annotations:     map[string]string{"prometheus.io/scrape": "true"},
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "kubectl", Operation: metav1.ManagedFieldsOperationApply},
			},
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-abc", UID: "rs-1"}},
		},
		Spec: corev1.PodSpec{
			NodeName:       "node-1",
			HostNetwork:    false,
			Containers:     []corev1.Container{heavy("app"), heavy("sidecar")},
			InitContainers: []corev1.Container{heavy("init")},
			EphemeralContainers: []corev1.EphemeralContainer{{
				EphemeralContainerCommon: corev1.EphemeralContainerCommon{
					Name: "debug", Image: "busybox",
					Ports:     []corev1.ContainerPort{{Name: "dbg", ContainerPort: 7777}},
					Env:       []corev1.EnvVar{{Name: "X", Value: "y"}},
					Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
				}}},
			Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
			Tolerations:               []corev1.Toleration{{Key: "node.kubernetes.io/unreachable"}},
			NodeSelector:              map[string]string{"kubernetes.io/os": "linux"},
			SecurityContext:           &corev1.PodSecurityContext{RunAsNonRoot: ptr(true)},
			ImagePullSecrets:          []corev1.LocalObjectReference{{Name: "regcred"}},
			TopologySpreadConstraints: []corev1.TopologySpreadConstraint{{TopologyKey: "zone"}},
			Affinity:                  &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}},
			HostAliases:               []corev1.HostAlias{{IP: "10.0.0.1"}},
			// Every remaining field trimPod nils. Leaving any of them unset here
			// makes the equivalence assertion below compare nil to nil for it —
			// the guarantee would hold vacuously for exactly the field that was
			// forgotten.
			ReadinessGates:  []corev1.PodReadinessGate{{ConditionType: "example.com/ready"}},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: "example.com/gate"}},
			ResourceClaims:  []corev1.PodResourceClaim{{Name: "claim"}},
			Overhead:        corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("64Mi")},
			DNSConfig:       &corev1.PodDNSConfig{Nameservers: []string{"10.0.0.10"}},
		},
		Status: fatStatus(),
	}
}

// fatStatus carries every status field trimPodStatus removes, and every field
// it must preserve — including the three per-container arms (running, waiting,
// terminated) and a lastState, since each is read through a different branch
// of convertContainer. A field left unset here makes the equivalence assertion
// below compare zero to zero for it, and the guarantee holds vacuously for
// exactly the field that was forgotten.
func fatStatus() corev1.PodStatus {
	// A FIXED instant, not metav1.Now(): the guard below converts two
	// separately built fixtures and compares the results, and a clock reading
	// would differ between them for reasons that have nothing to do with the
	// trim.
	now := metav1.NewTime(time.Date(2026, 7, 5, 12, 0, 0, 0, time.UTC))
	fat := func(c corev1.ContainerStatus) corev1.ContainerStatus {
		rro := corev1.RecursiveReadOnlyDisabled
		c.Ready = true
		// The RESOLVED image, deliberately not the spec's: FromPod takes the
		// spec image, and if it ever started taking this one instead, a fixture
		// that spelled them the same would pass while the trim silently emptied
		// the field.
		c.Image = "registry.example.com/" + c.Name + "@sha256:resolved"
		c.ImageID = "registry.example.com/" + c.Name + "@sha256:deadbeef"
		c.Started = ptr(true)
		c.Resources = &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("128Mi")},
			Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m")},
		}
		c.AllocatedResources = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")}
		c.AllocatedResourcesStatus = []corev1.ResourceStatus{{Name: "example.com/gpu"}}
		c.VolumeMounts = []corev1.VolumeMountStatus{
			{Name: "data", MountPath: "/data", RecursiveReadOnly: &rro}}
		c.User = &corev1.ContainerUser{Linux: &corev1.LinuxContainerUser{UID: 1000, GID: 1000}}
		c.StopSignal = ptr(corev1.SIGTERM)
		return c
	}
	return corev1.PodStatus{
		Phase: corev1.PodRunning, PodIP: "10.1.2.3",
		PodIPs:    []corev1.PodIP{{IP: "10.1.2.3"}, {IP: "fd00::1"}},
		HostIP:    "192.168.1.1",
		StartTime: &now,
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: now,
				Reason: "SomeReason", Message: "some message"},
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: now},
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: now},
		},
		InitContainerStatuses: []corev1.ContainerStatus{fat(corev1.ContainerStatus{
			Name: "init", ContainerID: "containerd://init1", RestartCount: 1,
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: 0, Signal: 9, Reason: "Completed", Message: "done",
				StartedAt: now, FinishedAt: now, ContainerID: "containerd://init1"}},
		})},
		ContainerStatuses: []corev1.ContainerStatus{
			fat(corev1.ContainerStatus{
				Name: "app", ContainerID: "containerd://abc123", RestartCount: 2,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 137, Signal: 9, Reason: "OOMKilled", Message: "killed",
					StartedAt: now, FinishedAt: now, ContainerID: "containerd://old"}},
			}),
			fat(corev1.ContainerStatus{
				Name: "sidecar", ContainerID: "containerd://side1", RestartCount: 7,
				State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{
					Reason: "CrashLoopBackOff", Message: "back-off 5m0s restarting failed container"}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 1, Reason: "Error", Message: "panic: nil pointer dereference",
					StartedAt: now, FinishedAt: now, ContainerID: "containerd://side0"}},
			}),
		},
		EphemeralContainerStatuses: []corev1.ContainerStatus{fat(corev1.ContainerStatus{
			Name: "debug", ContainerID: "containerd://dbg1",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}},
		})},
		// Every remaining pod-level status field trimPodStatus drops.
		Message: "pod message", Reason: "Evicted", NominatedNodeName: "node-2",
		QOSClass:                    corev1.PodQOSBurstable,
		HostIPs:                     []corev1.HostIP{{IP: "192.168.1.1"}},
		Resize:                      "InProgress",
		ObservedGeneration:          4,
		ResourceClaimStatuses:       []corev1.PodResourceClaimStatus{{Name: "claim"}},
		ExtendedResourceClaimStatus: &corev1.PodExtendedResourceClaimStatus{ResourceClaimName: "erc"},
		AllocatedResources:          corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m")},
		Resources: &corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
	}
}

func ptr[T any](v T) *T { return &v }

// TestTrimPodPreservesEverythingFromPodReads is the guarantee that makes the
// trim safe.
func TestTrimPodPreservesEverythingFromPodReads(t *testing.T) {
	original := fatPod()
	wantPod, wantByID := kubeconvert.FromPod(original)

	trimmed, err := trimPod(fatPod())
	if err != nil {
		t.Fatalf("trimPod: %v", err)
	}
	gotPod, gotByID := kubeconvert.FromPod(trimmed.(*corev1.Pod))

	if !reflect.DeepEqual(wantPod, gotPod) {
		t.Errorf("trimming changed the converted pod:\n original: %+v\n trimmed:  %+v", wantPod, gotPod)
	}
	if !reflect.DeepEqual(wantByID, gotByID) {
		t.Errorf("trimming changed the container index:\n original: %+v\n trimmed:  %+v", wantByID, gotByID)
	}
}

// The trim must actually trim — otherwise the test above passes vacuously and
// the memory win silently disappears.
func TestTrimPodActuallyDropsTheHeavyFields(t *testing.T) {
	trimmed, err := trimPod(fatPod())
	if err != nil {
		t.Fatalf("trimPod: %v", err)
	}
	p := trimmed.(*corev1.Pod)
	if p.ManagedFields != nil {
		t.Error("managedFields retained")
	}
	for _, f := range []struct {
		name string
		got  any
	}{
		{"Spec.Volumes", p.Spec.Volumes},
		{"Spec.Tolerations", p.Spec.Tolerations},
		{"Spec.Affinity", p.Spec.Affinity},
		{"Spec.NodeSelector", p.Spec.NodeSelector},
		{"Spec.SecurityContext", p.Spec.SecurityContext},
		{"Spec.ImagePullSecrets", p.Spec.ImagePullSecrets},
		{"Spec.TopologySpreadConstraints", p.Spec.TopologySpreadConstraints},
		{"Spec.HostAliases", p.Spec.HostAliases},
		{"Spec.ReadinessGates", p.Spec.ReadinessGates},
		{"Spec.SchedulingGates", p.Spec.SchedulingGates},
		{"Spec.ResourceClaims", p.Spec.ResourceClaims},
		{"Spec.Overhead", p.Spec.Overhead},
		{"Spec.DNSConfig", p.Spec.DNSConfig},
		{"container Env", p.Spec.Containers[0].Env},
		{"container EnvFrom", p.Spec.Containers[0].EnvFrom},
		{"container Args", p.Spec.Containers[0].Args},
		{"container WorkingDir", p.Spec.Containers[0].WorkingDir},
		{"container VolumeMounts", p.Spec.Containers[0].VolumeMounts},
		{"container VolumeDevices", p.Spec.Containers[0].VolumeDevices},
		{"container ResizePolicy", p.Spec.Containers[0].ResizePolicy},
		{"container LivenessProbe", p.Spec.Containers[0].LivenessProbe},
		{"container ReadinessProbe", p.Spec.Containers[0].ReadinessProbe},
		{"container StartupProbe", p.Spec.Containers[0].StartupProbe},
		{"container Lifecycle", p.Spec.Containers[0].Lifecycle},
		{"container SecurityContext", p.Spec.Containers[0].SecurityContext},
		{"container Command", p.Spec.Containers[0].Command},
		{"init container Env", p.Spec.InitContainers[0].Env},
		{"ephemeral container Env", p.Spec.EphemeralContainers[0].Env},
	} {
		if !reflect.ValueOf(f.got).IsZero() {
			t.Errorf("%s was not trimmed: %+v", f.name, f.got)
		}
	}
	if !reflect.ValueOf(p.Spec.Containers[0].Resources).IsZero() {
		t.Error("container Resources was not trimmed")
	}
	// ...and the fields that must survive.
	if p.Spec.NodeName != "node-1" || p.Spec.Containers[0].Image == "" ||
		len(p.Spec.Containers[0].Ports) != 2 || p.Spec.Containers[0].Name != "app" {
		t.Errorf("trim removed something FromPod needs: %+v", p.Spec.Containers[0])
	}
}

// The status half, which on a current kubelet is nearly half of what the spec
// trim leaves behind. Same vacuity argument as above: without it the
// equivalence guard passes just as happily over a transform that trims
// nothing.
func TestTrimPodActuallyDropsTheHeavyStatusFields(t *testing.T) {
	trimmed, err := trimPod(fatPod())
	if err != nil {
		t.Fatalf("trimPod: %v", err)
	}
	st := trimmed.(*corev1.Pod).Status

	for _, f := range []struct {
		name string
		got  any
	}{
		{"Status.Message", st.Message},
		{"Status.Reason", st.Reason},
		{"Status.NominatedNodeName", st.NominatedNodeName},
		{"Status.QOSClass", st.QOSClass},
		{"Status.HostIPs", st.HostIPs},
		{"Status.Resize", st.Resize},
		{"Status.ObservedGeneration", st.ObservedGeneration},
		{"Status.ResourceClaimStatuses", st.ResourceClaimStatuses},
		{"Status.ExtendedResourceClaimStatus", st.ExtendedResourceClaimStatus},
		{"Status.AllocatedResources", st.AllocatedResources},
		{"Status.Resources", st.Resources},
	} {
		if !reflect.ValueOf(f.got).IsZero() {
			t.Errorf("%s was not trimmed: %+v", f.name, f.got)
		}
	}

	// Only the PodReady condition survives, and only its Status.
	if len(st.Conditions) != 1 || st.Conditions[0].Type != corev1.PodReady ||
		st.Conditions[0].Status != corev1.ConditionTrue {
		t.Fatalf("conditions were not reduced to PodReady: %+v", st.Conditions)
	}
	if !reflect.DeepEqual(st.Conditions[0],
		corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}) {
		t.Errorf("the kept condition carries more than its status: %+v", st.Conditions[0])
	}
	// A reslice would keep the other three conditions' array alive, which is
	// the cost the reduction exists to remove.
	if cap(st.Conditions) != 1 {
		t.Errorf("the kept condition still points into the full array (cap %d): the dropped conditions are retained", cap(st.Conditions))
	}

	all := [][]corev1.ContainerStatus{st.InitContainerStatuses, st.ContainerStatuses, st.EphemeralContainerStatuses}
	if len(st.InitContainerStatuses) != 1 || len(st.ContainerStatuses) != 2 || len(st.EphemeralContainerStatuses) != 1 {
		t.Fatalf("a container status list was dropped: %+v", all)
	}
	for _, list := range all {
		for i := range list {
			c := &list[i]
			for _, f := range []struct {
				name string
				got  any
			}{
				{"Image", c.Image},
				{"Started", c.Started},
				{"Resources", c.Resources},
				{"AllocatedResources", c.AllocatedResources},
				{"AllocatedResourcesStatus", c.AllocatedResourcesStatus},
				{"VolumeMounts", c.VolumeMounts},
				{"User", c.User},
				{"StopSignal", c.StopSignal},
			} {
				if !reflect.ValueOf(f.got).IsZero() {
					t.Errorf("container %q status %s was not trimmed: %+v", c.Name, f.name, f.got)
				}
			}
			for arm, s := range map[string]*corev1.ContainerState{"state": &c.State, "lastState": &c.LastTerminationState} {
				if s.Waiting != nil && s.Waiting.Message != "" {
					t.Errorf("container %q %s.waiting.message retained: %q", c.Name, arm, s.Waiting.Message)
				}
				if s.Terminated != nil {
					if s.Terminated.Reason != "" || s.Terminated.Message != "" || s.Terminated.Signal != 0 {
						t.Errorf("container %q %s.terminated kept reason/message/signal: %+v", c.Name, arm, s.Terminated)
					}
				}
			}
		}
	}

	// ...and what the status must still carry.
	if st.Phase != corev1.PodRunning || st.PodIP == "" || len(st.PodIPs) != 2 ||
		st.HostIP == "" || st.StartTime == nil {
		t.Errorf("trim removed a pod status field FromPod reads: %+v", st)
	}
	app := st.ContainerStatuses[0]
	if app.Name != "app" || app.ContainerID == "" || app.ImageID == "" ||
		app.RestartCount != 2 || !app.Ready || app.State.Running == nil ||
		app.LastTerminationState.Terminated == nil ||
		app.LastTerminationState.Terminated.ContainerID == "" {
		t.Errorf("trim removed a container status field FromPod reads: %+v", app)
	}
}

// The trim's contract covers everything that reads the CACHED object, and
// store.UpsertPod reads three fields off it that never pass through FromPod:
// the UID it keys records by, the resourceVersion its resync short-circuit
// compares, and the owner references that become every served pod's owner
// chain. The equivalence guard above cannot see them — a trim that nilled
// OwnerReferences would pass it and silently strip the owner chain from every
// pod document.
func TestTrimPodPreservesEverythingUpsertPodReads(t *testing.T) {
	original := fatPod()
	trimmed, err := trimPod(fatPod())
	if err != nil {
		t.Fatalf("trimPod: %v", err)
	}
	p := trimmed.(*corev1.Pod)

	if p.UID != original.UID {
		t.Errorf("UID changed: %q -> %q; the store keys every record by it", original.UID, p.UID)
	}
	if p.ResourceVersion != original.ResourceVersion {
		t.Errorf("resourceVersion changed: %q -> %q; the resync short-circuit compares it, so every resync would rewrite the store under the write lock",
			original.ResourceVersion, p.ResourceVersion)
	}
	if !reflect.DeepEqual(p.OwnerReferences, original.OwnerReferences) {
		t.Errorf("ownerReferences changed:\n original: %+v\n trimmed:  %+v\n every served pod's owner chain is resolved from these",
			original.OwnerReferences, p.OwnerReferences)
	}
	if len(p.OwnerReferences) == 0 || p.ResourceVersion == "" {
		t.Fatal("the fixture must populate these, or this guard holds vacuously")
	}
}

// Services share the pod informer's factory, and services.Index reads their
// SPEC. The transform must leave every non-pod object alone apart from
// managedFields.
func TestTrimPodLeavesServicesIntact(t *testing.T) {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "prod", Name: "web",
			ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "kubectl"}},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "web"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt32(8080)}},
		},
	}
	out, err := trimPod(svc)
	if err != nil {
		t.Fatalf("trimPod: %v", err)
	}
	got := out.(*corev1.Service)
	if got.ManagedFields != nil {
		t.Error("service managedFields retained")
	}
	if len(got.Spec.Selector) != 1 || len(got.Spec.Ports) != 1 || got.Spec.Ports[0].Port != 80 {
		t.Fatalf("service spec was trimmed; scrape discovery depends on it: %+v", got.Spec)
	}
}

// client-go may apply a transform more than once to the same object.
//
// The comparison must be against a SNAPSHOT taken before the second call.
// trimPod mutates in place and returns its argument, so comparing its two
// return values compares a pointer with itself — reflect.DeepEqual
// short-circuits on pointer equality and the assertion can never fail, whatever
// the second application does.
func TestTrimPodIsIdempotent(t *testing.T) {
	first, err := trimPod(fatPod())
	if err != nil {
		t.Fatal(err)
	}
	p := first.(*corev1.Pod)
	before := p.DeepCopy()
	if _, err := trimPod(p); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, p) {
		t.Error("trimPod is not idempotent: a second application changed the object")
	}
}

// Every informer's transform must drop the annotations this API refuses to
// serve, for the same reason trimPod drops the pod spec: kubemeta's filter runs
// on the way OUT, so a cached last-applied-configuration is resident for the
// process lifetime and can never be read by anything. On a kubectl- or
// kapp-managed cluster it is the largest field on most objects.
func TestTransformsDropUnservableAnnotations(t *testing.T) {
	const applied = `{"apiVersion":"apps/v1","kind":"Deployment","spec":{"template":{"spec":{"containers":[{"env":[{"name":"TOKEN","value":"s3cret"}]}]}}}}`
	meta := func() metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Namespace: "prod", Name: "web",
			Annotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": applied,
				"kapp.k14s.io/original":                            applied,
				"prometheus.io/scrape":                             "true",
			},
		}
	}
	check := func(t *testing.T, ann map[string]string) {
		t.Helper()
		for _, k := range []string{
			"kubectl.kubernetes.io/last-applied-configuration",
			"kapp.k14s.io/original",
		} {
			if _, ok := ann[k]; ok {
				t.Errorf("%q cached by the informer transform; it is never served and never freed", k)
			}
		}
		if ann["prometheus.io/scrape"] != "true" {
			t.Errorf("the transform dropped an annotation the service reads: %v", ann)
		}
	}

	// The owner/namespace/node/monitor informers: PartialObjectMetadata and
	// unstructured alike, both through apimeta.Accessor.
	pom := &metav1.PartialObjectMetadata{ObjectMeta: meta()}
	out, err := stripManagedFields(pom)
	if err != nil {
		t.Fatal(err)
	}
	check(t, out.(*metav1.PartialObjectMetadata).Annotations)

	u := &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{
			"name": "sm", "namespace": "prod",
			"annotations": map[string]any{
				"kubectl.kubernetes.io/last-applied-configuration": applied,
				"kapp.k14s.io/original":                            applied,
				"prometheus.io/scrape":                             "true",
			},
		},
	}}
	if _, err := stripManagedFields(u); err != nil {
		t.Fatal(err)
	}
	check(t, u.GetAnnotations())

	// And the pod informer, which has its own transform.
	p, err := trimPod(&corev1.Pod{ObjectMeta: meta()})
	if err != nil {
		t.Fatal(err)
	}
	check(t, p.(*corev1.Pod).Annotations)
}
