package main

// The pod informer's transform drops most of the pod SPEC to keep the cache
// small. That is only safe while the dropped parts are exactly the parts
// nothing reads — and "nothing reads them" is a property of
// kubeconvert.FromPod, which can change. So assert it the strong way: convert
// the ORIGINAL and the TRIMMED pod and require the results to be identical.
// A future FromPod that starts reading, say, container resources fails here
// rather than in production, where it would surface as a silently empty field.

import (
	"reflect"
	"testing"

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
			Labels:      map[string]string{"app": "web"},
			Annotations: map[string]string{"prometheus.io/scrape": "true"},
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
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning, PodIP: "10.1.2.3",
			PodIPs: []corev1.PodIP{{IP: "10.1.2.3"}, {IP: "fd00::1"}},
			HostIP: "192.168.1.1",
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", ContainerID: "containerd://abc123", Ready: true, RestartCount: 2,
				Image: "registry.example.com/app:v1.2.3", ImageID: "sha256:deadbeef",
				State:                corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ContainerID: "containerd://old"}},
			}},
		},
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
	if p.Status.PodIP == "" || len(p.Status.ContainerStatuses) != 1 {
		t.Error("trim must not touch status")
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
