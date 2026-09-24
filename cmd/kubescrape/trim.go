package main

// Informer transforms: what the cached objects are stripped to before they are
// stored, so the informer caches hold only what a response can use.

import (
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"

	"github.com/JohanLindvall/kubescrape/internal/logdedupe"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// stripManagedFields drops managedFields — and the annotations this API refuses
// to serve — before objects are stored in the informer caches. It goes through
// apimeta.Accessor, so it serves the typed, metadata-only AND unstructured
// informers alike — every one of them must call it.
//
// The annotations are dropped here for the reason trimPod drops the pod spec:
// they are resident for the process lifetime and can NEVER be read, since
// owners.Resolve and the namespace/node lookups funnel everything through
// kubemeta.CopyMeta → FilterAnnotations. A kubectl- or kapp-managed cluster
// with a few thousand Deployments/ReplicaSets/Jobs/CronJobs carries several
// megabytes of them in PartialObjectMetadata alone, against a 128Mi request.
func stripManagedFields(obj any) (any, error) {
	acc, err := apimeta.Accessor(obj)
	if err != nil {
		// A "should not happen" branch that used to be a silent `err == nil`
		// guard: the object goes into the cache UNTRIMMED, so the only symptom
		// is RSS climbing with managedFields nothing can ever read. Throttled,
		// because it would fire once per object per resync if it ever fired at
		// all, and typed rather than dumped — the value is an informer object
		// and this line must not print a whole pod.
		if transformWarn.Allow(transformWarnEvery) {
			slog.Default().Warn("informer transform could not read an object's metadata; it is cached untrimmed",
				"error", err, "type", fmt.Sprintf("%T", obj))
		}
		return obj, nil
	}
	acc.SetManagedFields(nil)
	// SetAnnotations only when something was actually removed: the unstructured
	// accessor rebuilds the whole map on a set, and the overwhelmingly common
	// object carries neither key.
	if ann := acc.GetAnnotations(); kubemeta.StripDroppedAnnotations(ann) {
		acc.SetAnnotations(ann)
	}
	return obj, nil
}

// The transform's failure throttle. One line per interval for a condition that
// would otherwise repeat per object per resync — the repo's keyless throttle
// (internal/logdedupe), never a hand-rolled one.
var transformWarn logdedupe.Throttle

const transformWarnEvery = 5 * time.Minute

// trimPod is the pod informer's transform: strip managedFields like every
// other informer, then drop the parts of the SPEC and the STATUS nothing here
// reads.
//
// The pod cache is the service's dominant memory cost, and kubeconvert.FromPod
// consumes a thin slice of the spec — NodeName, HostNetwork, and each
// container's Name/Image/Ports. Everything else the API server sends (env
// vars, volumes and their mounts, resource requirements, the three probes,
// lifecycle hooks, affinity, tolerations, scheduling gates) is retained
// verbatim for the process lifetime and never read: on a large cluster that is
// tens of megabytes of resident heap against a 128Mi request. trimPodStatus
// below does the same for the status, which on a current kubelet is nearly
// HALF of what this trim leaves behind.
//
// It runs as a TYPE SWITCH rather than trimming unconditionally because the
// Service informer shares this factory and services.Index genuinely reads
// Service specs (selector and ports) — trimming those would break scrape
// discovery. Anything that is not a *corev1.Pod — Services — falls through to
// the managedFields strip alone.
//
// Tombstones do NOT reach here: both FIFO implementations skip the transformer
// for DeletedFinalStateUnknown (delta_fifo.go and the_real_fifo.go in client-go
// v0.36.3), and the tombstone's inner object was already transformed on its way
// into the store.
//
// Trimming is idempotent, which matters: client-go may apply a transform to an
// object more than once.
func trimPod(obj any) (any, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return stripManagedFields(obj)
	}
	pod.ManagedFields = nil
	// The same never-readable annotations stripManagedFields drops for every
	// other informer: kubeconvert.FromPod serves pod annotations through
	// CopyMeta, which filters them out, so a cached copy is pure resident cost.
	kubemeta.StripDroppedAnnotations(pod.Annotations)

	trimContainers(pod.Spec.Containers)
	trimContainers(pod.Spec.InitContainers)
	for i := range pod.Spec.EphemeralContainers {
		// EphemeralContainerCommon is field-identical to Container by
		// construction (upstream keeps them in lockstep), so ONE trim serves
		// both. It was written out twice, and the copy was exercised for a
		// single field — exactly the shape that drifts.
		trimContainer((*corev1.Container)(&pod.Spec.EphemeralContainers[i].EphemeralContainerCommon))
	}

	// Pod-level spec fields, none of which reach kubemeta.Pod.
	pod.Spec.Volumes = nil
	pod.Spec.Tolerations = nil
	pod.Spec.Affinity = nil
	pod.Spec.NodeSelector = nil
	pod.Spec.SecurityContext = nil
	pod.Spec.ImagePullSecrets = nil
	pod.Spec.TopologySpreadConstraints = nil
	pod.Spec.ReadinessGates = nil
	pod.Spec.SchedulingGates = nil
	pod.Spec.ResourceClaims = nil
	pod.Spec.Overhead = nil
	pod.Spec.DNSConfig = nil
	pod.Spec.HostAliases = nil

	trimPodStatus(&pod.Status)
	return pod, nil
}

func trimContainers(cs []corev1.Container) {
	for i := range cs {
		trimContainer(&cs[i])
	}
}

// trimContainer drops the fields of one container that never reach
// kubemeta.Container (which takes Name, Image and Ports, and nothing else).
func trimContainer(c *corev1.Container) {
	c.Command = nil
	c.Args = nil
	c.WorkingDir = ""
	c.Env = nil
	c.EnvFrom = nil
	c.Resources = corev1.ResourceRequirements{}
	c.ResizePolicy = nil
	c.VolumeMounts = nil
	c.VolumeDevices = nil
	c.LivenessProbe = nil
	c.ReadinessProbe = nil
	c.StartupProbe = nil
	c.Lifecycle = nil
	c.SecurityContext = nil
}

// trimPodStatus is the STATUS half of the trim: the spec trim above left the
// status whole, and every release since 1.31 has widened ContainerStatus with
// a per-container field nothing here reads. Measured as RETAINED heap over
// protobuf-decoded pods (two containers — the shape client-go actually
// caches), spec-trimmed and then status-trimmed:
//
//	pre-1.31 kubelet                          5896 ->  5400 B/pod
//	+ volumeMounts        (1.31, beta/on)     6792 ->  5416 B/pod
//	+ user/resources      (1.33, beta/on)     9832 ->  5416 B/pod  = 88 MB @20k pods
//	+ allocatedResources  (alpha)            11256 ->  5416 B/pod  = 117 MB @20k pods
//	a CrashLoopBackOff pod, 1.33               10824 ->  5848 B/pod
//
// against a chart that requests 128Mi and sets no limit. The 1.33 row is 45%
// of what the informer cache holds per pod — an unchanged 20k-pod deployment
// goes from 256 MB to 168 MB of pods, cache plus store. Nothing names the
// waste while it accumulates: the store holds none of it, so
// kubescrape_store_pods is unchanged and the only symptom is RSS.
//
// What it costs is one allocation and ~450 ns per pod event, on the informer
// goroutine (indicative; measured on a loaded machine).
//
// These are KEEP-lists, not drop-lists, and deliberately so: the drop-list
// style trimContainer uses above has to be extended by hand for every field a
// future k8s adds, and the fields being added are exactly the fat ones
// (in-place resize alone put a ResourceRequirements on every container status,
// worth 1.4 KB/container). Rebuilding the struct from the fields
// kubeconvert.FromPod reads means a new API field costs nothing the day it
// ships. It is a struct assignment, so it allocates nothing; only the kept
// condition does, and it frees a four-element array to do it.
//
// What FromPod reads is the whole of the keep-list below and nothing else —
// TestTrimPodPreservesEverythingFromPodReads converts the fat pod and the
// trimmed pod and requires the results to be identical, over a fixture that
// populates every field named here (and gives the status a DIFFERENT resolved
// image than the spec, so a future read of ContainerStatus.Image fails the
// guard instead of matching by coincidence).
func trimPodStatus(st *corev1.PodStatus) {
	trimContainerStatuses(st.InitContainerStatuses)
	trimContainerStatuses(st.ContainerStatuses)
	trimContainerStatuses(st.EphemeralContainerStatuses)
	*st = corev1.PodStatus{
		Phase:                      st.Phase,
		Conditions:                 readyConditionOnly(st.Conditions),
		HostIP:                     st.HostIP,
		PodIP:                      st.PodIP,
		PodIPs:                     st.PodIPs,
		StartTime:                  st.StartTime,
		InitContainerStatuses:      st.InitContainerStatuses,
		ContainerStatuses:          st.ContainerStatuses,
		EphemeralContainerStatuses: st.EphemeralContainerStatuses,
	}
}

// readyConditionOnly reduces the condition list to the one condition
// kubemeta.Pod.Ready is derived from, carrying only its Status.
//
// A fresh one-element slice rather than a reslice of the original: the
// kubelet reports four conditions on every healthy pod, and a reslice keeps
// the whole four-element array (with both timestamps and the reason/message
// strings of the three dropped entries) alive for the process lifetime, which
// is the cost this is here to avoid.
func readyConditionOnly(cs []corev1.PodCondition) []corev1.PodCondition {
	for i := range cs {
		if cs[i].Type == corev1.PodReady {
			return []corev1.PodCondition{{Type: cs[i].Type, Status: cs[i].Status}}
		}
	}
	return nil
}

func trimContainerStatuses(cs []corev1.ContainerStatus) {
	for i := range cs {
		trimContainerStatus(&cs[i])
	}
}

// trimContainerStatus keeps the fields convertContainer and
// previousIncarnation fold into kubemeta.Container. Image is NOT among them:
// the model's Image comes from the SPEC container, and the status copy is the
// runtime-resolved duplicate of it.
func trimContainerStatus(c *corev1.ContainerStatus) {
	trimContainerState(&c.State)
	trimContainerState(&c.LastTerminationState)
	*c = corev1.ContainerStatus{
		Name:                 c.Name,
		State:                c.State,
		LastTerminationState: c.LastTerminationState,
		Ready:                c.Ready,
		RestartCount:         c.RestartCount,
		ImageID:              c.ImageID,
		ContainerID:          c.ContainerID,
	}
}

// trimContainerState keeps the arms in place (replacing them would allocate on
// every informer event) and clears the fields inside them that no reader
// takes. The two casualties are the ones worth naming: a waiting container's
// Message is the "back-off 5m0s restarting failed container=..." line, and a
// terminated one's is up to 4 KiB of termination message — per container, on
// exactly the pods a struggling cluster has most of.
//
// Terminated.ContainerID is kept in BOTH states although only
// LastTerminationState's is read (it is the previous incarnation's ID). One
// trim serves both arms, and the cost is one string on terminated containers.
func trimContainerState(s *corev1.ContainerState) {
	if s.Waiting != nil {
		*s.Waiting = corev1.ContainerStateWaiting{Reason: s.Waiting.Reason}
	}
	if s.Terminated != nil {
		*s.Terminated = corev1.ContainerStateTerminated{
			ExitCode:    s.Terminated.ExitCode,
			StartedAt:   s.Terminated.StartedAt,
			FinishedAt:  s.Terminated.FinishedAt,
			ContainerID: s.Terminated.ContainerID,
		}
	}
	// ContainerStateRunning holds StartedAt and nothing else; there is
	// nothing to drop, and a keep-list here would be a struct copy onto
	// itself.
}
