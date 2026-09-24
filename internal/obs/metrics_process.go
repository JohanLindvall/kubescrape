package obs

// The process itself: its own-pod lookup and its readiness gates (both
// binaries), and the agent's gated debug surfaces.

// SelfMetadataLookups counts the agent's own-pod lookups by outcome, SEPARATELY
// from kubescrape_metadata_requests_total.
//
// That counter is documented as the container-attribution health signal, and
// the self lookup retries forever: a fleet where /v1/self cannot resolve
// (hostNetwork, a NAT hop) would otherwise contribute a permanent stream of
// not_found to it and fire an alert about an attribution problem that does not
// exist. Separation is not achieved by this counter alone — the agent gives
// the self lookup its OWN metaclient.Client, without the Observe hook, since
// the hook is per-client and fires inside every fetch (main.go). Counting
// here and observing there would have double-counted the outcome into the
// very metric the split exists to keep clean.
var SelfMetadataLookups = Registry.CounterVec("kubescrape_self_metadata_lookups_total",
	"Own-pod metadata lookups for -self-attributes, by outcome.", "outcome")

// SelfMetadataLookups' outcome label values, shared by the two binaries'
// resolvers so their series stay unionable (each binary re-typing the strings
// is how one grows a spelling the other's dashboards do not match).
const (
	SelfLookupSelf   = "self"    // resolved via GET /v1/self (the agent's first try)
	SelfLookupByName = "by_name" // resolved via the namespace/name fallback
	SelfLookupError  = "error"   // not resolved; the error is returned to the poller
)

// RegisterSelfMetadata exposes whether this process has resolved the pod it
// runs in, whose attributes it stamps on the metrics it generates about itself
// (-self-attributes). Both binaries register it whenever the lookup RUNS, and
// only then: a registered gauge means "this process is trying", so a 0 always
// means unresolved and never "the feature is off".
//
// Without it an unattributed process is invisible: the agent's failed lookups
// were indistinguishable from any other in kubescrape_metadata_requests_total,
// and the metadata service's own lookup touches no counter at all — so "my
// agents' own metrics carry no pod" is unalertable, and the symptom (a missing
// label on one job) is easy to read as a dashboard problem.
func RegisterSelfMetadata(resolved func() bool) {
	Registry.GaugeFunc("kubescrape_self_metadata_resolved",
		"1 when this process has resolved its own pod's metadata for -self-attributes, 0 while it has not.",
		func() float64 {
			if resolved() {
				return 1
			}
			return 0
		})
}

// RegisterReadiness publishes one gauge per startup gate /readyz waits on, so
// a fleet stuck unready is diagnosable from the metrics as well as from the
// probe body — registered by both binaries, exactly when they have gates.
//
// The probe already names its pending gates, but only to whoever curls it: a
// rolling update that stops at the first node shows up as a Deployment/DaemonSet
// that will not progress, and the pod that could answer the question is the one
// nobody has a shell on. The self-metrics push runs from startup regardless of
// readiness, so an unready process still reports this.
//
// A gate is published from the moment it is REQUIRED, which is what makes 0
// mean "waiting" rather than "absent"; a gate that never appears was never
// wired for this deployment (its pipeline is off). The value flips to 1 once
// and stays there — these are STARTUP gates, not liveness.
func RegisterReadiness(gates func() map[string]bool) {
	Registry.GaugeFuncVec("kubescrape_readiness_gate",
		"1 when this startup gate is satisfied, 0 while it is still pending, by gate. /readyz is 200 only when every gate reads 1, and a rolling update advances on /readyz — so a gate sitting at 0 across the fleet IS the stalled rollout, and the label says which subsystem to look at. Gates are the pipelines this process actually wired, so an absent gate means that pipeline is off rather than healthy. Alert on a gate at 0 for longer than a pod's startup budget.",
		"gate", func() map[string]float64 {
			state := gates()
			out := make(map[string]float64, len(state))
			for name, ok := range state {
				if ok {
					out[name] = 1
				} else {
					out[name] = 0
				}
			}
			return out
		})
}

// Debug surfaces (agent, -listen). The data-bearing three — /debug/otlp, its
// UI and /debug/tailer — stream or enumerate this node's whole telemetry feed
// on a port every pod in the cluster can reach, so they are gated (a local
// connection, or the -debug-token-file bearer token) and the gate is counted:
// a refusal an operator cannot see is a refusal that gets configured away, and
// an ACCEPTED read leaves no other trace than a throttled attach line.

// DebugRefused counts those refusals, by reason.
var DebugRefused = Registry.CounterVec("kubescrape_debug_refused_total",
	"Requests for the agent's data-bearing debug surfaces (/debug/otlp, /debug/otlp/ui, /debug/tailer) that "+
		"were refused, by reason. no_token = no -debug-token-file is configured, so these are served only to a "+
		"local connection (kubectl port-forward, or a container in this pod) and this one came from elsewhere — "+
		"set the flag and hand the token to whoever needs to read an agent remotely; unauthenticated = a token "+
		"file IS configured and the request carried no valid bearer token, i.e. a stale token after a rotation, "+
		"the wrong Secret mounted, or somebody probing the port; forwarded = the connection is local but the "+
		"request carries a forwarding header (Forwarded, Via, X-Forwarded-For or X-Real-Ip, by presence), so the address belongs to a relay and cannot stand in for the "+
		"caller; host = the connection is local but its Host header names something other than localhost, "+
		"which is what a DNS-rebound browser page reaching a kubectl port-forward looks like (a client that "+
		"dialled this port directly sends localhost or 127.0.0.1). A steady rate on no_token or "+
		"unauthenticated from pods that are not yours is somebody trying to read this node's log lines, and "+
		"any rate on host is a browser being pointed at an operator's port-forward.", "reason")
