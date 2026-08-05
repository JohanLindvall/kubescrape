package kubemeta

// droppedAnnotations are annotation keys stripped from every object this API
// serves — pods, owners and namespaces alike.
//
// The metadata routes are unauthenticated by design ("it carries no secret
// material and agents poll it constantly"), and that claim only holds if the
// annotations riding along carry none either. An applied-object copy breaks it
// outright: it is the whole spec verbatim, so anything a user inlined — an env
// var with a token, a connection string, a webhook URL — is served to any
// caller that can reach the port, and lands in every log record's resource
// attributes downstream.
//
// Such a copy is also pure bloat as telemetry metadata: it duplicates the spec
// the rest of the response already models, and on a CronJob or Deployment it is
// routinely the largest field in the payload.
//
// The entries are the deploy tools that write one: kubectl apply (and every
// provider following it — Terraform, Pulumi, Argo CD's default tracking) and
// kapp, which writes its own copy on every object it deploys. Sibling keys that
// merely FINGERPRINT the applied object (kapp's `-diff-md5`, Flux's checksums)
// carry no spec content and stay.
var droppedAnnotations = map[string]bool{
	"kubectl.kubernetes.io/last-applied-configuration": true,
	"kapp.k14s.io/original":                            true,
}

// CopyMeta deep-copies an object's labels verbatim and passes its annotations
// through FilterAnnotations, returning nil for either when empty so the model
// fields stay omitempty. Every labels+annotations pair the unauthenticated API
// serves — pods, owners, namespaces/nodes, Services — goes through here: the
// pairing is the invariant, because when the two were copied by separate
// per-package helpers, Services (the fourth annotation-bearing object) got the
// verbatim copy for BOTH and served kubectl's last-applied-configuration on
// every service- and monitor-derived scrape target.
func CopyMeta(labels, annotations map[string]string) (map[string]string, map[string]string) {
	return cloneMap(labels), FilterAnnotations(annotations)
}

// cloneMap copies m, nil for empty (omitempty stays omitted).
func cloneMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// FilterAnnotations copies m without the annotations this API refuses to
// serve. It returns nil for an empty result so the field stays omitempty.
//
// The filter is deliberately a fixed, tiny denylist rather than a config knob:
// every entry is a deploy-tool convention that no consumer of this API wants,
// and a per-deployment allowlist would make "is this endpoint safe to expose"
// depend on configuration nobody reviews.
func FilterAnnotations(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		if droppedAnnotations[k] {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
