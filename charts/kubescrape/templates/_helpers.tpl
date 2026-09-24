{{/*
kubescrape.image renders the image reference for all four workloads.

A DIGEST pin is `repository@sha256:...`, never `repository:@sha256:...`: joining
repository and tag with ':' unconditionally turned the documented digest pin
(`image.tag: "@sha256:..."`) into an empty tag followed by a digest, an invalid
reference that failed every workload with InvalidImageName. So a digest may be
given as `image.digest` (with or without a `tag` beside it — `repo:tag@digest`
is a valid reference and the digest is what the runtime pulls), or, for
compatibility with the spelling the docs used, as a `tag` that starts with `@`
or `sha256:`. Both at once is refused rather than guessed between.
*/}}
{{- define "kubescrape.image" -}}
{{- $repo := .Values.image.repository -}}
{{- $tag := .Values.image.tag | default "" -}}
{{- $digest := .Values.image.digest | default "" -}}
{{- $tagIsDigest := or (hasPrefix "@" $tag) (hasPrefix "sha256:" $tag) -}}
{{- if and $digest $tagIsDigest -}}
{{- fail (printf "image.digest (%s) and a digest-shaped image.tag (%s) are both set; set one" $digest $tag) -}}
{{- end -}}
{{- if $digest -}}
{{ $repo }}{{ if $tag }}:{{ $tag }}{{ end }}@{{ $digest }}
{{- else if $tagIsDigest -}}
{{ $repo }}@{{ trimPrefix "@" $tag }}
{{- else -}}
{{ $repo }}:{{ $tag | default .Chart.AppVersion }}
{{- end -}}
{{- end -}}

{{/*
kubescrape.imagePullPolicy renders imagePullPolicy for all four workloads.

An explicit `image.pullPolicy` wins. With none set it follows KUBERNETES' OWN
rule, which the chart used to override: a FLOATING tag (`latest`, or an unset
tag with the chart's `appVersion: latest`) gets `Always`, a pinned one gets
`IfNotPresent`.

That override is what made a republished image invisible. `helm upgrade` against
a re-pushed `:latest` renders byte-identical Deployment/DaemonSet specs — the
image string is unchanged and the only rollout-forcing annotation hashes the
CONFIG — so nothing rolls at all; and a manual `kubectl rollout restart` then
starts pods that, under IfNotPresent, reuse the `latest` layer every node
already cached. The operator sees a successful upgrade with the old binary
running, and the only evidence anywhere is the `version=` field on the startup
log line.

`Always` is not a fix for a floating tag, it is the mitigation: PIN
`image.tag` to a released version, or `image.digest` to a digest, at which
point this helper returns IfNotPresent by itself and the pull cost goes away
with it. A digest is immutable, so it is IfNotPresent even beside an unset tag
(whose appVersion default is `latest`).
*/}}
{{- define "kubescrape.imagePullPolicy" -}}
{{- if .Values.image.pullPolicy -}}
{{- .Values.image.pullPolicy -}}
{{- else if .Values.image.digest -}}
IfNotPresent
{{- else if eq (.Values.image.tag | default .Chart.AppVersion) "latest" -}}
Always
{{- else -}}
IfNotPresent
{{- end -}}
{{- end -}}

{{/*
kubescrape.port renders the PORT of a listen address, for containerPort, a
NetworkPolicy port and the prometheus.io/port annotation. It takes the address
string as its context.

It is the digits after the LAST colon, and a render failure when there are
none. The 32 inline copies it replaced read `(split ":" X)._1`, the SECOND
colon-separated field, so a bracketed IPv6 address (`[::]:9090`) rendered
containerPort 0, a NetworkPolicy `port: 0` and `prometheus.io/port: ""` — the
first two fail the install loudly, the empty scrape annotation fails silently.
A value with no port at all rendered 0 the same way; the binary cannot listen
on it either, so refusing the render is the earlier form of the same answer.
*/}}
{{- define "kubescrape.port" -}}
{{- $p := regexFind ":[0-9]+$" (toString .) | trimPrefix ":" -}}
{{- if not $p -}}
{{- fail (printf "listen address %q names no port: want host:port, e.g. \":9090\" or \"[::]:9090\"" (toString .)) -}}
{{- end -}}
{{- $p -}}
{{- end -}}

{{- define "kubescrape.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/*
kubescrape.podSecurityContext renders the pod-level securityContext of all four
workloads from the top-level seccompProfile value. Takes the root context; the
caller renders it only when .Values.seccompProfile is set, so an unset one
leaves no line behind.

Kubernetes' own default is Unconfined, which is LOOSER than what a plain
`docker run` gets, and PodSecurity's `restricted` profile rejects a pod without
RuntimeDefault outright — so on a namespace labelled restricted the metadata
service, the events singleton and the trace tier used to fail ADMISSION, which
on a first install looks like the chart being broken. (It does not make the
agent DaemonSet admissible there: that pod runs as UID 0 and mounts hostPath
/var/log, which `restricted` forbids whatever this says.)

To omit the field, set seccompProfile to `null` — NOT `{}`, which cannot work:
helm COALESCES a user map onto the chart's default map, so an empty one
arrives here as the default `{type: RuntimeDefault}` and renders. Only an
explicit null removes the key. This instruction used to be copied into every
workload template, and was wrong in all of them at once (internal/chartcheck
seccompomit_test.go).
*/}}
{{- define "kubescrape.podSecurityContext" -}}
securityContext:
  seccompProfile:
    {{- toYaml .Values.seccompProfile | nindent 4 }}
{{- end -}}

{{/*
kubescrape.nonRootSecurityContext is the container securityContext of the three
workloads that need no privilege at all — the metadata service, the events
singleton and the trace tier: the image's nonroot UID, a read-only root
filesystem and no capabilities. (The agent DaemonSet has its own, as UID 0:
container log files are root-readable only.)
*/}}
{{- define "kubescrape.nonRootSecurityContext" -}}
securityContext:
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: true
  runAsNonRoot: true
  runAsUser: 65532
  capabilities:
    drop: ["ALL"]
{{- end -}}

{{/*
kubescrape.logsMetricsMaxBytes validates and renders agent.logsMetrics.maxBytes
for the -logs-metrics-max-bytes flag. The flag takes integer BYTES, and helm's
int64 parses any non-number to 0 — which the agent defines as "no byte bound,
one payload per export": a human-format "3MiB" passed the string-typed schema,
was truthy for `with`, and silently rendered the OPPOSITE of the requested
bound. The schema's digits-only pattern refuses it first in the normal path;
this guard is what still refuses it under --skip-schema-validation and in
subchart use, where the parent's schema does not apply. Its one caller is
kubescrape.logsMetricsArgs, which both workloads rendering the flag (the
DaemonSet and the events/Azure singleton) include; it passes the VALUE, never
empty, from inside a `with`.
*/}}
{{- define "kubescrape.logsMetricsMaxBytes" -}}
{{- if not (regexMatch "^[0-9]+$" (toString .)) -}}
{{- fail (printf "agent.logsMetrics.maxBytes must be integer bytes as a string (e.g. \"3145728\"), got %q" (toString .)) -}}
{{- end -}}
{{- int64 . -}}
{{- end -}}

{{/*
kubescrape.debugTokenPath is where agent.debug.tokenSecret is mounted, and
therefore what -debug-token-file names. ONE definition, because it is rendered
by three templates (the DaemonSet, the events/Azure singleton and the trace-tier
StatefulSet) and a path that drifted from its mount would not fail the render —
it would open the port with the gate silently falling back to local-only, which
is the failure this gate exists to make impossible to reach by accident. Takes
the root context.
*/}}
{{- define "kubescrape.debugTokenPath" -}}
/etc/kubescrape/debug/{{ .Values.agent.debug.tokenSecret.key | default "token" }}
{{- end -}}

{{/*
kubescrape.scrapeAuthSecretName and kubescrape.scrapeAuthTokenPath are the
/v1/scrape-auth bearer token's Secret and the file both binaries read it from.
The metadata service ACCEPTS the token and the agent PRESENTS it, from the same
Secret mounted into each (service.yaml creates it unless existingSecret names
one), so the name, the mount and -scrape-auth-token-file must agree in both
templates: one definition each, for debugTokenPath's reason — a path that
drifted from its mount would not fail the render, it would turn every
secret-backed monitor into up=0 behind a 401. The mount point itself is
/etc/kubescrape/scrape-auth in both. Take the root context.
*/}}
{{- define "kubescrape.scrapeAuthSecretName" -}}
{{- .Values.service.scrapeAuthToken.existingSecret | default (printf "%s-scrape-auth" .Release.Name) -}}
{{- end -}}

{{- define "kubescrape.scrapeAuthTokenPath" -}}
/etc/kubescrape/scrape-auth/{{ .Values.service.scrapeAuthToken.key | default "token" }}
{{- end -}}

{{/*
ARGUMENT BLOCKS shared by the workloads that run the agent binary. Three
workloads — the DaemonSet, the events/Azure singleton and the trace tier —
take their exporter from the ONE agent.otlp block, and two of them run the same
logMetrics set or the same ingest lookup keys, so each must render ALL of its
block. As hand-kept copies these drifted (retry/backoff and max-send-bytes on
the DaemonSet only; the log-metrics knobs on the DaemonSet only), each time
silently: a line a workload does not render is that workload running on the
flag's default while the operator's value applies elsewhere.
internal/chartcheck TestAgentBinaryWorkloadsRenderTheSameSharedFlags pins the
rendered result, including the per-template lines no helper holds (the log
level and the listeners — pprofListen was the third drift).

Three conventions, each load-bearing:

  * Written at the container-args indentation (12) and included WITHOUT
    nindent — `{{- include "kubescrape.agentOtlpArgs" . }}` — because a block
    whose every line is optional can render nothing, and nindent would then
    leave a whitespace-only line in the manifest. A define that is not
    right-trimmed keeps the newline and indentation before its first line.
  * The values are named as literal .Values paths (root context): the text
    scans guarding these flags — internal/manifestcheck's "the binary still
    defines it" check and internal/chartcheck's duration and byte-size guards —
    read a helper where it is included (manifestcheck.ExpandIncludes), and
    resolve .Values paths, not a dict argument.
  * The include sits alone on its line, which is what that expansion splices.

The metadata service's -otlp-* block reads service.otlp and stays in
service.yaml: a different binary, and only the flags the two share.
*/}}
{{- define "kubescrape.agentOtlpArgs" }}
            - -otlp-endpoint={{ .Values.agent.otlp.endpoint }}
            - -otlp-protocol={{ .Values.agent.otlp.protocol }}
            - -otlp-compression={{ .Values.agent.otlp.compression }}
            - -otlp-compression-level={{ int64 .Values.agent.otlp.compressionLevel }}
            - -otlp-insecure={{ .Values.agent.otlp.insecure }}
            - -otlp-tls-insecure-skip-verify={{ .Values.agent.otlp.tlsInsecureSkipVerify }}
            {{- with .Values.agent.otlp.caFile }}
            - -otlp-tls-ca-file={{ . }}
            {{- end }}
            - -otlp-timeout={{ .Values.agent.otlp.timeout }}
            - -otlp-retry-attempts={{ .Values.agent.otlp.retryAttempts }}
            - -otlp-retry-backoff={{ .Values.agent.otlp.retryBackoff }}
            {{- with .Values.agent.otlp.maxSendBytes }}
            - -otlp-max-send-bytes={{ int64 . }}
            {{- end }}
            {{- if .Values.agent.otlp.bearerTokenSecret.name }}
            - -otlp-bearer-token-file=/etc/kubescrape/otlp-token/{{ .Values.agent.otlp.bearerTokenSecret.key | default "token" }}
            {{- end }}
{{- end }}

{{/*
kubescrape.logsMetricsArgs: the -logs-metrics-* knobs, for the DaemonSet and the
events/Azure singleton, which runs the SAME logMetrics set (the chain is
compiled in run(), so it survives -logs=false) — missing there, one declared
rule became two metric NAMES on two cadences. See the argument-block
conventions above.
*/}}
{{- define "kubescrape.logsMetricsArgs" }}
            {{- with .Values.agent.logsMetrics.interval }}
            - -logs-metrics-interval={{ . }}
            {{- end }}
            {{- with .Values.agent.logsMetrics.maxBytes }}
            - -logs-metrics-max-bytes={{ include "kubescrape.logsMetricsMaxBytes" . }}
            {{- end }}
            {{- with .Values.agent.logsMetrics.namePrefix }}
            - -logs-metrics-name-prefix={{ . }}
            {{- end }}
{{- end }}

{{/*
kubescrape.ingestKeyArgs: what an OTLP receiver resolves a sender by, and its
message cap, for the DaemonSet's -ingest listeners and the trace tier's
application ports. Both take them from agent.ingest — on the tier even with
agent.ingest disabled — because the lookup keys also decide which keys the
receiver's identity strip exempts, and two receivers disagreeing about that
attribute one sender two ways. See the argument-block conventions above.
*/}}
{{- define "kubescrape.ingestKeyArgs" }}
            {{- with .Values.agent.ingest.containerIdKeys }}
            - -ingest-container-id-keys={{ join "," . }}
            {{- end }}
            {{- with .Values.agent.ingest.podUidKeys }}
            - -ingest-pod-uid-keys={{ join "," . }}
            {{- end }}
            {{- with .Values.agent.ingest.grpcMaxRecvBytes }}
            - -ingest-grpc-max-recv-bytes={{ int64 . }}
            {{- end }}
{{- end }}

{{/*
kubescrape.agentConfig renders the agent's unified config with the staticAttrs
convenience value merged into resourceAttributes.static (an explicit config
value wins). It returns YAML, which callers parse with fromYaml.

It is shared because BOTH workloads run the agent binary with the same config:
the DaemonSet and the events/Azure singleton. The singleton used to recompute
the config from the raw `agent.config` without the merge, so with
`agent.staticAttrs` set it got no -config, no mount and no checksum at all —
every Kubernetes Event and Azure record shipped without the cluster label the
rest of the telemetry carried, and changing staticAttrs rolled the DaemonSet
while the singleton kept emitting the old value indefinitely.
*/}}
{{- define "kubescrape.agentConfig" -}}
{{- $cfg := .Values.agent.config | default dict }}
{{- if .Values.agent.staticAttrs }}
{{-   $ra := (get $cfg "resourceAttributes") | default dict }}
{{-   $static := merge (deepCopy ((get $ra "static") | default dict)) .Values.agent.staticAttrs }}
{{-   $ra = merge (dict "static" $static) (deepCopy $ra) }}
{{-   $cfg = merge (dict "resourceAttributes" $ra) (deepCopy $cfg) }}
{{- end }}
{{- toYaml $cfg }}
{{- end -}}

{{/*
kubescrape.inClusterNamespace reads the Kubernetes NAMESPACE out of one OTLP
endpoint, or renders empty when the endpoint does not name an in-cluster
Service. It takes the endpoint STRING as its context (with or without a scheme,
port or path).

ANY URL scheme is stripped, not just http(s): a gRPC endpoint is handed to
grpc.NewClient verbatim, so `dns:///otel-collector.observability.svc:4317` and
`passthrough:///...` are accepted spellings of the same destination, and
stripping only http:// and https:// left `dns:` as the "host" — no namespace
derived, and the agent tailed its own collector's namespace: the very loop this
exists to close. Requiring `://` leaves a bare `svc.ns:4317` untouched. gRPC's
`dns://<resolver>/<host>` form names a DNS SERVER as its authority, and its
`dns:<host>:<port>` form has no slashes at all, so for `dns:` the scheme and any
authority go — the one scheme stripped without `://`, because grpc-go resolves
`dns:` as a scheme whatever follows it.

It is the feedback-loop guard's one derivation, used by agent.yaml for the ONE
in-cluster LOGS destination the chart renders — `agent.config.export.logs.endpoint`
when it names one, otherwise the -otlp-endpoint flag, mirroring
`otlpexport.ExportConfig.signalConfig`, where a non-empty per-signal endpoint REPLACES
the base rather than adding to it — because every log line the agent tails goes
there, including the destination's own, and that amplifies precisely when the
destination is already struggling. It lived inline beside the flag and so covered
only the flag: a collectorless
`export.logs.endpoint: http://loki-gateway.logging.svc:4318` reopened exactly the
loop the default exists to close, silently, since files are skipped at DISCOVERY
and nothing counts or warns. Reading BOTH is the mirror-image error and just as
silent: with `export.logs` set the flag base is a metrics/traces-only address
(with all three signals overridden, one nothing dials), so excluding its
namespace stops collecting logs it never caused.

`agent.config.routing` routes are deliberately NOT read here. A route is SELECTED
BY namespace, so its endpoint receives only the namespaces its globs match, and
the usual per-tenant shape names the tenant's own namespace on both sides
(`namespaces: [tenant-a]` → `otel-collector.tenant-a.svc`). Excluding that
namespace would drop precisely the logs the route exists to deliver — the whole
tenant, at discovery, with nothing counted and nothing warned. Whether a route
loops at all depends on its runtime globs, which the chart cannot evaluate, so
the safe default is to leave routes out and let an operator name a genuinely
looping route in `agent.logsExcludeNamespaces` explicitly.

In-cluster is `<svc>.<ns>` or anything under `.svc`. Deriving a namespace from
ANY host read the second label of an EXTERNAL endpoint as one — `otel.grafana.net`
excluded a namespace called `grafana`, whose pods were then dropped with no
counter and no warning to tell that apart from "no logs".

The namespace is the label BEFORE `svc`, not the second one: those coincide for
`<svc>.<ns>.svc...` and not for a StatefulSet's per-pod `<pod>.<svc>.<ns>.svc...`,
where taking the second excluded a namespace named after the SERVICE and left the
destination's own namespace tailed — the loop reopened, plus an unrelated
namespace silently dropped. Bare `<svc>.<ns>` has no `svc` label and keeps the
second.
*/}}
{{- define "kubescrape.inClusterNamespace" -}}
{{- $target := regexReplaceAll "^dns:(//[^/]*/)?" (. | default "") "" -}}
{{- $hostport := first (splitList "/" (regexReplaceAll "^[A-Za-z][A-Za-z0-9+.-]*://+" $target "")) -}}
{{- $host := first (splitList ":" $hostport) -}}
{{- $parts := splitList "." $host -}}
{{- if and (ge (len $parts) 2) (or (eq (len $parts) 2) (hasSuffix ".svc" $host) (contains ".svc." $host)) -}}
{{-   $ns := index $parts 1 -}}
{{-   range $i, $label := $parts -}}
{{-     if and (eq $label "svc") (gt $i 0) -}}
{{-       $ns = index $parts (sub $i 1) -}}
{{-     end -}}
{{-   end -}}
{{-   $ns -}}
{{- end -}}
{{- end -}}

{{/*
kubescrape.azureNamespaces renders -azure-eventhub-namespace's value.

`namespaces` (a list) wins over the singular `namespace` when set, so the
common one-namespace deployment keeps the scalar it always had while a
multi-namespace one is a plain list rather than a hand-joined string.
*/}}
{{- define "kubescrape.azureNamespaces" -}}
{{- $eh := .Values.azure.eventhub -}}
{{- if $eh.namespaces -}}
{{- join "," $eh.namespaces -}}
{{- else -}}
{{- $eh.namespace -}}
{{- end -}}
{{- end -}}

{{/*
kubescrape.azureConnStringFiles renders -azure-eventhub-connection-string-file:
one path per secret key, comma-separated.

The whole Secret is mounted at one path, so several connection strings are
several KEYS of it — which is what entity-scoped credentials force, one per
hub. `keys` (a list) wins over the singular `key` when set.
*/}}
{{- define "kubescrape.azureConnStringFiles" -}}
{{- $s := .Values.azure.eventhub.connectionStringSecret -}}
{{- $keys := $s.keys | default (list $s.key) -}}
{{- $paths := list -}}
{{- range $keys -}}
{{- $paths = append $paths (printf "/etc/kubescrape/azure-eventhub/%s" .) -}}
{{- end -}}
{{- join "," $paths -}}
{{- end -}}
