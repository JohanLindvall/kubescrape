{{- define "kubescrape.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "kubescrape.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/*
kubescrape.logsMetricsMaxBytes validates and renders agent.logsMetrics.maxBytes
for the -logs-metrics-max-bytes flag. The flag takes integer BYTES, and helm's
int64 parses any non-number to 0 — which the agent defines as "no byte bound,
one payload per export": a human-format "3MiB" passed the string-typed schema,
was truthy for `with`, and silently rendered the OPPOSITE of the requested
bound. The schema's digits-only pattern refuses it first in the normal path;
this guard is what still refuses it under --skip-schema-validation and in
subchart use, where the parent's schema does not apply. Shared by BOTH
workloads that render the flag (the DaemonSet and the events/Azure singleton)
so the two sites cannot drift. Callers pass the VALUE, never empty — both
sites sit inside `with`.
*/}}
{{- define "kubescrape.logsMetricsMaxBytes" -}}
{{- if not (regexMatch "^[0-9]+$" (toString .)) -}}
{{- fail (printf "agent.logsMetrics.maxBytes must be integer bytes as a string (e.g. \"3145728\"), got %q" (toString .)) -}}
{{- end -}}
{{- int64 . -}}
{{- end -}}

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

It is the feedback-loop guard's one derivation, used by agent.yaml for the
UNCONDITIONAL destinations the chart renders — the -otlp-endpoint flag and each
per-signal `agent.config.export` override — because every log line the agent
tails goes to those, including the destination's own, and that amplifies
precisely when the destination is already struggling. It lived inline beside the
flag and so covered only the flag: a collectorless
`export.logs.endpoint: http://loki-gateway.logging.svc:4318` reopened exactly the
loop the default exists to close, silently, since files are skipped at DISCOVERY
and nothing counts or warns.

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
{{- $hostport := first (splitList "/" (trimPrefix "https://" (trimPrefix "http://" (. | default "")))) -}}
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
