{{- define "kubescrape.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "kubescrape.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
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
