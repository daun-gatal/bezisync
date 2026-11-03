{{- define "bezisync.fullname" -}}{{ .Release.Name | trunc 40 | trimSuffix "-" }}{{- end -}}
{{- define "bezisync.labels" -}}
app.kubernetes.io/name: bezisync
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
{{- define "bezisync.secretName" -}}{{ .Values.existingSecret | default (printf "%s-secrets" (include "bezisync.fullname" .)) }}{{- end -}}
{{- define "bezisync.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}{{- end -}}
{{- define "bezisync.webImage" -}}{{ .Values.webImage.repository }}:{{ .Values.webImage.tag | default .Chart.AppVersion }}{{- end -}}
{{- /* A pod securityContext: .base plus .Values.appArmorProfile when it is set. */ -}}
{{- define "bezisync.podSecurityContext" -}}
{{- $ctx := deepCopy .base -}}
{{- with .root.Values.appArmorProfile }}{{ $_ := set $ctx "appArmorProfile" . }}{{ end -}}
{{- toYaml $ctx -}}
{{- end -}}
{{- define "bezisync.env" -}}
- name: BEZISYNC_ENVIRONMENT
  value: {{ .Values.environment | quote }}
- name: BEZISYNC_PUBLIC_URL
  value: {{ .Values.publicUrl | quote }}
- name: BEZISYNC_COOKIE_SECURE
  value: {{ .Values.cookieSecure | quote }}
{{- with .Values.agent.url }}
- name: BEZISYNC_AGENT_URL
  value: {{ . | quote }}
{{- end }}
- name: BEZISYNC_AGENT_IMAGE
  value: {{ .Values.agent.image | default (include "bezisync.image" .) | quote }}
{{- with index .Chart.Annotations "bezisync.io/agent-chart-version" }}
- name: BEZISYNC_AGENT_CHART_VERSION
  value: {{ . | quote }}
{{- end }}
{{- with .Values.agent.chart }}
- name: BEZISYNC_AGENT_CHART
  value: {{ . | quote }}
{{- end }}
{{- if .Values.allowPlainHttp }}
- name: BEZISYNC_ALLOW_PLAIN_HTTP
  value: "true"
{{- end }}
- name: BEZISYNC_K8S_NAMESPACE
  valueFrom: { fieldRef: { fieldPath: metadata.namespace } }
{{- if .Values.kubernetesRuntime.enabled }}
- name: BEZISYNC_K8S_IN_CLUSTER
  value: "true"
{{- end }}
{{- with .Values.extraEnv }}
{{ toYaml . }}
{{- end }}
{{- end -}}
{{- define "bezisync.envFrom" -}}
- secretRef:
    name: {{ include "bezisync.secretName" . }}
{{- end -}}
