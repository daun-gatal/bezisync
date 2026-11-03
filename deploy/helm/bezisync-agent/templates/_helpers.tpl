{{- define "agent.fullname" -}}{{ .Release.Name | trunc 50 | trimSuffix "-" }}{{- end -}}
{{- define "agent.labels" -}}
app.kubernetes.io/name: bezisync-agent
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}
{{- define "agent.secretName" -}}{{ .Values.existingSecret | default (printf "%s-token" (include "agent.fullname" .)) }}{{- end -}}
