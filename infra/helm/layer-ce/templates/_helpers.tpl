{{- define "layer-ce.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "layer-ce.fullname" -}}
{{- printf "%s" .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "layer-ce.labels" -}}
app.kubernetes.io/name: {{ include "layer-ce.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "layer-ce.selectorLabels" -}}
app.kubernetes.io/name: {{ include "layer-ce.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "layer-ce.gatewaySecretName" -}}
{{- default (printf "%s-gateway" (include "layer-ce.fullname" .)) .Values.vectorStore.credential.existingSecret -}}
{{- end }}

{{- define "layer-ce.licenseSecretName" -}}
{{- default (printf "%s-license" (include "layer-ce.fullname" .)) .Values.license.existingSecret -}}
{{- end }}

{{- define "layer-ce.dashboardSecretName" -}}
{{- default (printf "%s-dashboard" (include "layer-ce.fullname" .)) .Values.dashboard.basicAuth.existingSecret -}}
{{- end }}
