{{/*
Expand the name of the chart.
*/}}
{{- define "layer.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "layer.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "layer.labels" -}}
helm.sh/chart: {{ include "layer.name" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "layer.selectorLabels" -}}
app.kubernetes.io/name: {{ include "layer.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "layer.serviceAccountName" -}}
{{- default (include "layer.fullname" .) .Values.serviceAccount.name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Dashboard service account name.
*/}}
{{- define "layer.dashboardServiceAccountName" -}}
{{- default (printf "%s-dashboard" (include "layer.fullname" .)) .Values.dashboard.serviceAccount.name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Secret name containing the bearer token operator-managed components use when
calling the gateway.
*/}}
{{- define "layer.gatewayApiSecretName" -}}
{{- $mode := .Values.vectorStore.inboundAuth.mode | default "deriveFromStore" | trim -}}
{{- if eq $mode "keys" -}}
{{- default (include "layer.fullname" .) .Values.vectorStore.inboundAuth.workerSecretName | trunc 63 | trimSuffix "-" }}
{{- else -}}
{{- include "layer.fullname" . }}
{{- end -}}
{{- end }}

{{/*
Secret key containing the bearer token operator-managed components use when
calling the gateway.
*/}}
{{- define "layer.gatewayApiSecretKey" -}}
{{- $mode := .Values.vectorStore.inboundAuth.mode | default "deriveFromStore" | trim -}}
{{- if eq $mode "keys" -}}
{{- default "layer-inbound-worker-api-key" .Values.vectorStore.inboundAuth.workerSecretKey }}
{{- else -}}
{{- default "turbopuffer-api-key" .Values.vectorStore.credential.secretKey }}
{{- end -}}
{{- end }}

{{/*
Node selector for a layer.hev.dev/node-role pool ("system",
"document-cache", …). Terraform labels the pools (node-groups.tf);
templates call this with (dict "role" "<role>").
*/}}
{{- define "layer.nodeSelector" -}}
layer.hev.dev/node-role: {{ .role | quote }}
{{- end }}

{{/*
Toleration matching the NoSchedule taint Terraform puts on each
layer.hev.dev/node-role pool.
*/}}
{{- define "layer.roleToleration" -}}
- key: layer.hev.dev/node-role
  operator: Equal
  value: {{ .role | quote }}
  effect: NoSchedule
{{- end }}

{{/*
priorityClassName line for a platform tier ("gateway" or "platform").
Renders nothing when priorityClasses.create is false, so pods stay at the
default priority and remain schedulable. Call with (dict "root" $ "tier" "…").
*/}}
{{- define "layer.priorityClassName" -}}
{{- $root := .root -}}
{{- if $root.Values.priorityClasses.create -}}
priorityClassName: {{ index $root.Values.priorityClasses .tier "name" }}
{{- end -}}
{{- end }}
