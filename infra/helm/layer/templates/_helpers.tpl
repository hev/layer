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
templates call this with (dict "root" $ "role" "<role>"). cloud: azure adds
the local-nvme readyLabel for the roles whose NVMe the DaemonSet mounts.
*/}}
{{- define "layer.nodeSelector" -}}
layer.hev.dev/node-role: {{ .role | quote }}
{{- with .root }}
{{- $nvme := .Values.azure.localNvme }}
{{- if and (include "layer.azure" .) $nvme.enabled $nvme.readyLabel (has $.role $nvme.nodeRoles) }}
{{ $nvme.readyLabel.key }}: {{ $nvme.readyLabel.value | quote }}
{{- end }}
{{- end }}
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

{{/*
Target cloud: "aws" (default) or "azure". Fails on anything else.
*/}}
{{- define "layer.cloud" -}}
{{- $cloud := default "aws" .Values.cloud -}}
{{- if not (has $cloud (list "aws" "azure")) -}}
{{- fail (printf "cloud must be aws or azure, got %q" $cloud) -}}
{{- end -}}
{{- $cloud -}}
{{- end }}

{{/*
"true" when cloud is azure, else empty. Use as a condition:
{{- if include "layer.azure" . }}
*/}}
{{- define "layer.azure" -}}
{{- if eq (include "layer.cloud" .) "azure" -}}true{{- end -}}
{{- end }}

{{/*
S3 bucket for S3_BUCKET and S3 consumers: s3.bucket, or the bucket of an
s3:// objectStore.url when s3.bucket is blank.
*/}}
{{- define "layer.s3Bucket" -}}
{{- $url := .Values.objectStore.url | default "" | trim -}}
{{- if .Values.s3.bucket -}}
{{- .Values.s3.bucket -}}
{{- else if hasPrefix "s3://" $url -}}
{{- index (splitList "/" (trimPrefix "s3://" $url)) 0 -}}
{{- end -}}
{{- end }}

{{/*
Blob container of an az:// objectStore.url (az://<container>[/prefix]).
*/}}
{{- define "layer.azureContainer" -}}
{{- $url := .Values.objectStore.url | default "" | trim -}}
{{- if hasPrefix "az://" $url -}}
{{- index (splitList "/" (trimPrefix "az://" $url)) 0 -}}
{{- end -}}
{{- end }}

{{/*
Object store env shared by the gateway and operator: OBJECT_STORE_URL when
objectStore.url is set, plus AZURE_STORAGE_ACCOUNT_NAME on Azure (required
for an az:// URL; an s3:// URL such as in-cluster MinIO also works on AKS).
Renders
nothing on a default AWS install; call inside an `if` so no blank line is left:
{{- with include "layer.objectStoreEnv" . }}{{ . | nindent 12 }}{{ end }}
*/}}
{{- define "layer.objectStoreEnv" -}}
{{- $url := .Values.objectStore.url | default "" | trim -}}
{{- $azure := include "layer.azure" . -}}
{{- if and $url (not (or (hasPrefix "s3://" $url) (hasPrefix "az://" $url))) -}}
{{- fail (printf "objectStore.url must be s3://<bucket>[/prefix] or az://<container>[/prefix], got %q" $url) -}}
{{- end -}}
{{- if and (not $azure) (hasPrefix "az://" $url) -}}
{{- fail "an az:// objectStore.url needs cloud=azure (Workload Identity and azure.storageAccountName)" -}}
{{- end -}}
{{- if $url }}
- name: OBJECT_STORE_URL
  value: {{ $url | quote }}
{{- end }}
{{- if and $azure (or (hasPrefix "az://" $url) .Values.azure.storageAccountName) }}
- name: AZURE_STORAGE_ACCOUNT_NAME
  value: {{ required "azure.storageAccountName is required for an az:// objectStore.url" .Values.azure.storageAccountName | quote }}
{{- end }}
{{- if and $azure .Values.azure.storageEndpoint }}
- name: AZURE_STORAGE_ENDPOINT
  value: {{ .Values.azure.storageEndpoint | quote }}
{{- end }}
{{- end }}

{{/*
Service account annotations for the target cloud. Call with
(dict "root" $ "roleArn" <arn> "clientId" <id>). AWS: eks.amazonaws.com/role-arn
when roleArn is set. Azure: azure.workload.identity/client-id when clientId is
set. Renders nothing when there is nothing to annotate.
*/}}
{{- define "layer.serviceAccountAnnotations" -}}
{{- if include "layer.azure" .root -}}
{{- if .clientId }}
annotations:
  azure.workload.identity/client-id: {{ .clientId | quote }}
{{- end }}
{{- else if .roleArn }}
annotations:
  eks.amazonaws.com/role-arn: {{ .roleArn | quote }}
{{- end }}
{{- end }}

{{/*
StorageClass for a chart PVC. On Azure the AWS default "gp3" (which the chart
creates only on AWS) maps to azure.storageClassName. Call with
(dict "root" $ "class" <class>).
*/}}
{{- define "layer.storageClass" -}}
{{- if and (include "layer.azure" .root) (eq .class "gp3") -}}
{{- .root.Values.azure.storageClassName -}}
{{- else -}}
{{- .class -}}
{{- end -}}
{{- end }}

{{/*
Ingress class, annotations and TLS for cloud: azure (AKS app routing +
cert-manager). Call with (dict "root" $ "values" <ingress values> "host" <host>
"component" <name>). Emits the metadata.annotations body.
*/}}
{{- define "layer.azureIngressAnnotations" -}}
{{- $root := .root -}}
{{- $annotations := merge (dict) (default (dict) .values.annotations) (default (dict) $root.Values.azure.ingress.annotations) -}}
{{- if $root.Values.azure.ingress.clusterIssuer }}
{{- $_ := set $annotations "cert-manager.io/cluster-issuer" $root.Values.azure.ingress.clusterIssuer }}
{{- end }}
{{- toYaml $annotations }}
{{- end }}

{{/*
TLS block for a cloud: azure Ingress. Call like layer.azureIngressAnnotations.
*/}}
{{- define "layer.azureIngressTls" -}}
tls:
  - hosts:
      - {{ .host | quote }}
    secretName: {{ default (printf "%s-%s-tls" (include "layer.fullname" .root) .component) .values.tlsSecretName | quote }}
{{- end }}

{{/*
rclone remote for an artifact URI on Azure: az://<container>/<path> becomes
:azureblob:<container>/<path>. The init container sets
RCLONE_AZUREBLOB_ACCOUNT and RCLONE_AZUREBLOB_ENV_AUTH so rclone signs in
with the pod's Workload Identity.
*/}}
{{- define "layer.rcloneRemote" -}}
{{- if not (hasPrefix "az://" .) -}}
{{- fail (printf "cloud=azure artifact URIs must be az://<container>/<path>, got %q" .) -}}
{{- end -}}
{{- printf ":azureblob:%s" (trimSuffix "/" (trimPrefix "az://" .)) -}}
{{- end }}

{{/*
Env for the rclone artifact copier on Azure.
*/}}
{{- define "layer.rcloneEnv" -}}
- name: RCLONE_AZUREBLOB_ACCOUNT
  value: {{ required "azure.storageAccountName is required when cloud=azure" .Values.azure.storageAccountName | quote }}
- name: RCLONE_AZUREBLOB_ENV_AUTH
  value: "true"
{{- end }}

{{/*
Operator-created worker pods (pipelines, Functions) run under their own
federated service account on AKS: cloud=azure with workerIdentity.clientId.
Renders "true" or nothing.
*/}}
{{- define "layer.workerIdentity" -}}
{{- if and (include "layer.azure" .) .Values.operator.enabled .Values.workerIdentity.clientId -}}true{{- end -}}
{{- end }}
