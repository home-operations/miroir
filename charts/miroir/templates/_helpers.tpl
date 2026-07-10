{{/*
Expand the name of the chart. Honours nameOverride.
*/}}
{{- define "miroir.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some K8s name fields are limited to this (DNS-1123 subdomain).
If release name already contains the chart name, the release name is used directly.
*/}}
{{- define "miroir.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label. "+" is replaced because
labels must be DNS-1123 compliant.
*/}}
{{- define "miroir.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Controller/agent image reference. A digest pins the image immutably and wins over
the tag (the release pipeline fills it with the published image's digest); otherwise
the tag is used, defaulting to .Chart.AppVersion so the chart and image versions match.
*/}}
{{- define "miroir.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end -}}

{{/*
Return the image pull policy, defaulting to IfNotPresent.
*/}}
{{- define "miroir.imagePullPolicy" -}}
{{- .Values.image.pullPolicy | default "IfNotPresent" -}}
{{- end -}}

{{/*
Agent image ref — the Debian image carrying the storage userland; used by
the agent DaemonSet and the setup Job.
*/}}
{{- define "miroir.agentImage" -}}
{{- if .Values.agent.image.digest -}}
{{- printf "%s@%s" .Values.agent.image.repository .Values.agent.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.agent.image.repository (.Values.agent.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end -}}

{{- define "miroir.agentImagePullPolicy" -}}
{{- .Values.agent.image.pullPolicy | default "IfNotPresent" -}}
{{- end -}}

{{/*
Standard labels applied to every resource this chart produces. Component is added at the
call site so each workload remains self-describing ("controller", "agent", "setup",
"uninstall").
*/}}
{{- define "miroir.labels" -}}
helm.sh/chart: {{ include "miroir.chart" . }}
{{ include "miroir.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{/*
Immutable labels: name + instance. Use these in Deployment/DaemonSet matchLabels so
upgrades don't drift. Component is added at the call site.
*/}}
{{- define "miroir.selectorLabels" -}}
app.kubernetes.io/name: {{ include "miroir.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Resource names. All derive from fullname so nameOverride / fullnameOverride flow through.
*/}}
{{- define "miroir.controllerName" -}}
{{- printf "%s-controller" (include "miroir.fullname" .) -}}
{{- end -}}
{{- define "miroir.agentName" -}}
{{- printf "%s-agent" (include "miroir.fullname" .) -}}
{{- end -}}
{{- define "miroir.nodesConfigName" -}}
{{- printf "%s-nodes" (include "miroir.fullname" .) -}}
{{- end -}}
{{- define "miroir.drbdConfigName" -}}
{{- printf "%s-drbd-conf" (include "miroir.fullname" .) -}}
{{- end -}}
{{- define "miroir.setupServiceAccountName" -}}
{{- printf "%s-setup" (include "miroir.fullname" .) -}}
{{- end -}}
{{- define "miroir.uninstallServiceAccountName" -}}
{{- printf "%s-uninstall" (include "miroir.fullname" .) -}}
{{- end -}}

{{/*
CSI driver name — also the StorageClass provisioner and VolumeSnapshotClass driver.
Always pinned to .Chart.Name so a nameOverride can't break volume provisioning.
*/}}
{{- define "miroir.csiDriverName" -}}
{{- .Chart.Name }}.home-operations.com
{{- end -}}
