{{- /* Standard labels for the rendered configuration objects. */ -}}
{{- define "miroir-config.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- /* The CSI driver name the sibling miroir chart registers. */ -}}
{{- define "miroir-config.csiDriverName" -}}
miroir.home-operations.com
{{- end -}}
