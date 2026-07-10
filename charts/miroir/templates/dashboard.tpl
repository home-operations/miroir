{{- if .Values.monitoring.dashboards.enabled }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "miroir.fullname" . }}-dashboard
  namespace: {{ .Values.monitoring.dashboards.namespace | default .Release.Namespace }}
  {{- with .Values.monitoring.dashboards.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    {{- if not .Values.monitoring.dashboards.grafanaOperator.enabled }}
    grafana_dashboard: "1"
    {{- end }}
    {{- with .Values.monitoring.dashboards.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
data:
  miroir.json: |
{{ .Files.Get "dashboards/miroir.json" | nindent 4 }}
{{- end }}
