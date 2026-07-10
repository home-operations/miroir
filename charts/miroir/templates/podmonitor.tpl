{{- if .Values.monitoring.podMonitor.enabled }}
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: {{ include "miroir.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    {{- with .Values.monitoring.podMonitor.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with .Values.monitoring.podMonitor.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  # Both the controller pod and every agent pod expose a port named
  # "metrics" — the per-volume miroir_volume_* gauges live on the agents,
  # so scraping the controller alone would miss all of them.
  selector:
    matchLabels:
      {{- include "miroir.selectorLabels" . | nindent 6 }}
    matchExpressions:
      - key: app.kubernetes.io/component
        operator: In
        values: [controller, agent]
  podMetricsEndpoints:
    - port: metrics
      interval: {{ .Values.monitoring.podMonitor.interval | default "30s" }}
      scrapeTimeout: {{ .Values.monitoring.podMonitor.scrapeTimeout | default "10s" }}
      path: {{ .Values.monitoring.podMonitor.path | default "/metrics" }}
      relabelings:
        # Agents run hostNetwork, so instance is a bare node IP; a node
        # label keeps per-node storage series readable.
        - sourceLabels: [__meta_kubernetes_pod_node_name]
          targetLabel: node
        {{- with .Values.monitoring.podMonitor.relabelings }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- with .Values.monitoring.podMonitor.metricRelabelings }}
      metricRelabelings:
        {{- toYaml . | nindent 8 }}
      {{- end }}
  {{- with .Values.monitoring.podMonitor.podTargetLabels }}
  podTargetLabels:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
