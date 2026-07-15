{{- if .Values.monitoring.podMonitor.enabled }}
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: {{ include "miroir.fullname" . }}-gateway
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
  # Per-RWX-volume NFS gateway pods are created dynamically by the
  # controller and carry their own labels (see shareLabels in
  # internal/export/workloads.go), not the chart's selector labels, so the
  # main PodMonitor cannot match them. Harmless with no RWX volumes: the
  # selector simply matches nothing.
  selector:
    matchLabels:
      app.kubernetes.io/name: miroir-gateway
  podMetricsEndpoints:
    - port: metrics
      interval: {{ .Values.monitoring.podMonitor.interval | default "30s" }}
      scrapeTimeout: {{ .Values.monitoring.podMonitor.scrapeTimeout | default "10s" }}
      path: {{ .Values.monitoring.podMonitor.path | default "/metrics" }}
      relabelings:
        - sourceLabels: [__meta_kubernetes_pod_node_name]
          targetLabel: node
        # Stamp the served volume so gateway series join the volume-labelled
        # miroir_volume_* / miroir_export_ready series.
        - sourceLabels: [__meta_kubernetes_pod_label_miroir_home_operations_com_volume]
          targetLabel: volume
        {{- with .Values.monitoring.podMonitor.relabelings }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- with .Values.monitoring.podMonitor.metricRelabelings }}
      metricRelabelings:
        {{- toYaml . | nindent 8 }}
      {{- end }}
{{- end }}
