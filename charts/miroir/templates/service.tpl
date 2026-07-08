{{- if .Values.monitoring.serviceMonitor.enabled }}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "miroir.metricsServiceName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    app.kubernetes.io/component: controller
spec:
  type: ClusterIP
  ports:
    - port: 8081
      targetPort: metrics
      protocol: TCP
      name: metrics
  selector:
    {{- include "miroir.selectorLabels" . | nindent 4 }}
    app.kubernetes.io/component: controller
{{- end }}
