{{- if not .Values.nodes }}
{{- fail "Helm values must define `nodes` — the per-node storage topology (see values.yaml)" }}
{{- end }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: homefs-nodes
  namespace: {{ .Release.Namespace }}
data:
  nodes.yaml: |
    {{- .Values.nodes | toYaml | nindent 4 }}
