{{- if not .Values.nodes }}
{{- fail "Helm values must define `nodes` — the per-node storage topology (see values.yaml)" }}
{{- end }}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "miroir.nodesConfigName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
data:
  nodes.yaml: |
    {{- .Values.nodes | toYaml | nindent 4 }}
---
# Minimal drbd-utils global config: the drbd.d hostPath bind shadows the
# image-baked copy. Per-resource settings live in rendered .res files.
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "miroir.drbdConfigName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
data:
  global_common.conf: |
    global {
        usage-count no;
        udev-always-use-vnr;
    }
    common {
        handlers {}
        startup {}
        options {}
        disk {}
        net {}
    }