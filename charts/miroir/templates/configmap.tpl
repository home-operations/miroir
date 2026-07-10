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
# drbd-utils global config: the drbd.d hostPath bind shadows the image-baked
# copy. Cluster-wide resync tuning goes in common{} (inherited by every
# resource); per-resource settings live in the rendered .res files.
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
        disk {
        {{- with .Values.drbd.onIoError }}
            on-io-error {{ . }};
        {{- end }}
        {{- with .Values.drbd.resync }}
        {{- if .planAhead }}
            c-plan-ahead {{ .planAhead }};
        {{- end }}
        {{- if .fillTarget }}
            c-fill-target {{ .fillTarget }};
        {{- end }}
        {{- if .maxRate }}
            c-max-rate {{ .maxRate }};
        {{- end }}
        {{- if .minRate }}
            c-min-rate {{ .minRate }};
        {{- end }}
        {{- if .rate }}
            resync-rate {{ .rate }};
        {{- end }}
        {{- if .discardGranularity }}
            rs-discard-granularity {{ .discardGranularity }};
        {{- end }}
        {{- end }}
        }
        net {
        {{- with .Values.drbd.net.maxBuffers }}
            max-buffers {{ . }};
        {{- end }}
        {{- with .Values.drbd.verifyAlg }}
            verify-alg {{ . }};
        {{- end }}
        }
    }