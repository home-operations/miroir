{{- if not .Values.nodes }}
{{- fail "Helm values must define `nodes` — the per-node storage topology (see values.yaml)" }}
{{- end }}
{{- range $name, $node := .Values.nodes }}
{{- if hasKey $node "backend" }}
{{- fail (printf "nodes.%s uses the pre-0.10 flat single-pool shape; move backend/device/zfsDataset/zfsVolBlockSize/zfsCompression/baseDir/thinPoolSize under `pools: {default: {...}}` (zone and address stay node-level)" $name) }}
{{- end }}
{{- if not $node.pools }}
{{- fail (printf "nodes.%s declares no pools; declare at least pools.default (see values.yaml)" $name) }}
{{- end }}
{{- end }}
{{- if hasKey .Values.drbd "verifyAlg" }}
{{- fail "drbd.verifyAlg was renamed to drbd.verify.algorithm" }}
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
        {{- with .Values.drbd.alExtents }}
            al-extents {{ . }};
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
        {{- with .Values.drbd.verify.algorithm }}
            verify-alg {{ . }};
        {{- end }}
        }
    }