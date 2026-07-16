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
{{- range $poolName, $pool := $node.pools }}
{{- if eq (toString $pool.backend) "zfs" }}
{{- $blockSize := upper (toString (default "4K" (dig "zfsVolBlockSize" "" $pool))) }}
{{- if not (has $blockSize (list "4K" "8K" "16K" "32K" "64K" "128K")) }}
{{- fail (printf "nodes.%s.pools.%s.zfsVolBlockSize must be one of 4K, 8K, 16K, 32K, 64K, or 128K" $name $poolName) }}
{{- end }}
{{- $compression := lower (toString (default "lz4" (dig "zfsCompression" "" $pool))) }}
{{- if and (ne $compression "inherit") (not (regexMatch "^(on|off|lz4|lzjb|zle|gzip(-[1-9])?|zstd(-([1-9]|1[0-9]))?|zstd-fast(-(10|[1-9]|[2-9]0|100|500|1000))?)$" $compression)) }}
{{- fail (printf "nodes.%s.pools.%s.zfsCompression is not a supported OpenZFS compression value" $name $poolName) }}
{{- end }}
{{- end }}
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