{{- if hasKey .Values.drbd "verifyAlg" }}
{{- fail "drbd.verifyAlg was renamed to drbd.verify.algorithm" }}
{{- end }}
{{- if .Values.nodes }}
{{- fail "the `nodes` value is gone: the storage topology lives in MiroirNode custom resources applied separately from the chart — see https://miroir.home-operations.com/upgrading/" }}
{{- end }}
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
    {{- range $section := list "handlers" "startup" "options" }}
    {{- $extra := index $.Values.drbd.extraConfig $section }}
    {{- if $extra }}
        {{ $section }} {
            {{- trim $extra | nindent 12 }}
        }
    {{- else }}
        {{ $section }} {}
    {{- end }}
    {{- end }}
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
        {{- with .Values.drbd.extraConfig.disk }}
            {{- trim . | nindent 12 }}
        {{- end }}
        }
        net {
        {{- with .Values.drbd.net.maxBuffers }}
            max-buffers {{ . }};
        {{- end }}
        {{- with .Values.drbd.verify.algorithm }}
            verify-alg {{ . }};
        {{- end }}
        {{- with .Values.drbd.extraConfig.net }}
            {{- trim . | nindent 12 }}
        {{- end }}
        }
    }