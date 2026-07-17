{{- /* Standard labels for the rendered configuration objects. */ -}}
{{- define "miroir-config.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- /* The CSI driver name the sibling miroir chart registers. */ -}}
{{- define "miroir-config.csiDriverName" -}}
miroir.home-operations.com
{{- end -}}

{{- /* Structural key check for a pools list, shared by the node and
       node-group renderers so the two cannot drift: the API server
       silently PRUNES unknown fields, so a typo (thinPoolSize for
       poolSize) would apply cleanly and misconfigure the pool — or, from
       a group template, the whole fleet. Args: dict with `path` (the
       values path for messages) and `pools`. */ -}}
{{- define "miroir-config.validatePools" -}}
{{- $poolKeys := list "name" "lvmthin" "zfs" "loopfile" }}
{{- $backendKeys := dict
      "lvmthin" (list "device" "poolSize")
      "zfs" (list "dataset" "compression" "volBlockSize")
      "loopfile" (list "baseDir") }}
{{- $pools := list }}
{{- if kindIs "slice" .pools }}
{{- $pools = .pools }}
{{- end }}
{{- range $i, $pool := $pools }}
{{- if kindIs "map" $pool }}
{{- range $key, $block := $pool }}
{{- if not (has $key $poolKeys) }}
{{- fail (printf "%s[%d].%s: unknown field (the API server would silently drop it); valid fields: %s" $.path $i $key (join ", " $poolKeys)) }}
{{- end }}
{{- if and (hasKey $backendKeys $key) (kindIs "map" $block) }}
{{- range $bkey, $_ := $block }}
{{- if not (has $bkey (get $backendKeys $key)) }}
{{- fail (printf "%s[%d].%s.%s: unknown field (the API server would silently drop it); valid fields: %s" $.path $i $key $bkey (join ", " (get $backendKeys $key))) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end -}}
