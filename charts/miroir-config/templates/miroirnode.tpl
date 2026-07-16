{{- /* Pure pass-through renderer (the rook-ceph-cluster model): the values
       ARE the MiroirNode spec, verbatim. Validation lives in the CRD
       (schema + CEL), not here — the only chart-side checks are structural:
       where the spec lives (so a pre-0.11 values file gets a migration
       pointer instead of a CRD rejection it cannot decode) and that every
       key is one the CRD knows. The key check exists because the API
       server silently PRUNES unknown fields instead of rejecting them — a
       typo like thinPoolSize for poolSize would otherwise apply cleanly
       and misconfigure the pool. The backend is the block that is present
       (lvmthin/zfs/loopfile); the CRD enforces exactly one. */ -}}
{{- $specKeys := list "zone" "address" "autoEvict" "pools" }}
{{- $poolKeys := list "name" "lvmthin" "zfs" "loopfile" }}
{{- $backendKeys := dict
      "lvmthin" (list "device" "poolSize")
      "zfs" (list "dataset" "compression" "volBlockSize")
      "loopfile" (list "baseDir") }}
{{- range $name, $node := .Values.nodes }}
{{- $spec := dict }}
{{- if kindIs "map" $node }}
{{- $spec = $node.spec }}
{{- end }}
{{- if not $spec }}
{{- fail (printf "nodes.%s: the node must be declared under `spec:` (the MiroirNode spec, verbatim) — pre-0.11 values need the topology migration; see https://miroir.home-operations.com/upgrading/" $name) }}
{{- end }}
{{- if not (kindIs "map" $spec) }}
{{- fail (printf "nodes.%s.spec: must be a map (the MiroirNode spec, verbatim)" $name) }}
{{- end }}
{{- range $key, $_ := $spec }}
{{- if not (has $key $specKeys) }}
{{- fail (printf "nodes.%s.spec.%s: unknown field (the API server would silently drop it); valid fields: %s" $name $key (join ", " $specKeys)) }}
{{- end }}
{{- end }}
{{- $pools := list }}
{{- if kindIs "slice" $spec.pools }}
{{- $pools = $spec.pools }}
{{- end }}
{{- range $i, $pool := $pools }}
{{- if kindIs "map" $pool }}
{{- range $key, $block := $pool }}
{{- if not (has $key $poolKeys) }}
{{- fail (printf "nodes.%s.spec.pools[%d].%s: unknown field (the API server would silently drop it); valid fields: %s" $name $i $key (join ", " $poolKeys)) }}
{{- end }}
{{- if and (hasKey $backendKeys $key) (kindIs "map" $block) }}
{{- range $bkey, $_ := $block }}
{{- if not (has $bkey (get $backendKeys $key)) }}
{{- fail (printf "nodes.%s.spec.pools[%d].%s.%s: unknown field (the API server would silently drop it); valid fields: %s" $name $i $key $bkey (join ", " (get $backendKeys $key))) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
---
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
  name: {{ $name }}
  annotations:
    # Uninstalling (or dropping an entry from) this chart must never tear
    # the topology out from under live volumes: Helm leaves kept objects
    # in place, and decommissioning a node stays an explicit
    # `kubectl delete miroirnode <name>`.
    helm.sh/resource-policy: keep
  labels:
    {{- include "miroir-config.labels" $ | nindent 4 }}
spec:
  {{- $spec | toYaml | nindent 2 }}
{{- end }}
