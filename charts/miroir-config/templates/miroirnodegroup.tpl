{{- /* Pass-through renderer for MiroirNodeGroup CRs: `template` is the
       MiroirNode spec verbatim (the CRD validates it), `nodeSelector` a
       standard LabelSelector. The same structural key check as nodes.tpl:
       the API server silently PRUNES unknown fields, so a typo would
       apply cleanly and misconfigure the fleet. */ -}}
{{- $groupKeys := list "nodeSelector" "template" }}
{{- $specKeys := list "zone" "address" "autoEvict" "pools" }}
{{- $poolKeys := list "name" "lvmthin" "zfs" "loopfile" }}
{{- range $name, $group := .Values.nodeGroups }}
{{- $spec := dict }}
{{- if kindIs "map" $group }}
{{- $spec = $group.spec }}
{{- end }}
{{- if not (kindIs "map" $spec) }}
{{- fail (printf "nodeGroups.%s: the group must be declared under `spec:` (the MiroirNodeGroup spec, verbatim)" $name) }}
{{- end }}
{{- range $key, $_ := $spec }}
{{- if not (has $key $groupKeys) }}
{{- fail (printf "nodeGroups.%s.spec.%s: unknown field (the API server would silently drop it); valid fields: %s" $name $key (join ", " $groupKeys)) }}
{{- end }}
{{- end }}
{{- range $key, $_ := ($spec.template | default dict) }}
{{- if not (has $key $specKeys) }}
{{- fail (printf "nodeGroups.%s.spec.template.%s: unknown field (the API server would silently drop it); valid fields: %s" $name $key (join ", " $specKeys)) }}
{{- end }}
{{- end }}
{{- range $i, $pool := (($spec.template | default dict).pools | default list) }}
{{- if kindIs "map" $pool }}
{{- range $key, $_ := $pool }}
{{- if not (has $key $poolKeys) }}
{{- fail (printf "nodeGroups.%s.spec.template.pools[%d].%s: unknown field (the API server would silently drop it); valid fields: %s" $name $i $key (join ", " $poolKeys)) }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
---
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNodeGroup
metadata:
  name: {{ $name }}
  labels:
    {{- include "miroir-config.labels" $ | nindent 4 }}
spec:
  {{- $spec | toYaml | nindent 2 }}
{{- end }}
