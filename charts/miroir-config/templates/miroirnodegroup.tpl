{{- /* Pass-through renderer for MiroirNodeGroup CRs: `template` is the
       MiroirNode spec verbatim (the CRD validates it), `nodeSelector` a
       standard LabelSelector. The same structural key check as nodes.tpl:
       the API server silently PRUNES unknown fields, so a typo would
       apply cleanly and misconfigure the fleet. */ -}}
{{- $groupKeys := list "nodeSelector" "template" }}
{{- $specKeys := list "zone" "autoEvict" "pools" }}
{{- $selectorKeys := list "matchLabels" "matchExpressions" }}
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
{{- /* A pruned selector key leaves an empty selector, and an empty
       selector matches EVERY node — the worst possible typo. */ -}}
{{- if kindIs "map" $spec.nodeSelector }}
{{- range $key, $_ := $spec.nodeSelector }}
{{- if not (has $key $selectorKeys) }}
{{- fail (printf "nodeGroups.%s.spec.nodeSelector.%s: unknown field (the API server would silently drop it, leaving an empty selector that matches every node in the cluster); valid fields: %s" $name $key (join ", " $selectorKeys)) }}
{{- end }}
{{- end }}
{{- end }}
{{- $template := dict }}
{{- if kindIs "map" $spec.template }}
{{- $template = $spec.template }}
{{- end }}
{{- if hasKey $template "address" }}
{{- fail (printf "nodeGroups.%s.spec.template.address: an address is a per-node fact the CRD rejects in a template — annotate each Node with miroir.home-operations.com/address instead, or author a direct MiroirNode" $name) }}
{{- end }}
{{- range $key, $_ := $template }}
{{- if not (has $key $specKeys) }}
{{- fail (printf "nodeGroups.%s.spec.template.%s: unknown field (the API server would silently drop it); valid fields: %s" $name $key (join ", " $specKeys)) }}
{{- end }}
{{- end }}
{{- include "miroir-config.validatePools" (dict "path" (printf "nodeGroups.%s.spec.template.pools" $name) "pools" $template.pools) }}
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
