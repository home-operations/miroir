{{- /* Pure pass-through renderer (the rook-ceph-cluster model): the values
       ARE the MiroirNode spec, verbatim. Validation lives in the CRD
       (schema + CEL), not here — the only chart-side checks are structural
       (where the spec lives), so a pre-0.11 values file gets a migration
       pointer instead of a CRD rejection it cannot decode. */ -}}
{{- if not .Values.nodes }}
{{- fail "Helm values must define `nodes` — the per-node storage topology (see values.yaml)" }}
{{- end }}
{{- range $name, $node := .Values.nodes }}
{{- if not $node.spec }}
{{- fail (printf "nodes.%s: the node must be declared under `spec:` (the MiroirNode spec, verbatim) — pre-0.11 values need the topology migration; see https://miroir.home-operations.com/upgrading/" $name) }}
{{- end }}
---
apiVersion: miroir.home-operations.com/v1alpha1
kind: MiroirNode
metadata:
  name: {{ $name }}
  labels:
    {{- include "miroir.labels" $ | nindent 4 }}
spec:
  {{- $node.spec | toYaml | nindent 2 }}
{{- end }}
