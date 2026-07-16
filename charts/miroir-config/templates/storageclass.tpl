{{- $first := true }}
{{- range $sc := .Values.storageClasses }}
{{- if $first }}{{ $first = false }}{{ else }}
---
{{- end }}
{{- $replicas := $sc.replicas | default 1 }}
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: {{ $sc.name }}
  {{- with $sc.labels }}
  labels:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  annotations:
    {{- /* merge: the isDefault knob wins over a conflicting user annotation. */}}
    {{- toYaml (merge (dict "storageclass.kubernetes.io/is-default-class" ($sc.isDefault | default false | toString)) ($sc.annotations | default dict)) | nindent 4 }}
provisioner: {{ include "miroir-config.csiDriverName" $ }}
volumeBindingMode: {{ $sc.volumeBindingMode | default "WaitForFirstConsumer" }}
allowVolumeExpansion: true
reclaimPolicy: {{ $sc.reclaimPolicy | default "Delete" }}
{{- with $sc.mountOptions }}
mountOptions:
  {{- toYaml . | nindent 2 }}
{{- end }}
parameters:
  miroir.home-operations.com/replicas: {{ $replicas | quote }}
  {{- if $sc.pool }}
  miroir.home-operations.com/pool: {{ $sc.pool | quote }}
  {{- end }}
  {{- if gt (int $replicas) 1 }}
  miroir.home-operations.com/quorum: {{ $sc.quorum | default "freeze" }}
  {{- if hasKey $sc "allowRemoteVolumeAccess" }}
  miroir.home-operations.com/allowRemoteVolumeAccess: {{ $sc.allowRemoteVolumeAccess | quote }}
  {{- end }}
  {{- if $sc.bitmapGranularity }}
  miroir.home-operations.com/bitmapGranularity: {{ $sc.bitmapGranularity | quote }}
  {{- end }}
  {{- end }}
  csi.storage.k8s.io/fstype: {{ $sc.fsType | default "ext4" }}
{{- end }}
{{- range $vsc := .Values.volumeSnapshotClasses }}
{{- if $first }}{{ $first = false }}{{ else }}
---
{{- end }}
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: {{ $vsc.name }}
  {{- with $vsc.labels }}
  labels:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- $annotations := $vsc.annotations | default dict }}
  {{- if $vsc.isDefault }}
  {{- $annotations = merge (dict "snapshot.storage.kubernetes.io/is-default-class" "true") $annotations }}
  {{- end }}
  {{- with $annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
driver: {{ include "miroir-config.csiDriverName" $ }}
deletionPolicy: {{ $vsc.deletionPolicy | default "Delete" }}
{{- end }}
