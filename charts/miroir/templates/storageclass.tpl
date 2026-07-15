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
  annotations:
    storageclass.kubernetes.io/is-default-class: {{ $sc.isDefault | default false | quote }}
provisioner: {{ include "miroir.csiDriverName" $ }}
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: {{ $sc.reclaimPolicy | default "Delete" }}
parameters:
  miroir.home-operations.com/replicas: {{ $replicas | quote }}
  {{- if $sc.pool }}
  miroir.home-operations.com/pool: {{ $sc.pool }}
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
  {{- if $vsc.isDefault }}
  annotations:
    snapshot.storage.kubernetes.io/is-default-class: "true"
  {{- end }}
driver: {{ include "miroir.csiDriverName" $ }}
deletionPolicy: {{ $vsc.deletionPolicy | default "Delete" }}
{{- end }}
