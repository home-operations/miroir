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
  {{- if gt (int $replicas) 1 }}
  # freeze: never diverges, halts on any disconnect; last-man-standing:
  # survivor keeps writing on node loss, split-brain alerts on reconnect.
  miroir.home-operations.com/quorum: {{ $sc.quorum | default "freeze" }}
  {{- if hasKey $sc "allowRemoteVolumeAccess" }}
  # Absent defaults to allowed (controller-side); rendered only when the
  # entry pins it either way.
  miroir.home-operations.com/allowRemoteVolumeAccess: {{ $sc.allowRemoteVolumeAccess | quote }}
  {{- end }}
  {{- if $sc.bitmapGranularity }}
  # DRBD bitmap block size in bytes, fixed when a replica's metadata is
  # created; absent means DRBD's default (4096).
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
