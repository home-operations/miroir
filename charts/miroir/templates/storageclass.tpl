{{- range $i, $sc := .Values.storageClasses }}
{{- if $i }}
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
  {{- end }}
  csi.storage.k8s.io/fstype: {{ $sc.fsType | default "ext4" }}
{{- end }}
{{- if .Values.volumeSnapshotClass.create }}
{{- if .Values.storageClasses }}
---
{{- end }}
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: {{ .Values.volumeSnapshotClass.name }}
driver: {{ include "miroir.csiDriverName" . }}
deletionPolicy: {{ .Values.volumeSnapshotClass.deletionPolicy }}
{{- end }}
