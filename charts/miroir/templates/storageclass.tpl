{{- if .Values.storageClass.create }}
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: {{ .Values.storageClass.name }}
  annotations:
    storageclass.kubernetes.io/is-default-class: {{ .Values.storageClass.isDefault | quote }}
provisioner: miroir.io
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: {{ .Values.storageClass.reclaimPolicy }}
parameters:
  miroir.io/replicas: {{ .Values.storageClass.replicas | quote }}
  csi.storage.k8s.io/fstype: {{ .Values.storageClass.fsType }}
{{- end }}
{{- if .Values.replicatedStorageClass.create }}
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: {{ .Values.replicatedStorageClass.name }}
  annotations:
    storageclass.kubernetes.io/is-default-class: {{ .Values.replicatedStorageClass.isDefault | quote }}
provisioner: miroir.io
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: {{ .Values.replicatedStorageClass.reclaimPolicy }}
parameters:
  miroir.io/replicas: "2"
  # last-man-standing: survivor keeps writing on node loss, split-brain
  # alerts on reconnect; freeze: never diverges, halts on any disconnect.
  miroir.io/quorum: {{ .Values.replicatedStorageClass.quorum }}
  csi.storage.k8s.io/fstype: {{ .Values.replicatedStorageClass.fsType }}
{{- end }}
{{- if .Values.volumeSnapshotClass.create }}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: {{ .Values.volumeSnapshotClass.name }}
driver: miroir.io
deletionPolicy: {{ .Values.volumeSnapshotClass.deletionPolicy }}
{{- end }}
