{{- if .Values.storageClass.create }}
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: {{ .Values.storageClass.name }}
  annotations:
    storageclass.kubernetes.io/is-default-class: {{ .Values.storageClass.isDefault | quote }}
provisioner: {{ include "miroir.csiDriverName" . }}
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: {{ .Values.storageClass.reclaimPolicy }}
parameters:
  miroir.home-operations.com/replicas: {{ .Values.storageClass.replicas | quote }}
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
provisioner: {{ include "miroir.csiDriverName" . }}
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: {{ .Values.replicatedStorageClass.reclaimPolicy }}
parameters:
  miroir.home-operations.com/replicas: "2"
  # last-man-standing: survivor keeps writing on node loss, split-brain
  # alerts on reconnect; freeze: never diverges, halts on any disconnect.
  miroir.home-operations.com/quorum: {{ .Values.replicatedStorageClass.quorum }}
  csi.storage.k8s.io/fstype: {{ .Values.replicatedStorageClass.fsType }}
{{- end }}
{{- if and .Values.replicatedStorageClass.create .Values.replicatedStorageClass.remoteAccessClass }}
---
# Remote-access variant: identical volumes without PV node affinity. A
# separate class because StorageClass parameters are immutable.
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: {{ .Values.replicatedStorageClass.name }}-remote
  annotations:
    storageclass.kubernetes.io/is-default-class: "false"
provisioner: {{ include "miroir.csiDriverName" . }}
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
reclaimPolicy: {{ .Values.replicatedStorageClass.reclaimPolicy }}
parameters:
  miroir.home-operations.com/replicas: "2"
  miroir.home-operations.com/quorum: {{ .Values.replicatedStorageClass.quorum }}
  miroir.home-operations.com/allowRemoteVolumeAccess: "true"
  csi.storage.k8s.io/fstype: {{ .Values.replicatedStorageClass.fsType }}
{{- end }}
{{- if .Values.volumeSnapshotClass.create }}
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: {{ .Values.volumeSnapshotClass.name }}
driver: {{ include "miroir.csiDriverName" . }}
deletionPolicy: {{ .Values.volumeSnapshotClass.deletionPolicy }}
{{- end }}