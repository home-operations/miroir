{{- if .Values.storageClass.create }}
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: {{ .Values.storageClass.name }}
  annotations:
    storageclass.kubernetes.io/is-default-class: {{ .Values.storageClass.isDefault | quote }}
provisioner: homefs.io
volumeBindingMode: WaitForFirstConsumer
# Expansion is not implemented yet (M4).
allowVolumeExpansion: false
reclaimPolicy: {{ .Values.storageClass.reclaimPolicy }}
parameters:
  homefs.io/replicas: {{ .Values.storageClass.replicas | quote }}
  csi.storage.k8s.io/fstype: {{ .Values.storageClass.fsType }}
{{- end }}
