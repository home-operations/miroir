apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: {{ include "miroir.csiDriverName" . }}
spec:
  attachRequired: false
  podInfoOnMount: false
  # The kubelet applies fsGroup to an RWX volume (an NFS mount from the
  # gateway) only under File.
  fsGroupPolicy: File
  volumeLifecycleModes:
    - Persistent
  storageCapacity: {{ .Values.storageCapacity.enabled }}