apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: {{ include "miroir.csiDriverName" . }}
spec:
  attachRequired: false
  podInfoOnMount: false
  fsGroupPolicy: ReadWriteOnceWithFSType
  volumeLifecycleModes:
    - Persistent
  storageCapacity: {{ .Values.storageCapacity.enabled }}