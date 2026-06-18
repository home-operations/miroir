apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: miroir.io
spec:
  attachRequired: false
  podInfoOnMount: false
  fsGroupPolicy: ReadWriteOnceWithFSType
  volumeLifecycleModes:
    - Persistent
  storageCapacity: false
