apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "miroir.controllerName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "miroir.agentName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "miroir.controllerName" . }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
rules:
  # miroir desired state
  - apiGroups: ["miroir.io"]
    resources: ["miroirvolumes", "miroirsnapshots"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["miroir.io"]
    resources: ["miroirvolumes/status", "miroirsnapshots/status"]
    # patch: the controller records the Formatted flag on a restored
    # (clone-from-snapshot) volume so the agent skips mkfs.
    verbs: ["get", "update", "patch"]
  # capacity-aware placement reads the pool stats agents publish
  - apiGroups: ["miroir.io"]
    resources: ["miroirnodes"]
    verbs: ["get", "list", "watch"]
  # external-provisioner sidecar (topology needs nodes + csinodes)
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "watch", "create", "delete", "patch"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "update"]
  - apiGroups: ["storage.k8s.io"]
    resources: ["storageclasses"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["storage.k8s.io"]
    resources: ["csinodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["list", "watch", "create", "update", "patch"]
  # external-snapshotter sidecar
  - apiGroups: ["snapshot.storage.k8s.io"]
    resources: ["volumesnapshotclasses", "volumesnapshots"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["snapshot.storage.k8s.io"]
    resources: ["volumesnapshotcontents"]
    verbs: ["get", "list", "watch", "update", "patch"]
  - apiGroups: ["snapshot.storage.k8s.io"]
    resources: ["volumesnapshotcontents/status"]
    verbs: ["update", "patch"]
  - apiGroups: ["groupsnapshot.storage.k8s.io"]
    resources: ["volumegroupsnapshotclasses", "volumegroupsnapshotcontents"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["groupsnapshot.storage.k8s.io"]
    resources: ["volumegroupsnapshotcontents/status"]
    verbs: ["update", "patch"]
  # external-resizer sidecar
  - apiGroups: [""]
    resources: ["persistentvolumeclaims/status"]
    verbs: ["update", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "miroir.controllerName" . }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "miroir.controllerName" . }}
subjects:
  - kind: ServiceAccount
    name: {{ include "miroir.controllerName" . }}
    namespace: {{ .Release.Namespace }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "miroir.agentName" . }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
rules:
  - apiGroups: ["miroir.io"]
    resources: ["miroirvolumes", "miroirsnapshots"]
    verbs: ["get", "list", "watch", "update"] # update releases finalizers
  - apiGroups: ["miroir.io"]
    resources: ["miroirvolumes/status", "miroirsnapshots/status"]
    verbs: ["get", "patch"]
  # each agent owns its own MiroirNode, publishing pool capacity
  - apiGroups: ["miroir.io"]
    resources: ["miroirnodes"]
    verbs: ["get", "list", "watch", "create", "update", "patch"]
  - apiGroups: ["miroir.io"]
    resources: ["miroirnodes/status"]
    verbs: ["get", "update", "patch"]
  # PoolUsageHigh events at the 80% capacity warn line
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "miroir.agentName" . }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "miroir.agentName" . }}
subjects:
  - kind: ServiceAccount
    name: {{ include "miroir.agentName" . }}
    namespace: {{ .Release.Namespace }}