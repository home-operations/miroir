apiVersion: v1
kind: ServiceAccount
metadata:
  name: homefs-controller
  namespace: {{ .Release.Namespace }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: homefs-agent
  namespace: {{ .Release.Namespace }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: homefs-controller
rules:
  # homefs desired state
  - apiGroups: ["homefs.io"]
    resources: ["homefsvolumes", "homefssnapshots"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["homefs.io"]
    resources: ["homefsvolumes/status", "homefssnapshots/status"]
    verbs: ["get"]
  # external-provisioner sidecar (topology needs nodes + csinodes)
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "watch", "create", "delete"]
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
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: homefs-controller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: homefs-controller
subjects:
  - kind: ServiceAccount
    name: homefs-controller
    namespace: {{ .Release.Namespace }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: homefs-agent
rules:
  - apiGroups: ["homefs.io"]
    resources: ["homefsvolumes", "homefssnapshots"]
    verbs: ["get", "list", "watch", "update"] # update releases finalizers
  - apiGroups: ["homefs.io"]
    resources: ["homefsvolumes/status", "homefssnapshots/status"]
    verbs: ["get", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: homefs-agent
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: homefs-agent
subjects:
  - kind: ServiceAccount
    name: homefs-agent
    namespace: {{ .Release.Namespace }}
