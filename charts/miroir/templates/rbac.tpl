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
  - apiGroups: ["miroir.home-operations.com"]
    resources: ["miroirvolumes", "miroirsnapshots"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["miroir.home-operations.com"]
    resources: ["miroirvolumes/status", "miroirsnapshots/status"]
    verbs: ["get", "update", "patch"]
  - apiGroups: ["miroir.home-operations.com"]
    resources: ["miroirnodes"]
    verbs: ["get", "list", "watch"]
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
  - apiGroups: [""]
    resources: ["persistentvolumeclaims/status"]
    verbs: ["update", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  {{- if .Values.storageCapacity.enabled }}
  - apiGroups: ["storage.k8s.io"]
    resources: ["csistoragecapacities"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["apps"]
    resources: ["replicasets"]
    verbs: ["get"]
  {{- end }}
  {{- if eq (include "miroir.leaderElectionEnabled" .) "true" }}
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  {{- end }}
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
  - apiGroups: ["miroir.home-operations.com"]
    resources: ["miroirvolumes", "miroirsnapshots"]
    verbs: ["get", "list", "watch", "update"]
  - apiGroups: ["miroir.home-operations.com"]
    resources: ["miroirvolumes/status", "miroirsnapshots/status"]
    verbs: ["get", "patch"]
  - apiGroups: ["miroir.home-operations.com"]
    resources: ["miroirnodes"]
    verbs: ["get", "list", "watch", "create", "update", "patch"]
  - apiGroups: ["miroir.home-operations.com"]
    resources: ["miroirnodes/status"]
    verbs: ["get", "update", "patch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "patch"]
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "watch"]
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
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "miroir.controllerName" . }}-gateway
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
rules:
  - apiGroups: ["apps"]
    resources: ["deployments"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "miroir.controllerName" . }}-gateway
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ include "miroir.controllerName" . }}-gateway
subjects:
  - kind: ServiceAccount
    name: {{ include "miroir.controllerName" . }}
    namespace: {{ .Release.Namespace }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "miroir.gatewayName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "miroir.gatewayName" . }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
rules:
  - apiGroups: ["miroir.home-operations.com"]
    resources: ["miroirvolumes"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["miroir.home-operations.com"]
    resources: ["miroirvolumes/status"]
    verbs: ["get", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "miroir.gatewayName" . }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "miroir.gatewayName" . }}
subjects:
  - kind: ServiceAccount
    name: {{ include "miroir.gatewayName" . }}
    namespace: {{ .Release.Namespace }}