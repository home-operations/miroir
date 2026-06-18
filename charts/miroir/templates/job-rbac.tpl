apiVersion: v1
kind: ServiceAccount
metadata:
  name: miroir-setup
  namespace: {{ .Release.Namespace }}
  annotations:
    helm.sh/hook: post-install,post-upgrade
    helm.sh/hook-weight: "-10"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: miroir-uninstall
  namespace: {{ .Release.Namespace }}
  annotations:
    helm.sh/hook: pre-delete
    helm.sh/hook-weight: "5"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: miroir-uninstall
  annotations:
    helm.sh/hook: pre-delete
    helm.sh/hook-weight: "5"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
rules:
  - apiGroups: ["miroir.io"]
    resources: ["miroirvolumes", "miroirsnapshots"]
    verbs: ["get", "list", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: miroir-uninstall
  annotations:
    helm.sh/hook: pre-delete
    helm.sh/hook-weight: "5"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: miroir-uninstall
subjects:
  - kind: ServiceAccount
    name: miroir-uninstall
    namespace: {{ .Release.Namespace }}
