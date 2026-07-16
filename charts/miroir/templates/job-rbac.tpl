{{- /* Rendered only when the uninstall hook is armed; the confirmation
value itself is validated in uninstall-job.tpl. */}}
{{- if eq .Values.uninstall.confirmation "yes-really-destroy-data" }}
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "miroir.uninstallServiceAccountName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    app.kubernetes.io/component: uninstall
  annotations:
    helm.sh/hook: pre-delete
    helm.sh/hook-weight: "5"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {{ include "miroir.uninstallServiceAccountName" . }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    app.kubernetes.io/component: uninstall
  annotations:
    helm.sh/hook: pre-delete
    helm.sh/hook-weight: "5"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
rules:
  - apiGroups: ["miroir.home-operations.com"]
    resources: ["miroirvolumes", "miroirsnapshots"]
    verbs: ["get", "list", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {{ include "miroir.uninstallServiceAccountName" . }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    app.kubernetes.io/component: uninstall
  annotations:
    helm.sh/hook: pre-delete
    helm.sh/hook-weight: "5"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: {{ include "miroir.uninstallServiceAccountName" . }}
subjects:
  - kind: ServiceAccount
    name: {{ include "miroir.uninstallServiceAccountName" . }}
    namespace: {{ .Release.Namespace }}
{{- end }}