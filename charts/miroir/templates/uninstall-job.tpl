{{- /* Data destruction is opt-in: without the exact confirmation the hook
(and its RBAC) is not rendered, and helm uninstall leaves every
MiroirVolume/MiroirSnapshot — and the data — in place. */}}
{{- if .Values.uninstall.confirmation }}
{{- if ne .Values.uninstall.confirmation "yes-really-destroy-data" }}
{{- fail "uninstall.confirmation must be exactly \"yes-really-destroy-data\" (or empty to keep the data on uninstall)" }}
{{- end }}
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "miroir.uninstallServiceAccountName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    app.kubernetes.io/component: uninstall
  annotations:
    helm.sh/hook: pre-delete
    helm.sh/hook-weight: "10"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
spec:
  template:
    spec:
      serviceAccountName: {{ include "miroir.uninstallServiceAccountName" . }}
      {{- include "miroir.imagePullSecrets" . | nindent 6 }}
      restartPolicy: Never
      containers:
        - name: kubectl
          image: {{ .Values.uninstall.image }}
          args:
            - delete
            - miroirsnapshots,miroirvolumes
            - --all
            - --ignore-not-found
{{- end }}