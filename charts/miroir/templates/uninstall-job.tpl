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