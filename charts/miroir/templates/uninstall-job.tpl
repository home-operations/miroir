apiVersion: batch/v1
kind: Job
metadata:
  name: miroir-uninstall
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/name: miroir
    app.kubernetes.io/component: uninstall
  annotations:
    helm.sh/hook: pre-delete
    helm.sh/hook-weight: "10"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
spec:
  template:
    spec:
      serviceAccountName: miroir-uninstall
      restartPolicy: Never
      containers:
        - name: kubectl
          image: bitnami/kubectl:1.31
          command:
            - /bin/sh
            - -c
            - |
              kubectl delete miroirsnapshots --all --ignore-not-found
              kubectl delete miroirvolumes --all --ignore-not-found
