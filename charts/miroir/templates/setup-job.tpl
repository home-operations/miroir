{{- range $name, $node := .Values.nodes }}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "miroir.fullname" $ }}-setup-{{ $name }}
  namespace: {{ $.Release.Namespace }}
  labels:
    {{- include "miroir.labels" $ | nindent 4 }}
    app.kubernetes.io/component: setup
  annotations:
    helm.sh/hook: post-install,post-upgrade
    helm.sh/hook-weight: "-5"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded
spec:
  template:
    spec:
      serviceAccountName: {{ include "miroir.setupServiceAccountName" $ }}
      nodeName: {{ $name }}
      restartPolicy: Never
      hostNetwork: true
      hostPID: true
      tolerations:
        - operator: Exists
      containers:
        - name: setup
          image: "{{ $.Values.image.repository }}:{{ $.Values.image.tag }}"
          imagePullPolicy: {{ $.Values.image.pullPolicy }}
          args:
            - --mode=setup
            - --node-name={{ $name }}
            - --nodes-config=/etc/miroir/nodes.yaml
          securityContext:
            privileged: true
          volumeMounts:
            - name: nodes
              mountPath: /etc/miroir
              readOnly: true
            - name: dev
              mountPath: /dev
            - name: run-udev
              mountPath: /run/udev
              readOnly: true
            - name: run-lvm
              mountPath: /run/lvm
            - name: modules
              mountPath: /lib/modules
              readOnly: true
{{- if eq (toString $node.backend) "loopfile" }}
            - name: loopfile-base
              mountPath: {{ $node.baseDir }}
{{- end }}
      volumes:
        - name: nodes
          configMap:
            name: {{ include "miroir.nodesConfigName" $ }}
        - name: dev
          hostPath:
            path: /dev
            type: Directory
        - name: run-udev
          hostPath:
            path: /run/udev
            type: Directory
        - name: run-lvm
          hostPath:
            path: /run/lvm
            type: DirectoryOrCreate
        - name: modules
          hostPath:
            path: /lib/modules
{{- if eq (toString $node.backend) "loopfile" }}
        - name: loopfile-base
          hostPath:
            path: {{ $node.baseDir }}
            type: DirectoryOrCreate
{{- end }}
{{- end }}