{{- range $name, $node := .Values.nodes }}
{{- $loopDirs := list }}
{{- range $poolName, $pool := $node.pools }}
{{- if eq (toString $pool.backend) "loopfile" }}
{{- $loopDirs = append $loopDirs $pool.baseDir }}
{{- end }}
{{- end }}
{{- $loopDirs = $loopDirs | uniq }}
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
      {{- include "miroir.imagePullSecrets" $ | nindent 6 }}
      nodeName: {{ $name }}
      restartPolicy: Never
      hostNetwork: true
      hostPID: true
      tolerations:
        - operator: Exists
      containers:
        - name: setup
          image: {{ include "miroir.agentImage" $ }}
          imagePullPolicy: {{ include "miroir.agentImagePullPolicy" $ }}
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
{{- range $i, $dir := $loopDirs }}
            - name: loopfile-base-{{ $i }}
              mountPath: {{ $dir }}
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
{{- range $i, $dir := $loopDirs }}
        - name: loopfile-base-{{ $i }}
          hostPath:
            path: {{ $dir }}
            type: DirectoryOrCreate
{{- end }}
{{- end }}