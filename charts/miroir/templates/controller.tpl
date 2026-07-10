apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "miroir.controllerName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    app.kubernetes.io/component: controller
spec:
  replicas: 1
  strategy:
    type: Recreate # one writer for allocations; no leader election
  selector:
    matchLabels:
      {{- include "miroir.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: controller
  template:
    metadata:
      labels:
        {{- include "miroir.labels" . | nindent 8 }}
        app.kubernetes.io/component: controller
    spec:
      serviceAccountName: {{ include "miroir.controllerName" . }}
      {{- include "miroir.imagePullSecrets" . | nindent 6 }}
      {{- with .Values.global.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.global.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      {{- with .Values.global.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      priorityClassName: {{ .Values.priorityClassName }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: controller
          image: {{ include "miroir.image" . }}
          imagePullPolicy: {{ include "miroir.imagePullPolicy" . }}
          args:
            - --mode=controller
            - --csi-socket=/csi/csi.sock
            - --nodes-config=/etc/miroir/nodes.yaml
            - --provision-timeout={{ .Values.provisionTimeout }}
            - --overcommit-ratio={{ .Values.overcommitRatio }}
            - --auto-tie-breaker={{ .Values.autoTieBreaker }}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: { drop: [ALL] }
          ports:
            # Serves /metrics plus the /healthz and /readyz probes (single
            # operational port; see cmd/main.go).
            - name: metrics
              containerPort: 8081
          livenessProbe:
            httpGet: { path: /healthz, port: metrics }
            initialDelaySeconds: 10
          readinessProbe:
            httpGet: { path: /readyz, port: metrics }
          resources: {{- toYaml .Values.resources | nindent 12 }}
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: nodes
              mountPath: /etc/miroir
              readOnly: true
        - name: csi-provisioner
          image: {{ .Values.sidecars.provisioner.image }}
          args:
            - --csi-address=/csi/csi.sock
            - --timeout={{ .Values.sidecars.provisioner.timeout }}
            - --leader-election=false
            - --default-fstype=ext4
          resources:
            requests: { cpu: 10m, memory: 32Mi }
            limits: { memory: 128Mi }
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
        - name: csi-snapshotter
          image: {{ .Values.sidecars.snapshotter.image }}
          args:
            - --csi-address=/csi/csi.sock
            - --timeout={{ .Values.sidecars.snapshotter.timeout }}
            - --leader-election=false
          resources:
            requests: { cpu: 10m, memory: 32Mi }
            limits: { memory: 128Mi }
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
        - name: csi-resizer
          image: {{ .Values.sidecars.resizer.image }}
          args:
            - --csi-address=/csi/csi.sock
            - --timeout={{ .Values.sidecars.resizer.timeout }}
            - --leader-election=false
          resources:
            requests: { cpu: 10m, memory: 32Mi }
            limits: { memory: 128Mi }
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
      volumes:
        - name: socket-dir
          emptyDir: {}
        - name: nodes
          configMap:
            name: {{ include "miroir.nodesConfigName" . }}