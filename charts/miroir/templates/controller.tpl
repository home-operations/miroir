apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "miroir.controllerName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    app.kubernetes.io/component: controller
spec:
  replicas: {{ .Values.replicaCount }}
  strategy:
  {{- if eq (include "miroir.leaderElectionEnabled" .) "true" }}
    type: RollingUpdate
  {{- else }}
    type: Recreate
  {{- end }}
  selector:
    matchLabels:
      {{- include "miroir.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: controller
  template:
    metadata:
      labels:
        {{- include "miroir.labels" . | nindent 8 }}
        app.kubernetes.io/component: controller
        {{- with .Values.podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- /* No topology checksum: the controller folds the MiroirNode CRs
      from its cache per RPC/reconcile, so a topology edit needs no
      restart. */}}
      {{- with .Values.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
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
            - --provision-timeout={{ .Values.provisionTimeout }}
            - --overcommit-ratio={{ .Values.overcommitRatio }}
            - --free-space-ratio={{ .Values.freeSpaceRatio }}
            - --auto-tie-breaker={{ .Values.autoTieBreaker }}
            {{- with .Values.autoDiskfulAfter }}
            - --auto-diskful-after={{ . }}
            {{- end }}
            {{- with .Values.autoEvictAfter }}
            - --auto-evict-after={{ . }}
            {{- end }}
            - --drbd-port-base={{ .Values.drbd.portBase }}
            {{- if .Values.gateway.enabled }}
            - --gateway-image={{ include "miroir.gatewayImage" . }}
            - --gateway-service-account={{ include "miroir.gatewayName" . }}
            {{- end }}
            {{- if eq (include "miroir.leaderElectionEnabled" .) "true" }}
            - --leader-elect=true
            - --leader-election-id={{ include "miroir.leaderElectionID" . }}
            {{- end }}
            - --zap-log-level={{ .Values.logging.level }}
            - --zap-encoder={{ .Values.logging.format }}
            {{- with .Values.extraArgs }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          env:
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            {{- with .Values.extraEnv }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: { drop: [ALL] }
          ports:
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
        - name: csi-provisioner
          image: {{ .Values.sidecars.provisioner.image }}
          args:
            - --csi-address=/csi/csi.sock
            - --timeout={{ .Values.sidecars.provisioner.timeout }}
            - --leader-election={{ include "miroir.leaderElectionEnabled" . }}
            - --default-fstype=ext4
            {{- if .Values.storageCapacity.enabled }}
            - --enable-capacity
            - --capacity-ownerref-level=2
            {{- end }}
          {{- if .Values.storageCapacity.enabled }}
          env:
            - name: NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          {{- end }}
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
            - --leader-election={{ include "miroir.leaderElectionEnabled" . }}
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
            - --leader-election={{ include "miroir.leaderElectionEnabled" . }}
          resources:
            requests: { cpu: 10m, memory: 32Mi }
            limits: { memory: 128Mi }
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
        {{- if .Values.sidecars.healthMonitor.enabled }}
        - name: csi-external-health-monitor-controller
          image: {{ .Values.sidecars.healthMonitor.image }}
          args:
            - --csi-address=/csi/csi.sock
            - --monitor-interval={{ .Values.sidecars.healthMonitor.interval }}
            - --leader-election={{ include "miroir.leaderElectionEnabled" . }}
          resources:
            requests: { cpu: 10m, memory: 32Mi }
            limits: { memory: 128Mi }
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
        {{- end }}
      volumes:
        - name: socket-dir
          emptyDir: {}
{{- if gt (int .Values.replicaCount) 1 }}
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "miroir.controllerName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    app.kubernetes.io/component: controller
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      {{- include "miroir.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: controller
{{- end }}