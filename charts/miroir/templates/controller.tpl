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
    type: RollingUpdate # leader election arbitrates the rollout overlap
  {{- else }}
    type: Recreate # one writer for allocations; no leader election
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
            - --nodes-config=/etc/miroir/nodes.yaml
            - --provision-timeout={{ .Values.provisionTimeout }}
            - --overcommit-ratio={{ .Values.overcommitRatio }}
            - --auto-tie-breaker={{ .Values.autoTieBreaker }}
            {{- with .Values.autoDiskfulAfter }}
            - --auto-diskful-after={{ . }}
            {{- end }}
            - --drbd-port-base={{ .Values.drbd.portBase }}
            # RWX gateway: the controller spawns per-volume NFS-Ganesha
            # Deployments in its own namespace from this image.
            - --gateway-image={{ include "miroir.gatewayImage" . }}
            - --gateway-service-account={{ include "miroir.gatewayName" . }}
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
            # The controller creates gateway workloads in its own namespace.
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
            - --leader-election={{ include "miroir.leaderElectionEnabled" . }}
            - --default-fstype=ext4
            {{- if .Values.storageCapacity.enabled }}
            - --enable-capacity
            # Own the CSIStorageCapacity objects from the controller Deployment
            # (pod → ReplicaSet → Deployment) so they are garbage-collected with it.
            - --capacity-ownerref-level=2
            {{- end }}
          {{- if .Values.storageCapacity.enabled }}
          env:
            # The provisioner reads these to create CSIStorageCapacity objects in
            # this namespace and resolve the owning Deployment.
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
        - name: nodes
          configMap:
            name: {{ include "miroir.nodesConfigName" . }}
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
  # Drains take controllers one at a time, so the warm standby picks up the
  # Lease instead of provisioning stalling on a full pod reschedule.
  maxUnavailable: 1
  selector:
    matchLabels:
      {{- include "miroir.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: controller
{{- end }}