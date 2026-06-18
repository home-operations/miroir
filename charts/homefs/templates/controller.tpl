apiVersion: apps/v1
kind: Deployment
metadata:
  name: homefs-controller
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/name: homefs
    app.kubernetes.io/component: controller
spec:
  replicas: 1
  strategy:
    type: Recreate # one writer for allocations; no leader election
  selector:
    matchLabels:
      app.kubernetes.io/name: homefs
      app.kubernetes.io/component: controller
  template:
    metadata:
      labels:
        app.kubernetes.io/name: homefs
        app.kubernetes.io/component: controller
    spec:
      serviceAccountName: homefs-controller
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        seccompProfile: { type: RuntimeDefault }
      containers:
        - name: controller
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          args:
            - --mode=controller
            - --csi-socket=/csi/csi.sock
            - --nodes-config=/etc/homefs/nodes.yaml
            - --provision-timeout={{ .Values.controller.provisionTimeout }}
          securityContext:
            allowPrivilegeEscalation: false
            capabilities: { drop: [ALL] }
          ports:
            - name: healthz
              containerPort: 8081
            - name: metrics
              containerPort: 8080
          livenessProbe:
            httpGet: { path: /healthz, port: healthz }
            initialDelaySeconds: 10
          readinessProbe:
            httpGet: { path: /readyz, port: healthz }
          resources: {{- toYaml .Values.controller.resources | nindent 12 }}
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: nodes
              mountPath: /etc/homefs
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
            - --timeout={{ .Values.sidecars.provisioner.timeout }}
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
            - --timeout={{ .Values.sidecars.provisioner.timeout }}
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
            name: homefs-nodes
