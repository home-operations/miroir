{{- /* Distinct loopfile base directories across nodes, identity-mounted
       (host path == container path) so losetup/reflink see the same path
       the agent reads from nodes.yaml. DirectoryOrCreate is harmless on
       nodes that don't use the loopfile backend. */ -}}
{{- $loopDirs := list }}
{{- range $name, $node := .Values.nodes }}
{{-   if eq (toString $node.backend) "loopfile" }}
{{-     $loopDirs = append $loopDirs $node.baseDir }}
{{-   end }}
{{- end }}
{{- $loopDirs = $loopDirs | uniq }}
{{- if and .Values.drbd.verify.schedule (not .Values.drbd.verifyAlg) }}
{{- fail "drbd.verify.schedule requires drbd.verifyAlg — a scheduled verify is meaningless without an arming verify-alg" }}
{{- end }}
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {{ include "miroir.agentName" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    app.kubernetes.io/component: agent
spec:
  selector:
    matchLabels:
      {{- include "miroir.selectorLabels" . | nindent 6 }}
      app.kubernetes.io/component: agent
  template:
    metadata:
      labels:
        {{- include "miroir.labels" . | nindent 8 }}
        app.kubernetes.io/component: agent
        {{- with .Values.agent.podLabels }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- with .Values.agent.podAnnotations }}
      annotations:
        {{- toYaml . | nindent 8 }}
      {{- end }}
    spec:
      serviceAccountName: {{ include "miroir.agentName" . }}
      # hostNetwork so the pod IP is the node IP and the container
      # hostname matches the node — DRBD peers dial the host IP and
      # match `on <hostname>` blocks; agent ports must be host-unique.
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      # drbdadm/drbdsetup need the host PID namespace for /proc access
      # to the kernel module's worker threads.
      hostPID: true
      # system-node-critical so kubelet graceful shutdown stops the agent
      # after workloads — their DRBD legs are then Secondary and safe to
      # release (see agentShutdownSweep). Needs Talos
      # shutdownGracePeriodCriticalPods >= the grace period below (see README).
      {{- include "miroir.imagePullSecrets" . | nindent 6 }}
      priorityClassName: system-node-critical
      # Longer grace lets the cordon-gated DRBD teardown finish before SIGKILL;
      # routine restarts stay schedulable and skip it.
      terminationGracePeriodSeconds: 60
      tolerations:
        - operator: Exists # CSI node service must run on every schedulable node
      containers:
        - name: agent
          image: {{ include "miroir.agentImage" . }}
          imagePullPolicy: {{ include "miroir.agentImagePullPolicy" . }}
          args:
            - --mode=agent
            - --csi-socket=/csi/csi.sock
            - --nodes-config=/etc/miroir/nodes.yaml
            - --metrics-bind-address=:9810
            - --pool-stats-interval={{ .Values.agent.poolStatsInterval }}
            - --volume-workers={{ .Values.agent.volumeWorkers }}
            {{- if and .Values.drbd.verify.schedule (not .Values.drbd.verify.suspend) }}
            - --verify-schedule={{ .Values.drbd.verify.schedule }}
            {{- end }}
            - --zap-log-level={{ .Values.logging.level }}
            - --zap-encoder={{ .Values.logging.format }}
            {{- with .Values.agent.extraArgs }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            {{- with .Values.agent.extraEnv }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          securityContext:
            privileged: true
          ports:
            # Serves /metrics plus the /healthz and /readyz probes (single
            # operational port; see cmd/main.go). Stays in the agent's 98xx
            # host-port range: hostNetwork means this binds on the node, and
            # :8081 is the org-standard pod port other workloads use.
            - name: metrics
              containerPort: 9810
          livenessProbe:
            httpGet: { path: /healthz, port: metrics }
            initialDelaySeconds: 15
            periodSeconds: 20
          readinessProbe:
            httpGet: { path: /readyz, port: metrics }
            initialDelaySeconds: 5
            periodSeconds: 10
          resources: {{- toYaml .Values.agent.resources | nindent 12 }}
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: nodes
              mountPath: /etc/miroir
              readOnly: true
            - name: kubelet
              mountPath: {{ .Values.agent.kubeletDir }}
              mountPropagation: Bidirectional
            # Plain bind of host /dev (no mountPropagation): the
            # container must share the host's devtmpfs inode table so
            # kernel-created nodes (zvol/LV activation) appear in-pod.
            - name: dev
              mountPath: /dev
            # libzfs/libblkid read partition metadata from the host
            # udev runtime DB; without it zvol operations can see an
            # empty DB and misread device state.
            - name: run-udev
              mountPath: /run/udev
              readOnly: true
            - name: run-lvm
              mountPath: /run/lvm
            - name: modules
              mountPath: /lib/modules
              readOnly: true
            # Rendered .res files + create-md/seed markers live on the
            # host so DRBD state survives pod restarts; the container
            # path is drbdadm's default include dir. /etc is read-only
            # on Talos, hence the /var/lib host backing.
            - name: drbd-cfg
              mountPath: /etc/drbd.d
            # The hostPath bind shadows the image-baked global config;
            # re-introduce it via subPath or drbdadm warns on every
            # invocation.
            - name: drbd-global-conf
              mountPath: /etc/drbd.d/global_common.conf
              subPath: global_common.conf
              readOnly: true
{{- range $i, $dir := $loopDirs }}
            # loopfile backing files live on the host filesystem; identity
            # mount so the path matches nodes.yaml baseDir.
            - name: loopfile-base-{{ $i }}
              mountPath: {{ $dir }}
{{- end }}
        - name: node-driver-registrar
          image: {{ .Values.agent.registrar.image }}
          args:
            - --csi-address=/csi/csi.sock
            - --kubelet-registration-path={{ .Values.agent.kubeletDir }}/plugins/miroir.home-operations.com/csi.sock
          resources:
            requests: { cpu: 5m, memory: 16Mi }
            limits: { memory: 64Mi }
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: registration
              mountPath: /registration
      volumes:
        - name: nodes
          configMap:
            name: {{ include "miroir.nodesConfigName" . }}
        - name: socket-dir
          hostPath:
            path: {{ .Values.agent.kubeletDir }}/plugins/miroir.home-operations.com
            type: DirectoryOrCreate
        - name: registration
          hostPath:
            path: {{ .Values.agent.kubeletDir }}/plugins_registry
            type: Directory
        - name: kubelet
          hostPath:
            path: {{ .Values.agent.kubeletDir }}
            type: Directory
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
        - name: drbd-cfg
          hostPath:
            path: /var/lib/miroir-drbd.d
            type: DirectoryOrCreate
        - name: drbd-global-conf
          configMap:
            name: {{ include "miroir.drbdConfigName" . }}
            items:
              - key: global_common.conf
                path: global_common.conf
{{- range $i, $dir := $loopDirs }}
        - name: loopfile-base-{{ $i }}
          hostPath:
            path: {{ $dir }}
            type: DirectoryOrCreate
{{- end }}