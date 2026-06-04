apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: homefs-agent
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/name: homefs
    app.kubernetes.io/component: agent
spec:
  selector:
    matchLabels:
      app.kubernetes.io/name: homefs
      app.kubernetes.io/component: agent
  template:
    metadata:
      labels:
        app.kubernetes.io/name: homefs
        app.kubernetes.io/component: agent
    spec:
      serviceAccountName: homefs-agent
      # hostNetwork so the pod IP is the node IP and the container
      # hostname matches the node — DRBD peers dial the host IP and
      # match `on <hostname>` blocks; agent ports must be host-unique.
      hostNetwork: true
      dnsPolicy: ClusterFirstWithHostNet
      # drbdadm/drbdsetup need the host PID namespace for /proc access
      # to the kernel module's worker threads.
      hostPID: true
      # No teardown hooks: kernel-side storage state is host-scoped and
      # survives the pod; reconcile converges on next start. A short
      # grace period keeps DaemonSet rollouts unblocked.
      terminationGracePeriodSeconds: 10
      tolerations:
        - operator: Exists # CSI node service must run on every schedulable node
      containers:
        - name: agent
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          args:
            - --mode=agent
            - --csi-socket=/csi/csi.sock
            - --nodes-config=/etc/homefs/nodes.yaml
            - --metrics-bind-address=:9810
            - --health-probe-bind-address=:9811
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
          securityContext:
            privileged: true
          ports:
            - name: healthz
              containerPort: 9811
          livenessProbe:
            httpGet: { path: /healthz, port: healthz }
            initialDelaySeconds: 15
            periodSeconds: 20
          readinessProbe:
            httpGet: { path: /readyz, port: healthz }
            initialDelaySeconds: 5
            periodSeconds: 10
          resources: {{- toYaml .Values.agent.resources | nindent 12 }}
          volumeMounts:
            - name: socket-dir
              mountPath: /csi
            - name: nodes
              mountPath: /etc/homefs
              readOnly: true
            - name: kubelet
              mountPath: {{ .Values.kubeletDir }}
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
        - name: node-driver-registrar
          image: {{ .Values.sidecars.registrar.image }}
          args:
            - --csi-address=/csi/csi.sock
            - --kubelet-registration-path={{ .Values.kubeletDir }}/plugins/homefs.io/csi.sock
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
            name: homefs-nodes
        - name: socket-dir
          hostPath:
            path: {{ .Values.kubeletDir }}/plugins/homefs.io
            type: DirectoryOrCreate
        - name: registration
          hostPath:
            path: {{ .Values.kubeletDir }}/plugins_registry
            type: Directory
        - name: kubelet
          hostPath:
            path: {{ .Values.kubeletDir }}
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
            path: /var/lib/homefs-drbd.d
            type: DirectoryOrCreate
        - name: drbd-global-conf
          configMap:
            name: homefs-drbd-conf
            items:
              - key: global_common.conf
                path: global_common.conf
