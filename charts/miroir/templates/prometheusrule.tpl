{{- /* Per-rule labels: severity, then the user's additionalRuleLabels
(severity wins), then the per-alert override labels (which win over both,
so an override can reclassify one alert's severity). */}}
{{- define "miroir.alertRuleLabels" -}}
{{- $labels := dict "severity" .severity -}}
{{- with .root.Values.monitoring.prometheusRule.additionalRuleLabels -}}
{{- $labels = merge $labels . -}}
{{- end -}}
{{- $ov := default dict (get .root.Values.monitoring.prometheusRule.overrides .alert) -}}
{{- $labels = mergeOverwrite $labels (default dict (get $ov "labels")) -}}
{{- toYaml $labels -}}
{{- end -}}
{{- /* Empty when the alert is dropped via overrides.<alert>.disabled. */}}
{{- define "miroir.alertEnabled" -}}
{{- $ov := default dict (get .root.Values.monitoring.prometheusRule.overrides .alert) -}}
{{- if not (get $ov "disabled") }}true{{- end -}}
{{- end -}}
{{- /* The rule's wait period: the per-alert override, else the default. */}}
{{- define "miroir.alertRuleFor" -}}
{{- $ov := default dict (get .root.Values.monitoring.prometheusRule.overrides .alert) -}}
{{- default (.for | default "") (get $ov "for") -}}
{{- end -}}
{{- /* Non-empty when at least one of .alerts survives the overrides —
a rule group must not render empty. */}}
{{- define "miroir.anyAlertEnabled" -}}
{{- $root := .root -}}
{{- range .alerts -}}
{{- include "miroir.alertEnabled" (dict "root" $root "alert" .) -}}
{{- end -}}
{{- end -}}
{{- if .Values.monitoring.prometheusRule.enabled }}
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: {{ include "miroir.fullname" . }}
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "miroir.labels" . | nindent 4 }}
    {{- with .Values.monitoring.prometheusRule.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with .Values.monitoring.prometheusRule.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  groups:
    {{- /* Keep in sync with the rules below. */}}
    {{- $volumeAlerts := list "MiroirVolumeSplitBrain" "MiroirVolumeQuorumLost" "MiroirVolumeSuspendedBarrier" "MiroirVolumeTeardownWedged" "MiroirVolumeDiskFailed" "MiroirVolumeNotUpToDate" "MiroirVolumeDisconnected" "MiroirVolumeOutOfSync" "MiroirVolumeRemoteConsumer" }}
    {{- if .Values.drbd.verify.schedule }}
    {{- $volumeAlerts = append $volumeAlerts "MiroirVolumeVerifyStale" }}
    {{- end }}
    {{- if include "miroir.anyAlertEnabled" (dict "root" $ "alerts" $volumeAlerts) }}
    - name: miroir.volumes
      rules:
        {{- $rule := dict "root" $ "alert" "MiroirVolumeSplitBrain" "severity" "critical" "for" "1m" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirVolumeSplitBrain
          expr: miroir_volume_split_brain == 1
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }}
              (pool {{ "{{" }} $labels.pool {{ "}}" }}) is split-brain on
              {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              DRBD detected divergent data and refused to reconnect. Resolve
              manually: run drbdadm connect --discard-my-data on the losing
              node (its writes since the split are lost).
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}

        {{- $rule = dict "root" $ "alert" "MiroirVolumeQuorumLost" "severity" "critical" "for" "2m" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirVolumeQuorumLost
          expr: miroir_volume_quorum == 0
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }}
              (pool {{ "{{" }} $labels.pool {{ "}}" }}) lost quorum on
              {{ "{{" }} $labels.node {{ "}}" }} — writes are failing
            description: >-
              The replica partition no longer holds a quorum majority and
              DRBD is returning I/O errors for this volume's writes; the
              filesystem on top has likely gone read-only. Restore
              connectivity to the peers (or the tie-breaker), then restart
              affected pods.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}

        {{- $rule = dict "root" $ "alert" "MiroirVolumeSuspendedBarrier" "severity" "critical" "for" "10m" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirVolumeSuspendedBarrier
          expr: miroir_volume_suspended == 1
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }}
              (pool {{ "{{" }} $labels.pool {{ "}}" }}) IO has been
              suspended for 10m on {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              A snapshot write barrier (suspend-io) has been held far longer
              than a snapshot round takes — likely a stranded barrier. The
              agent lifts stale barriers on restart; check agent logs and
              the MiroirSnapshot status.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}

        {{- $rule = dict "root" $ "alert" "MiroirVolumeTeardownWedged" "severity" "critical" "for" "1m" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirVolumeTeardownWedged
          expr: miroir_volume_wedged == 1
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }} teardown is wedged
              in the DRBD kernel module on {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              The kernel can no longer tear down this volume's DRBD resource
              (device stuck Detaching after a refcount underflow,
              LINBIT/drbd#137). The agent parked the teardown at a slow
              retry; reboot the node to clear the kernel state.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}

        {{- $rule = dict "root" $ "alert" "MiroirVolumeDiskFailed" "severity" "warning" "for" "5m" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirVolumeDiskFailed
          expr: miroir_volume_disk_failed == 1
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Backing disk failed for volume
              {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }}
              (pool {{ "{{" }} $labels.pool {{ "}}" }}) on
              {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              The backing device was detached after an I/O error and is
              latched failed; the volume serves via its peer. Replace the
              disk, then remove and re-add this replica to rebuild it.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}

        {{- $rule = dict "root" $ "alert" "MiroirVolumeNotUpToDate" "severity" "warning" "for" "15m" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirVolumeNotUpToDate
          expr: miroir_volume_up_to_date == 0
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Replica of {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }}
              (pool {{ "{{" }} $labels.pool {{ "}}" }}) on
              {{ "{{" }} $labels.node {{ "}}" }} is not UpToDate
            description: >-
              The leg has not returned to UpToDate within 15 minutes —
              a resync may be stuck or the disk detached. Check
              kubectl describe miroirvolume {{ "{{" }} $labels.volume {{ "}}" }}.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}

        {{- $rule = dict "root" $ "alert" "MiroirVolumeDisconnected" "severity" "warning" "for" "10m" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirVolumeDisconnected
          expr: miroir_volume_connected == 0
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }}
              (pool {{ "{{" }} $labels.pool {{ "}}" }}) replication links
              down from {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              Not all replication links to diskful peers are established;
              writes are not being replicated to the disconnected peers.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}

        {{- $rule = dict "root" $ "alert" "MiroirVolumeOutOfSync" "severity" "warning" "for" "1h" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirVolumeOutOfSync
          expr: miroir_volume_out_of_sync_bytes > 0
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }}
              (pool {{ "{{" }} $labels.pool {{ "}}" }}) has
              {{ "{{" }} $value | humanize1024 {{ "}}" }}B out of sync on
              {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              A peer has been out of sync for over an hour without a resync
              draining it — a down peer accumulating exposure, a stalled
              resync, or blocks flagged by drbdadm verify. A verify finding
              needs a disconnect/connect cycle to resync.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}

        {{- $rule = dict "root" $ "alert" "MiroirVolumeRemoteConsumer" "severity" "info" "for" "30m" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirVolumeRemoteConsumer
          expr: miroir_volume_diskless_primary == 1
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }} is consumed remotely
              from {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              The pod runs on a diskless DRBD leg at replication-network
              speed. Set autoDiskfulAfter to convert settled consumers to a
              local replica, pin the workload to a replica node, or accept
              the network path.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}
        {{- $rule = dict "root" $ "alert" "MiroirVolumeVerifyStale" "severity" "warning" }}
        {{- if and .Values.drbd.verify.schedule (include "miroir.alertEnabled" $rule) }}

        - alert: MiroirVolumeVerifyStale
          expr: >-
            time() - miroir_volume_verify_last_timestamp_seconds
            > {{ .Values.monitoring.prometheusRule.verifyStaleDays }} * 86400
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }}
              (pool {{ "{{" }} $labels.pool {{ "}}" }}) has not completed
              an online verify in {{ "{{" }} $value | humanizeDuration {{ "}}" }}
            description: >-
              The scheduled drbdadm verify has not completed within the
              expected window — the coordinating agent may have been down
              over the cron slot, verify may be suspended, or a pass is
              wedged. Check the agent logs on the volume's first diskful
              replica node.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}
    {{- end }}

    {{- if include "miroir.anyAlertEnabled" (dict "root" $ "alerts" (list "MiroirPoolUsageHigh" "MiroirPoolMetaUsageHigh")) }}
    - name: miroir.pools
      rules:
        {{- $rule := dict "root" $ "alert" "MiroirPoolUsageHigh" "severity" "warning" "for" "15m" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirPoolUsageHigh
          expr: miroir_pool_allocated_bytes / miroir_pool_capacity_bytes > 0.80
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Storage pool {{ "{{" }} $labels.pool {{ "}}" }} on
              {{ "{{" }} $labels.node {{ "}}" }} is
              {{ "{{" }} $value | humanizePercentage {{ "}}" }} full
            description: >-
              Pool usage crossed 80%. Thin-provisioned pools fail writes when
              exhausted and ZFS performance degrades past ~85% — expand the
              pool or move volumes off this node.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}

        {{- $rule = dict "root" $ "alert" "MiroirPoolMetaUsageHigh" "severity" "warning" "for" "15m" }}
        {{- if include "miroir.alertEnabled" $rule }}
        - alert: MiroirPoolMetaUsageHigh
          expr: miroir_pool_meta_used_ratio > 0.80
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              Thin-pool metadata of pool {{ "{{" }} $labels.pool {{ "}}" }} on
              {{ "{{" }} $labels.node {{ "}}" }} is
              {{ "{{" }} $value | humanizePercentage {{ "}}" }} used
            description: >-
              dm-thin metadata usage crossed 80%; exhausting it corrupts the
              pool even while data space remains. Extend the metadata LV
              (lvextend --poolmetadatasize).
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
        {{- end }}
    {{- end }}

    {{- $rule := dict "root" $ "alert" "MiroirExportUnavailable" "severity" "critical" "for" "5m" }}
    {{- if and .Values.gateway.enabled (include "miroir.alertEnabled" $rule) }}
    - name: miroir.exports
      rules:
        - alert: MiroirExportUnavailable
          expr: miroir_export_ready == 0
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              RWX export for volume {{ "{{" }} $labels.pvc_namespace {{ "}}" }}/{{ "{{" }} $labels.pvc {{ "}}" }}
              is unavailable — NFS clients are hanging
            description: >-
              The NFS gateway has no available pod (or no published export
              address). Check the miroir-share-{{ "{{" }} $labels.volume {{ "}}" }}
              Deployment: a failover stuck this long usually means no
              schedulable replica node or DRBD refusing promotion.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
    {{- end }}

    {{- $rule = dict "root" $ "alert" "MiroirAgentDown" "severity" "critical" "for" "10m" }}
    {{- if include "miroir.alertEnabled" $rule }}
    - name: miroir.agents
      rules:
        - alert: MiroirAgentDown
          expr: up{container="agent", namespace="{{ .Release.Namespace }}"} == 0
          {{- with include "miroir.alertRuleFor" $rule }}
          for: {{ . }}
          {{- end }}
          labels:
            {{- include "miroir.alertRuleLabels" $rule | nindent 12 }}
          annotations:
            summary: >-
              miroir agent on {{ "{{" }} $labels.node {{ "}}" }} is down —
              its volumes are unmonitored
            description: >-
              The agent pod has not answered scrapes for 10 minutes: every
              miroir_volume_* series from this node is absent (no alert on
              them can fire) and CSI mount/unmount on the node is stalled.
              A crash-loop right after a node change usually means the DRBD
              kernel module is below the agent's floor or the backend is
              misconfigured — check the agent logs.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
    {{- end }}
{{- end }}
