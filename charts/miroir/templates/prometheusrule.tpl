{{- /* Per-rule labels: severity plus the user's additionalRuleLabels. */}}
{{- define "miroir.alertRuleLabels" -}}
{{- $labels := dict "severity" .severity -}}
{{- with .root.Values.monitoring.prometheusRule.additionalRuleLabels -}}
{{- $labels = merge $labels . -}}
{{- end -}}
{{- toYaml $labels -}}
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
    - name: miroir.volumes
      rules:
        # DRBD refused to reconnect after divergence; data is forked and
        # only an operator can pick the losing side.
        - alert: MiroirVolumeSplitBrain
          expr: miroir_volume_split_brain == 1
          for: 1m
          labels:
            {{- include "miroir.alertRuleLabels" (dict "root" $ "severity" "critical") | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.volume {{ "}}" }} is split-brain on
              {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              DRBD detected divergent data and refused to reconnect. Resolve
              manually: run drbdadm connect --discard-my-data on the losing
              node (its writes since the split are lost).
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}

        # A freeze volume without quorum suspends IO: pods using it are
        # hanging right now, even though the local disk is healthy.
        - alert: MiroirVolumeQuorumLost
          expr: miroir_volume_quorum == 0
          for: 2m
          labels:
            {{- include "miroir.alertRuleLabels" (dict "root" $ "severity" "critical") | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.volume {{ "}}" }} lost quorum on
              {{ "{{" }} $labels.node {{ "}}" }} — IO is suspended
            description: >-
              The replica partition no longer holds a quorum majority and
              DRBD is freezing writes; workloads on this volume are hanging.
              Restore connectivity to the peers (or the tie-breaker) to
              resume IO.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}

        # Snapshot write barriers last seconds; a sustained suspend means a
        # stranded barrier freezing the workload.
        - alert: MiroirVolumeSuspendedBarrier
          expr: miroir_volume_suspended == 1
          for: 10m
          labels:
            {{- include "miroir.alertRuleLabels" (dict "root" $ "severity" "critical") | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.volume {{ "}}" }} IO has been
              suspended for 10m on {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              A snapshot write barrier (suspend-io) has been held far longer
              than a snapshot round takes — likely a stranded barrier. The
              agent lifts stale barriers on restart; check agent logs and
              the MiroirSnapshot status.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}

        # on-io-error detach latched a failing backing disk; serving
        # continues via the peer but redundancy is reduced until re-added.
        - alert: MiroirVolumeDiskFailed
          expr: miroir_volume_disk_failed == 1
          for: 5m
          labels:
            {{- include "miroir.alertRuleLabels" (dict "root" $ "severity" "warning") | nindent 12 }}
          annotations:
            summary: >-
              Backing disk failed for volume
              {{ "{{" }} $labels.volume {{ "}}" }} on
              {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              The backing device was detached after an I/O error and is
              latched failed; the volume serves via its peer. Replace the
              disk, then remove and re-add this replica to rebuild it.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}

        # A leg that stays out of UpToDate is running on reduced redundancy.
        - alert: MiroirVolumeNotUpToDate
          expr: miroir_volume_up_to_date == 0
          for: 15m
          labels:
            {{- include "miroir.alertRuleLabels" (dict "root" $ "severity" "warning") | nindent 12 }}
          annotations:
            summary: >-
              Replica of {{ "{{" }} $labels.volume {{ "}}" }} on
              {{ "{{" }} $labels.node {{ "}}" }} is not UpToDate
            description: >-
              The leg has not returned to UpToDate within 15 minutes —
              a resync may be stuck or the disk detached. Check
              kubectl describe miroirvolume {{ "{{" }} $labels.volume {{ "}}" }}.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}

        # Replication links to diskful peers are down.
        - alert: MiroirVolumeDisconnected
          expr: miroir_volume_connected == 0
          for: 10m
          labels:
            {{- include "miroir.alertRuleLabels" (dict "root" $ "severity" "warning") | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.volume {{ "}}" }} replication links
              down from {{ "{{" }} $labels.node {{ "}}" }}
            description: >-
              Not all replication links to diskful peers are established;
              writes are not being replicated to the disconnected peers.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}

        # Out-of-sync data that a resync does not drain within an hour is
        # stuck exposure — or online-verify found silent corruption.
        - alert: MiroirVolumeOutOfSync
          expr: miroir_volume_out_of_sync_bytes > 0
          for: 1h
          labels:
            {{- include "miroir.alertRuleLabels" (dict "root" $ "severity" "warning") | nindent 12 }}
          annotations:
            summary: >-
              Volume {{ "{{" }} $labels.volume {{ "}}" }} has
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

    - name: miroir.pools
      rules:
        # Matches the agent's PoolUsageHigh condition: thin pools and ZFS
        # degrade badly past ~85% full.
        - alert: MiroirPoolUsageHigh
          expr: miroir_pool_allocated_bytes / miroir_pool_capacity_bytes > 0.80
          for: 15m
          labels:
            {{- include "miroir.alertRuleLabels" (dict "root" $ "severity" "warning") | nindent 12 }}
          annotations:
            summary: >-
              Storage pool on {{ "{{" }} $labels.node {{ "}}" }} is
              {{ "{{" }} $value | humanizePercentage {{ "}}" }} full
            description: >-
              Pool usage crossed 80%. Thin-provisioned pools fail writes when
              exhausted and ZFS performance degrades past ~85% — expand the
              pool or move volumes off this node.
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}

        # dm-thin metadata exhaustion corrupts the pool even with data
        # space free.
        - alert: MiroirPoolMetaUsageHigh
          expr: miroir_pool_meta_used_ratio > 0.80
          for: 15m
          labels:
            {{- include "miroir.alertRuleLabels" (dict "root" $ "severity" "warning") | nindent 12 }}
          annotations:
            summary: >-
              Thin-pool metadata on {{ "{{" }} $labels.node {{ "}}" }} is
              {{ "{{" }} $value | humanizePercentage {{ "}}" }} used
            description: >-
              dm-thin metadata usage crossed 80%; exhausting it corrupts the
              pool even while data space remains. Extend the metadata LV
              (lvextend --poolmetadatasize).
            {{- with .Values.monitoring.prometheusRule.additionalRuleAnnotations }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
{{- end }}
