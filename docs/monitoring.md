# Monitoring

Volume health in miroir is reported per node: each agent exports
what its own leg of every volume sees, so a problem shows up as a
metric on the node that has it (and the controller adds the few
cluster-level signals the agents can't know, like RWX gateway
health).

`monitoring.podMonitor.enabled: true` creates a Prometheus Operator
PodMonitor scraping the controller **and every agent** on their
`metrics` ports (the per-volume gauges are exported by the agent on
each storage node; a `node` label is added to every series). The
diskful per-volume gauges also carry a `pool` label naming the pool
backing that node's leg — pools are per-node, so two legs of one
volume can report different pools — which lets you scope volume
health to a pool (the shipped dashboard's `pool` variable does
exactly that). `miroir_volume_diskless_primary` is the exception: a
diskless leg holds no backing device in any pool, so it stays
volume-only. The agent exports, per volume on that node:

| Metric                                        | Meaning                                                                                                                                   |
| --------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `miroir_volume_up_to_date`                    | 1 when this node's replica is UpToDate (unreplicated volumes are always 1 once created)                                                   |
| `miroir_volume_connected`                     | 1 when all replication links to diskful peers are established (tie-breaker links excluded)                                                |
| `miroir_volume_split_brain`                   | 1 when DRBD refused to reconnect after divergence; manual resolution required                                                             |
| `miroir_volume_suspended`                     | 1 while the snapshot write barrier freezes IO; sustained means a stranded barrier                                                         |
| `miroir_volume_resync_ratio`                  | fraction (0-1) in sync of the least-synced diskful peer; 1 when fully in sync                                                             |
| `miroir_volume_quorum`                        | 0 while a `freeze` volume has lost quorum and refuses writes, the "workloads are failing I/O" signal (always 1 under `last-man-standing`) |
| `miroir_volume_disk_failed`                   | 1 when this leg's disk was detached after an I/O error and latched failed; replace the disk, then remove and re-add the replica           |
| `miroir_volume_out_of_sync_bytes`             | worst per-peer out-of-sync bytes: the exposure if the healthiest peer is lost; also counts online-verify findings                         |
| `miroir_volume_primary`                       | 1 while this node's diskful leg is Primary: the consumer pod or the RWX gateway runs here and this leg serves the I/O                     |
| `miroir_volume_diskless_primary`              | 1 while a diskless leg (client or tie-breaker) is Primary here: the consumer pays network I/O; see auto-diskful                           |
| `miroir_volume_verify_last_timestamp_seconds` | unix time of the last completed scheduled verify; alert on staleness to catch a schedule that stopped firing                              |
| `miroir_volume_verify_out_of_sync_bytes`      | out-of-sync bytes the last scheduled verify found (0 = clean)                                                                             |

Each agent additionally exports its pool capacities
(`miroir_pool_capacity_bytes` / `miroir_pool_allocated_bytes` /
`miroir_pool_meta_used_ratio`, one series per named pool via the
`pool` label), the same sample that feeds capacity-aware placement
and the `PoolUsageHigh` condition, so pool exhaustion is alertable —
and two pools on one node stay distinguishable — not just an Event. It also exports
`miroir_node_drbd_kernel_info` (always 1, `version` label): the DRBD
kernel module version probed at startup, from client-only nodes too
(which have no `MiroirNode` status). Query it for fleet version skew
before a release raises the kernel floor.

For RWX volumes the **controller** exports `miroir_export_ready`: 1
while the volume's NFS gateway is serving (gateway pod available,
export address published). This is the signal the per-volume gauges
cannot give you: DRBD replicas stay healthy while a dead gateway
leaves every NFS client hanging.

Each **gateway** pod additionally serves its own metrics endpoint
(scraped by a second PodMonitor, with `node` and `volume` labels):
`miroir_gateway_nfs_healthy` is the result of the last liveness
probe's NFS NULL call against the pod's local ganesha. The same
probe backs the pod's `/healthz`, so a ganesha that still accepts
TCP connections but has stopped answering NFS fails liveness and is
restarted — previously that failure mode was invisible.

Prometheus is not the only surface. Volume health also flows through
the CSI `VolumeCondition`: enable `sidecars.healthMonitor.enabled`
and split-brain, failed-disk, and degraded volumes surface as events
on their PVCs (`kubectl describe pvc`).

## Starter alerts and dashboard

`monitoring.prometheusRule.enabled: true` ships starter alerts for
all of the above (split-brain, quorum lost, stranded barrier, disk
failed, degraded replication, sustained out-of-sync, an unavailable
RWX export, a stale verify schedule, pool and thin-metadata usage,
and a down agent — a node whose agent stops answering scrapes loses
every `miroir_*` series, so none of the per-volume alerts can fire
for it; the kernel-floor refusal to start looks exactly like this),
and `monitoring.dashboards.enabled: true` installs a Grafana
dashboard, either a sidecar-labelled ConfigMap or a grafana-operator
`GrafanaDashboard` CR via `monitoring.dashboards.grafanaOperator`.

The per-volume alerts inherit the `pool` label and name the pool in
their summaries, so Alertmanager routes and silences can target a
single pool. The dashboard's `pool` variable defaults to **All**;
narrowing it filters the volume-health and pool panels together.
