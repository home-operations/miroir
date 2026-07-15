# Coexistence with other provisioners

- **OpenEBS LocalPV-ZFS**: keep your pool and let `openebs-zfs` stay
  the default StorageClass. miroir scopes itself to the dataset you
  configure in Helm values.
- **Other LVM tenants**: bound the thin pool with
  `nodes.<node>.pools.<pool>.thinPoolSize` (e.g. `400g`) and let the
  co-tenant allocate from the VG's remainder.
- **Rook/Ceph**: miroir's default DRBD port base (7000) collides with
  the Ceph mgr dashboard's non-SSL default on host-network clusters.
  Set `drbd.portBase` in Helm values to move miroir's range, or move
  the dashboard (`cephClusterSpec.dashboard.port`). See
  [Troubleshooting](troubleshooting.md).
