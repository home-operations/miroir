# Uninstall

```bash
helm uninstall miroir -n miroir-system
```

By default this removes the miroir workloads but **keeps every volume**:
the MiroirVolume and MiroirSnapshot objects, the DRBD resources, and the
backing devices on the nodes are all left in place, and the CRDs (installed
from the chart's `crds/` directory) survive the uninstall too. Mounted
volumes keep serving I/O without the agents; a later reinstall re-adopts
everything as it was.

## Destroying the data

To delete every volume as part of the uninstall, arm the pre-delete hook
first. Helm bakes hooks into the release at install/upgrade time, so the
confirmation must be set **before** running `helm uninstall`:

```bash
helm upgrade miroir oci://ghcr.io/home-operations/charts/miroir \
  -n miroir-system --reuse-values \
  --set uninstall.confirmation=yes-really-destroy-data
helm uninstall miroir -n miroir-system
```

The hook Job deletes every MiroirVolume and MiroirSnapshot and waits while
each node's agent tears down its DRBD resources and backing devices through
the finalizers — including volumes whose PV reclaimPolicy is `Retain`. If a
teardown cannot finish (a node is down), the job blocks:
`kubectl get miroirvolumes` shows what is stuck, and the agent log on the
affected node
(`kubectl logs -n miroir-system -l app.kubernetes.io/component=agent`)
shows the failing call to clean up manually.
