# Uninstall

```bash
helm uninstall miroir -n miroir-system
```

A pre-delete job deletes every MiroirVolume and MiroirSnapshot and
waits while each node's agent tears down its DRBD resources and
backing devices through the finalizers. If a teardown cannot finish
(a node is down), the job blocks: `kubectl get miroirvolumes` shows
what is stuck, and the agent log on the affected node
(`kubectl logs -n miroir-system -l app.kubernetes.io/component=agent`)
shows the failing call to clean up manually.
