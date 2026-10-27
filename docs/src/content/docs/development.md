---
title: Development
---

Planned work lives in the
[issue tracker](https://github.com/home-operations/miroir/issues).

Tooling is pinned with [mise](https://mise.jdx.dev); the everyday
tasks:

```bash
mise run test              # unit tests (regenerates manifests first)
mise run test-integration  # envtest apiserver: CRD schema + CEL rules
mise run test-sanity       # upstream csi-test sanity suite, in-process
mise run lint          # golangci-lint
mise run build         # bin/miroir
mise run manifests     # CRD + RBAC generation
mise run helm-test     # helm-unittest against the chart
mise run docs-serve    # this docs site, live-reloading
```

The end-to-end suite runs against a real Talos cluster booted under QEMU by
[talosctl-cluster-action](https://github.com/home-operations/talosctl-cluster-action);
CI runs it on every PR, and `test/e2e-qemu/README.md` documents how to run it locally.

The docs site is Astro Starlight: pages live under
`docs/src/content/docs/`, the sidebar lives in `docs/astro.config.mjs`,
and `mise run docs` builds the site, failing on a broken internal
link. CI runs the same build on every PR.
