# Changelog

## [0.2.2](https://github.com/home-operations/miroir/compare/0.2.1...0.2.2) (2026-06-21)


### Bug Fixes

* **csi:** treat Degraded as provisioned so large replicated volumes bind ([4b38e97](https://github.com/home-operations/miroir/commit/4b38e9775f4b203cf9a0d115f9d77a2f66914014))


### Miscellaneous Chores

* **mise:** Update tool zizmor (1.25.2 → 1.26.1) ([#29](https://github.com/home-operations/miroir/issues/29)) ([11d2527](https://github.com/home-operations/miroir/commit/11d25276847e74bd20efd7248c8d93757671c058))

## [0.2.1](https://github.com/home-operations/miroir/compare/0.2.0...0.2.1) (2026-06-20)


### Features

* **chart:** pin image to chart version with digest override ([f833f0e](https://github.com/home-operations/miroir/commit/f833f0e9372082ed4c8fb851b28d920dfce3bcb3))


### Bug Fixes

* **agent:** defer DRBD resize while a peer is resyncing ([223f71e](https://github.com/home-operations/miroir/commit/223f71e551b8aff3fe8b1cb75fa03a7ac3abf26a))

## [0.2.0](https://github.com/home-operations/miroir/compare/0.1.2...0.2.0) (2026-06-20)


### ⚠ BREAKING CHANGES

* rename CRD group and CSI driver to miroir.home-operations.com ([#26](https://github.com/home-operations/miroir/issues/26))

### Features

* add loopfile backend storing volumes as loop-backed sparse files ([0c8bd92](https://github.com/home-operations/miroir/commit/0c8bd9232f158382b64b99bfa55c68fb78deb9da))
* capacity-aware placement via MiroirNode pool stats ([#21](https://github.com/home-operations/miroir/issues/21)) ([b5aa79c](https://github.com/home-operations/miroir/commit/b5aa79c3bc1da3200bbb967a4f5bf5717020326a))
* **container:** update image alpine (3.23 → 3.24) ([#7](https://github.com/home-operations/miroir/issues/7)) ([96e7877](https://github.com/home-operations/miroir/commit/96e7877d5c8234363bf0d8e15d689c7806d0cea3))
* **container:** update image registry.k8s.io/kubectl (v1.31.0 → v1.36.2) ([#22](https://github.com/home-operations/miroir/issues/22)) ([5243893](https://github.com/home-operations/miroir/commit/52438931783be31a6ee3420e0d9c800875d3f3dd))
* **container:** update image registry.k8s.io/sig-storage/csi-snapshotter (v8.5.0 → v8.6.0) ([#10](https://github.com/home-operations/miroir/issues/10)) ([566a754](https://github.com/home-operations/miroir/commit/566a754433f9d77321697e97f48246bbbad0d6d2))
* **deps:** update module github.com/onsi/ginkgo/v2 (v2.27.4 → v2.31.0) ([#23](https://github.com/home-operations/miroir/issues/23)) ([04d247c](https://github.com/home-operations/miroir/commit/04d247c5badf3fde6f7215574cf2181dc9d7a42e))
* **deps:** update module github.com/onsi/gomega (v1.40.0 → v1.42.0) ([#24](https://github.com/home-operations/miroir/issues/24)) ([8e52fc8](https://github.com/home-operations/miroir/commit/8e52fc82d966a10897320734a91cb9f4454f8f96))
* drbd based CSI ([1002809](https://github.com/home-operations/miroir/commit/1002809fa94ab54dcb7e442229503f23e757f885))
* DRBD synchronous replication ([52ceb00](https://github.com/home-operations/miroir/commit/52ceb00c20683043e09124e820d1c858aeb60cb8))
* name the metrics ports for scrape discovery ([f1a0390](https://github.com/home-operations/miroir/commit/f1a039037d8747d26a0978866a8a169e10a20902))
* publish chart and image on every main push ([c5cb86c](https://github.com/home-operations/miroir/commit/c5cb86cd0544d39230bcf437a8176fc02ed1bcc9))
* reconcile replica membership edits on live volumes ([3b46b1e](https://github.com/home-operations/miroir/commit/3b46b1e412cea09a3925fe33d07b70dc2f52248e))
* rename CRD group and CSI driver to miroir.home-operations.com ([#26](https://github.com/home-operations/miroir/issues/26)) ([967afca](https://github.com/home-operations/miroir/commit/967afca030bfe3730ffacae37f05fd636c7f91ab))
* snapshots, restore, and online expansion ([3b520d9](https://github.com/home-operations/miroir/commit/3b520d974d3aff3db0e252c07cbdb5e3caa18d53))


### Bug Fixes

* **agent:** recover replica backing when source snapshot is deleted ([#14](https://github.com/home-operations/miroir/issues/14)) ([5175955](https://github.com/home-operations/miroir/commit/5175955e99065dcdd6eb5bf2b8678e42ea2d8e37))
* align pinned Go toolchain with go.mod requirement ([c7adbb2](https://github.com/home-operations/miroir/commit/c7adbb229074958a7b85d8cfe1e6a157d91c7247))
* **chart:** use maintained kubectl image for the uninstall hook ([#18](https://github.com/home-operations/miroir/issues/18)) ([62f34cc](https://github.com/home-operations/miroir/commit/62f34ccbeff7d26c0fc9ee498166d92d6033d28e))
* correct registry and module paths to eleboucher ([b676cba](https://github.com/home-operations/miroir/commit/b676cbad4c1b1d42245555aa2e4b6c643533d2f8))
* crash-safe GI seeding and well-defined drbdmeta addressing ([3ec244d](https://github.com/home-operations/miroir/commit/3ec244d0daa6aeb16ea5fba90b5a1f6a7802045f))
* drop per-command --noudevsync, rely on lvmlocal.conf ([80084cb](https://github.com/home-operations/miroir/commit/80084cb6112363dcc45e5d1cf63a40274095b3e0))
* echo ContentSource on snapshot-restore CreateVolume ([b48da8a](https://github.com/home-operations/miroir/commit/b48da8a39b0c7c6de7c58faed9c566c747c4eeac))
* end-to-end flow review findings ([3d072e9](https://github.com/home-operations/miroir/commit/3d072e981a709edf55c0283db52fdc8af746ac08))
* goconst findings from the pinned linter version ([8b3d6bf](https://github.com/home-operations/miroir/commit/8b3d6bfb8bfd0b9cfce93a971d7f48cb3b123eff))
* keep snapshot LV names out of LVM's reserved namespace ([ca3183e](https://github.com/home-operations/miroir/commit/ca3183ee75eabec4adb3025b50c882cbdc7832d7))
* raise controller provision timeout to match csi-provisioner sidecar ([cd26cc9](https://github.com/home-operations/miroir/commit/cd26cc93a0a521961c9af8031820fad82ed9910e))
* replay the activity log before probing cloned metadata ([61d5f3f](https://github.com/home-operations/miroir/commit/61d5f3fa47133809f756bf3c79bfde0fabab513c))
* resolve ZFS clone dependencies and reactivate restored LVs ([636e784](https://github.com/home-operations/miroir/commit/636e78472b58f09411b7147a4b9dfaebb78a2434))
* route the snapshot write barrier through drbdadm ([39bc85a](https://github.com/home-operations/miroir/commit/39bc85a948ae3924992d67a69178eab82ac920f3))
* run go mod tidy ([40e78a9](https://github.com/home-operations/miroir/commit/40e78a9cf29935b640d1c12f3162dd053e2d6609))
* setup mode exits after pool ready, clear managedFields for SSA ([b68e848](https://github.com/home-operations/miroir/commit/b68e8482717f42b19cc8f857879d6e6686ce4f1c))
* snapshot flush barrier and real sidecar image tags ([b8bd894](https://github.com/home-operations/miroir/commit/b8bd89406ca2ce9b0fa82c331f5b2f02ce031dec))


### Documentation

* drop internal milestone references ([45fc4c5](https://github.com/home-operations/miroir/commit/45fc4c5b0ffc2a808be0fdccf9e5ad736e316975))


### Miscellaneous Chores

* **main:** release 0.1.0 ([#1](https://github.com/home-operations/miroir/issues/1)) ([8c71147](https://github.com/home-operations/miroir/commit/8c7114733a6e3afcdd4b72a2b5959dbb6fcb2e17))
* **main:** release 0.1.1 ([#16](https://github.com/home-operations/miroir/issues/16)) ([06516fb](https://github.com/home-operations/miroir/commit/06516fb5f7ee71442d117c75ec19060893fd030f))
* **main:** release 0.1.2 ([#19](https://github.com/home-operations/miroir/issues/19)) ([c4bfa3b](https://github.com/home-operations/miroir/commit/c4bfa3b78f57b4108db5c5f44ce4ae83fdcf56f6))
* **mise:** Update tool helm (4.2.0 → 4.2.2) ([#3](https://github.com/home-operations/miroir/issues/3)) ([ce0c92f](https://github.com/home-operations/miroir/commit/ce0c92f8e4c60269f7eea8aec29d06d3c78a3ba6))
* **mise:** Update tool opentofu (1.12.1 → 1.12.3) ([#4](https://github.com/home-operations/miroir/issues/4)) ([0ca3a0b](https://github.com/home-operations/miroir/commit/0ca3a0b8feb2b2586c55611bb94576821b992180))
* **mise:** Update tool oxfmt (0.54.0 → 0.55.0) ([#6](https://github.com/home-operations/miroir/issues/6)) ([368a779](https://github.com/home-operations/miroir/commit/368a779720965ab1536df97be26550f3e06c3e96))
* remove work-in-progress replication files committed by accident ([7e34490](https://github.com/home-operations/miroir/commit/7e34490d6ffb2379de8ac57b32ed501f3438d8d5))
* rename to home-operations/miroir ([76e06d2](https://github.com/home-operations/miroir/commit/76e06d26ad2889f109fe5833f0396853ad48cec6))
* update to reflect home-ops quality ([d426ff1](https://github.com/home-operations/miroir/commit/d426ff1767ee96af69e2eb303fbf603c4828d340))

## [0.1.2](https://github.com/home-operations/miroir/compare/0.1.1...0.1.2) (2026-06-19)


### Bug Fixes

* **chart:** use maintained kubectl image for the uninstall hook ([#18](https://github.com/home-operations/miroir/issues/18)) ([62f34cc](https://github.com/home-operations/miroir/commit/62f34ccbeff7d26c0fc9ee498166d92d6033d28e))

## [0.1.1](https://github.com/home-operations/miroir/compare/0.1.0...0.1.1) (2026-06-19)


### Features

* add loopfile backend storing volumes as loop-backed sparse files ([0c8bd92](https://github.com/home-operations/miroir/commit/0c8bd9232f158382b64b99bfa55c68fb78deb9da))
* drbd based CSI ([1002809](https://github.com/home-operations/miroir/commit/1002809fa94ab54dcb7e442229503f23e757f885))
* DRBD synchronous replication ([52ceb00](https://github.com/home-operations/miroir/commit/52ceb00c20683043e09124e820d1c858aeb60cb8))
* name the metrics ports for scrape discovery ([f1a0390](https://github.com/home-operations/miroir/commit/f1a039037d8747d26a0978866a8a169e10a20902))
* publish chart and image on every main push ([c5cb86c](https://github.com/home-operations/miroir/commit/c5cb86cd0544d39230bcf437a8176fc02ed1bcc9))
* reconcile replica membership edits on live volumes ([3b46b1e](https://github.com/home-operations/miroir/commit/3b46b1e412cea09a3925fe33d07b70dc2f52248e))
* snapshots, restore, and online expansion ([3b520d9](https://github.com/home-operations/miroir/commit/3b520d974d3aff3db0e252c07cbdb5e3caa18d53))


### Bug Fixes

* **agent:** recover replica backing when source snapshot is deleted ([#14](https://github.com/home-operations/miroir/issues/14)) ([5175955](https://github.com/home-operations/miroir/commit/5175955e99065dcdd6eb5bf2b8678e42ea2d8e37))
* align pinned Go toolchain with go.mod requirement ([c7adbb2](https://github.com/home-operations/miroir/commit/c7adbb229074958a7b85d8cfe1e6a157d91c7247))
* correct registry and module paths to eleboucher ([b676cba](https://github.com/home-operations/miroir/commit/b676cbad4c1b1d42245555aa2e4b6c643533d2f8))
* crash-safe GI seeding and well-defined drbdmeta addressing ([3ec244d](https://github.com/home-operations/miroir/commit/3ec244d0daa6aeb16ea5fba90b5a1f6a7802045f))
* drop per-command --noudevsync, rely on lvmlocal.conf ([80084cb](https://github.com/home-operations/miroir/commit/80084cb6112363dcc45e5d1cf63a40274095b3e0))
* echo ContentSource on snapshot-restore CreateVolume ([b48da8a](https://github.com/home-operations/miroir/commit/b48da8a39b0c7c6de7c58faed9c566c747c4eeac))
* end-to-end flow review findings ([3d072e9](https://github.com/home-operations/miroir/commit/3d072e981a709edf55c0283db52fdc8af746ac08))
* goconst findings from the pinned linter version ([8b3d6bf](https://github.com/home-operations/miroir/commit/8b3d6bfb8bfd0b9cfce93a971d7f48cb3b123eff))
* keep snapshot LV names out of LVM's reserved namespace ([ca3183e](https://github.com/home-operations/miroir/commit/ca3183ee75eabec4adb3025b50c882cbdc7832d7))
* raise controller provision timeout to match csi-provisioner sidecar ([cd26cc9](https://github.com/home-operations/miroir/commit/cd26cc93a0a521961c9af8031820fad82ed9910e))
* replay the activity log before probing cloned metadata ([61d5f3f](https://github.com/home-operations/miroir/commit/61d5f3fa47133809f756bf3c79bfde0fabab513c))
* resolve ZFS clone dependencies and reactivate restored LVs ([636e784](https://github.com/home-operations/miroir/commit/636e78472b58f09411b7147a4b9dfaebb78a2434))
* route the snapshot write barrier through drbdadm ([39bc85a](https://github.com/home-operations/miroir/commit/39bc85a948ae3924992d67a69178eab82ac920f3))
* run go mod tidy ([40e78a9](https://github.com/home-operations/miroir/commit/40e78a9cf29935b640d1c12f3162dd053e2d6609))
* setup mode exits after pool ready, clear managedFields for SSA ([b68e848](https://github.com/home-operations/miroir/commit/b68e8482717f42b19cc8f857879d6e6686ce4f1c))
* snapshot flush barrier and real sidecar image tags ([b8bd894](https://github.com/home-operations/miroir/commit/b8bd89406ca2ce9b0fa82c331f5b2f02ce031dec))


### Documentation

* drop internal milestone references ([45fc4c5](https://github.com/home-operations/miroir/commit/45fc4c5b0ffc2a808be0fdccf9e5ad736e316975))


### Miscellaneous Chores

* **main:** release 0.1.0 ([#1](https://github.com/home-operations/miroir/issues/1)) ([8c71147](https://github.com/home-operations/miroir/commit/8c7114733a6e3afcdd4b72a2b5959dbb6fcb2e17))
* **mise:** Update tool helm (4.2.0 → 4.2.2) ([#3](https://github.com/home-operations/miroir/issues/3)) ([ce0c92f](https://github.com/home-operations/miroir/commit/ce0c92f8e4c60269f7eea8aec29d06d3c78a3ba6))
* **mise:** Update tool oxfmt (0.54.0 → 0.55.0) ([#6](https://github.com/home-operations/miroir/issues/6)) ([368a779](https://github.com/home-operations/miroir/commit/368a779720965ab1536df97be26550f3e06c3e96))
* remove work-in-progress replication files committed by accident ([7e34490](https://github.com/home-operations/miroir/commit/7e34490d6ffb2379de8ac57b32ed501f3438d8d5))
* rename to home-operations/miroir ([76e06d2](https://github.com/home-operations/miroir/commit/76e06d26ad2889f109fe5833f0396853ad48cec6))
* update to reflect home-ops quality ([d426ff1](https://github.com/home-operations/miroir/commit/d426ff1767ee96af69e2eb303fbf603c4828d340))

## 0.1.0 (2026-06-19)


### Features

* add loopfile backend storing volumes as loop-backed sparse files ([0c8bd92](https://github.com/home-operations/miroir/commit/0c8bd9232f158382b64b99bfa55c68fb78deb9da))
* drbd based CSI ([1002809](https://github.com/home-operations/miroir/commit/1002809fa94ab54dcb7e442229503f23e757f885))
* DRBD synchronous replication ([52ceb00](https://github.com/home-operations/miroir/commit/52ceb00c20683043e09124e820d1c858aeb60cb8))
* name the metrics ports for scrape discovery ([f1a0390](https://github.com/home-operations/miroir/commit/f1a039037d8747d26a0978866a8a169e10a20902))
* publish chart and image on every main push ([c5cb86c](https://github.com/home-operations/miroir/commit/c5cb86cd0544d39230bcf437a8176fc02ed1bcc9))
* reconcile replica membership edits on live volumes ([3b46b1e](https://github.com/home-operations/miroir/commit/3b46b1e412cea09a3925fe33d07b70dc2f52248e))
* snapshots, restore, and online expansion ([3b520d9](https://github.com/home-operations/miroir/commit/3b520d974d3aff3db0e252c07cbdb5e3caa18d53))


### Bug Fixes

* **agent:** recover replica backing when source snapshot is deleted ([#14](https://github.com/home-operations/miroir/issues/14)) ([5175955](https://github.com/home-operations/miroir/commit/5175955e99065dcdd6eb5bf2b8678e42ea2d8e37))
* align pinned Go toolchain with go.mod requirement ([c7adbb2](https://github.com/home-operations/miroir/commit/c7adbb229074958a7b85d8cfe1e6a157d91c7247))
* correct registry and module paths to eleboucher ([b676cba](https://github.com/home-operations/miroir/commit/b676cbad4c1b1d42245555aa2e4b6c643533d2f8))
* crash-safe GI seeding and well-defined drbdmeta addressing ([3ec244d](https://github.com/home-operations/miroir/commit/3ec244d0daa6aeb16ea5fba90b5a1f6a7802045f))
* drop per-command --noudevsync, rely on lvmlocal.conf ([80084cb](https://github.com/home-operations/miroir/commit/80084cb6112363dcc45e5d1cf63a40274095b3e0))
* echo ContentSource on snapshot-restore CreateVolume ([b48da8a](https://github.com/home-operations/miroir/commit/b48da8a39b0c7c6de7c58faed9c566c747c4eeac))
* end-to-end flow review findings ([3d072e9](https://github.com/home-operations/miroir/commit/3d072e981a709edf55c0283db52fdc8af746ac08))
* goconst findings from the pinned linter version ([8b3d6bf](https://github.com/home-operations/miroir/commit/8b3d6bfb8bfd0b9cfce93a971d7f48cb3b123eff))
* keep snapshot LV names out of LVM's reserved namespace ([ca3183e](https://github.com/home-operations/miroir/commit/ca3183ee75eabec4adb3025b50c882cbdc7832d7))
* raise controller provision timeout to match csi-provisioner sidecar ([cd26cc9](https://github.com/home-operations/miroir/commit/cd26cc93a0a521961c9af8031820fad82ed9910e))
* replay the activity log before probing cloned metadata ([61d5f3f](https://github.com/home-operations/miroir/commit/61d5f3fa47133809f756bf3c79bfde0fabab513c))
* resolve ZFS clone dependencies and reactivate restored LVs ([636e784](https://github.com/home-operations/miroir/commit/636e78472b58f09411b7147a4b9dfaebb78a2434))
* route the snapshot write barrier through drbdadm ([39bc85a](https://github.com/home-operations/miroir/commit/39bc85a948ae3924992d67a69178eab82ac920f3))
* run go mod tidy ([40e78a9](https://github.com/home-operations/miroir/commit/40e78a9cf29935b640d1c12f3162dd053e2d6609))
* setup mode exits after pool ready, clear managedFields for SSA ([b68e848](https://github.com/home-operations/miroir/commit/b68e8482717f42b19cc8f857879d6e6686ce4f1c))
* snapshot flush barrier and real sidecar image tags ([b8bd894](https://github.com/home-operations/miroir/commit/b8bd89406ca2ce9b0fa82c331f5b2f02ce031dec))


### Documentation

* drop internal milestone references ([45fc4c5](https://github.com/home-operations/miroir/commit/45fc4c5b0ffc2a808be0fdccf9e5ad736e316975))


### Miscellaneous Chores

* **mise:** Update tool helm (4.2.0 → 4.2.2) ([#3](https://github.com/home-operations/miroir/issues/3)) ([ce0c92f](https://github.com/home-operations/miroir/commit/ce0c92f8e4c60269f7eea8aec29d06d3c78a3ba6))
* **mise:** Update tool oxfmt (0.54.0 → 0.55.0) ([#6](https://github.com/home-operations/miroir/issues/6)) ([368a779](https://github.com/home-operations/miroir/commit/368a779720965ab1536df97be26550f3e06c3e96))
* remove work-in-progress replication files committed by accident ([7e34490](https://github.com/home-operations/miroir/commit/7e34490d6ffb2379de8ac57b32ed501f3438d8d5))
* rename to home-operations/miroir ([76e06d2](https://github.com/home-operations/miroir/commit/76e06d26ad2889f109fe5833f0396853ad48cec6))
* update to reflect home-ops quality ([d426ff1](https://github.com/home-operations/miroir/commit/d426ff1767ee96af69e2eb303fbf603c4828d340))
