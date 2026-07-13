# Changelog

## [0.6.0](https://github.com/home-operations/miroir/compare/0.5.0...0.6.0) (2026-07-13)


### ⚠ BREAKING CHANGES

* consume replicated volumes from nodes without a replica (diskless client legs) ([#165](https://github.com/home-operations/miroir/issues/165))

### Features

* consume replicated volumes from nodes without a replica (diskless client legs) ([#165](https://github.com/home-operations/miroir/issues/165)) ([cc9df89](https://github.com/home-operations/miroir/commit/cc9df89d61b141810c12ec6ee0288453bbfe0d51))

## [0.5.0](https://github.com/home-operations/miroir/compare/0.4.14...0.5.0) (2026-07-13)


### ⚠ BREAKING CHANGES

* **chart:** unify storage & snapshot classes into arrays ([#177](https://github.com/home-operations/miroir/issues/177))

### Features

* **chart:** unify storage & snapshot classes into arrays ([#177](https://github.com/home-operations/miroir/issues/177)) ([2988ead](https://github.com/home-operations/miroir/commit/2988ead1e233112ce16373d0be7f431e648405a0))

## [0.4.14](https://github.com/home-operations/miroir/compare/0.4.13...0.4.14) (2026-07-13)


### Bug Fixes

* **controller:** scope gateway Deployment/Service informers to the namespace ([#174](https://github.com/home-operations/miroir/issues/174)) ([ba89f6a](https://github.com/home-operations/miroir/commit/ba89f6a67be50cf5a5e9d770a6c5e5bd471502ec))

## [0.4.13](https://github.com/home-operations/miroir/compare/0.4.12...0.4.13) (2026-07-13)


### Bug Fixes

* **csi:** seed Node objects in the RWX CreateVolume test ([#172](https://github.com/home-operations/miroir/issues/172)) ([a4d1b56](https://github.com/home-operations/miroir/commit/a4d1b566fb3ed4e69715ecf0380bd0fc4556206d))

## [0.4.12](https://github.com/home-operations/miroir/compare/0.4.11...0.4.12) (2026-07-13)


### Features

* enable RWX (ReadWriteMany) over the NFS gateway ([#164](https://github.com/home-operations/miroir/issues/164)) ([072e09c](https://github.com/home-operations/miroir/commit/072e09c97d8b8d016fd641977f4a3493dd4472a6))

## [0.4.11](https://github.com/home-operations/miroir/compare/0.4.10...0.4.11) (2026-07-13)


### Features

* RWX export reconciler — per-volume NFS gateway workloads ([#163](https://github.com/home-operations/miroir/issues/163)) ([47731f3](https://github.com/home-operations/miroir/commit/47731f3e91914894bd0753d5473b136ea7310366))
* RWX gateway runtime — NFS-Ganesha share manager ([#162](https://github.com/home-operations/miroir/issues/162)) ([766bfc1](https://github.com/home-operations/miroir/commit/766bfc1bbc024063ee32d5cda0a3f13f04425200))
* RWX groundwork: internal/stage extraction and export CRD types ([#161](https://github.com/home-operations/miroir/issues/161)) ([fbf123f](https://github.com/home-operations/miroir/commit/fbf123febe91ed01374c70129a3d66e351092fdf))

## [0.4.10](https://github.com/home-operations/miroir/compare/0.4.9...0.4.10) (2026-07-12)


### Features

* allow a per-node replication address override ([#155](https://github.com/home-operations/miroir/issues/155)) ([5dde99e](https://github.com/home-operations/miroir/commit/5dde99e210f7906dd225fc7a513cd145fcc5cfae))
* scheduled drbdadm verify with results in status and metrics ([#158](https://github.com/home-operations/miroir/issues/158)) ([16cc651](https://github.com/home-operations/miroir/commit/16cc6515a30f549cb26efe33fa9f9f609a1d5a72))


### Bug Fixes

* **agent:** discard verify results when a peer drops mid-pass ([#159](https://github.com/home-operations/miroir/issues/159)) ([d2f2e6f](https://github.com/home-operations/miroir/commit/d2f2e6f2862752bc44d0312fd8ab9a0a79e10bb4))
* **nodemap:** reject duplicate replication addresses at load ([#157](https://github.com/home-operations/miroir/issues/157)) ([4bc24b8](https://github.com/home-operations/miroir/commit/4bc24b80b3dc13340315caa3dbbff14e0cedeed1))

## [0.4.9](https://github.com/home-operations/miroir/compare/0.4.8...0.4.9) (2026-07-12)


### Bug Fixes

* **agent:** latch Activated from the live Primary role ([#151](https://github.com/home-operations/miroir/issues/151)) ([69707bb](https://github.com/home-operations/miroir/commit/69707bb47a2c7657b063d6f2fc7812e6ced64799))
* bound drbd-port-base at install and startup ([#152](https://github.com/home-operations/miroir/issues/152)) ([f96ab1a](https://github.com/home-operations/miroir/commit/f96ab1a88fdbcc4dd9f669be2d6acc4c6d0d020b))

## [0.4.8](https://github.com/home-operations/miroir/compare/0.4.7...0.4.8) (2026-07-12)


### Features

* make DRBD replication port base configurable ([#149](https://github.com/home-operations/miroir/issues/149)) ([74ff82e](https://github.com/home-operations/miroir/commit/74ff82eb66d725d30e343ab7296ad25dc4201dcb))

## [0.4.7](https://github.com/home-operations/miroir/compare/0.4.6...0.4.7) (2026-07-12)


### Bug Fixes

* trigger split-brain recovery on the losing leg via peer-reported state ([#145](https://github.com/home-operations/miroir/issues/145)) ([a81311e](https://github.com/home-operations/miroir/commit/a81311e2084a112412d5c0fb40d5089813ab276c))

## [0.4.6](https://github.com/home-operations/miroir/compare/0.4.5...0.4.6) (2026-07-11)


### Bug Fixes

* repair split-brain auto-recovery and activated-latch timing ([#141](https://github.com/home-operations/miroir/issues/141)) ([#142](https://github.com/home-operations/miroir/issues/142)) ([6f8f5e3](https://github.com/home-operations/miroir/commit/6f8f5e38f19c749be2db7adfd8910f399058a2e1))

## [0.4.5](https://github.com/home-operations/miroir/compare/0.4.4...0.4.5) (2026-07-11)


### Bug Fixes

* auto-recover split-brain on fresh replicated volumes ([e0a8cad](https://github.com/home-operations/miroir/commit/e0a8cad052bb40681b441502a0821fbafe76d116))


### Documentation

* **readme:** add loopfile node to quickstart topology example ([#137](https://github.com/home-operations/miroir/issues/137)) ([29fd1b8](https://github.com/home-operations/miroir/commit/29fd1b8c9e7a0d651fcff285ca6187b613cfc87f))

## [0.4.4](https://github.com/home-operations/miroir/compare/0.4.3...0.4.4) (2026-07-11)


### Features

* **controller:** optional leader election for HA replicas ([#133](https://github.com/home-operations/miroir/issues/133)) ([7d398c3](https://github.com/home-operations/miroir/commit/7d398c37903e6fce4d1382b9806b2b5d8645ebed))

## [0.4.3](https://github.com/home-operations/miroir/compare/0.4.2...0.4.3) (2026-07-10)


### Features

* **deps:** update module sigs.k8s.io/structured-merge-diff/v6 (v6.3.2 → v6.4.2) ([#130](https://github.com/home-operations/miroir/issues/130)) ([642925e](https://github.com/home-operations/miroir/commit/642925e65c9874838b3e9ffa47d575ca81a6614f))

## [0.4.2](https://github.com/home-operations/miroir/compare/0.4.1...0.4.2) (2026-07-10)


### Bug Fixes

* **agent:** gate DRBD EventWatcher + startup sweeps on drbdsetup presence ([#127](https://github.com/home-operations/miroir/issues/127)) ([#129](https://github.com/home-operations/miroir/issues/129)) ([e51d5e1](https://github.com/home-operations/miroir/commit/e51d5e11024f8e8ffb5bb416922ce074efc49fb5))
* opt out of the workqueue priority queue — starved initial-list events wedge volume realization ([#122](https://github.com/home-operations/miroir/issues/122)) ([ecc1675](https://github.com/home-operations/miroir/commit/ecc167526c71a6e32a91a4c8e81ed437b31312f3))


### Performance Improvements

* **agent:** concurrent volume workers + realized-state fast path ([#128](https://github.com/home-operations/miroir/issues/128)) ([b481a77](https://github.com/home-operations/miroir/commit/b481a77c6dbe805725a9b57364b00ce02b8fc105))
* strip managedFields from cached objects ([#126](https://github.com/home-operations/miroir/issues/126)) ([77f57bd](https://github.com/home-operations/miroir/commit/77f57bd4e678f81af1fd4e34329009f2db295e97))


### Code Refactoring

* migrate SSA status patches to typed apply configurations ([#127](https://github.com/home-operations/miroir/issues/127)) ([2644372](https://github.com/home-operations/miroir/commit/264437268106f689d054c49b16ce09fabb6670b8))

## [0.4.1](https://github.com/home-operations/miroir/compare/0.4.0...0.4.1) (2026-07-10)


### Features

* **agent:** auto rs-discard-granularity per leg ([#120](https://github.com/home-operations/miroir/issues/120)) ([48fb768](https://github.com/home-operations/miroir/commit/48fb768457839530d42d7099506ab63e73814bd3))

## [0.4.0](https://github.com/home-operations/miroir/compare/0.3.3...0.4.0) (2026-07-10)


### ⚠ BREAKING CHANGES

* split the image — distroless controller, Debian agent ([#118](https://github.com/home-operations/miroir/issues/118))

### Features

* split the image — distroless controller, Debian agent ([#118](https://github.com/home-operations/miroir/issues/118)) ([6fa1469](https://github.com/home-operations/miroir/commit/6fa1469c3050611d906e2a580e92a0dedd71497c))

## [0.3.3](https://github.com/home-operations/miroir/compare/0.3.2...0.3.3) (2026-07-10)


### Features

* **chart:** starter PrometheusRule alerts and Grafana dashboard ([#117](https://github.com/home-operations/miroir/issues/117)) ([bb1ec30](https://github.com/home-operations/miroir/commit/bb1ec3046175f5ca79b4f8d306b94a91049597ac))
* **metrics:** quorum, disk-failed, out-of-sync, and pool capacity gauges ([#116](https://github.com/home-operations/miroir/issues/116)) ([d02097e](https://github.com/home-operations/miroir/commit/d02097ef5e0ce4e744fc58388dfb609a729c92de))


### Bug Fixes

* **observability:** scrape agent metrics via PodMonitor; correct gauge accuracy ([#114](https://github.com/home-operations/miroir/issues/114)) ([edd19a6](https://github.com/home-operations/miroir/commit/edd19a656f5053db6b0b9d7adbf6c9965f9c828f))

## [0.3.2](https://github.com/home-operations/miroir/compare/0.3.1...0.3.2) (2026-07-10)


### Features

* **agent:** latch failed disks and skip re-attach ([#101](https://github.com/home-operations/miroir/issues/101)) ([#113](https://github.com/home-operations/miroir/issues/113)) ([f381e84](https://github.com/home-operations/miroir/commit/f381e845a01c127de24b374626e3230062cbad29))


### Bug Fixes

* parse replication-state from peer_devices; expose resync percent ([#111](https://github.com/home-operations/miroir/issues/111)) ([0ca4baf](https://github.com/home-operations/miroir/commit/0ca4bafc7c936370129c29a3c7d5715c95e4b315))

## [0.3.1](https://github.com/home-operations/miroir/compare/0.3.0...0.3.1) (2026-07-10)


### Features

* explain a detached backing disk in volume status ([#100](https://github.com/home-operations/miroir/issues/100)) ([cdca6ff](https://github.com/home-operations/miroir/commit/cdca6ff55c03186ea793e8cc16db845c71402b5e))
* tune chart defaults for redundancy-restore, integrity, and control-plane resilience ([#105](https://github.com/home-operations/miroir/issues/105)) ([10d769e](https://github.com/home-operations/miroir/commit/10d769eb4140cc9c6ed389e1ea93362e1a30efb6))


### Bug Fixes

* bound host commands, classify held-open teardown, self-heal stale metadata marker ([#98](https://github.com/home-operations/miroir/issues/98)) ([e32eed9](https://github.com/home-operations/miroir/commit/e32eed9a14010938006a39edc9ab238d44c67e9d))
* CSI restore + AlreadyExists idempotency edges ([#103](https://github.com/home-operations/miroir/issues/103)) ([49f7468](https://github.com/home-operations/miroir/commit/49f7468164ea419a80f783e39fcc8d9ea46f008d))
* release a volume's DRBD minor on teardown ([#109](https://github.com/home-operations/miroir/issues/109)) ([b5d6cdf](https://github.com/home-operations/miroir/commit/b5d6cdf4b6f7674c290dd0527341519018253795))
* snapshot coordinator fails over from a dead replicas[0] ([#99](https://github.com/home-operations/miroir/issues/99)) ([66b7eb9](https://github.com/home-operations/miroir/commit/66b7eb951a53838e60b6323dbf813cd144e204eb))


### Performance Improvements

* stop per-poll drbdadm resize and dedup CreateVolume's volume List ([#104](https://github.com/home-operations/miroir/issues/104)) ([6e01ea3](https://github.com/home-operations/miroir/commit/6e01ea317b04841d58a29fcd983723ac6743a235))


### Miscellaneous Chores

* **mise:** Update tool helm (4.2.2 → 4.2.3) ([#96](https://github.com/home-operations/miroir/issues/96)) ([15eee90](https://github.com/home-operations/miroir/commit/15eee9047874733c619bb1a75bdd0ce5b0a21167))

## [0.3.0](https://github.com/home-operations/miroir/compare/0.2.11...0.3.0) (2026-07-09)


### ⚠ BREAKING CHANGES

* overlap kind boot with the e2e image build ([#84](https://github.com/home-operations/miroir/issues/84))

### Features

* auto-place diskless tie-breakers and default quorum to freeze ([#81](https://github.com/home-operations/miroir/issues/81)) ([9439d2a](https://github.com/home-operations/miroir/commit/9439d2abb39a97b937d930f583ee4fc423c04b92))
* **drbd:** diskless tie-breaker for 2-replica quorum ([#70](https://github.com/home-operations/miroir/issues/70)) ([#74](https://github.com/home-operations/miroir/issues/74)) ([a9ed1fb](https://github.com/home-operations/miroir/commit/a9ed1fba5cb286c116b540a68bd252a840a0f623))
* opt-in rs-discard-granularity and verify-alg chart knobs ([#93](https://github.com/home-operations/miroir/issues/93)) ([16bbed8](https://github.com/home-operations/miroir/commit/16bbed867c32dc64f0af6f05895d794cd3a96ea7))


### Bug Fixes

* **agent:** emit the pool-usage Warning once per transition ([#80](https://github.com/home-operations/miroir/issues/80)) ([fe0812e](https://github.com/home-operations/miroir/commit/fe0812ed1ea36820d1dfa92917b6e640c30afcfd))
* **agent:** gate snapshots and removal on diskful peers only ([#78](https://github.com/home-operations/miroir/issues/78)) ([5fec68c](https://github.com/home-operations/miroir/commit/5fec68c83794515a427a81eb2c18ad9e27a1ded0))
* **cmd:** run the shutdown sweep on error exit, bound its budgets ([#79](https://github.com/home-operations/miroir/issues/79)) ([cb8367f](https://github.com/home-operations/miroir/commit/cb8367ff6b2d10e7f67ebae4423ba28baaa69894))
* drbd and backend robustness batch ([#90](https://github.com/home-operations/miroir/issues/90)) ([2bc5778](https://github.com/home-operations/miroir/commit/2bc5778d9e7b9264c37c36441484057e777cf1e3))
* **drbd:** harden the diskless tie-breaker ([#74](https://github.com/home-operations/miroir/issues/74) follow-up) ([#77](https://github.com/home-operations/miroir/issues/77)) ([a0d8c9d](https://github.com/home-operations/miroir/commit/a0d8c9dfc36bd09e16d7970223d9eb961c2c2ae9))
* expand retries wait for realization; restores clean replica entries ([#89](https://github.com/home-operations/miroir/issues/89)) ([b9c0afe](https://github.com/home-operations/miroir/commit/b9c0afea82f2bdb4d81331a52870f02b0b4577f2))
* guard the day0 re-seed after a node wipe; default on-io-error detach ([#88](https://github.com/home-operations/miroir/issues/88)) ([56a2a09](https://github.com/home-operations/miroir/commit/56a2a09ecf8db39ad4a378e4b6da50cdca754afd))
* pin spec.drbd presence with a CEL transition rule ([#87](https://github.com/home-operations/miroir/issues/87)) ([b0a7f7f](https://github.com/home-operations/miroir/commit/b0a7f7f9c489bed51ce33162785d0b77fc1c779b))
* serialize snapshot rounds per volume and harden deletion ([#85](https://github.com/home-operations/miroir/issues/85)) ([a93ada4](https://github.com/home-operations/miroir/commit/a93ada4d7ef0b22d9830b9840985255340e58bf7))
* tie-breaker retrofit must not re-add a node mid-removal ([#86](https://github.com/home-operations/miroir/issues/86)) ([ab6d224](https://github.com/home-operations/miroir/commit/ab6d224f0cf58a1abec521c12002aead37046357))


### Documentation

* explain quorum policies and the diskless tie-breaker ([#82](https://github.com/home-operations/miroir/issues/82)) ([6be7157](https://github.com/home-operations/miroir/commit/6be715772c1c8a29e77f6f7d821139299564542e))
* failure modes + how miroir compares to LINSTOR and blockstor ([#95](https://github.com/home-operations/miroir/issues/95)) ([841bceb](https://github.com/home-operations/miroir/commit/841bcebe90bf7a3f3445620b92d3e35bfa4ca004))
* fix stale README claims ([#83](https://github.com/home-operations/miroir/issues/83)) ([fd52775](https://github.com/home-operations/miroir/commit/fd5277586fc58e2a16db257f19b635941de75d0b))


### Miscellaneous Chores

* review sweep — dedup, dead code, Go 1.26 idioms ([#91](https://github.com/home-operations/miroir/issues/91)) ([92491ab](https://github.com/home-operations/miroir/commit/92491abd9c9b63c456b775fd372e84c114d6d62b))
* update mise tools ([c3e3c54](https://github.com/home-operations/miroir/commit/c3e3c54d9445facf11ed878e4b431a6a4ddce236))


### Continuous Integration

* overlap kind boot with the e2e image build ([#84](https://github.com/home-operations/miroir/issues/84)) ([c8a16a4](https://github.com/home-operations/miroir/commit/c8a16a41537c1e89e258ff2d91feadddee5182d0))

## [0.2.11](https://github.com/home-operations/miroir/compare/0.2.10...0.2.11) (2026-07-09)


### Performance Improvements

* **backend:** direct-io loop devices, lz4 on zvols ([#71](https://github.com/home-operations/miroir/issues/71)) ([e29d5f0](https://github.com/home-operations/miroir/commit/e29d5f09e42b907846d9a390fd57d5f0b57bd180))

## [0.2.10](https://github.com/home-operations/miroir/compare/0.2.9...0.2.10) (2026-07-08)


### Features

* **chart:** expose cluster-wide DRBD resync tuning ([#67](https://github.com/home-operations/miroir/issues/67)) ([d80b043](https://github.com/home-operations/miroir/commit/d80b043725db93b870138ab9c718159066c85c62))
* **csi:** spread replicas across failure-domain zones ([#69](https://github.com/home-operations/miroir/issues/69)) ([286c1df](https://github.com/home-operations/miroir/commit/286c1df1580a8905bb94e434b006f52049587d53))

## [0.2.9](https://github.com/home-operations/miroir/compare/0.2.8...0.2.9) (2026-07-08)


### Features

* **deps:** update module golang.org/x/sys (v0.46.0 → v0.47.0) ([#53](https://github.com/home-operations/miroir/issues/53)) ([b1bd566](https://github.com/home-operations/miroir/commit/b1bd56673ee27008412c603d61b9c094a64d2ea8))


### Bug Fixes

* **agent:** scope a snapshot peer's barrier write to its own slot ([#66](https://github.com/home-operations/miroir/issues/66)) ([b6189d6](https://github.com/home-operations/miroir/commit/b6189d6b530909e3bd46ce9eecb7cb09d5bf4e16))
* **agent:** scope volume status apply to this node's slot ([#60](https://github.com/home-operations/miroir/issues/60)) ([74cd98e](https://github.com/home-operations/miroir/commit/74cd98e31e206aac749af5cc76e7ef0e4295bba3))
* **backend:** crash-safe reflink clones, locale-stable exec ([#64](https://github.com/home-operations/miroir/issues/64)) ([68e5012](https://github.com/home-operations/miroir/commit/68e501243a957ff7f40bd28a70c06e15dd1bae34))
* **backend:** typed ErrBusy for retryable teardown failures ([#61](https://github.com/home-operations/miroir/issues/61)) ([d3b8eb2](https://github.com/home-operations/miroir/commit/d3b8eb23bb880338fd8b8339f2ee699e1028d94e))
* **csi:** grow filesystem on every stage, not only a fresh mount ([#62](https://github.com/home-operations/miroir/issues/62)) ([5c4d43f](https://github.com/home-operations/miroir/commit/5c4d43fbf3a2d9677b33f04a8f8dd9629ee847ff))
* **csi:** serialize CreateVolume placement with allocation ([#63](https://github.com/home-operations/miroir/issues/63)) ([8e39dca](https://github.com/home-operations/miroir/commit/8e39dca393de9a123ba742b0575d5636fd6d36b2))
* **drbd:** crash-safe minor allocation, atomic config writes, robust event scan ([#58](https://github.com/home-operations/miroir/issues/58)) ([670071f](https://github.com/home-operations/miroir/commit/670071f3741733342182f2c2a094dcef1fd130cd))
* **membership:** requeue transient replica-completion failures ([#59](https://github.com/home-operations/miroir/issues/59)) ([bac65bf](https://github.com/home-operations/miroir/commit/bac65bfa0031f5b886f08169976a4fa83c08e8f5))


### Miscellaneous Chores

* **mise:** Update tool go (1.26.4 → 1.26.5) ([#57](https://github.com/home-operations/miroir/issues/57)) ([9190fb5](https://github.com/home-operations/miroir/commit/9190fb50b0a214e3ae6b0b350d834ecedf9d08c6))
* skip the manager in setup mode; nodemap tests; errors.AsType ([#65](https://github.com/home-operations/miroir/issues/65)) ([75ca29c](https://github.com/home-operations/miroir/commit/75ca29c23b27e8192200de72dbcd42173ee48c16))

## [0.2.8](https://github.com/home-operations/miroir/compare/0.2.7...0.2.8) (2026-07-08)


### Bug Fixes

* **drbd:** stamp WasUpToDate on non-winner seed and seed per-peer ([#54](https://github.com/home-operations/miroir/issues/54)) ([b925da4](https://github.com/home-operations/miroir/commit/b925da404cb94a2dcb76d33931cb7b3c6407bb76))


### Miscellaneous Chores

* **mise:** Update tool lefthook (2.1.9 → 2.1.10) ([#52](https://github.com/home-operations/miroir/issues/52)) ([cf74d94](https://github.com/home-operations/miroir/commit/cf74d9476d1ae480611d81c569a4eac36083a5a8))

## [0.2.7](https://github.com/home-operations/miroir/compare/0.2.6...0.2.7) (2026-07-08)


### Features

* **chart:** add Prometheus Operator ServiceMonitor ([1cb6b63](https://github.com/home-operations/miroir/commit/1cb6b635c53f624c9521d7f21c17019c01c03482))


### Bug Fixes

* **deps:** update k8s.io/utils digest (be93311 → cf1189d) ([#50](https://github.com/home-operations/miroir/issues/50)) ([48eaa78](https://github.com/home-operations/miroir/commit/48eaa7848f86f97a41554ce0d714e6b5a7602b08))


### Miscellaneous Chores

* **mise:** Update tool oxfmt (0.57.0 → 0.58.0) ([#49](https://github.com/home-operations/miroir/issues/49)) ([fbaa871](https://github.com/home-operations/miroir/commit/fbaa871ea6c6dc900b948c05255a32d75cbcd5a8))

## [0.2.6](https://github.com/home-operations/miroir/compare/0.2.5...0.2.6) (2026-07-04)


### Features

* consolidate on a single operational port per workload (org standard) ([#47](https://github.com/home-operations/miroir/issues/47)) ([3e724d8](https://github.com/home-operations/miroir/commit/3e724d847bc012f19ca8ccdc559a705edca5de74))
* **deps:** update module google.golang.org/grpc (v1.81.1 → v1.82.0) ([#45](https://github.com/home-operations/miroir/issues/45)) ([55e4b3b](https://github.com/home-operations/miroir/commit/55e4b3b493a5bbf75d5a202ce211271b5a1daa61))

## [0.2.5](https://github.com/home-operations/miroir/compare/0.2.4...0.2.5) (2026-07-02)


### Bug Fixes

* **container:** update image registry.k8s.io/sig-storage/csi-resizer (v2.2.0 → v2.2.1) ([#43](https://github.com/home-operations/miroir/issues/43)) ([b63ebb6](https://github.com/home-operations/miroir/commit/b63ebb6173f0fb2301c7c6c9f504505f420eef7c))


### Miscellaneous Chores

* **mise:** Lock file maintenance tool ([#46](https://github.com/home-operations/miroir/issues/46)) ([0d05c1a](https://github.com/home-operations/miroir/commit/0d05c1ae036e53a91651851667f2857775676a49))
* **mise:** Update tool oxfmt (0.56.0 → 0.57.0) ([#44](https://github.com/home-operations/miroir/issues/44)) ([81dcae8](https://github.com/home-operations/miroir/commit/81dcae8cd6178a75577e798514aaaf829e61993d))
* **renovate:** inherit shared toolchain + chart-docs presets ([b135a75](https://github.com/home-operations/miroir/commit/b135a75106f39eb736f34ad6c4381eb020f541be))

## [0.2.4](https://github.com/home-operations/miroir/compare/0.2.3...0.2.4) (2026-06-26)


### Bug Fixes

* **deps:** update k8s.io/utils digest (a95e086 → be93311) ([#40](https://github.com/home-operations/miroir/issues/40)) ([93c6504](https://github.com/home-operations/miroir/commit/93c6504b4844f1ff8be036c3445de2c7d2513db8))
* **deps:** update module github.com/onsi/gomega (v1.42.0 → v1.42.1) ([#38](https://github.com/home-operations/miroir/issues/38)) ([f1d8fd0](https://github.com/home-operations/miroir/commit/f1d8fd03c2f26e21a7e4706c8635e0e106484cd1))


### Miscellaneous Chores

* add minimumGroupSize to Go toolchain configuration ([3cbffe4](https://github.com/home-operations/miroir/commit/3cbffe460d3e59a932dfba2706cf2e4761f2f612))
* **mise:** Update tool hcloud (1.65.0 → 1.66.0) ([#39](https://github.com/home-operations/miroir/issues/39)) ([6774b20](https://github.com/home-operations/miroir/commit/6774b204e38a9591176c3882681ac323f5691133))
* **mise:** Update tool oxfmt (0.55.0 → 0.56.0) ([#32](https://github.com/home-operations/miroir/issues/32)) ([14da7d5](https://github.com/home-operations/miroir/commit/14da7d5b41820476f349614fc7588bb9cdb92fcd))

## [0.2.3](https://github.com/home-operations/miroir/compare/0.2.2...0.2.3) (2026-06-23)


### Features

* **deps:** update module github.com/onsi/ginkgo/v2 (v2.31.0 → v2.32.0) ([#34](https://github.com/home-operations/miroir/issues/34)) ([0cf5bfd](https://github.com/home-operations/miroir/commit/0cf5bfd064363807bdcda6ae0b71575cb1504a6c))


### Bug Fixes

* **agent:** release DRBD backings on node shutdown to unblock reboots ([#35](https://github.com/home-operations/miroir/issues/35)) ([578c22b](https://github.com/home-operations/miroir/commit/578c22b9272c0f91c6139682a5979f30c9982c86))


### Miscellaneous Chores

* **mise:** Update tool jq (1.8.1 → 1.8.2) ([#28](https://github.com/home-operations/miroir/issues/28)) ([33c2566](https://github.com/home-operations/miroir/commit/33c2566a5e5c9cb34ec42b1d376072c54112da65))
* **mise:** Update tool talosctl (1.13.4 → 1.13.5) ([#33](https://github.com/home-operations/miroir/issues/33)) ([ae44890](https://github.com/home-operations/miroir/commit/ae44890a00b3e2dca7c63e59da384f4be51987ed))

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
