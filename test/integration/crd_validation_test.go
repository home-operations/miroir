/*
Copyright 2026.

Licensed under the GNU Affero General Public License, Version 3 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.gnu.org/licenses/agpl-3.0.html

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package integration

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	nodeA    = "node-a"
	nodeB    = "node-b"
	nodeC    = "node-c"
	nodeD    = "node-d"
	poolFast = "fast"

	poolDefault = "default"
	datasetTank = "tank/miroir"
)

// unreplicatedVolume is the minimal valid single-replica volume.
func unreplicatedVolume(name string) *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 1 << 30,
			Replicas:  []miroirv1alpha1.Replica{{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin}},
		},
	}
}

// replicatedVolume has three diskful replicas so a single leg can flip
// state without also tripping the min-diskful-count rule — transition
// rules must be tested in isolation.
func replicatedVolume(name string) *miroirv1alpha1.MiroirVolume {
	return &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: miroirv1alpha1.MiroirVolumeSpec{
			SizeBytes: 1 << 30,
			Replicas: []miroirv1alpha1.Replica{
				{Node: nodeA, Backend: miroirv1alpha1.BackendLVMThin, NodeID: 0},
				{Node: nodeB, Backend: miroirv1alpha1.BackendLVMThin, NodeID: 1},
				{Node: nodeC, Backend: miroirv1alpha1.BackendLVMThin, NodeID: 2},
			},
			QuorumPolicy: miroirv1alpha1.QuorumFreeze,
			DRBD:         &miroirv1alpha1.DRBDSpec{Port: 7000},
		},
	}
}

var _ = Describe("MiroirVolume CEL validation", func() {
	It("rejects shrinking sizeBytes and allows growth", func() {
		vol := unreplicatedVolume("pvc-shrink-guard")
		Expect(k8sClient.Create(ctx, vol)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, vol)).To(Succeed()) })

		vol.Spec.SizeBytes = 1 << 29
		err := k8sClient.Update(ctx, vol)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "shrink must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("cannot shrink"))

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(vol), vol)).To(Succeed())
		vol.Spec.SizeBytes = 2 << 30
		Expect(k8sClient.Update(ctx, vol)).To(Succeed(), "growth must stay allowed")
	})

	It("rejects duplicate replica nodes", func() {
		vol := replicatedVolume("pvc-dup-replica")
		vol.Spec.Replicas[2].Node = nodeA
		err := k8sClient.Create(ctx, vol)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "duplicate replica nodes must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("must be unique"))
	})

	It("rejects duplicate client-leg nodes", func() {
		vol := replicatedVolume("pvc-dup-client")
		vol.Spec.Replicas = vol.Spec.Replicas[:2]
		vol.Spec.Clients = []miroirv1alpha1.VolumeClient{{Node: nodeD}, {Node: nodeD}}
		err := k8sClient.Create(ctx, vol)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "duplicate client nodes must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("must be unique"))
	})

	It("rejects changing the allocated DRBD port", func() {
		vol := replicatedVolume("pvc-port-pin")
		Expect(k8sClient.Create(ctx, vol)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, vol)).To(Succeed()) })

		vol.Spec.DRBD.Port = 7001
		err := k8sClient.Update(ctx, vol)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "port change must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("port is immutable"))
	})

	It("rejects retargeting or adding a clone source", func() {
		vol := unreplicatedVolume("pvc-source-pin")
		vol.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: "snap-a"}
		Expect(k8sClient.Create(ctx, vol)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, vol)).To(Succeed()) })

		vol.Spec.Source.SnapshotName = "snap-b"
		err := k8sClient.Update(ctx, vol)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "source retarget must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("source is immutable"))

		unsourced := unreplicatedVolume("pvc-source-add")
		Expect(k8sClient.Create(ctx, unsourced)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, unsourced)).To(Succeed()) })

		unsourced.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: "snap-a"}
		err = k8sClient.Update(ctx, unsourced)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "adding a source after creation must be rejected, got: %v", err)
	})

	It("rejects changing the export filesystem after formatting", func() {
		vol := replicatedVolume("pvc-fstype-pin")
		vol.Spec.Export = &miroirv1alpha1.ExportSpec{FSType: "ext4"}
		Expect(k8sClient.Create(ctx, vol)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, vol)).To(Succeed()) })

		vol.Spec.Export.FSType = "xfs"
		err := k8sClient.Update(ctx, vol)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "fsType change must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("fsType is immutable"))
	})

	It("rejects retargeting a completed replica's pool", func() {
		vol := replicatedVolume("pvc-pool-pin")
		for i := range vol.Spec.Replicas {
			vol.Spec.Replicas[i].Address = "10.0.0." + string(rune('1'+i))
			vol.Spec.Replicas[i].Pool = poolFast
		}
		Expect(k8sClient.Create(ctx, vol)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, vol)).To(Succeed()) })

		vol.Spec.Replicas[2].Pool = "slow"
		err := k8sClient.Update(ctx, vol)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "pool retarget must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("pool is immutable"))
	})

	It("rejects mixed-pool diskful replicas", func() {
		vol := replicatedVolume("pvc-pool-mixed")
		for i := range vol.Spec.Replicas {
			vol.Spec.Replicas[i].Address = "10.0.1." + string(rune('1'+i))
			vol.Spec.Replicas[i].Pool = poolFast
		}
		vol.Spec.Replicas[2].Pool = "slow"
		err := k8sClient.Create(ctx, vol)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "mixed-pool replicas must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("one pool"))
	})

	// The membership completion flow must stay admissible: an
	// operator-added bare entry (no address, no pool) joins a named-pool
	// volume, and its later completion sets pool+address in one update.
	It("allows adding and completing a replica on a named-pool volume", func() {
		vol := replicatedVolume("pvc-pool-add")
		vol.Spec.Replicas = vol.Spec.Replicas[:2]
		for i := range vol.Spec.Replicas {
			vol.Spec.Replicas[i].Address = "10.0.2." + string(rune('1'+i))
			vol.Spec.Replicas[i].Pool = poolFast
		}
		Expect(k8sClient.Create(ctx, vol)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, vol)).To(Succeed()) })

		vol.Spec.Replicas = append(vol.Spec.Replicas, miroirv1alpha1.Replica{Node: nodeC})
		Expect(k8sClient.Update(ctx, vol)).To(Succeed(),
			"an incomplete add must pass the uniformity rule")

		vol.Spec.Replicas[2].Pool = poolFast
		vol.Spec.Replicas[2].Backend = miroirv1alpha1.BackendLVMThin
		vol.Spec.Replicas[2].NodeID = 2
		vol.Spec.Replicas[2].Address = "10.0.2.3"
		vol.Spec.Replicas[2].FullSync = true
		Expect(k8sClient.Update(ctx, vol)).To(Succeed(),
			"completing the entry must pass both pool rules")
	})

	// The auto-evict swap shape: MaxItems=3 forbids add-before-remove, so
	// the dead entry leaves and the bare replacement arrives in one
	// update — which must clear the size, min-diskful, first-diskful, and
	// per-node transition rules simultaneously.
	It("allows the atomic evict swap: dead replica out, bare entry in", func() {
		vol := replicatedVolume("pvc-evict-swap")
		vol.Spec.Replicas = vol.Spec.Replicas[:2]
		vol.Spec.Replicas = append(vol.Spec.Replicas,
			miroirv1alpha1.Replica{Node: nodeC, NodeID: 2, Address: "10.0.3.3", Diskless: true})
		for i := range vol.Spec.Replicas[:2] {
			vol.Spec.Replicas[i].Address = "10.0.3." + string(rune('1'+i))
		}
		Expect(k8sClient.Create(ctx, vol)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, vol)).To(Succeed()) })

		// node-b died: its completed entry is replaced by a bare diskful
		// add, the surviving diskful leg stays first, the tie-breaker keeps
		// its slot.
		vol.Spec.Replicas = []miroirv1alpha1.Replica{
			vol.Spec.Replicas[0],
			{Node: nodeD},
			vol.Spec.Replicas[2],
		}
		Expect(k8sClient.Update(ctx, vol)).To(Succeed(),
			"the one-update swap must pass every replica rule")
	})

	// Canary for the pre-existing transition rule the agents rely on: a
	// leg's on-disk DRBD metadata cannot be discarded in place.
	It("rejects flipping a diskful replica to diskless in place", func() {
		vol := replicatedVolume("pvc-diskless-flip")
		Expect(k8sClient.Create(ctx, vol)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, vol)).To(Succeed()) })

		vol.Spec.Replicas[2].Diskless = true
		vol.Spec.Replicas[2].Backend = ""
		err := k8sClient.Update(ctx, vol)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "diskful→diskless must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("cannot become diskless"))
	})
})

var _ = Describe("MiroirSnapshot CEL validation", func() {
	It("rejects retargeting volumeName", func() {
		snap := &miroirv1alpha1.MiroirSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "snap-retarget"},
			Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: "pvc-a"},
		}
		Expect(k8sClient.Create(ctx, snap)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, snap)).To(Succeed()) })

		snap.Spec.VolumeName = "pvc-b"
		err := k8sClient.Update(ctx, snap)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "retarget must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("immutable"))
	})
})

// minimalNode is a valid single-pool MiroirNode for the CEL cases below.
func minimalNode(name string, pool miroirv1alpha1.MiroirNodePool) *miroirv1alpha1.MiroirNode {
	return &miroirv1alpha1.MiroirNode{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       miroirv1alpha1.MiroirNodeSpec{Pools: []miroirv1alpha1.MiroirNodePool{pool}},
	}
}

func lvmthinPool(name string) miroirv1alpha1.MiroirNodePool {
	return miroirv1alpha1.MiroirNodePool{
		Name: name, Backend: miroirv1alpha1.BackendLVMThin,
		LVMThin: &miroirv1alpha1.LVMThinPool{Device: "/dev/sdb"},
	}
}

var _ = Describe("MiroirNode CEL validation", func() {
	It("accepts each backend with its own block and applies the zfs defaults", func() {
		node := &miroirv1alpha1.MiroirNode{
			ObjectMeta: metav1.ObjectMeta{Name: "min-valid"},
			Spec: miroirv1alpha1.MiroirNodeSpec{
				Zone:    "rack-1",
				Address: "10.0.100.11",
				Pools: []miroirv1alpha1.MiroirNodePool{
					lvmthinPool(poolDefault),
					{Name: "fast", Backend: miroirv1alpha1.BackendZFS,
						ZFS: &miroirv1alpha1.ZFSPool{Dataset: datasetTank}},
					{Name: "scratch", Backend: miroirv1alpha1.BackendLoopfile,
						Loopfile: &miroirv1alpha1.LoopfilePool{BaseDir: "/var/lib/miroir"}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, node)).To(Succeed()) })

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(node), node)).To(Succeed())
		Expect(node.Spec.Pools[1].ZFS.Compression).To(Equal("lz4"), "CRD default")
		Expect(node.Spec.Pools[1].ZFS.VolBlockSize).To(Equal("4K"), "CRD default")
	})

	It("rejects a pool whose backend block is missing (the 0.10-agent truncation guard)", func() {
		node := minimalNode("min-block-missing", miroirv1alpha1.MiroirNodePool{
			Name: poolDefault, Backend: miroirv1alpha1.BackendZFS,
		})
		err := k8sClient.Create(ctx, node)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "missing zfs block must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("exactly the selected backend's configuration block"))

		// The same rule is what refuses a 0.10 agent's truncating update:
		// spec.pools rebuilt as bare {name, backend} loses the block.
		valid := minimalNode("min-truncation", lvmthinPool(poolDefault))
		Expect(k8sClient.Create(ctx, valid)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, valid)).To(Succeed()) })
		valid.Spec.Pools = []miroirv1alpha1.MiroirNodePool{
			{Name: poolDefault, Backend: miroirv1alpha1.BackendLVMThin},
		}
		err = k8sClient.Update(ctx, valid)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "a truncating spec write must be rejected, got: %v", err)
	})

	It("rejects another backend's options — they are unrepresentable, not ignored", func() {
		node := minimalNode("min-cross-block", miroirv1alpha1.MiroirNodePool{
			Name: poolDefault, Backend: miroirv1alpha1.BackendLVMThin,
			LVMThin: &miroirv1alpha1.LVMThinPool{Device: "/dev/sdb"},
			ZFS:     &miroirv1alpha1.ZFSPool{Dataset: datasetTank},
		})
		err := k8sClient.Create(ctx, node)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "a zfs block on an lvmthin pool must be rejected, got: %v", err)
	})

	It("requires dataset and baseDir inside their blocks (plain schema, not CEL)", func() {
		node := minimalNode("min-no-dataset", miroirv1alpha1.MiroirNodePool{
			Name: poolDefault, Backend: miroirv1alpha1.BackendZFS, ZFS: &miroirv1alpha1.ZFSPool{},
		})
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, node))).To(BeTrue())

		node = minimalNode("min-no-basedir", miroirv1alpha1.MiroirNodePool{
			Name: poolDefault, Backend: miroirv1alpha1.BackendLoopfile, Loopfile: &miroirv1alpha1.LoopfilePool{},
		})
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, node))).To(BeTrue())
	})

	It("allows an empty lvmthin block for a pre-provisioned VG", func() {
		node := minimalNode("min-vg-exists", miroirv1alpha1.MiroirNodePool{
			Name: poolDefault, Backend: miroirv1alpha1.BackendLVMThin, LVMThin: &miroirv1alpha1.LVMThinPool{},
		})
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, node)).To(Succeed()) })
	})

	It("rejects two pools sharing a backing", func() {
		node := &miroirv1alpha1.MiroirNode{
			ObjectMeta: metav1.ObjectMeta{Name: "min-shared-backing"},
			Spec: miroirv1alpha1.MiroirNodeSpec{Pools: []miroirv1alpha1.MiroirNodePool{
				lvmthinPool("a"),
				lvmthinPool("b"),
			}},
		}
		err := k8sClient.Create(ctx, node)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "shared device must be rejected, got: %v", err)
		Expect(err.Error()).To(ContainSubstring("share a device"))
	})

	It("rejects invalid enum, pattern, and address values", func() {
		bad := minimalNode("min-bad-blocksize", miroirv1alpha1.MiroirNodePool{
			Name: poolDefault, Backend: miroirv1alpha1.BackendZFS,
			ZFS: &miroirv1alpha1.ZFSPool{Dataset: datasetTank, VolBlockSize: "12K"},
		})
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, bad))).To(BeTrue(), "12K is not a valid volBlockSize")

		// Canonical spellings only: the CRD validates, it does not fold case.
		bad = minimalNode("min-bad-case", miroirv1alpha1.MiroirNodePool{
			Name: poolDefault, Backend: miroirv1alpha1.BackendZFS,
			ZFS: &miroirv1alpha1.ZFSPool{Dataset: datasetTank, VolBlockSize: "16k"},
		})
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, bad))).To(BeTrue(), "lowercase 16k must be rejected")

		bad = minimalNode("min-bad-compression", miroirv1alpha1.MiroirNodePool{
			Name: poolDefault, Backend: miroirv1alpha1.BackendZFS,
			ZFS: &miroirv1alpha1.ZFSPool{Dataset: datasetTank, Compression: "snappy"},
		})
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, bad))).To(BeTrue(), "snappy is not an OpenZFS algorithm")

		bad = minimalNode("min-bad-poolname", lvmthinPool("Fast_NVMe"))
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, bad))).To(BeTrue(), "pool names stay DNS-label-style")

		bad = minimalNode("min-bad-address", lvmthinPool(poolDefault))
		bad.Spec.Address = "not-an-ip"
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, bad))).To(BeTrue(), "address must be an IP")

		bad = minimalNode("min-cidr-address", lvmthinPool(poolDefault))
		bad.Spec.Address = "10.0.0.0/24"
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, bad))).To(BeTrue(), "a CIDR is not a host address")
	})

	It("requires at least one pool", func() {
		node := &miroirv1alpha1.MiroirNode{ObjectMeta: metav1.ObjectMeta{Name: "min-no-pools"}}
		Expect(apierrors.IsInvalid(k8sClient.Create(ctx, node))).To(BeTrue())
	})
})
