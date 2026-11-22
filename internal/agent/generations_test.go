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

package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// The seed of a padded restore sits on fresh create-md metadata that
// nothing else can promote (the birth flow refuses data-bearing volumes,
// and there was no metadata to adopt): the agent must mint its data
// generation with a full bitmap so the joiners full-sync from it.
func TestReconcilePaddedRestoreSeedMintsGeneration(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := paddedRestoreVol()
	// statusSeq, not statusJSON: the metadata must read as this agent's
	// own fresh create (see TestReconcilePaddedRestoreGrowsCloneBeforeCreateMD)
	// — adopted metadata carries a generation and correctly refuses the mint.
	inc := `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateInconsistent + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`
	fe := &fakeDRBDExec{statusSeq: []string{statusNotInKernel, statusNotInKernel, inc, inc, inc, inc}}

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, "src-vol")).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	fe.calledWith(t, "drbdadm primary --force "+volPvc1)
	fe.calledWith(t, "drbdadm secondary "+volPvc1)
	fe.notCalledWith(t, "new-current-uuid")
}

// A seed leg recreated after a node wipe wears the same fresh-metadata
// signature as first bring-up, but its peer holds the data now: the mint
// must refuse and let the leg join as a plain full SyncTarget.
func TestReconcilePaddedRestoreSeedMintRefusesWhenPeerHasData(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := paddedRestoreVol()
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeB: {DiskState: diskStateUpToDate, DeviceCreated: true},
	}
	// statusSeq like the mint test: with freshly created metadata the
	// peer-data guard must be the thing refusing, not metadata adoption.
	inc := `[{"name":"` + volPvc1 + `",
		"devices":[{"disk-state":"` + diskStateInconsistent + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`
	fe := &fakeDRBDExec{statusSeq: []string{statusNotInKernel, statusNotInKernel, inc, inc, inc, inc}}

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, "src-vol")).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	fe.notCalledWith(t, "primary --force")
}

// A padded-restore seed found Primary and UpToDate without ever being
// Activated can only be a mint whose demote never ran (the crash window
// between promote and secondary): the next pass must demote it so a
// consumer on the peer node can promote.
func TestReconcilePaddedRestoreSeedDemotesInterruptedMint(t *testing.T) {
	s := newScheme(t)
	fb := newFakeBackend()
	v := paddedRestoreVol()
	up := `[{"name":"` + volPvc1 + `","role":"Primary",
		"devices":[{"disk-state":"` + diskStateUpToDate + `"}],
		"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`
	fe := &fakeDRBDExec{statusSeq: []string{statusNotInKernel, statusNotInKernel, up, up, up, up}}

	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v, snapObj(snapSnap1, "src-vol")).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeA, Pools: poolsOf(fb),
		DRBD: &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}

	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err != nil {
		t.Fatal(err)
	}

	fe.calledWith(t, "drbdadm secondary "+volPvc1)
	fe.notCalledWith(t, "primary --force")
}

// --- birth generation ---------------------------------------------------

const (
	birthReadyJSON = `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"Inconsistent"}],
		"connections":[{"peer-node-id":%d,"connection-state":"Connected",
			"peer_devices":[{"peer-disk-state":"Inconsistent"}]}]}]`
	birthDoneJSON = `[{"name":"pvc-1","role":"Secondary",
		"devices":[{"disk-state":"UpToDate"}],
		"connections":[{"peer-node-id":%d,"connection-state":"Connected",
			"peer_devices":[{"peer-disk-state":"UpToDate"}]}]}]`
)

// birthSetup builds a fresh 2-diskful volume (node-a node id 0 = winner,
// node-b node id 1) and a reconciler on nodeName whose kernel answers the
// queued --json statuses (peerNodeID fills the connection entries).
func birthSetup(t *testing.T, nodeName string, peerNodeID int, statusSeq ...string) (*VolumeReconciler, *fakeDRBDExec, *miroirv1alpha1.MiroirVolume) {
	t.Helper()
	s := newScheme(t)
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.QuorumPolicy = miroirv1alpha1.QuorumLastManStanding
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB

	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, "pvc-1.res"), []byte(
		"resource \"pvc-1\" {\n    on \""+nodeName+"\" {\n        device minor 1000;\n    }\n}\n",
	), 0o640); err != nil {
		t.Fatal(err)
	}
	fe := &fakeDRBDExec{}
	if len(statusSeq) > 0 {
		// The leading not-in-kernel reads are Apply's peer snapshot and
		// probeMetadata's: a fresh volume must take the create-md path, not
		// read the kernel view as adopted metadata.
		seq := make([]string, 0, 2+len(statusSeq))
		seq = append(seq, statusNotInKernel, statusNotInKernel)
		for _, tmpl := range statusSeq {
			seq = append(seq, fmt.Sprintf(tmpl, peerNodeID))
		}
		fe.statusSeq = seq
		fe.statusJSON = seq[len(seq)-1] // stable fallback once drained
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithObjects(v).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
	r := &VolumeReconciler{
		Client: c, NodeName: nodeName, Pools: poolsOf(newFakeBackend()),
		DRBD: &drbd.Driver{StateDir: stateDir, Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
	}
	return r, fe, v
}

// The winner mints the birth generation once every diskful leg sits
// Inconsistent over established connections, and the same pass proceeds
// against the resulting UpToDate state.
func TestReconcileBirthWinnerMintsInitialUUID(t *testing.T) {
	r, fe, _ := birthSetup(t, nodeA, 1, birthReadyJSON, birthDoneJSON)
	reconcile(t, r, volPvc1)
	// The full fresh path: metadata created and left just-created, then the
	// one replicated generation minted over the live connections.
	fe.calledWith(t, "drbdadm create-md --force --max-peers 7 pvc-1/0")
	fe.calledWith(t, "drbdadm new-current-uuid --clear-bitmap pvc-1/0")

	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if st := got.Status.PerNode[nodeA]; st.DiskState != diskStateUpToDate {
		t.Fatalf("winner must report the post-birth state, got %+v", st)
	}
}

// The loser waits: only the winner may mint, or two generations could race.
func TestReconcileBirthLoserWaits(t *testing.T) {
	r, fe, _ := birthSetup(t, nodeB, 0, birthReadyJSON)
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "new-current-uuid")
}

// No mint while a diskful peer is still disconnected or its disk state is
// unknown — the generation must land on every leg over live connections.
func TestReconcileBirthWaitsForPeers(t *testing.T) {
	for name, status := range map[string]string{
		"peer connecting": `[{"name":"pvc-1","role":"Secondary",
			"devices":[{"disk-state":"Inconsistent"}],
			"connections":[{"peer-node-id":%d,"connection-state":"Connecting",
				"peer_devices":[{"peer-disk-state":"DUnknown"}]}]}]`,
		"peer disk unknown": `[{"name":"pvc-1","role":"Secondary",
			"devices":[{"disk-state":"Inconsistent"}],
			"connections":[{"peer-node-id":%d,"connection-state":"Connected",
				"peer_devices":[{"peer-disk-state":"DUnknown"}]}]}]`,
	} {
		r, fe, _ := birthSetup(t, nodeA, 1, status)
		reconcile(t, r, volPvc1)
		if n := countCalls(fe, "new-current-uuid"); n != 0 {
			t.Fatalf("%s: must not mint, got %d calls", name, n)
		}
	}
}

// A FullSync joiner means the volume already has a generation elsewhere:
// never mint over it.
func TestReconcileBirthSkipsFullSyncJoiner(t *testing.T) {
	r, fe, v := birthSetup(t, nodeA, 1, birthReadyJSON)
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: v.Name}, got); err != nil {
		t.Fatal(err)
	}
	got.Spec.Replicas[1].FullSync = true
	if err := r.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "new-current-uuid")
}

// An Activated volume held data: an all-Inconsistent state is then a real
// incident, never something to paper over with a fresh generation.
func TestReconcileBirthSkipsActivatedVolume(t *testing.T) {
	r, fe, _ := birthSetup(t, nodeA, 1, birthReadyJSON)
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	got.Status.Activated = true
	if err := r.Status().Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "new-current-uuid")
}

// A restored backing contains snapshot data even when its DRBD metadata looks
// freshly created. Never mint a birth UUID over it: that destructive command
// is reserved for volumes born empty.
func TestReconcileBirthSkipsSnapshotDerivedVolume(t *testing.T) {
	r, fe, v := birthSetup(t, nodeA, 1, birthReadyJSON)
	if err := r.Create(t.Context(), snapObj(snapSnap1, "source-volume")); err != nil {
		t.Fatal(err)
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: v.Name}, got); err != nil {
		t.Fatal(err)
	}
	got.Spec.Source = &miroirv1alpha1.VolumeSource{SnapshotName: snapSnap1}
	if err := r.Update(t.Context(), got); err != nil {
		t.Fatal(err)
	}

	reconcile(t, r, volPvc1)
	fe.notCalledWith(t, "new-current-uuid")
}

// A failed mint surfaces as a reconcile error (retried with backoff) and
// keeps DeviceCreated so the phase never reads as a hard provisioning
// failure.
func TestReconcileBirthMintErrorRetries(t *testing.T) {
	r, fe, _ := birthSetup(t, nodeA, 1, birthReadyJSON)
	fe.errOn = map[string]error{"new-current-uuid": errors.New("exit status 1")}
	if _, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: volPvc1}}); err == nil {
		t.Fatal("a failed mint must surface as a reconcile error")
	}
	got := &miroirv1alpha1.MiroirVolume{}
	if err := r.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.PerNode[nodeA].DeviceCreated {
		t.Fatal("DeviceCreated must survive a failed mint")
	}
}
