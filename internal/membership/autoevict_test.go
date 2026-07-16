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

package membership

import (
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/constants"
	"github.com/home-operations/miroir/internal/drbd"
)

const (
	nodeD   = "node-d"
	volPvc1 = "pvc-1"
)

// minAt is a MiroirNode carrying a zfs default pool whose heartbeat is
// age old, with the given free space in the pool. The reconciler folds
// its topology from these CRs — the nodes seeded per test ARE the
// topology.
func minAt(name string, age time.Duration, free int64) *miroirv1alpha1.MiroirNode {
	at := metav1.NewTime(time.Now().Add(-age))
	return &miroirv1alpha1.MiroirNode{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: miroirv1alpha1.MiroirNodeSpec{
			Pools: []miroirv1alpha1.MiroirNodePool{{
				Name: poolDefault, Backend: miroirv1alpha1.BackendZFS,
			}},
		},
		Status: miroirv1alpha1.MiroirNodeStatus{
			Pools: []miroirv1alpha1.MiroirNodePoolStatus{{
				Name: poolDefault, CapacityBytes: 100 << 30, AllocatedBytes: 100<<30 - free,
			}},
			ObservedAt: &at,
		},
	}
}

// evictVol is a completed 2-diskful volume on node-a+node-b whose
// node-a survivor holds a clean copy and, as expected with a diskful
// peer down, reports its links not fully connected.
func evictVol() *miroirv1alpha1.MiroirVolume {
	v := replicatedVol()
	v.Spec.Replicas = v.Spec.Replicas[:2]
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DiskState: drbd.DiskUpToDate, Connected: false},
	}
	return v
}

func reconcileAE(t *testing.T, r *AutoEvictReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(t.Context(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func newAE(t *testing.T, objs ...client.Object) *AutoEvictReconciler {
	t.Helper()
	c := fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		WithObjects(objs...).Build()
	return &AutoEvictReconciler{Client: c, After: time.Hour}
}

// A node stale past the threshold gets its diskful leg swapped in one
// update: dead entry out, a bare replacement in for the membership
// reconciler to complete. The dead node's teardown finalizer stays — it
// is the record that the leg was never cleaned up there.
func TestAutoEvictSwapsDeadDiskful(t *testing.T) {
	r := newAE(t, evictVol(),
		minAt(nodeA, time.Minute, 50<<30),
		minAt(nodeB, 2*time.Hour, 50<<30),
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if slices.ContainsFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeB
	}) {
		t.Fatalf("dead replica must be removed: %+v", got.Spec.Replicas)
	}
	idx := slices.IndexFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeD
	})
	if idx < 0 {
		t.Fatalf("replacement on the spare node must be appended: %+v", got.Spec.Replicas)
	}
	rep := got.Spec.Replicas[idx]
	if rep.Diskless || rep.Address != "" {
		t.Fatalf("replacement must be a bare diskful entry for membership completion: %+v", rep)
	}
	if got.Spec.Replicas[0].Node != nodeA {
		t.Fatalf("surviving diskful leg must stay first: %+v", got.Spec.Replicas)
	}
	if !slices.Contains(got.Finalizers, constants.FinalizerPrefix+nodeB) {
		t.Fatalf("dead node's teardown finalizer must stay for its return: %v", got.Finalizers)
	}
	if v := testutil.ToFloat64(metricEvictions.WithLabelValues("replica")); v < 1 {
		t.Fatalf("eviction counter must increment, got %v", v)
	}
}

// A heartbeat younger than the threshold only schedules a re-check.
func TestAutoEvictWaitsForThreshold(t *testing.T) {
	r := newAE(t, evictVol(),
		minAt(nodeB, 30*time.Minute, 10<<30))

	res := reconcileAE(t, r, nodeB)

	if res.RequeueAfter <= 0 || res.RequeueAfter > 30*time.Minute+time.Second {
		t.Fatalf("must requeue for the remaining threshold, got %v", res.RequeueAfter)
	}
	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if len(got.Spec.Replicas) != 2 {
		t.Fatalf("volume must be untouched: %+v", got.Spec.Replicas)
	}
}

// More than one stale heartbeat reads as an observer-side problem, not
// two simultaneous dead nodes: the safety valve stands the whole pass down.
func TestAutoEvictValveMultipleStale(t *testing.T) {
	r := newAE(t, evictVol(),
		minAt(nodeA, 2*time.Hour, 50<<30),
		minAt(nodeB, 2*time.Hour, 50<<30),
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if len(got.Spec.Replicas) != 2 {
		t.Fatalf("valve must block all evictions: %+v", got.Spec)
	}
	if v := testutil.ToFloat64(metricEvictStanddown.WithLabelValues("multiple_stale")); v < 1 {
		t.Fatalf("stand-down counter must increment, got %v", v)
	}
}

// A spec opt-out exempts the node no matter how stale it is.
func TestAutoEvictOptOut(t *testing.T) {
	optOut := minAt(nodeB, 2*time.Hour, 50<<30)
	no := false
	optOut.Spec.AutoEvict = &no
	r := newAE(t, evictVol(),
		optOut,
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if len(got.Spec.Replicas) != 2 {
		t.Fatalf("opted-out node must never be evicted: %+v", got.Spec)
	}
}

// No eviction while a surviving leg lacks a clean copy: dropping the
// dead leg then would gamble the volume on a single suspect replica.
func TestAutoEvictBlocksOnDirtySurvivor(t *testing.T) {
	v := evictVol()
	v.Status.PerNode[nodeA] = miroirv1alpha1.ReplicaStatus{DiskState: drbd.DiskInconsistent}
	r := newAE(t, v,
		minAt(nodeB, 2*time.Hour, 50<<30),
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if len(got.Spec.Replicas) != 2 {
		t.Fatalf("dirty survivor must block eviction: %+v", got.Spec)
	}
}

// A survivor whose links are fully established includes the "dead"
// node: it is alive and only its API-server path is broken. The whole
// pass stands down.
func TestAutoEvictStandsDownWhenPeerConnected(t *testing.T) {
	v := evictVol()
	v.Status.PerNode[nodeA] = miroirv1alpha1.ReplicaStatus{
		DiskState: drbd.DiskUpToDate, Connected: true,
	}
	r := newAE(t, v,
		minAt(nodeB, 2*time.Hour, 50<<30),
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if len(got.Spec.Replicas) != 2 {
		t.Fatalf("established links to the dead node must block eviction: %+v", got.Spec)
	}
	if v := testutil.ToFloat64(metricEvictStanddown.WithLabelValues("peer_connected")); v < 1 {
		t.Fatalf("stand-down counter must increment, got %v", v)
	}
}

// Snapshots pin diskful replicas: a replacement leg would not carry
// their CoW state, so eviction waits until they are gone.
func TestAutoEvictBlocksOnSnapshot(t *testing.T) {
	snap := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-1"},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: volPvc1},
	}
	r := newAE(t, evictVol(), snap,
		minAt(nodeB, 2*time.Hour, 50<<30),
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if len(got.Spec.Replicas) != 2 {
		t.Fatalf("snapshot must block eviction: %+v", got.Spec)
	}
}

// A dead diskless tie-breaker is the cheap case: swapped for a spare
// node in one update, no full sync involved.
func TestAutoEvictSwapsDeadTieBreaker(t *testing.T) {
	v := evictVol()
	v.Spec.Replicas = append(v.Spec.Replicas,
		miroirv1alpha1.Replica{Node: nodeC, NodeID: 2, Address: addrC, Diskless: true})
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeC)
	v.Status.PerNode[nodeA] = miroirv1alpha1.ReplicaStatus{
		DiskState: drbd.DiskUpToDate, Connected: true, // diskful peers all up
	}
	v.Status.PerNode[nodeB] = miroirv1alpha1.ReplicaStatus{
		DiskState: drbd.DiskUpToDate, Connected: true,
	}
	r := newAE(t, v,
		minAt(nodeA, time.Minute, 50<<30),
		minAt(nodeB, time.Minute, 50<<30),
		minAt(nodeC, 2*time.Hour, 50<<30),
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeC)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	idx := slices.IndexFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeD
	})
	if idx < 0 || !got.Spec.Replicas[idx].Diskless || got.Spec.Replicas[idx].Address != "" {
		t.Fatalf("tie-breaker must be re-placed as a bare diskless entry: %+v", got.Spec.Replicas)
	}
	if slices.ContainsFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeC
	}) {
		t.Fatalf("dead tie-breaker must be removed: %+v", got.Spec.Replicas)
	}
	if !slices.Contains(got.Finalizers, constants.FinalizerPrefix+nodeC) {
		t.Fatalf("dead node's teardown finalizer must stay for its return: %v", got.Finalizers)
	}
}

// A dead consumer's client leg is dropped outright — the pod is gone,
// nothing replaces it.
func TestAutoEvictDropsDeadClient(t *testing.T) {
	v := evictVol()
	v.Spec.Clients = []miroirv1alpha1.VolumeClient{
		{Node: nodeC, NodeID: 2, Address: addrC},
	}
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeC)
	v.Status.PerNode[nodeA] = miroirv1alpha1.ReplicaStatus{
		DiskState: drbd.DiskUpToDate, Connected: true,
	}
	v.Status.PerNode[nodeB] = miroirv1alpha1.ReplicaStatus{
		DiskState: drbd.DiskUpToDate, Connected: true,
	}
	r := newAE(t, v,
		minAt(nodeC, 2*time.Hour, 50<<30))

	reconcileAE(t, r, nodeC)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if len(got.Spec.Clients) != 0 {
		t.Fatalf("dead client leg must be dropped: %+v", got.Spec.Clients)
	}
	if !slices.Contains(got.Finalizers, constants.FinalizerPrefix+nodeC) {
		t.Fatalf("dead node's teardown finalizer must stay for its return: %v", got.Finalizers)
	}
}

// With no spare node, a live diskless tie-breaker is flipped diskful in
// place (toggle-disk) so the volume returns to two data copies.
func TestAutoEvictFlipsTieBreakerWithoutSpare(t *testing.T) {
	// No node-d in the topology: the fold only sees the seeded CRs.
	v := evictVol()
	v.Spec.Replicas = append(v.Spec.Replicas,
		miroirv1alpha1.Replica{Node: nodeC, NodeID: 2, Address: addrC, Diskless: true})
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeC)
	r := newAE(t, v,
		minAt(nodeA, time.Minute, 50<<30),
		minAt(nodeB, 2*time.Hour, 50<<30),
		minAt(nodeC, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if slices.ContainsFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeB
	}) {
		t.Fatalf("dead replica must be removed: %+v", got.Spec.Replicas)
	}
	idx := slices.IndexFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeC
	})
	if idx < 0 {
		t.Fatalf("tie-breaker must remain: %+v", got.Spec.Replicas)
	}
	rep := got.Spec.Replicas[idx]
	if rep.Diskless || !rep.FullSync || rep.Backend != miroirv1alpha1.BackendZFS {
		t.Fatalf("tie-breaker must be flipped to a FullSync diskful replica: %+v", rep)
	}
	// The flip keeps the leg's live DRBD identity.
	if rep.NodeID != 2 || rep.Address != addrC {
		t.Fatalf("flip must keep node-id/address: %+v", rep)
	}
}

// With neither a spare node nor a tie-breaker, the volume is left alone
// and the pass re-checks later.
func TestAutoEvictNoSpareLeavesVolume(t *testing.T) {
	// Only the dead node is in the topology: no spare, no tie-breaker.
	r := newAE(t, evictVol(),
		minAt(nodeB, 2*time.Hour, 50<<30))

	res := reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if len(got.Spec.Replicas) != 2 {
		t.Fatalf("volume must be untouched without a spare: %+v", got.Spec.Replicas)
	}
	if res.RequeueAfter != evictRecheckInterval {
		t.Fatalf("must re-check later, got %v", res.RequeueAfter)
	}
}

// An opted-out node is expected to go dark: its stale heartbeat must not
// trip the multiple-stale valve and freeze eviction of a node that
// actually died.
func TestAutoEvictValveIgnoresOptedOutNodes(t *testing.T) {
	optOut := minAt(nodeC, 3*time.Hour, 50<<30) // dark for hours, by design
	no := false
	optOut.Spec.AutoEvict = &no
	r := newAE(t, evictVol(),
		minAt(nodeA, time.Minute, 50<<30),
		minAt(nodeB, 2*time.Hour, 50<<30), // genuinely dead
		optOut,
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if slices.ContainsFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeB
	}) {
		t.Fatalf("the opted-out node's staleness must not block the eviction: %+v", got.Spec.Replicas)
	}
}

// A Ready kubelet is proof of life independent of the agent: a
// crash-looping agent pod must not get the node's legs — replica or
// client — severed while the node is alive and doing IO.
func TestAutoEvictStandsDownWhenNodeReady(t *testing.T) {
	kubeletAlive := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeB},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
		}},
	}
	r := newAE(t, evictVol(), kubeletAlive,
		minAt(nodeB, 2*time.Hour, 50<<30),
		minAt(nodeD, time.Minute, 50<<30))

	res := reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if len(got.Spec.Replicas) != 2 {
		t.Fatalf("a Ready node must never be evicted: %+v", got.Spec)
	}
	if res.RequeueAfter != evictRecheckInterval {
		t.Fatalf("must re-check later, got %v", res.RequeueAfter)
	}
	if v := testutil.ToFloat64(metricEvictStanddown.WithLabelValues("node_ready")); v < 1 {
		t.Fatalf("stand-down counter must increment, got %v", v)
	}
}

// A NotReady Node object does not stand the pass down — that is the dead
// node the reconciler exists for.
func TestAutoEvictProceedsWhenNodeNotReady(t *testing.T) {
	kubeletDead := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeB},
		Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{
			{Type: corev1.NodeReady, Status: corev1.ConditionUnknown},
		}},
	}
	r := newAE(t, evictVol(), kubeletDead,
		minAt(nodeB, 2*time.Hour, 50<<30),
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if slices.ContainsFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeB
	}) {
		t.Fatalf("a NotReady node must still be evicted: %+v", got.Spec.Replicas)
	}
}

// One membership edit at a time: an entry still awaiting completion
// blocks the swap — acting on a half-completed spec races the membership
// reconciler's update.
func TestAutoEvictBlocksOnIncompleteMembershipChange(t *testing.T) {
	v := evictVol()
	v.Spec.Replicas = append(v.Spec.Replicas, miroirv1alpha1.Replica{Node: nodeC}) // bare, mid-completion
	r := newAE(t, v,
		minAt(nodeB, 2*time.Hour, 50<<30),
		minAt(nodeC, time.Minute, 50<<30),
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	if !slices.ContainsFunc(got.Spec.Replicas, func(rep miroirv1alpha1.Replica) bool {
		return rep.Node == nodeB
	}) {
		t.Fatalf("an in-flight membership change must block eviction: %+v", got.Spec.Replicas)
	}
}

// A swap with a surviving tie-breaker must insert the bare diskful
// replacement BEFORE the diskless leg: the CEL first-replica-diskful rule
// holds no matter which entry died.
func TestAutoEvictSwapKeepsDisklessLast(t *testing.T) {
	v := evictVol()
	v.Spec.Replicas = append(v.Spec.Replicas,
		miroirv1alpha1.Replica{Node: nodeC, NodeID: 2, Address: addrC, Diskless: true})
	v.Finalizers = append(v.Finalizers, constants.FinalizerPrefix+nodeC)
	r := newAE(t, v,
		minAt(nodeA, time.Minute, 50<<30),
		minAt(nodeB, 2*time.Hour, 50<<30),
		minAt(nodeC, time.Minute, 50<<30),
		minAt(nodeD, time.Minute, 50<<30))

	reconcileAE(t, r, nodeB)

	got := get(t, &Reconciler{Client: r.Client}, volPvc1)
	nodes := make([]string, 0, len(got.Spec.Replicas))
	for _, rep := range got.Spec.Replicas {
		nodes = append(nodes, rep.Node)
	}
	if !slices.Equal(nodes, []string{nodeA, nodeD, nodeC}) {
		t.Fatalf("replacement must precede the diskless leg: %v", nodes)
	}
	if got.Spec.Replicas[2].Node != nodeC || !got.Spec.Replicas[2].Diskless {
		t.Fatalf("tie-breaker must stay last and diskless: %+v", got.Spec.Replicas)
	}
}
