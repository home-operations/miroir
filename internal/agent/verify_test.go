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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// statusUpToDate is a healthy, connected, idle status — the gate lets a
// verify through on this.
const statusUpToDate = `[{"name":"pvc-1",
	"devices":[{"disk-state":"UpToDate"}],
	"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`

// statusVerifying is the same pair mid online-verify.
const statusVerifying = `[{"name":"pvc-1",
	"devices":[{"disk-state":"UpToDate"}],
	"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"replication-state":"VerifyT"}]}]}]`

// statusDone is a completed verify reporting 256 KiB out of sync.
const statusDone = `[{"name":"pvc-1",
	"devices":[{"disk-state":"UpToDate"}],
	"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"replication-state":"Established","out-of-sync":256}]}]}]`

// statusClean is a completed verify with nothing out of sync.
const statusClean = `[{"name":"pvc-1",
	"devices":[{"disk-state":"UpToDate"}],
	"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"replication-state":"Established","out-of-sync":0}]}]}]`

// statusAborted is a verify cut short by a peer disconnect: no longer
// verifying, but the pair is down and the out-of-sync count mixes any
// findings with bits accrued while the peer is away.
const statusAborted = `[{"name":"pvc-1",
	"devices":[{"disk-state":"UpToDate"}],
	"connections":[{"peer-node-id":1,"connection-state":"Connecting",
		"peer_devices":[{"replication-state":"Off","out-of-sync":512}]}]}]`

func replicatedVol() *miroirv1alpha1.MiroirVolume {
	v := vol(volPvc1, nodeA, nodeB)
	v.Spec.DRBD = &miroirv1alpha1.DRBDSpec{Port: 7000}
	v.Spec.Replicas[0].NodeID = 0
	v.Spec.Replicas[0].Address = addrA
	v.Spec.Replicas[1].NodeID = 1
	v.Spec.Replicas[1].Address = addrB
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true},
	}
	return v
}

func newVerifyScheduler(t *testing.T, node string, c client.Client, fe *fakeDRBDExec, rec events.EventRecorder) *VerifyScheduler {
	t.Helper()
	return &VerifyScheduler{
		Client:       c,
		NodeName:     node,
		DRBD:         &drbd.Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: func(string, uint32, int) error { return nil }},
		Recorder:     rec,
		PollInterval: time.Millisecond,
	}
}

func newClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&miroirv1alpha1.MiroirVolume{}).
		Build()
}

func getVol(t *testing.T, c client.Client) *miroirv1alpha1.MiroirVolume {
	t.Helper()
	got := &miroirv1alpha1.MiroirVolume{}
	if err := c.Get(t.Context(), types.NamespacedName{Name: volPvc1}, got); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestVerifySkipsNonCoordinator(t *testing.T) {
	v := replicatedVol()
	c := newClient(t, v)
	fe := &fakeDRBDExec{statusJSON: statusUpToDate}
	// Scheduler runs on node-b, but the coordinator (first diskful replica) is
	// node-a — node-b must not initiate.
	vs := newVerifyScheduler(t, nodeB, c, fe, nil)

	if err := vs.runOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "drbdadm verify")
}

func TestVerifySkipsUnreplicated(t *testing.T) {
	v := vol(volPvc1, nodeA) // no DRBD
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{nodeA: {DeviceCreated: true}}
	c := newClient(t, v)
	fe := &fakeDRBDExec{statusJSON: statusUpToDate}
	vs := newVerifyScheduler(t, nodeA, c, fe, nil)

	if err := vs.runOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	fe.notCalledWith(t, "drbdadm verify")
}

func TestVerifySkipsUnhealthy(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
	}{
		{"resyncing", `[{"name":"pvc-1","devices":[{"disk-state":"UpToDate"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected",
				"peer_devices":[{"replication-state":"SyncTarget"}]}]}]`},
		{"already-verifying", statusVerifying},
		{"not-uptodate", `[{"name":"pvc-1","devices":[{"disk-state":"Inconsistent"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`},
		{"disconnected", `[{"name":"pvc-1","devices":[{"disk-state":"UpToDate"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connecting"}]}]`},
		{"suspended", `[{"name":"pvc-1","suspended-user":true,"devices":[{"disk-state":"UpToDate"}],
			"connections":[{"peer-node-id":1,"connection-state":"Connected"}]}]`},
		{"split-brain", `[{"name":"pvc-1","devices":[{"disk-state":"UpToDate"}],
			"connections":[{"peer-node-id":1,"connection-state":"StandAlone"}]}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := replicatedVol()
			c := newClient(t, v)
			fe := &fakeDRBDExec{statusJSON: tc.status}
			vs := newVerifyScheduler(t, nodeA, c, fe, nil)

			if err := vs.runOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			fe.notCalledWith(t, "drbdadm verify")
			if got := getVol(t, c).Status.PerNode[nodeA].LastVerifyTime; got != nil {
				t.Fatalf("skipped verify must not record a result, got %v", got)
			}
		})
	}
}

func TestVerifyHappyPathRecordsCleanResult(t *testing.T) {
	v := replicatedVol()
	c := newClient(t, v)
	// gate → in-flight → complete-clean.
	fe := &fakeDRBDExec{statusSeq: []string{statusUpToDate, statusVerifying, statusClean}}
	rec := events.NewFakeRecorder(4)
	vs := newVerifyScheduler(t, nodeA, c, fe, rec)

	if err := vs.runOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm verify pvc-1")

	st := getVol(t, c).Status.PerNode[nodeA]
	if st.LastVerifyTime == nil {
		t.Fatal("a completed verify must record LastVerifyTime")
	}
	if st.LastVerifyOutOfSyncBytes == nil || *st.LastVerifyOutOfSyncBytes != 0 {
		t.Fatalf("clean verify must record 0 out-of-sync bytes, got %v", st.LastVerifyOutOfSyncBytes)
	}
	if v := testutil.ToFloat64(metricVerifyOutOfSyncBytes.WithLabelValues(volPvc1, miroirv1alpha1.DefaultPoolName, volPvc1, "")); v != 0 {
		t.Fatalf("verify oos gauge = %v, want 0", v)
	}
	select {
	case e := <-rec.Events:
		t.Fatalf("clean verify must not emit an event, got %q", e)
	default:
	}
}

func TestVerifyOutOfSyncRecordsAndEvents(t *testing.T) {
	v := replicatedVol()
	c := newClient(t, v)
	fe := &fakeDRBDExec{statusSeq: []string{statusUpToDate, statusVerifying, statusDone}}
	rec := events.NewFakeRecorder(4)
	vs := newVerifyScheduler(t, nodeA, c, fe, rec)

	if err := vs.runOnce(t.Context()); err != nil {
		t.Fatal(err)
	}

	st := getVol(t, c).Status.PerNode[nodeA]
	if st.LastVerifyOutOfSyncBytes == nil || *st.LastVerifyOutOfSyncBytes != 256*1024 {
		t.Fatalf("verify must record 256 KiB out of sync, got %v", st.LastVerifyOutOfSyncBytes)
	}
	if v := testutil.ToFloat64(metricVerifyOutOfSyncBytes.WithLabelValues(volPvc1, miroirv1alpha1.DefaultPoolName, volPvc1, "")); v != 256*1024 {
		t.Fatalf("verify oos gauge = %v, want %d", v, 256*1024)
	}
	select {
	case <-rec.Events:
	default:
		t.Fatal("a dirty verify must emit a VerifyOutOfSync event")
	}
}

// A peer disconnect mid-pass aborts the verify and leaves an out-of-sync
// count that mixes findings with disconnect-accrued bits — the result is
// unattributable and must be discarded, not recorded or alerted on.
func TestVerifyDiscardsResultWhenPeerDropsMidPass(t *testing.T) {
	v := replicatedVol()
	c := newClient(t, v)
	fe := &fakeDRBDExec{statusSeq: []string{statusUpToDate, statusVerifying, statusAborted}}
	rec := events.NewFakeRecorder(4)
	vs := newVerifyScheduler(t, nodeA, c, fe, rec)

	if err := vs.runOnce(t.Context()); err != nil {
		t.Fatal(err)
	}
	fe.calledWith(t, "drbdadm verify pvc-1")

	st := getVol(t, c).Status.PerNode[nodeA]
	if st.LastVerifyTime != nil {
		t.Fatalf("an aborted verify must not record a result, got %v", st.LastVerifyTime)
	}
	if st.VerifyStartedAt != nil {
		t.Fatal("a discarded verify must drop the started marker: its bits heal on the reconnect")
	}
	select {
	case e := <-rec.Events:
		t.Fatalf("an aborted verify must not emit an event, got %q", e)
	default:
	}
}

func TestVerifyContextCancelStopsCleanly(t *testing.T) {
	v := replicatedVol()
	c := newClient(t, v)
	// Always in-flight: the poll loop only leaves via ctx cancellation.
	fe := &fakeDRBDExec{statusJSON: statusVerifying, statusSeq: []string{statusUpToDate}}
	vs := newVerifyScheduler(t, nodeA, c, fe, nil)

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancelled before the poll loop runs

	if err := vs.verifyVolume(ctx, v); err != nil {
		t.Fatalf("cancellation is not a failure, got %v", err)
	}
	fe.calledWith(t, "drbdadm verify pvc-1")
	// A running kernel verify must never be aborted on shutdown — that needs
	// a disconnect, which would resync.
	fe.notCalledWith(t, "disconnect")
	fe.notCalledWith(t, "drbdadm down")
	st := getVol(t, c).Status.PerNode[nodeA]
	if st.LastVerifyTime != nil {
		t.Fatalf("an interrupted verify must not record a result, got %v", st.LastVerifyTime)
	}
	// The kernel may finish the verify with findings while no agent is
	// watching: the marker must outlive the shutdown so the stale-bitmap
	// self-heal keeps off until the next sweep records a result.
	if st.VerifyStartedAt == nil {
		t.Fatal("an interrupted verify must leave the started marker in place")
	}
}

// A marker left by an earlier run (shutdown mid-pass) guards findings the
// kernel may still hold unrecorded. A later sweep's kick failing must not
// withdraw it; only a marker the failed call itself added is withdrawn.
func TestVerifyFailedKickKeepsEarlierMarker(t *testing.T) {
	v := replicatedVol()
	earlier := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, VerifyStartedAt: &earlier},
	}
	c := newClient(t, v)
	fe := &fakeDRBDExec{statusJSON: statusUpToDate, errOn: map[string]error{"drbdadm verify": errors.New("refused")}}
	vs := newVerifyScheduler(t, nodeA, c, fe, nil)

	if err := vs.verifyVolume(t.Context(), v); err == nil {
		t.Fatal("a refused kick must surface as an error")
	}
	st := getVol(t, c).Status.PerNode[nodeA]
	if st.VerifyStartedAt == nil || !st.VerifyStartedAt.Equal(&earlier) {
		t.Fatalf("an earlier run's marker must survive a failed kick unchanged, got %v", st.VerifyStartedAt)
	}

	// The same failure on an unmarked slot withdraws the marker it added.
	fresh := replicatedVol()
	c2 := newClient(t, fresh)
	vs2 := newVerifyScheduler(t, nodeA, c2, fe, nil)
	if err := vs2.verifyVolume(t.Context(), fresh); err == nil {
		t.Fatal("a refused kick must surface as an error")
	}
	if got := getVol(t, c2).Status.PerNode[nodeA].VerifyStartedAt; got != nil {
		t.Fatalf("a failed kick must withdraw the marker it added, got %v", got)
	}
}

// The started marker brackets the verify: set before the kick, gone once
// the result is recorded, with the previous record carried across so
// marking a start never erases the last outcome.
func TestVerifyStartedMarkerBracketsTheRun(t *testing.T) {
	v := replicatedVol()
	prevFinding := int64(4096)
	prevTime := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	v.Status.PerNode = map[string]miroirv1alpha1.ReplicaStatus{
		nodeA: {DeviceCreated: true, LastVerifyTime: &prevTime, LastVerifyOutOfSyncBytes: &prevFinding},
	}
	c := newClient(t, v)
	fe := &fakeDRBDExec{statusJSON: statusVerifying, statusSeq: []string{statusUpToDate}}
	vs := newVerifyScheduler(t, nodeA, c, fe, nil)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := vs.verifyVolume(ctx, v); err != nil {
		t.Fatal(err)
	}
	st := getVol(t, c).Status.PerNode[nodeA]
	if st.VerifyStartedAt == nil {
		t.Fatal("the started marker must be set before the kick")
	}
	if st.LastVerifyOutOfSyncBytes == nil || *st.LastVerifyOutOfSyncBytes != prevFinding || st.LastVerifyTime == nil {
		t.Fatalf("marking a start must carry the previous record, got %+v", st)
	}

	// A completed pass records the result and drops the marker.
	fe2 := &fakeDRBDExec{statusSeq: []string{statusUpToDate, statusDone}}
	vs2 := newVerifyScheduler(t, nodeA, c, fe2, nil)
	if err := vs2.verifyVolume(t.Context(), getVol(t, c)); err != nil {
		t.Fatal(err)
	}
	st = getVol(t, c).Status.PerNode[nodeA]
	if st.VerifyStartedAt != nil {
		t.Fatal("recording the result must drop the started marker")
	}
	if st.LastVerifyOutOfSyncBytes == nil || *st.LastVerifyOutOfSyncBytes != 256*1024 {
		t.Fatalf("the completed pass must record its own result, got %v", st.LastVerifyOutOfSyncBytes)
	}
}
