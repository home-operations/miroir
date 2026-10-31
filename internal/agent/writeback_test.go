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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

const (
	lostAsyncWrite  = "3,9001,1000,-;Buffer I/O error on dev drbd1036, logical block 2, lost async page write"
	ext4Mounted     = "6,9002,2000,-;EXT4-fs (drbd1036): mounted filesystem 1b6c1a5e-0000-0000-0000-000000000000 r/w with ordered data mode. Quota mode: none."
	ext4Unmounting  = "6,9003,3000,-;EXT4-fs (drbd1036): unmounting filesystem 1b6c1a5e-0000-0000-0000-000000000000."
	drbd1036Mounted = "824 25 147:1036 / /var/lib/kubelet/plugins/kubernetes.io/csi/miroir/x/globalmount rw - ext4 /dev/drbd1036 rw\n"
	otherMountsOnly = "36 25 0:35 / /run rw - tmpfs tmpfs rw\n"
)

func TestParseFSEvent(t *testing.T) {
	cases := []struct {
		line  string
		minor int32
		want  fsEvent
	}{
		{lostAsyncWrite, 1036, fsEventLostWrite},
		{"3,1,1,-;Buffer I/O error on dev drbd1000, logical block 7, lost sync page write", 1000, fsEventLostWrite},
		{ext4Mounted, 1036, fsEventMounted},
		{ext4Unmounting, 1036, fsEventUnmounted},
		// A failed read leaves no writeback error behind.
		{"3,1,1,-;Buffer I/O error on dev drbd1036, logical block 2, async page read", 0, fsEventNone},
		{"3,1,1,-;Buffer I/O error on dev sda1, logical block 2, lost async page write", 0, fsEventNone},
		{"3,1,1,-;Buffer I/O error on dev drbd1036p1, logical block 2, lost async page write", 0, fsEventNone},
		{"3,1,1,-;EXT4-fs (sda1): mounted filesystem x r/w", 0, fsEventNone},
		{"3,1,1,-;EXT4-fs (drbd1036): re-mounted x.", 0, fsEventNone},
		{"3,1,1,-;EXT4-fs error (device drbd1036): ext4_journal_check_start:87: Detected aborted journal", 0, fsEventNone},
		// Userspace priorities (8+) are forgeable through /dev/kmsg writes.
		{"12,1,1,-;Buffer I/O error on dev drbd1036, logical block 2, lost async page write", 0, fsEventNone},
		{" SUBSYSTEM=block", 0, fsEventNone},
		{"", 0, fsEventNone},
	}
	for _, tc := range cases {
		t.Run(tc.line, func(t *testing.T) {
			minor, ev := parseFSEvent(tc.line)
			if ev != tc.want || minor != tc.minor {
				t.Errorf("parseFSEvent(%q) = (%d, %v), want (%d, %v)", tc.line, minor, ev, tc.minor, tc.want)
			}
		})
	}
}

// writebackVol is pvc-1 with legs on nodeA (minor 1036) and node-b; erred
// lists the nodes whose slot already carries a WritebackError.
func writebackVol(erred ...string) *miroirv1alpha1.MiroirVolume {
	vol := &miroirv1alpha1.MiroirVolume{
		ObjectMeta: metav1.ObjectMeta{Name: volPvc1},
		Status: miroirv1alpha1.MiroirVolumeStatus{
			PerNode: map[string]miroirv1alpha1.ReplicaStatus{
				nodeA: {DRBDMinor: 1036},
				nodeB: {DRBDMinor: 1036},
			},
		},
	}
	for _, node := range erred {
		st := vol.Status.PerNode[node]
		st.WritebackError = &miroirv1alpha1.WritebackError{Since: metav1.Now(), Record: "lost write"}
		vol.Status.PerNode[node] = st
	}
	if len(erred) > 0 {
		setRestageCondition(vol)
	}
	return vol
}

// writebackFixture is a fake /proc: one process seeing mountinfo, and
// btime at booted.
func writebackFixture(t *testing.T, mountinfo string, booted time.Time) string {
	t.Helper()
	dir := procFixture(t, map[int]string{1: mountinfo})
	stat := fmt.Sprintf("cpu  1 2 3 4\nbtime %d\nprocesses 1\n", booted.Unix())
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(stat), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func newWritebackWatcher(c client.Client, procDir string) (*WritebackWatcher, *events.FakeRecorder) {
	rec := events.NewFakeRecorder(4)
	return &WritebackWatcher{Client: c, NodeName: nodeA, Recorder: rec, ProcDir: procDir}, rec
}

func restageCond(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	return meta.FindStatusCondition(getVol(t, c).Status.Conditions, miroirv1alpha1.ConditionFilesystemNeedsRestage)
}

func writebackErrorOn(t *testing.T, c client.Client, node string) *miroirv1alpha1.WritebackError {
	t.Helper()
	return getVol(t, c).Status.PerNode[node].WritebackError
}

func TestWritebackWatcherFlagsALostWriteUnderAMount(t *testing.T) {
	metricNeedsRestage.Reset()
	vol := writebackVol()
	meta.SetStatusCondition(&vol.Status.Conditions, metav1.Condition{
		Type: miroirv1alpha1.ConditionNodeUnreachable, Status: metav1.ConditionFalse, Reason: "NodeReachable",
	})
	c := newClient(t, vol)
	w, rec := newWritebackWatcher(c, writebackFixture(t, drbd1036Mounted, time.Now().Add(-time.Hour)))

	w.handle(t.Context(), logr.Discard(), []string{lostAsyncWrite})

	we := writebackErrorOn(t, c, nodeA)
	if we == nil || !strings.Contains(we.Record, "logical block 2") {
		t.Fatalf("writebackError = %+v, want the kernel record", we)
	}
	cond := restageCond(t, c)
	if cond == nil || cond.Status != metav1.ConditionTrue || !strings.Contains(cond.Message, nodeA) ||
		!strings.Contains(cond.Message, "restart the consuming pod") {
		t.Fatalf("condition = %+v, want True naming %s and the consuming pod", cond, nodeA)
	}
	if meta.FindStatusCondition(getVol(t, c).Status.Conditions, miroirv1alpha1.ConditionNodeUnreachable) == nil {
		t.Fatal("the update must keep the volume's other conditions")
	}
	select {
	case e := <-rec.Events:
		if !strings.Contains(e, "Warning "+miroirv1alpha1.ConditionFilesystemNeedsRestage) {
			t.Fatalf("event = %q, want a FilesystemNeedsRestage warning", e)
		}
	default:
		t.Fatal("raising the error must emit a warning event")
	}
	if got := testutil.ToFloat64(metricNeedsRestage.WithLabelValues(volPvc1, volPvc1, "")); got != 1 {
		t.Fatalf("needs_restage gauge = %v, want 1", got)
	}

	// A second lost write on the flagged mount neither re-raises nor re-warns.
	w.handle(t.Context(), logr.Discard(), []string{lostAsyncWrite})
	select {
	case e := <-rec.Events:
		t.Fatalf("an already-flagged mount must not warn again, got %q", e)
	default:
	}
}

// An RWX volume's filesystem is mounted by its gateway, so the advice must
// not point at the NFS consumers.
func TestWritebackWatcherPointsRWXAtTheGateway(t *testing.T) {
	vol := writebackVol()
	vol.Spec.Export = &miroirv1alpha1.ExportSpec{}
	c := newClient(t, vol)
	w, _ := newWritebackWatcher(c, writebackFixture(t, drbd1036Mounted, time.Now().Add(-time.Hour)))
	w.handle(t.Context(), logr.Discard(), []string{lostAsyncWrite})
	if cond := restageCond(t, c); cond == nil || !strings.Contains(cond.Message, "NFS gateway pod") {
		t.Fatalf("condition = %+v, want the gateway named", cond)
	}
}

func TestWritebackWatcherSkips(t *testing.T) {
	cases := []struct {
		name      string
		mountinfo string
		lines     []string
	}{
		{"unmounted device", otherMountsOnly, []string{lostAsyncWrite}},
		{"lost write before a fresh mount", drbd1036Mounted, []string{lostAsyncWrite, ext4Mounted}},
		{"lost write before the unmount", drbd1036Mounted, []string{lostAsyncWrite, ext4Unmounting}},
		{"device of no volume here", drbd1036Mounted,
			[]string{"3,1,1,-;Buffer I/O error on dev drbd1999, logical block 2, lost async page write"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, writebackVol())
			w, _ := newWritebackWatcher(c, writebackFixture(t, tc.mountinfo, time.Now().Add(-time.Hour)))
			w.handle(t.Context(), logr.Discard(), tc.lines)
			if we := writebackErrorOn(t, c, nodeA); we != nil {
				t.Fatalf("writebackError = %+v, want none", we)
			}
			if cond := restageCond(t, c); cond != nil {
				t.Fatalf("condition = %+v, want none", cond)
			}
			if len(w.pending) != 0 {
				t.Fatalf("pending = %v, want the event settled", w.pending)
			}
		})
	}
}

func TestWritebackWatcherClears(t *testing.T) {
	cases := []struct {
		name      string
		mountinfo string
		booted    time.Time
		lines     []string
	}{
		{"fresh superblock", drbd1036Mounted, time.Now().Add(-time.Hour), []string{ext4Mounted}},
		{"superblock torn down", drbd1036Mounted, time.Now().Add(-time.Hour), []string{ext4Unmounting}},
		{"no mount left", otherMountsOnly, time.Now().Add(-time.Hour), nil},
		{"rebooted since", drbd1036Mounted, time.Now().Add(time.Hour), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			metricNeedsRestage.Reset()
			c := newClient(t, writebackVol(nodeA))
			w, _ := newWritebackWatcher(c, writebackFixture(t, tc.mountinfo, tc.booted))
			w.handle(t.Context(), logr.Discard(), tc.lines)
			if we := writebackErrorOn(t, c, nodeA); we != nil {
				t.Fatalf("writebackError = %+v, want cleared", we)
			}
			cond := restageCond(t, c)
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonRestaged {
				t.Fatalf("condition = %+v, want False/%s", cond, reasonRestaged)
			}
			if n := testutil.CollectAndCount(metricNeedsRestage); n != 0 {
				t.Fatalf("needs_restage series = %d, want none once cleared", n)
			}
		})
	}
}

// Each node's entry is its own: node-b's unresolved error survives this
// node raising and then clearing its own, and keeps the condition True.
func TestWritebackWatcherKeepsAnotherNodesError(t *testing.T) {
	metricNeedsRestage.Reset()
	c := newClient(t, writebackVol(nodeB))
	w, _ := newWritebackWatcher(c, writebackFixture(t, drbd1036Mounted, time.Now().Add(-time.Hour)))

	w.handle(t.Context(), logr.Discard(), []string{lostAsyncWrite})
	if cond := restageCond(t, c); cond == nil || !strings.Contains(cond.Message, nodeA+", "+nodeB) {
		t.Fatalf("condition = %+v, want both nodes named", cond)
	}

	w.handle(t.Context(), logr.Discard(), []string{ext4Mounted})
	if writebackErrorOn(t, c, nodeA) != nil {
		t.Fatal("this node's remount must clear its own entry")
	}
	if writebackErrorOn(t, c, nodeB) == nil {
		t.Fatal("node-b's entry must survive this node's clear")
	}
	if cond := restageCond(t, c); cond == nil || cond.Status != metav1.ConditionTrue || strings.Contains(cond.Message, nodeA) {
		t.Fatalf("condition = %+v, want True naming node-b only", cond)
	}
	if n := testutil.CollectAndCount(metricNeedsRestage); n != 0 {
		t.Fatalf("needs_restage series = %d, want none on this node", n)
	}
}

// laggingCache lists from a stale informer while every read through the
// API reader and every write reaches the live store.
type laggingCache struct {
	client.Client
	stale client.Client
}

func (l laggingCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return l.stale.List(ctx, list, opts...)
}

// The informer can lag this watcher's own raise. A sweep in that window
// must keep the flag, and a remount seen in it must still clear the entry.
func TestWritebackWatcherSurvivesALaggingCache(t *testing.T) {
	metricNeedsRestage.Reset()
	live := newClient(t, writebackVol())
	w, _ := newWritebackWatcher(live, writebackFixture(t, drbd1036Mounted, time.Now().Add(-time.Hour)))
	w.handle(t.Context(), logr.Discard(), []string{lostAsyncWrite})
	if writebackErrorOn(t, live, nodeA) == nil {
		t.Fatal("the raise must land")
	}

	w.Client = laggingCache{Client: live, stale: newClient(t, writebackVol())}
	w.Reader = live
	w.lastSweep = time.Time{}
	w.handle(t.Context(), logr.Discard(), nil)
	if !w.flagged[volPvc1] {
		t.Fatal("a sweep over a lagging cache must keep the flag")
	}
	if n := testutil.CollectAndCount(metricNeedsRestage); n != 1 {
		t.Fatalf("needs_restage series = %d, want 1", n)
	}

	w.handle(t.Context(), logr.Discard(), []string{ext4Mounted})
	if writebackErrorOn(t, live, nodeA) != nil {
		t.Fatal("a remount seen while the cache lags must still clear the entry")
	}
}

// A volume deleted between the cached lookup and the write is settled,
// not retried, and leaves no gauge behind.
func TestWritebackWatcherDropsADeletedVolume(t *testing.T) {
	metricNeedsRestage.Reset()
	cached := newClient(t, writebackVol())
	w, rec := newWritebackWatcher(cached, writebackFixture(t, drbd1036Mounted, time.Now().Add(-time.Hour)))
	w.Reader = newClient(t)
	w.handle(t.Context(), logr.Discard(), []string{lostAsyncWrite})
	if len(w.pending) != 0 {
		t.Fatalf("pending = %v, want the event settled", w.pending)
	}
	if w.flagged[volPvc1] {
		t.Fatal("a deleted volume must not be flagged")
	}
	select {
	case e := <-rec.Events:
		t.Fatalf("a deleted volume must not warn, got %q", e)
	default:
	}
}

// The sweep publishes the gauge for an entry raised before the agent
// started, so a restart does not lose the signal.
func TestWritebackWatcherSweepPublishesExistingError(t *testing.T) {
	metricNeedsRestage.Reset()
	c := newClient(t, writebackVol(nodeA))
	w, _ := newWritebackWatcher(c, writebackFixture(t, drbd1036Mounted, time.Now().Add(-time.Hour)))
	w.handle(t.Context(), logr.Discard(), nil)
	if writebackErrorOn(t, c, nodeA) == nil {
		t.Fatal("the entry must stay while the mount lives")
	}
	if got := testutil.ToFloat64(metricNeedsRestage.WithLabelValues(volPvc1, volPvc1, "")); got != 1 {
		t.Fatalf("needs_restage gauge = %v, want 1", got)
	}
}
