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

package drbd

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
)

func TestResolveSplitBrainWinnerReconnects(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	// node-a is node id 0 — the seed winner and the survivor. It disconnects
	// first (a bare connect aborts on a peer that already has a net-config)
	// then reconnects without discarding data.
	if err := d.ResolveSplitBrain(context.Background(), testResource(nodeA)); err != nil {
		t.Fatalf("ResolveSplitBrain: %v", err)
	}
	fe.calledWith(t, "drbdadm disconnect pvc-1")
	fe.calledWith(t, "drbdadm connect pvc-1")
	fe.notCalledWith(t, "discard-my-data")
}

func TestResolveSplitBrainLoserDiscards(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	// node-b is node id 1 — the loser: it must disconnect then reconnect
	// discarding its own generation so it becomes SyncTarget.
	if err := d.ResolveSplitBrain(context.Background(), testResource(nodeB)); err != nil {
		t.Fatalf("ResolveSplitBrain: %v", err)
	}
	fe.calledWith(t, "drbdadm disconnect pvc-1")
	fe.calledWith(t, "drbdadm connect --discard-my-data pvc-1")
}

func TestResolveSplitBrainDisklessReconnects(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	r := testResource(nodeB)
	r.LocalDiskless = true // a tie-breaker never discards data
	if err := d.ResolveSplitBrain(context.Background(), r); err != nil {
		t.Fatalf("ResolveSplitBrain: %v", err)
	}
	fe.calledWith(t, "drbdadm disconnect pvc-1")
	fe.calledWith(t, "drbdadm connect pvc-1")
	fe.notCalledWith(t, "discard-my-data")
}

// CyclePeerConnection disconnects then reconnects exactly the given peer,
// by node id, via drbdsetup (no config parse).
func TestCyclePeerConnection(t *testing.T) {
	fe := &fakeExec{}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := d.CyclePeerConnection(t.Context(), volPvc1, 1); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"drbdsetup disconnect " + volPvc1 + " 1",
		"drbdsetup connect " + volPvc1 + " 1",
	}
	if !slices.Equal(fe.calls, want) {
		t.Fatalf("want %v, got %v", want, fe.calls)
	}
}

// A failed disconnect must not be followed by a connect — the cycle is
// retried whole on a later pass.
func TestCyclePeerConnectionDisconnectFails(t *testing.T) {
	fe := &fakeExec{errOn: map[string]error{"disconnect": errors.New("exit status 11")}}
	d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
	if err := d.CyclePeerConnection(t.Context(), volPvc1, 1); err == nil {
		t.Fatal("want disconnect error")
	}
	fe.notCalledWith(t, "drbdsetup connect")
}

// stalledTargetJSON is an Inconsistent Secondary leg parked WFBitMapT
// toward its UpToDate peer (issue #576), as drbdsetup status reports it.
const stalledTargetJSON = `[{"name":"` + volPvc1 + `","role":"Secondary",
	"devices":[{"disk-state":"Inconsistent","quorum":true}],
	"connections":[{"peer-node-id":1,"connection-state":"Connected",
		"peer_devices":[{"replication-state":"WFBitMapT","peer-disk-state":"UpToDate","out-of-sync":0}]}]}]`

// The stalled-target cycle forces the disconnect only after the graceful
// one is refused and the leg re-reads as an Inconsistent Secondary; a leg
// promoted in between keeps its connection, and a failed force is not
// followed by a connect.
func TestCycleStalledTargetConnection(t *testing.T) {
	const disconnect = "disconnect"
	refused := errors.New("(-2) Need access to UpToDate data")
	for name, tc := range map[string]struct {
		errOnce map[string]error
		errOn   map[string]error
		status  string
		wantErr bool
		want    []string
	}{
		"graceful": {
			status: stalledTargetJSON,
			want:   []string{"drbdsetup disconnect " + volPvc1 + " 1", "drbdsetup connect " + volPvc1 + " 1"},
		},
		"refused then forced": {
			errOnce: map[string]error{disconnect: refused},
			status:  stalledTargetJSON,
			want: []string{"drbdsetup disconnect " + volPvc1 + " 1", "drbdsetup status --json " + volPvc1,
				"drbdsetup disconnect " + volPvc1 + " 1 --force", "drbdsetup connect " + volPvc1 + " 1"},
		},
		"promoted meanwhile": {
			errOnce: map[string]error{disconnect: refused},
			status:  strings.Replace(stalledTargetJSON, `"role":"Secondary"`, `"role":"Primary"`, 1),
			wantErr: true,
			want:    []string{"drbdsetup disconnect " + volPvc1 + " 1", "drbdsetup status --json " + volPvc1},
		},
		"force fails": {
			errOn:   map[string]error{disconnect: refused},
			status:  stalledTargetJSON,
			wantErr: true,
			want: []string{"drbdsetup disconnect " + volPvc1 + " 1", "drbdsetup status --json " + volPvc1,
				"drbdsetup disconnect " + volPvc1 + " 1 --force"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			fe := &fakeExec{errOnce: tc.errOnce, errOn: tc.errOn,
				responses: map[string]string{cmdDrbdsetupStatus: tc.status}}
			d := &Driver{StateDir: t.TempDir(), Exec: fe.run, Mknod: fakeMknod}
			err := d.CycleStalledTargetConnection(t.Context(), volPvc1, 1)
			if (err != nil) != tc.wantErr {
				t.Fatalf("wantErr %v, got %v", tc.wantErr, err)
			}
			if !slices.Equal(fe.calls, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, fe.calls)
			}
		})
	}
}
