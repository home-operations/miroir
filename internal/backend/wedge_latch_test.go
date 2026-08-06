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

package backend

import (
	"errors"
	"strings"
	"testing"
)

func TestLatchTripsRegardlessOfLimit(t *testing.T) {
	w, _ := stuckSet(3)
	w.Latch("kernel log assertion: drbd pvc-1: ASSERTION i >= 0 FAILED in put_ldev")
	if !w.Tripped() {
		t.Fatal("a latched kernel fault must trip the breaker below the stranded-children limit")
	}
}

func TestLatchSurvivesChildrenDraining(t *testing.T) {
	w, stuck := stuckSet(1, 10)
	w.record(10, "drbdsetup down pvc-a")
	w.Latch("kernel log assertion: drbd pvc-1: ASSERTION FAILED")
	delete(stuck, 10)
	if !w.Tripped() {
		t.Fatal("a kernel assertion must stay latched: the damaged refcount survives every child draining, and only a reboot clears it")
	}
	if got := w.Stranded(); got != 0 {
		t.Fatalf("Stranded() = %d, want 0: the latch is not a stranded child", got)
	}
}

func TestLatchErrNamesTheReason(t *testing.T) {
	w, _ := stuckSet(3)
	reason := "kernel log assertion: drbd pvc-1: ASSERTION i >= 0 FAILED in put_ldev"
	w.Latch(reason)
	err := w.Err()
	if !errors.Is(err, ErrNodeWedged) {
		t.Fatalf("Err() = %v, want it to wrap ErrNodeWedged", err)
	}
	if !strings.Contains(err.Error(), reason) {
		t.Fatalf("Err() = %q, want it to name the latch reason %q", err, reason)
	}
}

func TestLatchIsIdempotent(t *testing.T) {
	w, _ := stuckSet(3)
	w.Latch("one")
	w.Latch("two")
	if got := w.Commands(); len(got) != 1 {
		t.Fatalf("Commands() = %v, want the first latch reason kept: the storm repeats endlessly, the cause does not change", got)
	}
}

// A latch must not trip StrandedTripped.
func TestLatchDoesNotTripStranded(t *testing.T) {
	w, _ := stuckSet(3)
	w.Latch("kernel log assertion: drbd pvc-1: ASSERTION FAILED in put_ldev")
	if !w.Tripped() {
		t.Fatal("a latch must trip the command breaker")
	}
	if w.StrandedTripped() {
		t.Fatal("a latch must not trip StrandedTripped")
	}
}
