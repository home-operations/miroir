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
	"testing"
	"time"

	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/drbd"
)

func TestPlanTeardownError(t *testing.T) {
	busy := fmt.Errorf("drbdsetup down pvc-1: held open: %w", backend.ErrBusy)
	wedged := fmt.Errorf("down pvc-1: %w", drbd.ErrWedged)
	stranded := fmt.Errorf("down pvc-1: %w", drbd.ErrDownStranded)
	nodeWedged := fmt.Errorf("lvremove: %w", backend.ErrNodeWedged)
	other := errors.New("lvremove: exit status 5")

	cases := []struct {
		name        string
		err         error
		slotMessage string
		busyFails   int
		want        teardownPlan
	}{
		{
			name: "an unclassified error surfaces and resets the busy streak",
			err:  other, busyFails: busyFailLimit - 1,
			want: teardownPlan{step: teardownFail},
		},
		{
			name: "the first busy outcome takes the fast retry",
			err:  busy,
			want: teardownPlan{step: teardownRetryBusy, busyFails: 1, requeue: 10 * time.Second},
		},
		{
			name: "the last busy outcome inside the budget still takes the fast retry",
			err:  busy, busyFails: busyFailLimit - 2,
			want: teardownPlan{step: teardownRetryBusy, busyFails: busyFailLimit - 1, requeue: 10 * time.Second},
		},
		{
			name: "the busy outcome that crosses the limit parks, announces and stamps",
			err:  busy, busyFails: busyFailLimit - 1,
			want: teardownPlan{
				step: teardownParkBusy, busyFails: busyFailLimit, requeue: busyRetryAfter,
				announce: true, stamp: true,
			},
		},
		{
			name: "a parked busy outcome stays quiet once its cause is on the slot",
			err:  busy, busyFails: busyFailLimit, slotMessage: busy.Error(),
			want: teardownPlan{step: teardownParkBusy, busyFails: busyFailLimit + 1, requeue: busyRetryAfter},
		},
		{
			name: "a parked busy outcome whose cause changed restamps without announcing again",
			err:  busy, busyFails: busyFailLimit, slotMessage: "an older cause",
			want: teardownPlan{
				step: teardownParkBusy, busyFails: busyFailLimit + 1, requeue: busyRetryAfter,
				stamp: true,
			},
		},
		{
			name: "a crossing whose cause is already on the slot announces without a status write",
			err:  busy, busyFails: busyFailLimit - 1, slotMessage: busy.Error(),
			want: teardownPlan{
				step: teardownParkBusy, busyFails: busyFailLimit, requeue: busyRetryAfter,
				announce: true,
			},
		},
		{
			name: "a wedge parks at once, announces, and resets the busy streak",
			err:  wedged, busyFails: busyFailLimit - 1,
			want: teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue, announce: true, stamp: true},
		},
		{
			name: "a wedge already on the slot stays parked and quiet",
			err:  wedged, slotMessage: wedged.Error(),
			want: teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue},
		},
		{
			name: "a stranded down is a wedge",
			err:  stranded,
			want: teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue, announce: true, stamp: true},
		},
		{
			name: "an error that is both wedged and busy is a wedge",
			err:  fmt.Errorf("%w: %w", backend.ErrBusy, drbd.ErrWedged),
			want: teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue, announce: true, stamp: true},
		},
		{
			name: "a node-wedged refusal is a wedge, not a recovery",
			err:  nodeWedged, slotMessage: wedged.Error(),
			want: teardownPlan{step: teardownParkWedged, requeue: wedgedRequeue, announce: true, stamp: true},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := planTeardownError(tc.err, tc.slotMessage, tc.busyFails); got != tc.want {
				t.Errorf("plan = %+v, want %+v", got, tc.want)
			}
		})
	}
}
