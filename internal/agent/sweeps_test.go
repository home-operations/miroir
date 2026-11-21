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
	"slices"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

func TestResumeStaleBarriers(t *testing.T) {
	const suspendedStatus = `[{"name":"pvc-1","suspended-user":true}]`
	now := metav1.Now()
	liveRound := &miroirv1alpha1.MiroirSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-1"},
		Spec:       miroirv1alpha1.MiroirSnapshotSpec{VolumeName: "pvc-1"},
		Status:     miroirv1alpha1.MiroirSnapshotStatus{IOSuspended: true, SuspendedAt: &now},
	}
	tests := []struct {
		name       string
		status     string
		objs       []client.Object
		wantLists  bool
		wantResume bool
	}{
		{name: "nothing suspended reads no API", status: "[]"},
		{name: "barrier of a live round stays", status: suspendedStatus, objs: []client.Object{liveRound}, wantLists: true},
		{name: "barrier no round records is lifted", status: suspendedStatus, wantLists: true, wantResume: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := runtime.NewScheme()
			if err := miroirv1alpha1.AddToScheme(s); err != nil {
				t.Fatal(err)
			}
			lists := 0
			c := fake.NewClientBuilder().WithScheme(s).WithObjects(tt.objs...).
				WithInterceptorFuncs(interceptor.Funcs{
					List: func(ctx context.Context, c client.WithWatch, l client.ObjectList, o ...client.ListOption) error {
						lists++
						return c.List(ctx, l, o...)
					},
				}).Build()
			var calls []string
			// The kernel must be read before any round is listed.
			statusBeforeLists := false
			exec := func(_ context.Context, name string, args ...string) (string, error) {
				line := name + " " + strings.Join(args, " ")
				calls = append(calls, line)
				if strings.HasPrefix(line, "drbdsetup status") {
					statusBeforeLists = lists == 0
					return tt.status, nil
				}
				return "", nil
			}
			driver := &drbd.Driver{StateDir: t.TempDir(), Exec: exec}
			if err := ResumeStaleBarriers(t.Context(), c, driver, nil, "node-a", time.Second); err != nil {
				t.Fatal(err)
			}
			if !statusBeforeLists {
				t.Fatal("the kernel view must be read before the rounds are listed")
			}
			if (lists > 0) != tt.wantLists {
				t.Fatalf("API lists = %d, want any: %v", lists, tt.wantLists)
			}
			if got := slices.Contains(calls, "drbdadm resume-io pvc-1"); got != tt.wantResume {
				t.Fatalf("resume-io issued = %v, want %v (calls %v)", got, tt.wantResume, calls)
			}
		})
	}
}
