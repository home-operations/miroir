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
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
)

const poolGiB = 1 << 30

func newPublisher(t *testing.T, fb *fakeBackend, rec events.EventRecorder) (*PoolStatsPublisher, func() *miroirv1alpha1.MiroirNode) {
	t.Helper()
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&miroirv1alpha1.MiroirNode{}).
		Build()
	p := &PoolStatsPublisher{
		Client:      c,
		NodeName:    nodeKharkiv,
		Backend:     fb,
		BackendType: miroirv1alpha1.BackendLVMThin,
		Recorder:    rec,
	}
	get := func() *miroirv1alpha1.MiroirNode {
		n := &miroirv1alpha1.MiroirNode{}
		if err := c.Get(t.Context(), types.NamespacedName{Name: nodeKharkiv}, n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	return p, get
}

func TestPoolStatsPublisherPublishes(t *testing.T) {
	fb := newFakeBackend()
	fb.stats = backend.PoolStats{SizeBytes: 100 * poolGiB, UsedBytes: 50 * poolGiB}
	p, get := newPublisher(t, fb, nil)

	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := get()
	if n.Spec.Backend != miroirv1alpha1.BackendLVMThin {
		t.Fatalf("backend = %q, want lvmthin", n.Spec.Backend)
	}
	if n.Status.CapacityBytes != 100*poolGiB || n.Status.AllocatedBytes != 50*poolGiB {
		t.Fatalf("unexpected capacity figures: %+v", n.Status)
	}
	if n.Status.ObservedAt == nil {
		t.Fatal("ObservedAt must be set")
	}
	if c := meta.FindStatusCondition(n.Status.Conditions, ConditionPoolUsageHigh); c == nil || c.Status != metav1.ConditionFalse {
		t.Fatalf("PoolUsageHigh should be False at 50%%, got %+v", c)
	}
}

func TestPoolStatsPublisherRaisesHighDataUsage(t *testing.T) {
	fb := newFakeBackend()
	fb.stats = backend.PoolStats{SizeBytes: 100 * poolGiB, UsedBytes: 85 * poolGiB}
	rec := events.NewFakeRecorder(8)
	p, get := newPublisher(t, fb, rec)

	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	c := meta.FindStatusCondition(get().Status.Conditions, ConditionPoolUsageHigh)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != "DataUsageHigh" {
		t.Fatalf("expected DataUsageHigh True at 85%%, got %+v", c)
	}
	select {
	case <-rec.Events:
	default:
		t.Fatal("expected a PoolUsageHigh event on first crossing")
	}
}

// The Warning event fires once per False→True transition, not on every
// sample whose message differs — the message embeds the usage percentage,
// so gating on condition change re-fired it on each percent of drift.
func TestPoolStatsPublisherEventsOnlyOnTransition(t *testing.T) {
	fb := newFakeBackend()
	fb.stats = backend.PoolStats{SizeBytes: 100 * poolGiB, UsedBytes: 85 * poolGiB}
	rec := events.NewFakeRecorder(8)
	p, get := newPublisher(t, fb, rec)

	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-rec.Events // first crossing

	// Usage drifts but stays high: message changes, no new event.
	fb.stats.UsedBytes = 86 * poolGiB
	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-rec.Events:
		t.Fatalf("no event while the condition stays True, got %q", e)
	default:
	}

	// Recovery, then a fresh crossing: one new event.
	fb.stats.UsedBytes = 50 * poolGiB
	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(get().Status.Conditions, ConditionPoolUsageHigh); c == nil || c.Status != metav1.ConditionFalse {
		t.Fatalf("condition must recover to False, got %+v", c)
	}
	fb.stats.UsedBytes = 90 * poolGiB
	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-rec.Events:
	default:
		t.Fatal("expected a new event on the next False→True crossing")
	}
}

func TestPoolStatsPublisherRaisesHighMetadataUsage(t *testing.T) {
	fb := newFakeBackend()
	fb.stats = backend.PoolStats{SizeBytes: 100 * poolGiB, UsedBytes: 10 * poolGiB, MetaUsedPercent: 90}
	p, get := newPublisher(t, fb, nil)

	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := get()
	if n.Status.MetaUsedPercent != 90 {
		t.Fatalf("MetaUsedPercent = %d, want 90", n.Status.MetaUsedPercent)
	}
	c := meta.FindStatusCondition(n.Status.Conditions, ConditionPoolUsageHigh)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != "MetadataUsageHigh" {
		t.Fatalf("expected MetadataUsageHigh True, got %+v", c)
	}
}
