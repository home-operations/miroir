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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
)

const poolGiB = 1 << 30

func newPublisher(t *testing.T, pools Pools, rec events.EventRecorder) (*PoolStatsPublisher, func() *miroirv1alpha1.MiroirNode) {
	t.Helper()
	s := newScheme(t)
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&miroirv1alpha1.MiroirNode{}).
		Build()
	p := &PoolStatsPublisher{
		Client:   c,
		NodeName: nodeA,
		Pools:    pools,
		Recorder: rec,
	}
	get := func() *miroirv1alpha1.MiroirNode {
		n := &miroirv1alpha1.MiroirNode{}
		if err := c.Get(t.Context(), types.NamespacedName{Name: nodeA}, n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	return p, get
}

func singlePool(fb *fakeBackend) Pools {
	return Pools{poolDefault: {Backend: fb, Type: miroirv1alpha1.BackendLVMThin}}
}

func TestPoolStatsPublisherPublishes(t *testing.T) {
	fb := newFakeBackend()
	fb.stats = backend.PoolStats{SizeBytes: 100 * poolGiB, UsedBytes: 50 * poolGiB}
	p, get := newPublisher(t, singlePool(fb), nil)
	p.DRBDVersion = "9.3.2"

	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := get()
	if len(n.Spec.Pools) != 1 || n.Spec.Pools[0].Name != poolDefault ||
		n.Spec.Pools[0].Backend != miroirv1alpha1.BackendLVMThin {
		t.Fatalf("spec pools = %+v, want one lvmthin default", n.Spec.Pools)
	}
	st := n.Status.Pool(poolDefault)
	if st == nil || st.CapacityBytes != 100*poolGiB || st.AllocatedBytes != 50*poolGiB {
		t.Fatalf("unexpected capacity figures: %+v", n.Status.Pools)
	}
	if n.Status.DRBDVersion != "9.3.2" {
		t.Fatalf("drbdVersion = %q, want 9.3.2", n.Status.DRBDVersion)
	}
	if n.Status.ObservedAt == nil {
		t.Fatal("ObservedAt must be set")
	}
	if c := meta.FindStatusCondition(n.Status.Conditions, ConditionPoolUsageHigh); c == nil || c.Status != metav1.ConditionFalse {
		t.Fatalf("PoolUsageHigh should be False at 50%%, got %+v", c)
	}
}

func TestPoolStatsPublisherPublishesPerPool(t *testing.T) {
	bulk, fast := newFakeBackend(), newFakeBackend()
	bulk.stats = backend.PoolStats{SizeBytes: 100 * poolGiB, UsedBytes: 10 * poolGiB}
	fast.stats = backend.PoolStats{SizeBytes: 50 * poolGiB, UsedBytes: 45 * poolGiB}
	p, get := newPublisher(t, Pools{
		"bulk":   {Backend: bulk, Type: miroirv1alpha1.BackendLVMThin},
		poolFast: {Backend: fast, Type: miroirv1alpha1.BackendZFS},
	}, nil)

	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := get()
	if len(n.Spec.Pools) != 2 || len(n.Status.Pools) != 2 {
		t.Fatalf("want 2 pools in spec and status, got %+v / %+v", n.Spec.Pools, n.Status.Pools)
	}
	if st := n.Status.Pool("bulk"); st == nil || st.CapacityBytes != 100*poolGiB {
		t.Fatalf("bulk pool wrong: %+v", st)
	}
	if st := n.Status.Pool(poolFast); st == nil || st.AllocatedBytes != 45*poolGiB {
		t.Fatalf("fast pool wrong: %+v", st)
	}
	// fast sits at 90% — the node condition names the offending pool.
	c := meta.FindStatusCondition(n.Status.Conditions, ConditionPoolUsageHigh)
	if c == nil || c.Status != metav1.ConditionTrue || !strings.Contains(c.Message, poolFast) {
		t.Fatalf("expected PoolUsageHigh naming pool fast, got %+v", c)
	}
	if v := testutil.ToFloat64(metricPoolCapacity.WithLabelValues(poolFast)); v != 50*poolGiB {
		t.Fatalf("fast capacity gauge = %v, want %v", v, 50*poolGiB)
	}
}

// A pool whose backend cannot be sampled stays visible in status (with the
// error message) and must not take the healthy pool's figures down.
func TestPoolStatsPublisherIsolatesBadPool(t *testing.T) {
	good, bad := newFakeBackend(), newFakeBackend()
	good.stats = backend.PoolStats{SizeBytes: 100 * poolGiB, UsedBytes: 10 * poolGiB}
	bad.statsErr = errBoom
	p, get := newPublisher(t, Pools{
		poolDefault: {Backend: good, Type: miroirv1alpha1.BackendLVMThin},
		"broken":    {Backend: bad, Type: miroirv1alpha1.BackendLVMThin},
	}, nil)

	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := get()
	if st := n.Status.Pool(poolDefault); st == nil || st.CapacityBytes != 100*poolGiB {
		t.Fatalf("healthy pool must publish, got %+v", st)
	}
	st := n.Status.Pool("broken")
	if st == nil || st.Message == "" || st.CapacityBytes != 0 {
		t.Fatalf("broken pool must stay visible with its error, got %+v", st)
	}
}

// Regression (review): a pool whose stats read starts failing is unknown,
// not healthy — it must never clear a raised PoolUsageHigh condition (a
// pool that fills up and then wedges its lvs would otherwise flip its own
// warning off).
func TestPoolStatsPublisherKeepsConditionWhenSamplingBreaks(t *testing.T) {
	fb := newFakeBackend()
	fb.stats = backend.PoolStats{SizeBytes: 100 * poolGiB, UsedBytes: 92 * poolGiB}
	p, get := newPublisher(t, singlePool(fb), nil)

	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	if c := meta.FindStatusCondition(get().Status.Conditions, ConditionPoolUsageHigh); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("expected PoolUsageHigh True at 92%%, got %+v", c)
	}

	fb.statsErr = errBoom
	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := get()
	if c := meta.FindStatusCondition(n.Status.Conditions, ConditionPoolUsageHigh); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatalf("a broken pool must not clear the warn condition, got %+v", c)
	}
	if st := n.Status.Pool(poolDefault); st == nil || st.Message == "" {
		t.Fatalf("the errored pool must stay visible with its error: %+v", st)
	}
}

func TestPoolStatsPublisherRaisesHighDataUsage(t *testing.T) {
	fb := newFakeBackend()
	fb.stats = backend.PoolStats{SizeBytes: 100 * poolGiB, UsedBytes: 85 * poolGiB}
	rec := events.NewFakeRecorder(8)
	p, get := newPublisher(t, singlePool(fb), rec)

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
	p, get := newPublisher(t, singlePool(fb), rec)

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
	p, get := newPublisher(t, singlePool(fb), nil)

	if err := p.publish(t.Context()); err != nil {
		t.Fatal(err)
	}
	n := get()
	if v := testutil.ToFloat64(metricPoolCapacity.WithLabelValues(poolDefault)); v != 100*poolGiB {
		t.Fatalf("miroir_pool_capacity_bytes = %v, want %v", v, 100*poolGiB)
	}
	if v := testutil.ToFloat64(metricPoolAllocated.WithLabelValues(poolDefault)); v != 10*poolGiB {
		t.Fatalf("miroir_pool_allocated_bytes = %v, want %v", v, 10*poolGiB)
	}
	if v := testutil.ToFloat64(metricPoolMetaUsedRatio.WithLabelValues(poolDefault)); v != 0.9 {
		t.Fatalf("miroir_pool_meta_used_ratio = %v, want 0.9", v)
	}
	if st := n.Status.Pool(poolDefault); st == nil || st.MetaUsedPercent != 90 {
		t.Fatalf("MetaUsedPercent wrong: %+v", st)
	}
	c := meta.FindStatusCondition(n.Status.Conditions, ConditionPoolUsageHigh)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != "MetadataUsageHigh" {
		t.Fatalf("expected MetadataUsageHigh True, got %+v", c)
	}
}
