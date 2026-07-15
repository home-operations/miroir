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
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
)

const (
	// DefaultPoolStatsInterval is how often the agent republishes pool
	// capacity (every ~60s).
	DefaultPoolStatsInterval = 60 * time.Second
	// ConditionPoolUsageHigh fires on a MiroirNode once any pool's data or
	// metadata usage crosses poolUsageWarnPercent.
	ConditionPoolUsageHigh = "PoolUsageHigh"
	// poolUsageWarnPercent is the warn line for both data and dm-thin
	// metadata; ZFS also degrades badly past ~85% full.
	poolUsageWarnPercent = 80
	// reasonUsageNormal is the condition reason while every pool sits
	// below the warn line.
	reasonUsageNormal = "UsageNormal"
)

// PoolStatsPublisher samples this node's pool capacities on an interval
// and publishes them to the node's MiroirNode object, where the controller
// reads them for capacity-aware placement.
type PoolStatsPublisher struct {
	Client   client.Client
	NodeName string
	// Pools holds this node's storage pools; every tick samples each.
	Pools Pools
	// Interval between samples; DefaultPoolStatsInterval when zero.
	Interval time.Duration
	// Recorder emits the PoolUsageHigh event; optional.
	Recorder events.EventRecorder
	// DRBDVersion is the kernel module version probed at agent startup;
	// empty on nodes without the module. A module change means a node
	// reboot and thus an agent restart, so startup is current enough.
	DRBDVersion string
}

// Start publishes once promptly, then on the interval until ctx is done.
// A failed sample is logged and retried next tick — never fatal.
func (p *PoolStatsPublisher) Start(ctx context.Context) error {
	log := ctrl.LoggerFrom(ctx).WithName("poolstats")
	interval := p.Interval
	if interval <= 0 {
		interval = DefaultPoolStatsInterval
	}
	if err := p.publish(ctx); err != nil {
		log.Error(err, "initial pool stats publish failed")
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := p.publish(ctx); err != nil {
				log.Error(err, "pool stats publish failed")
			}
		}
	}
}

// sample is one pool's tick result: the stats, or the read error that
// keeps the pool visible in status instead of silently dropping out.
type sample struct {
	name  string
	stats backend.PoolStats
	err   error
}

func (p *PoolStatsPublisher) publish(ctx context.Context) error {
	// Sample every pool even when one errors: a bad pool must not take
	// the good one's figures down with it.
	names := slices.Sorted(maps.Keys(p.Pools))
	samples := make([]sample, 0, len(names))
	for _, name := range names {
		st, err := p.Pools[name].Backend.Stats(ctx)
		samples = append(samples, sample{name: name, stats: st, err: err})
		if err != nil {
			continue
		}
		// Publish the sample even when the CRD update below conflicts: the
		// gauges describe the pool, not the API object.
		recordPoolMetrics(name, st.SizeBytes, st.UsedBytes, st.MetaUsedPercent/100)
	}

	node := &miroirv1alpha1.MiroirNode{ObjectMeta: metav1.ObjectMeta{Name: p.NodeName}}
	if _, err := controllerutil.CreateOrUpdate(ctx, p.Client, node, func() error {
		node.Spec.Pools = node.Spec.Pools[:0]
		for _, name := range names {
			node.Spec.Pools = append(node.Spec.Pools, miroirv1alpha1.MiroirNodePool{
				Name: name, Backend: p.Pools[name].Type,
			})
		}
		return nil
	}); err != nil {
		return fmt.Errorf("upsert MiroirNode %s: %w", p.NodeName, err)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur := &miroirv1alpha1.MiroirNode{}
		if err := p.Client.Get(ctx, types.NamespacedName{Name: p.NodeName}, cur); err != nil {
			return err
		}
		cur.Status.Pools = cur.Status.Pools[:0]
		for _, s := range samples {
			entry := miroirv1alpha1.MiroirNodePoolStatus{Name: s.name}
			if s.err != nil {
				entry.Message = s.err.Error()
			} else {
				entry.CapacityBytes = s.stats.SizeBytes
				entry.AllocatedBytes = s.stats.UsedBytes
				entry.MetaUsedPercent = int32(math.Round(s.stats.MetaUsedPercent))
			}
			cur.Status.Pools = append(cur.Status.Pools, entry)
		}
		// Clear the deprecated pre-multi-pool figures a not-yet-rolled
		// agent may have written: the pools list supersedes them, and
		// stale flat values frozen in status would mislead readers.
		cur.Status.CapacityBytes, cur.Status.AllocatedBytes, cur.Status.MetaUsedPercent = 0, 0, 0 //nolint:staticcheck // deliberately clears the deprecated skew-compat fields
		cur.Status.DRBDVersion = p.DRBDVersion
		now := metav1.Now()
		cur.Status.ObservedAt = &now

		high, reason, msg := poolsUsageHigh(samples)
		condStatus := metav1.ConditionFalse
		if high {
			condStatus = metav1.ConditionTrue
		}
		// Event only on the False→True transition. SetStatusCondition's
		// changed also covers message drift, and the message embeds the
		// usage percentage — gating on it re-fires the Warning on every
		// percent of drift while usage stays high.
		prev := meta.FindStatusCondition(cur.Status.Conditions, ConditionPoolUsageHigh)
		wasHigh := prev != nil && prev.Status == metav1.ConditionTrue
		// A pool whose stats read failed is unknown, not healthy: it may
		// raise the condition (via the pools that did sample) but must
		// never clear it — a pool that fills up and then breaks its lvs
		// would otherwise flip its own warning off. The last known
		// condition is kept until every pool samples clean again.
		if high || !anySampleErr(samples) {
			meta.SetStatusCondition(&cur.Status.Conditions, metav1.Condition{
				Type:    ConditionPoolUsageHigh,
				Status:  condStatus,
				Reason:  reason,
				Message: msg,
			})
		}
		if err := p.Client.Status().Update(ctx, cur); err != nil {
			return err
		}
		if high && !wasHigh && p.Recorder != nil {
			p.Recorder.Eventf(cur, nil, corev1.EventTypeWarning, ConditionPoolUsageHigh, "Sample", msg)
		}
		return nil
	})
}

// anySampleErr reports whether any pool's stats read failed this tick.
func anySampleErr(samples []sample) bool {
	return slices.ContainsFunc(samples, func(s sample) bool { return s.err != nil })
}

// poolsUsageHigh folds the per-pool warn lines into the node's single
// PoolUsageHigh condition: True when any pool crossed 80% data or dm-thin
// metadata usage, with a message naming each offender. Errored samples
// are skipped here — the caller keeps the previous condition instead of
// letting an unreadable pool masquerade as healthy.
func poolsUsageHigh(samples []sample) (high bool, reason, msg string) {
	var parts []string
	worstReason := reasonUsageNormal
	for _, s := range samples {
		if s.err != nil {
			continue
		}
		h, r, m := poolUsageHigh(s.stats)
		if !h {
			continue
		}
		high = true
		worstReason = r
		parts = append(parts, "pool "+s.name+": "+m)
	}
	if high {
		return true, worstReason, strings.Join(parts, "; ")
	}
	return false, reasonUsageNormal, "all pools below the usage warn line"
}

// poolUsageHigh reports whether one pool's data or dm-thin metadata usage
// has crossed the warn line, with a condition reason and human message.
func poolUsageHigh(stats backend.PoolStats) (high bool, reason, msg string) {
	var dataPct float64
	if stats.SizeBytes > 0 {
		dataPct = float64(stats.UsedBytes) / float64(stats.SizeBytes) * 100
	}
	switch {
	case dataPct >= poolUsageWarnPercent:
		return true, "DataUsageHigh",
			fmt.Sprintf("pool %.0f%% full (warn at %d%%)", dataPct, poolUsageWarnPercent)
	case stats.MetaUsedPercent >= poolUsageWarnPercent:
		return true, "MetadataUsageHigh",
			fmt.Sprintf("dm-thin metadata %.0f%% full (warn at %d%%)", stats.MetaUsedPercent, poolUsageWarnPercent)
	default:
		return false, reasonUsageNormal,
			fmt.Sprintf("pool %.0f%% full", dataPct)
	}
}
