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
	"math"
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
	// capacity (DESIGN.md §4.6 — "every ~60s").
	DefaultPoolStatsInterval = 60 * time.Second
	// ConditionPoolUsageHigh fires on a MiroirNode once data or metadata
	// usage crosses poolUsageWarnPercent.
	ConditionPoolUsageHigh = "PoolUsageHigh"
	// poolUsageWarnPercent is the warn line for both data and dm-thin
	// metadata; ZFS also degrades badly past ~85% full (DESIGN.md §4.6).
	poolUsageWarnPercent = 80
)

// PoolStatsPublisher samples this node's backend pool capacity on an
// interval and publishes it to the node's MiroirNode object, where the
// controller reads it for capacity-aware placement (DESIGN.md §4.6).
type PoolStatsPublisher struct {
	Client      client.Client
	NodeName    string
	Backend     backend.Backend
	BackendType miroirv1alpha1.BackendType
	// Interval between samples; DefaultPoolStatsInterval when zero.
	Interval time.Duration
	// Recorder emits the PoolUsageHigh event; optional.
	Recorder events.EventRecorder
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

func (p *PoolStatsPublisher) publish(ctx context.Context) error {
	stats, err := p.Backend.Stats(ctx)
	if err != nil {
		return fmt.Errorf("read pool stats: %w", err)
	}

	node := &miroirv1alpha1.MiroirNode{ObjectMeta: metav1.ObjectMeta{Name: p.NodeName}}
	if _, err := controllerutil.CreateOrUpdate(ctx, p.Client, node, func() error {
		node.Spec.Backend = p.BackendType
		return nil
	}); err != nil {
		return fmt.Errorf("upsert MiroirNode %s: %w", p.NodeName, err)
	}

	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		cur := &miroirv1alpha1.MiroirNode{}
		if err := p.Client.Get(ctx, types.NamespacedName{Name: p.NodeName}, cur); err != nil {
			return err
		}
		cur.Status.CapacityBytes = stats.SizeBytes
		cur.Status.AllocatedBytes = stats.UsedBytes
		cur.Status.MetaUsedPercent = int32(math.Round(stats.MetaUsedPercent))
		now := metav1.Now()
		cur.Status.ObservedAt = &now

		high, reason, msg := poolUsageHigh(stats)
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
		meta.SetStatusCondition(&cur.Status.Conditions, metav1.Condition{
			Type:    ConditionPoolUsageHigh,
			Status:  condStatus,
			Reason:  reason,
			Message: msg,
		})
		if err := p.Client.Status().Update(ctx, cur); err != nil {
			return err
		}
		if high && !wasHigh && p.Recorder != nil {
			p.Recorder.Eventf(cur, nil, corev1.EventTypeWarning, ConditionPoolUsageHigh, "Sample", msg)
		}
		return nil
	})
}

// poolUsageHigh reports whether data or dm-thin metadata usage has crossed
// the warn line, with a condition reason and human message.
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
		return false, "UsageNormal",
			fmt.Sprintf("pool %.0f%% full", dataPct)
	}
}
