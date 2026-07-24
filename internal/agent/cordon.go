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
	"os"
	"path/filepath"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// CordonSentinelPath is where the agent mirrors the cordon state for the
// DaemonSet preStop hook (charts/miroir/templates/agent.tpl must match).
const CordonSentinelPath = "/run/miroir/cordoned"

// CordonWatcher caches whether this node is cordoned (unschedulable), read by
// the agent at shutdown. Cached from a watch, not fetched on demand: by
// shutdown the API server may be gone (a whole-cluster reboot takes etcd with
// it), but the cordon was observed earlier while it was still up.
type CordonWatcher struct {
	client.Client
	NodeName string
	// SentinelPath, when set, mirrors the cordon state to a file the
	// DaemonSet preStop hook reads: the hook's force-demote must fire
	// only on a cordoned node (shutdown), never on a routine pod restart
	// or chart rollout, where it would EIO every in-use volume.
	SentinelPath string
	cordoned     atomic.Bool
}

// Cordoned reports this node's last observed unschedulable state.
func (w *CordonWatcher) Cordoned() bool {
	return w.cordoned.Load()
}

// Reconcile refreshes the cached cordon state from the node object.
func (w *CordonWatcher) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	node := &corev1.Node{}
	if err := w.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	w.cordoned.Store(node.Spec.Unschedulable)
	w.syncSentinel(ctx, node.Spec.Unschedulable)
	return ctrl.Result{}, nil
}

// syncSentinel creates or removes the cordon sentinel file. Best effort:
// a failed write only degrades the preStop hook to never firing, which is
// the safe direction (the in-process shutdown demote still runs).
func (w *CordonWatcher) syncSentinel(ctx context.Context, cordoned bool) {
	if w.SentinelPath == "" {
		return
	}
	var err error
	if cordoned {
		if err = os.MkdirAll(filepath.Dir(w.SentinelPath), 0o755); err == nil {
			err = os.WriteFile(w.SentinelPath, nil, 0o644)
		}
	} else if err = os.Remove(w.SentinelPath); os.IsNotExist(err) {
		err = nil
	}
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to sync cordon sentinel", "path", w.SentinelPath)
	}
}

// SetupWithManager registers the watch, filtered to this node's own object.
func (w *CordonWatcher) SetupWithManager(mgr ctrl.Manager) error {
	thisNode := predicate.NewPredicateFuncs(func(o client.Object) bool {
		return o.GetName() == w.NodeName
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}, builder.WithPredicates(thisNode)).
		Named("agent-cordon").
		Complete(w)
}
