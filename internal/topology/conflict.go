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

// Package topology reconciles the cross-object rules of the MiroirNode
// topology — the checks a CRD cannot express because it validates one
// object at a time. Per-node validation lives in the CRD schema and its
// CEL rules.
package topology

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/nodemap"
)

// ConditionAddressConflict is True on a MiroirNode whose replication
// address is shared with another MiroirNode. Two nodes dialing one
// endpoint makes every shared volume's DRBD connections ambiguous at
// connect time, so placement skips conflicted nodes and address
// resolution refuses until the operator resolves the clash. Reported as
// a condition rather than refused at admission (rook's model): the write
// is accepted, the consequence is visible, and enforcement stays in the
// placement code that must hold anyway.
const ConditionAddressConflict = "AddressConflict"

const (
	reasonDuplicateAddress = "DuplicateAddress"
	reasonAddressUnique    = "AddressUnique"
	// topologyRequestKey is the fixed request name every MiroirNode event
	// maps to — one pass per edit, not one per node. The name is never
	// read; Reconcile ignores the request.
	topologyRequestKey = "topology"
)

// ConflictReconciler maintains the AddressConflict condition on every
// MiroirNode. It runs in the controller pod, beside the placement code
// that enforces the rule.
type ConflictReconciler struct {
	client.Client
	// Recorder emits the Warning event on a fresh conflict; optional.
	Recorder events.EventRecorder
}

// Reconcile recomputes every node's conflict state against the full
// topology in one pass and updates the conditions that changed. The
// request is a fixed sentinel (SetupWithManager), so the request name is
// ignored.
func (r *ConflictReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	list := &miroirv1alpha1.MiroirNodeList{}
	if err := r.List(ctx, list); err != nil {
		return ctrl.Result{}, err
	}
	topo := nodemap.FromNodes(list.Items)
	for i := range list.Items {
		// A mid-pass failure retries the whole pass: SetStatusCondition
		// skips already-updated nodes, so the retry is cheap and emits no
		// duplicate events.
		if err := r.reconcileNode(ctx, &list.Items[i], topo); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// reconcileNode updates one node's AddressConflict condition on change.
func (r *ConflictReconciler) reconcileNode(ctx context.Context, node *miroirv1alpha1.MiroirNode, topo nodemap.Map) error {
	cond := metav1.Condition{
		Type:    ConditionAddressConflict,
		Status:  metav1.ConditionFalse,
		Reason:  reasonAddressUnique,
		Message: "replication address is unique (or unset)",
	}
	if topo[node.Name].AddressConflict {
		// Group by the same canonical key the fold conflicts on: a raw
		// string compare would miss equal-but-differently-spelled IPv6
		// addresses and name no peer.
		key := nodemap.ConflictKey(node.Spec.Address)
		var peers []string
		for name, n := range topo {
			if name != node.Name && n.AddressConflict && nodemap.ConflictKey(n.Address) == key {
				peers = append(peers, name)
			}
		}
		slices.Sort(peers)
		cond.Status = metav1.ConditionTrue
		cond.Reason = reasonDuplicateAddress
		cond.Message = fmt.Sprintf("replication address %s is shared with %s; "+
			"this node is excluded from placement until the conflict is resolved",
			node.Spec.Address, strings.Join(peers, ", "))
	}

	prev := meta.FindStatusCondition(node.Status.Conditions, ConditionAddressConflict)
	wasConflicted := prev != nil && prev.Status == metav1.ConditionTrue
	if !meta.SetStatusCondition(&node.Status.Conditions, cond) {
		return nil
	}
	if err := r.Status().Update(ctx, node); err != nil {
		// Deleted mid-pass: its deletion event re-runs the pass anyway.
		return client.IgnoreNotFound(err)
	}
	if cond.Status == metav1.ConditionTrue && !wasConflicted && r.Recorder != nil {
		r.Recorder.Eventf(node, nil, corev1.EventTypeWarning, ConditionAddressConflict, "Reconcile",
			cond.Message)
	}
	return nil
}

// SetupWithManager registers the reconciler. Every topology edit enqueues
// one fixed request and the pass revisits all nodes — removing one node's
// address is what clears its former peer's conflict, so peers must be
// recomputed anyway, and a per-node fan-out would make one chart apply
// O(N²) reconcile work. Generation filter keeps the per-minute status
// heartbeats out.
func (r *ConflictReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named("topologyconflict").
		Watches(&miroirv1alpha1.MiroirNode{}, handler.EnqueueRequestsFromMapFunc(
			func(context.Context, client.Object) []ctrl.Request {
				return []ctrl.Request{{NamespacedName: types.NamespacedName{Name: topologyRequestKey}}}
			}), builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}
