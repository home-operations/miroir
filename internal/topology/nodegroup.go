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

package topology

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// NodeGroupReconciler materializes one MiroirNode per node matching a
// MiroirNodeGroup's selector, so a fleet with a shared storage layout is
// one object and joining it is labeling the node. It runs in the
// controller pod.
//
// Lifecycle rules, in order of what must never go wrong: membership
// changes never delete a MiroirNode (a node leaving the selector — or the
// group being deleted — orphans its MiroirNode in place; topology is
// removed only by an explicit kubectl delete), direct-authored
// MiroirNodes always win over groups, and one node has one manager (the
// first group's label sticks; later claimants report Conflict).
type NodeGroupReconciler struct {
	client.Client
}

// Reconcile converges one group: materialize matching nodes, revert
// drifted managed specs, and report membership/conflicts/orphans. One
// failing node (a bad address annotation, an API hiccup) must not block
// the rest of the fleet or the status report, so per-node errors are
// collected and joined after the pass.
func (r *NodeGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := ctrl.LoggerFrom(ctx)

	group := &miroirv1alpha1.MiroirNodeGroup{}
	if err := r.Get(ctx, req.NamespacedName, group); err != nil {
		// Deleted: members stay, orphaned in place by design.
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	selector, err := metav1.LabelSelectorAsSelector(&group.Spec.NodeSelector)
	if err != nil {
		// CRD-validated shapes cannot fail here; guard anyway.
		return ctrl.Result{}, fmt.Errorf("invalid nodeSelector: %w", err)
	}
	// Read-only pass over the cached Nodes (only name/labels/annotations
	// are consumed), so skip the informer deep copies.
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, client.MatchingLabelsSelector{Selector: selector}, client.UnsafeDisableDeepCopy); err != nil {
		return ctrl.Result{}, err
	}
	existing := &miroirv1alpha1.MiroirNodeList{}
	if err := r.List(ctx, existing); err != nil {
		return ctrl.Result{}, err
	}
	byName := make(map[string]*miroirv1alpha1.MiroirNode, len(existing.Items))
	for i := range existing.Items {
		byName[existing.Items[i].Name] = &existing.Items[i]
	}

	var members, conflicts []string
	var errs []error
	memberSet := make(map[string]struct{}, len(nodes.Items))
	errNodes := make(map[string]struct{})
	for i := range nodes.Items {
		node := &nodes.Items[i]
		member, err := r.materialize(ctx, group, node, byName[node.Name])
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("node %s: %w", node.Name, err))
			errNodes[node.Name] = struct{}{}
		case member:
			members = append(members, node.Name)
			memberSet[node.Name] = struct{}{}
		default:
			conflicts = append(conflicts, node.Name)
		}
	}
	slices.Sort(members)

	// Provenance-labeled MiroirNodes no longer matching the selector:
	// deliberately left in place, surfaced instead of silently drifting.
	// Nodes whose converge just failed are errors, not orphans.
	var orphans []string
	for name, mn := range byName {
		if mn.Labels[miroirv1alpha1.NodeGroupLabel] != group.Name {
			continue
		}
		if _, ok := memberSet[name]; ok {
			continue
		}
		if _, ok := errNodes[name]; ok {
			continue
		}
		orphans = append(orphans, name)
	}
	slices.Sort(orphans)
	slices.Sort(conflicts)

	if len(conflicts) > 0 {
		log.Info("node group skipped nodes with another manager",
			"group", group.Name, "nodes", conflicts)
	}
	if err := r.patchStatus(ctx, group, members, conflicts, orphans); err != nil {
		errs = append(errs, err)
	}
	return ctrl.Result{}, errors.Join(errs...)
}

// materialize creates or converges one member's MiroirNode, reporting
// member=false for a node whose MiroirNode has another manager. A
// MiroirNode managed by nobody (direct-authored) or by another group is
// not touched.
func (r *NodeGroupReconciler) materialize(ctx context.Context, group *miroirv1alpha1.MiroirNodeGroup, node *corev1.Node, current *miroirv1alpha1.MiroirNode) (bool, error) {
	desired := r.desiredSpec(group, node)
	if current == nil {
		mn := &miroirv1alpha1.MiroirNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:   node.Name,
				Labels: map[string]string{miroirv1alpha1.NodeGroupLabel: group.Name},
			},
			Spec: desired,
		}
		// AlreadyExists is cache lag (often our own creation from the
		// previous pass): surfacing it requeues, and the next pass
		// classifies the object by its label instead of reporting a
		// phantom conflict.
		if err := r.Create(ctx, mn); err != nil {
			return false, err
		}
		return true, nil
	}
	if current.Labels[miroirv1alpha1.NodeGroupLabel] != group.Name {
		return false, nil
	}
	if equality.Semantic.DeepEqual(current.Spec, desired) {
		return true, nil
	}
	// Managed means managed: template edits and node-fact changes (zone
	// label, address annotation) converge the spec; hand-edits revert.
	base := current.DeepCopy()
	current.Spec = desired
	if err := r.Patch(ctx, current, client.MergeFrom(base)); err != nil {
		return false, err
	}
	return true, nil
}

// desiredSpec resolves the template against one node's per-node facts: an
// empty zone inherits the node's standard failure-domain label, and the
// replication address comes from the node's annotation (the CRD forbids
// it in the template).
func (r *NodeGroupReconciler) desiredSpec(group *miroirv1alpha1.MiroirNodeGroup, node *corev1.Node) miroirv1alpha1.MiroirNodeSpec {
	spec := *group.Spec.Template.DeepCopy()
	if spec.Zone == "" {
		spec.Zone = node.Labels[corev1.LabelTopologyZone]
	}
	spec.Address = node.Annotations[miroirv1alpha1.NodeAddressAnnotation]
	return spec
}

// patchStatus records membership and the two lifecycle conditions.
func (r *NodeGroupReconciler) patchStatus(ctx context.Context, group *miroirv1alpha1.MiroirNodeGroup, members, conflicts, orphans []string) error {
	base := group.DeepCopy()
	group.Status.Nodes = members
	group.Status.ObservedGeneration = group.Generation

	conflictCond := metav1.Condition{
		Type: miroirv1alpha1.ConditionGroupConflict, Status: metav1.ConditionFalse,
		Reason: "NoConflict", Message: "every matching node's MiroirNode is managed by this group",
		ObservedGeneration: group.Generation,
	}
	if len(conflicts) > 0 {
		conflictCond.Status = metav1.ConditionTrue
		conflictCond.Reason = "AnotherManager"
		conflictCond.Message = "skipped nodes whose MiroirNode has another manager (a direct MiroirNode always wins): " +
			strings.Join(conflicts, ", ")
	}
	orphanCond := metav1.Condition{
		Type: miroirv1alpha1.ConditionGroupOrphaned, Status: metav1.ConditionFalse,
		Reason: "NoOrphans", Message: "every materialized MiroirNode still matches the selector",
		ObservedGeneration: group.Generation,
	}
	if len(orphans) > 0 {
		orphanCond.Status = metav1.ConditionTrue
		orphanCond.Reason = "LeftSelector"
		orphanCond.Message = "materialized MiroirNodes no longer match the selector and were left in place " +
			"(decommission with kubectl delete miroirnode): " + strings.Join(orphans, ", ")
	}
	meta.SetStatusCondition(&group.Status.Conditions, conflictCond)
	meta.SetStatusCondition(&group.Status.Conditions, orphanCond)
	if equality.Semantic.DeepEqual(base.Status, group.Status) {
		return nil
	}
	return r.Status().Patch(ctx, group, client.MergeFrom(base))
}

// SetupWithManager registers the reconciler. Node changes re-run every
// group, but only for the two node facts the reconciler consumes: labels
// (selectors, zone) and the replication-address annotation. MiroirNode
// events also re-run every group — an unlabeled object appearing, being
// deleted, or a stripped provenance label can start or end a conflict for
// any group claiming that node, and the events are rare (spec generation
// and label changes only).
func (r *NodeGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	enqueueAllGroups := handler.EnqueueRequestsFromMapFunc(
		func(ctx context.Context, _ client.Object) []ctrl.Request {
			groups := &miroirv1alpha1.MiroirNodeGroupList{}
			if err := r.List(ctx, groups, client.UnsafeDisableDeepCopy); err != nil {
				return nil
			}
			reqs := make([]ctrl.Request, 0, len(groups.Items))
			for i := range groups.Items {
				reqs = append(reqs, ctrl.Request{
					NamespacedName: client.ObjectKeyFromObject(&groups.Items[i]),
				})
			}
			return reqs
		})
	// Node annotations churn under unrelated controllers (ttl, CNI, cloud
	// providers); only the replication address matters here. Create and
	// delete events pass through (predicate.Funcs defaults).
	addressChanged := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetAnnotations()[miroirv1alpha1.NodeAddressAnnotation] !=
				e.ObjectNew.GetAnnotations()[miroirv1alpha1.NodeAddressAnnotation]
		},
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirNodeGroup{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.Node{}, enqueueAllGroups, builder.WithPredicates(
			predicate.Or(predicate.LabelChangedPredicate{}, addressChanged))).
		Watches(&miroirv1alpha1.MiroirNode{}, enqueueAllGroups, builder.WithPredicates(
			predicate.Or(predicate.GenerationChangedPredicate{}, predicate.LabelChangedPredicate{}))).
		Named("nodegroup").
		Complete(r)
}
