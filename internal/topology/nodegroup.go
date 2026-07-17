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
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// zoneLabel is the standard failure-domain label the group resolves an
// empty template zone from.
const zoneLabel = "topology.kubernetes.io/zone"

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
// drifted managed specs, and report membership/conflicts/orphans.
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
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, client.MatchingLabelsSelector{Selector: selector}); err != nil {
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
	for i := range nodes.Items {
		node := &nodes.Items[i]
		switch outcome, err := r.materialize(ctx, group, node, byName[node.Name]); {
		case err != nil:
			return ctrl.Result{}, err
		case outcome == memberOutcome:
			members = append(members, node.Name)
		default:
			conflicts = append(conflicts, node.Name)
		}
	}
	slices.Sort(members)

	// Provenance-labeled MiroirNodes no longer matching the selector:
	// deliberately left in place, surfaced instead of silently drifting.
	var orphans []string
	for name, mn := range byName {
		if mn.Labels[miroirv1alpha1.NodeGroupLabel] == group.Name && !slices.Contains(members, name) {
			orphans = append(orphans, name)
		}
	}
	slices.Sort(orphans)
	slices.Sort(conflicts)

	if len(conflicts) > 0 {
		log.Info("node group skipped nodes with another manager",
			"group", group.Name, "nodes", conflicts)
	}
	return ctrl.Result{}, r.patchStatus(ctx, group, members, conflicts, orphans)
}

// materializeOutcome classifies one node's reconciliation.
type materializeOutcome int

const (
	memberOutcome materializeOutcome = iota
	conflictOutcome
)

// materialize creates or converges one member's MiroirNode. A MiroirNode
// managed by nobody (direct-authored) or by another group is not touched.
func (r *NodeGroupReconciler) materialize(ctx context.Context, group *miroirv1alpha1.MiroirNodeGroup, node *corev1.Node, current *miroirv1alpha1.MiroirNode) (materializeOutcome, error) {
	desired := r.desiredSpec(group, node)
	if current == nil {
		mn := &miroirv1alpha1.MiroirNode{
			ObjectMeta: metav1.ObjectMeta{
				Name:   node.Name,
				Labels: map[string]string{miroirv1alpha1.NodeGroupLabel: group.Name},
			},
			Spec: desired,
		}
		if err := r.Create(ctx, mn); err != nil {
			// A concurrent creation (another group, a direct apply): the
			// next pass classifies it by its label.
			if apierrors.IsAlreadyExists(err) {
				return conflictOutcome, nil
			}
			return conflictOutcome, err
		}
		return memberOutcome, nil
	}
	if current.Labels[miroirv1alpha1.NodeGroupLabel] != group.Name {
		return conflictOutcome, nil
	}
	if equality.Semantic.DeepEqual(current.Spec, desired) {
		return memberOutcome, nil
	}
	// Managed means managed: template edits and node-fact changes (zone
	// label, address annotation) converge the spec; hand-edits revert.
	base := current.DeepCopy()
	current.Spec = desired
	if err := r.Patch(ctx, current, client.MergeFrom(base)); err != nil {
		return conflictOutcome, err
	}
	return memberOutcome, nil
}

// desiredSpec resolves the template against one node's per-node facts: an
// empty zone inherits the node's standard failure-domain label, and the
// replication address comes from the node's annotation (the CRD forbids
// it in the template).
func (r *NodeGroupReconciler) desiredSpec(group *miroirv1alpha1.MiroirNodeGroup, node *corev1.Node) miroirv1alpha1.MiroirNodeSpec {
	spec := *group.Spec.Template.DeepCopy()
	if spec.Zone == "" {
		spec.Zone = node.Labels[zoneLabel]
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
	changedA := meta.SetStatusCondition(&group.Status.Conditions, conflictCond)
	changedB := meta.SetStatusCondition(&group.Status.Conditions, orphanCond)
	if !changedA && !changedB && equality.Semantic.DeepEqual(base.Status.Nodes, group.Status.Nodes) &&
		base.Status.ObservedGeneration == group.Status.ObservedGeneration {
		return nil
	}
	return r.Status().Patch(ctx, group, client.MergeFrom(base))
}

// SetupWithManager registers the reconciler. Node label/annotation changes
// re-run every group (memberships may shift either way); managed
// MiroirNode edits re-run their manager so drift reverts.
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirNodeGroup{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&corev1.Node{}, enqueueAllGroups, builder.WithPredicates(
			predicate.Or(predicate.LabelChangedPredicate{}, predicate.AnnotationChangedPredicate{}))).
		Watches(&miroirv1alpha1.MiroirNode{}, handler.EnqueueRequestsFromMapFunc(
			func(_ context.Context, obj client.Object) []ctrl.Request {
				owner := obj.GetLabels()[miroirv1alpha1.NodeGroupLabel]
				if owner == "" {
					return nil
				}
				return []ctrl.Request{{NamespacedName: client.ObjectKey{Name: owner}}}
			}), builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("nodegroup").
		Complete(r)
}
