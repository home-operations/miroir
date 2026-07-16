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

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	mount "k8s.io/mount-utils"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

// TopologyWatcher restarts the agent when this node's MiroirNode pool spec
// no longer matches what the process bootstrapped from. The pools are read
// once at startup (backends are built before the manager starts), so a
// chart-applied edit — a pool added or reconfigured, the node joining or
// leaving the topology — needs a restart to take effect; this watcher is
// what makes that happen without the ConfigMap-checksum pod roll the
// pre-CR chart used. Node-level settings (zone, address, autoEvict) are
// controller inputs the agent never reads, so only spec.pools is compared.
type TopologyWatcher struct {
	client.Client
	NodeName string
	// BootedPools is the spec.pools snapshot the agent built its backends
	// from; nil when the node booted client-only (no MiroirNode).
	BootedPools []miroirv1alpha1.MiroirNodePool
	// Stop cancels the manager's context: the manager shuts down cleanly
	// (including the agent's shutdown sweep) and the kubelet restarts the
	// container, which re-bootstraps from the current spec.
	Stop context.CancelFunc
	// IsMounted reports whether a path is a mount point inside this pod;
	// nil selects the /proc/mounts-backed default. Tests inject a fake.
	IsMounted func(path string) bool
}

// Reconcile compares the current spec.pools against the boot snapshot and
// stops the manager on drift. Deletion of the MiroirNode counts as drift
// for a storage agent (restart into client-only), as does the MiroirNode
// appearing on a client-only agent. Pools compare as a name-keyed map:
// spec.pools is listType=map, so order carries no meaning and a re-render
// that only reorders entries must not cost a restart (a shutdown sweep and
// DRBD re-bootstrap).
func (w *TopologyWatcher) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	node := &miroirv1alpha1.MiroirNode{}
	var current []miroirv1alpha1.MiroirNodePool
	if err := w.Get(ctx, req.NamespacedName, node); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
	} else {
		current = node.Spec.Pools
	}
	if equality.Semantic.DeepEqual(poolsByName(current), poolsByName(w.BootedPools)) {
		return ctrl.Result{}, nil
	}
	// A new loopfile baseDir arrives as a hostPath mount in the NEW pod
	// template; the CR change reaches this watcher before the DaemonSet
	// rollout replaces the pod. Restarting inside the old pod would
	// bootstrap the pool on the container's own filesystem and crash-loop
	// on the reflink check, so hold the restart until the rollout delivers
	// the mount (the new pod boots from the current spec anyway).
	if dir := w.missingLoopfileMount(current); dir != "" {
		ctrl.LoggerFrom(ctx).Info("MiroirNode adds a loopfile baseDir not mounted in this pod; "+
			"waiting for the DaemonSet rollout instead of restarting",
			"node", w.NodeName, "baseDir", dir)
		return ctrl.Result{}, nil
	}
	ctrl.LoggerFrom(ctx).Info("MiroirNode pool spec changed since startup; restarting the agent to re-bootstrap",
		"node", w.NodeName)
	w.Stop()
	return ctrl.Result{}, nil
}

// missingLoopfileMount returns the first loopfile baseDir in the current
// spec that the agent did not boot with and that is not a mount point in
// this pod, or "" when a restart can serve every pool.
func (w *TopologyWatcher) missingLoopfileMount(current []miroirv1alpha1.MiroirNodePool) string {
	booted := map[string]bool{}
	for _, p := range w.BootedPools {
		if p.Loopfile != nil {
			booted[p.Loopfile.BaseDir] = true
		}
	}
	mounted := w.IsMounted
	if mounted == nil {
		mounted = defaultIsMounted
	}
	for _, p := range current {
		if p.Loopfile == nil || booted[p.Loopfile.BaseDir] {
			continue
		}
		if !mounted(p.Loopfile.BaseDir) {
			return p.Loopfile.BaseDir
		}
	}
	return ""
}

// poolsByName keys the pools by their listMapKey so comparison ignores
// list order; nil for an empty set so "no MiroirNode" and "no pools"
// compare equal.
func poolsByName(pools []miroirv1alpha1.MiroirNodePool) map[string]miroirv1alpha1.MiroirNodePool {
	if len(pools) == 0 {
		return nil
	}
	m := make(map[string]miroirv1alpha1.MiroirNodePool, len(pools))
	for _, p := range pools {
		m[p.Name] = p
	}
	return m
}

func defaultIsMounted(path string) bool {
	mounted, err := mount.New("").IsMountPoint(path)
	return err == nil && mounted
}

// SetupWithManager registers the watcher on this node's MiroirNode only.
// No generation filter: a deletion carries no generation bump, and the
// object sees one status patch a minute at most — Reconcile compares specs
// and no-ops on those.
func (w *TopologyWatcher) SetupWithManager(mgr ctrl.Manager) error {
	own := predicate.NewPredicateFuncs(func(o client.Object) bool {
		return o.GetName() == w.NodeName
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(&miroirv1alpha1.MiroirNode{}, builder.WithPredicates(own)).
		Named("topologywatcher").
		Complete(w)
}
