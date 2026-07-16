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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
)

func watcherNode(pools ...miroirv1alpha1.MiroirNodePool) *miroirv1alpha1.MiroirNode {
	return &miroirv1alpha1.MiroirNode{
		ObjectMeta: metav1.ObjectMeta{Name: nodeA},
		Spec:       miroirv1alpha1.MiroirNodeSpec{Pools: pools},
	}
}

func lvmPool(name, device string) miroirv1alpha1.MiroirNodePool {
	return miroirv1alpha1.MiroirNodePool{
		Name:    name,
		LVMThin: &miroirv1alpha1.LVMThinPool{Device: device},
	}
}

func loopfilePool(name, baseDir string) miroirv1alpha1.MiroirNodePool {
	return miroirv1alpha1.MiroirNodePool{
		Name:     name,
		Loopfile: &miroirv1alpha1.LoopfilePool{BaseDir: baseDir},
	}
}

func runWatcher(t *testing.T, booted []miroirv1alpha1.MiroirNodePool, objs ...client.Object) bool {
	t.Helper()
	return runWatcherMounts(t, booted, func(string) bool { return true }, objs...)
}

func runWatcherMounts(t *testing.T, booted []miroirv1alpha1.MiroirNodePool,
	isMounted func(string) bool, objs ...client.Object,
) bool {
	t.Helper()
	stopped := false
	w := &TopologyWatcher{
		Client: fake.NewClientBuilder().WithScheme(newScheme(t)).
			WithObjects(objs...).Build(),
		NodeName:    nodeA,
		BootedPools: booted,
		Stop:        func() { stopped = true },
		IsMounted:   isMounted,
	}
	if _, err := w.Reconcile(t.Context(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: nodeA},
	}); err != nil {
		t.Fatal(err)
	}
	return stopped
}

func TestTopologyWatcherNoDriftNoRestart(t *testing.T) {
	pools := []miroirv1alpha1.MiroirNodePool{lvmPool("default", "/dev/sdb")}
	if runWatcher(t, pools, watcherNode(pools...)) {
		t.Fatal("an unchanged pool spec must not restart the agent")
	}
}

func TestTopologyWatcherRestartsOnPoolEdit(t *testing.T) {
	booted := []miroirv1alpha1.MiroirNodePool{lvmPool("default", "/dev/sdb")}
	current := watcherNode(lvmPool("default", "/dev/sdb"), lvmPool("fast", "/dev/nvme0n1"))
	if !runWatcher(t, booted, current) {
		t.Fatal("a pool added since boot must restart the agent")
	}
}

func TestTopologyWatcherRestartsOnDeletion(t *testing.T) {
	booted := []miroirv1alpha1.MiroirNodePool{lvmPool("default", "/dev/sdb")}
	if !runWatcher(t, booted) {
		t.Fatal("a deleted MiroirNode must restart a storage agent (into client-only)")
	}
}

func TestTopologyWatcherRestartsClientOnlyWhenNodeAppears(t *testing.T) {
	if !runWatcher(t, nil, watcherNode(lvmPool("default", "/dev/sdb"))) {
		t.Fatal("a MiroirNode appearing must restart a client-only agent into a storage agent")
	}
}

func TestTopologyWatcherClientOnlyStaysWithoutNode(t *testing.T) {
	if runWatcher(t, nil) {
		t.Fatal("a client-only agent with no MiroirNode has nothing to restart for")
	}
}

func TestTopologyWatcherIgnoresPoolReorder(t *testing.T) {
	// spec.pools is listType=map: order carries no meaning, and a
	// re-render that only reorders entries must not cost a shutdown sweep
	// plus DRBD re-bootstrap.
	booted := []miroirv1alpha1.MiroirNodePool{
		lvmPool("default", "/dev/sdb"), lvmPool("fast", "/dev/nvme0n1"),
	}
	current := watcherNode(lvmPool("fast", "/dev/nvme0n1"), lvmPool("default", "/dev/sdb"))
	if runWatcher(t, booted, current) {
		t.Fatal("a reordered pool list must not restart the agent")
	}
}

func TestTopologyWatcherDefersRestartUntilLoopfileMountArrives(t *testing.T) {
	// A new loopfile baseDir rides the NEW pod template as a hostPath
	// mount; restarting inside the old pod would bootstrap the pool on the
	// container filesystem and crash-loop. The watcher holds the restart
	// until the DaemonSet rollout delivers the mount.
	booted := []miroirv1alpha1.MiroirNodePool{lvmPool("default", "/dev/sdb")}
	current := watcherNode(lvmPool("default", "/dev/sdb"), loopfilePool("files", "/var/lib/miroir"))
	if runWatcherMounts(t, booted, func(string) bool { return false }, current) {
		t.Fatal("a loopfile baseDir not yet mounted in this pod must defer the restart to the rollout")
	}
	if !runWatcherMounts(t, booted, func(string) bool { return true }, current) {
		t.Fatal("once the baseDir is mounted the drift must restart the agent")
	}
}
