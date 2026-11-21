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
	"slices"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/backend"
	"github.com/home-operations/miroir/internal/drbd"
	"github.com/home-operations/miroir/internal/nodemap"
)

// setupLog names the lines the agent logs while starting up and shutting down.
var setupLog = ctrl.Log.WithName("setup")

// FetchMiroirNode reads this node's MiroirNode straight from the API
// server (the cache has not started), retrying transient errors within the
// startup budget so a reboot that races control-plane recovery does not
// churn through CrashLoopBackOff. found is false when no MiroirNode names
// this node — it holds no storage and runs a client-only agent.
func FetchMiroirNode(r client.Reader, name string, budget time.Duration) (*miroirv1alpha1.MiroirNode, bool, error) {
	node := &miroirv1alpha1.MiroirNode{}
	err := APIWithRetry(context.Background(), budget, func(ctx context.Context) error {
		return r.Get(ctx, types.NamespacedName{Name: name}, node)
	})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return node, true, nil
}

// volumeGroupFor names the LVM VG backing a pool. The default pool keeps
// the pre-multi-pool name so existing VGs keep working across the upgrade;
// every other pool gets its own suffixed VG.
func volumeGroupFor(pool string) string {
	if pool == miroirv1alpha1.DefaultPoolName {
		return "vg-miroir"
	}
	return "vg-miroir-" + pool
}

// ClientOnlyReason explains why this node's agent serves client-only, or
// returns "" when the MiroirNode carries usable pools.
func ClientOnlyReason(found bool, miroirNode *miroirv1alpha1.MiroirNode, flat nodemap.Node) string {
	switch {
	case !found:
		return "no MiroirNode for this node; running client-only node service"
	case len(flat.Pools) > 0:
		return ""
	case len(miroirNode.Spec.Pools) > 0:
		// Pools exist in the spec but none flattened: block-less entries,
		// which only a pre-0.11 stored object can carry.
		return "MiroirNode pools carry no backend block (a pre-0.11 stored object?): " +
			"apply this node's MiroirNode manifest; running client-only node service"
	default:
		return "MiroirNode declares no usable pools; running client-only node service"
	}
}

// LoadTopology reads this node's MiroirNode (with the startup retry
// budget) and arms the watcher that restarts the agent when the pool spec
// drifts from this snapshot — or appears at all — so a chart-applied pool
// edit reaches the agent without the ConfigMap-checksum pod roll it used
// to ride. found is false for a client-only node (no MiroirNode).
func LoadTopology(mgr manager.Manager, nodeName string, stop context.CancelFunc,
) (*miroirv1alpha1.MiroirNode, bool, error) {
	miroirNode, found, err := FetchMiroirNode(mgr.GetAPIReader(), nodeName, APIStartupWait)
	if err != nil {
		return nil, false, fmt.Errorf("unable to read this node's MiroirNode: %w", err)
	}
	var bootedPools []miroirv1alpha1.MiroirNodePool
	if found {
		bootedPools = miroirNode.Spec.Pools
	}
	if err := (&TopologyWatcher{
		Client:      mgr.GetClient(),
		NodeName:    nodeName,
		BootedPools: bootedPools,
		Stop:        stop,
	}).SetupWithManager(mgr); err != nil {
		return nil, false, fmt.Errorf("unable to set up topology watcher: %w", err)
	}
	return miroirNode, found, nil
}

// PoolBackendsFor builds one backend per pool from this node's flattened
// MiroirNode spec. run is the shared wedge-tracking executor, so a kernel
// that strands an lvm or zfs child trips the same node-scoped breaker the
// DRBD driver reports to.
func PoolBackendsFor(nodeName string, entry nodemap.Node, run backend.Exec) (Pools, error) {
	pools := Pools{}
	for name, p := range entry.Pools {
		be, err := backend.New(p.Backend, backend.Config{
			VolumeGroup:     volumeGroupFor(name),
			ThinPool:        "thinpool",
			Device:          p.Device,
			Dataset:         p.ZFSDataset,
			ZFSVolBlockSize: p.ZFSVolBlockSizeBytes(),
			ZFSCompression:  p.ZFSCompression,
			PoolSize:        p.ThinPoolSize,
			BaseDir:         p.BaseDir,
		}, run)
		if err != nil {
			return nil, fmt.Errorf("backend for node %s pool %s: %w", nodeName, name, err)
		}
		pools[name] = PoolBackend{Backend: be, Type: p.Backend}
	}
	return pools, nil
}

// SetupPools bootstraps the node-local pools before the agent serves
// anything: first start on a fresh node creates PV/VG/thin-pool (lvmthin)
// or the parent dataset (zfs). One bad pool must not take the good ones
// down — its volumes fail their reconciles with real errors while the
// rest of the node keeps serving. All pools failing means the node is
// misconfigured wholesale; exit like the single-pool agent always did so
// the CrashLoopBackOff is impossible to miss.
func SetupPools(pools Pools) error {
	failed := 0
	for _, name := range slices.Sorted(maps.Keys(pools)) {
		if err := pools[name].Backend.Setup(context.Background()); err != nil {
			setupLog.Error(err, "backend pool setup failed; volumes in this pool will fail until it is fixed",
				"pool", name)
			failed++
		}
	}
	if failed == len(pools) {
		return fmt.Errorf("every pool failed setup (%d pools)", len(pools))
	}
	return nil
}

// ProbeDRBD reports whether the DRBD kernel side is usable and, when it
// is, the module and drbd-utils versions — an error below drbd.KernelFloor:
// a 9.3.1-era option rendered against an older module errors drbdadm for
// every resource on the node, so failing fast here beats poisoning them
// all later. Talos ≥ 1.13.0 ships a module at the floor.
func ProbeDRBD(driver *drbd.Driver) (version, utilsVersion string, ready bool, err error) {
	if !driver.KernelAvailable(context.Background()) {
		return "", "", false, nil
	}
	v, err := driver.KernelVersion(context.Background())
	if err != nil {
		// The module answered but the version read flaked; running
		// unchecked beats refusing a working node.
		setupLog.Error(err, "cannot read DRBD kernel module version; skipping floor check")
		return "", "", true, nil
	}
	if drbd.BelowKernelFloor(v) {
		return "", "", false, fmt.Errorf(
			"DRBD kernel module %s is below the supported floor %s; upgrade the node (Talos >= 1.13.0)",
			v, drbd.KernelFloor)
	}
	utils, err := driver.UtilsVersion(context.Background())
	if err != nil {
		// Informational only: an empty value beats refusing a working node.
		setupLog.Error(err, "cannot read drbd-utils version")
	}
	RecordDRBDVersions(v, utils)
	return v, utils, true, nil
}
