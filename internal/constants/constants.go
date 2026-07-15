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

// Package constants holds cross-component identifiers for the miroir driver.
package constants

import "time"

// StatsStaleAfter ignores MiroirNode figures older than this as unknown —
// the agent republishes every ~60s, so a few missed polls mean the node is
// down and its stats can't be trusted for placement or auto-diskful.
const StatsStaleAfter = 5 * time.Minute

const (
	// DriverName is the CSI driver name, also the CRD API group.
	DriverName = "miroir.home-operations.com"

	// TopologyKey is the CSI topology key reported by NodeGetInfo; its
	// value is the Kubernetes node name.
	TopologyKey = "miroir.home-operations.com/node"

	// FinalizerPrefix + node name blocks MiroirVolume deletion until that
	// node's agent has torn down its local state. One finalizer per
	// replica: each agent releases exactly its own.
	FinalizerPrefix = "miroir.home-operations.com/teardown-"

	// ParamReplicas is the StorageClass parameter for the replica count.
	ParamReplicas = "miroir.home-operations.com/replicas"

	// ParamQuorum is the StorageClass parameter for the 2-node policy.
	ParamQuorum = "miroir.home-operations.com/quorum"

	// ParamAllowRemoteAccess is the StorageClass parameter that lets pods
	// on nodes without a replica consume the volume through an ephemeral
	// diskless client leg (name matches the LINSTOR parameter operators
	// know). "true" drops the PV's node affinity.
	ParamAllowRemoteAccess = "miroir.home-operations.com/allowRemoteVolumeAccess"

	// ParamBitmapGranularity is the StorageClass parameter for the DRBD
	// bitmap block size in bytes (power of two, 4096–1048576), applied at
	// metadata creation. Replicated classes only.
	ParamBitmapGranularity = "miroir.home-operations.com/bitmapGranularity"

	// ParamPool is the StorageClass parameter naming the storage pool the
	// class provisions from (matches LINSTOR's storagePool concept). Every
	// replica of a volume lands in this pool on its node. Absent means the
	// default pool (v1alpha1.DefaultPoolName).
	ParamPool = "miroir.home-operations.com/pool"
)
