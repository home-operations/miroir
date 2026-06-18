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

const (
	// DriverName is the CSI driver name, also the CRD API group.
	DriverName = "miroir.io"

	// TopologyKey is the CSI topology key reported by NodeGetInfo; its
	// value is the Kubernetes node name (notes/DESIGN.md §6.5).
	TopologyKey = "miroir.io/node"

	// FinalizerPrefix + node name blocks MiroirVolume deletion until that
	// node's agent has torn down its local state. One finalizer per
	// replica: each agent releases exactly its own.
	FinalizerPrefix = "miroir.io/teardown-"

	// ParamReplicas is the StorageClass parameter for the replica count.
	ParamReplicas = "miroir.io/replicas"

	// ParamQuorum is the StorageClass parameter for the 2-node policy.
	ParamQuorum = "miroir.io/quorum"
)
