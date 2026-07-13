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

package csi

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/home-operations/miroir/internal/constants"
)

// Identity implements csi.IdentityServer for both the controller and the
// agent (notes/DESIGN.md §6.1).
type Identity struct {
	csi.UnimplementedIdentityServer
	// Version is injected from main's ldflags.
	Version string
	// WithController is true only in controller mode: the agent's Identity
	// must not advertise CONTROLLER_SERVICE (it serves no controller RPCs).
	WithController bool
}

// GetPluginInfo returns the driver name and version.
func (s *Identity) GetPluginInfo(_ context.Context, _ *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{
		Name:          constants.DriverName,
		VendorVersion: s.Version,
	}, nil
}

// GetPluginCapabilities advertises the controller service and topology
// constraints (PV nodeAffinity pins pods to replica nodes, §6.5). Also
// ONLINE volume expansion (ControllerExpandVolume + NodeExpandVolume).
func (s *Identity) GetPluginCapabilities(_ context.Context, _ *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	caps := []*csi.PluginCapability{
		{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS,
				},
			},
		},
		{
			Type: &csi.PluginCapability_VolumeExpansion_{
				VolumeExpansion: &csi.PluginCapability_VolumeExpansion{
					Type: csi.PluginCapability_VolumeExpansion_ONLINE,
				},
			},
		},
	}
	if s.WithController {
		caps = append(caps, &csi.PluginCapability{
			Type: &csi.PluginCapability_Service_{
				Service: &csi.PluginCapability_Service{
					Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
				},
			},
		})
	}
	return &csi.GetPluginCapabilitiesResponse{Capabilities: caps}, nil
}

// Probe reports liveness/readiness to the livenessprobe sidecar and kubelet.
// Serving the RPC is the signal: the gRPC server only comes up after the
// manager's cache has synced (see serveCSI).
func (s *Identity) Probe(_ context.Context, _ *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}
