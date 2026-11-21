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

	ctrl "sigs.k8s.io/controller-runtime"

	miroirv1alpha1 "github.com/home-operations/miroir/api/v1alpha1"
	"github.com/home-operations/miroir/internal/drbd"
)

// mintBirthUUID creates a fresh volume's birth generation and returns the
// refreshed status. A fresh volume's legs all attach Inconsistent at
// just-created metadata and wait, connected; the winner then mints the one
// UUID every leg adopts over the live connections — a single replicated
// generation cannot birth-diverge. A no-op (status returned
// unchanged) on every leg but the winner's, and on any volume not waiting
// on birth.
func (r *VolumeReconciler) mintBirthUUID(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, res drbd.Resource, st drbd.Status, localDiskless bool) (drbd.Status, error) {
	if !birthInitPending(vol, st, r.NodeName, localDiskless) || !drbd.IsWinner(res) {
		return st, nil
	}
	ctrl.LoggerFrom(ctx).Info("creating birth generation (skip initial sync)", "volume", vol.Name)
	if err := r.DRBD.InitialUUID(ctx, vol.Name); err != nil {
		return st, err
	}
	return r.DRBD.Status(ctx, vol.Name)
}

// mintGenerations runs the mutually exclusive one-time generation mints:
// the birth UUID for a fresh volume, or the restore seed UUID for a
// padded restore (its gates are disjoint from birth's — birth refuses
// FullSync legs and the seed mint requires them).
func (r *VolumeReconciler) mintGenerations(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, res drbd.Resource, st drbd.Status, localDiskless bool) (drbd.Status, error) {
	st, err := r.mintBirthUUID(ctx, vol, res, st, localDiskless)
	if err != nil {
		return st, err
	}
	return r.mintRestoreSeedUUID(ctx, vol, st, localDiskless)
}

// mintRestoreSeedUUID mints the data generation for the seed leg of a
// restore that crossed the replication boundary (an unreplicated source
// cloned into a replicated volume): create-md left the clone's fresh
// metadata at UUID_JUST_CREATED, the birth flow refuses (the volume holds
// real data, and its FullSync joiners must not be declared identical),
// and there was no metadata to adopt — so nothing else can ever move this
// volume past Inconsistent. ForceFullSyncSource flips the seed UpToDate
// with a full bitmap toward every peer; the just-created joiners then
// elect full SyncTarget on their handshakes. A crash between its promote
// and demote leaves the role Primary with the mint already durable
// (UpToDate), which the pending gate no longer matches — the recovery
// branch demotes it (best-effort: a real consumer holding the device
// refuses harmlessly, and Activated cannot have latched yet because the
// latch runs after this mint).
func (r *VolumeReconciler) mintRestoreSeedUUID(ctx context.Context, vol *miroirv1alpha1.MiroirVolume, st drbd.Status, localDiskless bool) (drbd.Status, error) {
	if r.restoreSeedStuckPrimary(vol, st, localDiskless) {
		ctrl.LoggerFrom(ctx).Info("demoting restore seed left Primary by an interrupted mint", "volume", vol.Name)
		if err := r.DRBD.Demote(ctx, vol.Name); err != nil {
			ctrl.LoggerFrom(ctx).V(1).Info("seed demote refused; leaving it to the consumer lifecycle",
				"volume", vol.Name, "error", err)
			return st, nil
		}
		return r.DRBD.Status(ctx, vol.Name)
	}
	if !r.restoreSeedPending(vol, st, localDiskless) {
		return st, nil
	}
	ctrl.LoggerFrom(ctx).Info("seeding restore data generation (forces full sync to joiners)", "volume", vol.Name)
	if err := r.DRBD.ForceFullSyncSource(ctx, vol.Name); err != nil {
		return st, err
	}
	return r.DRBD.Status(ctx, vol.Name)
}

// restoreSeedStuckPrimary reports a padded-restore seed left Primary by a
// mint whose demote never ran: same leg identity as the pending gate, but
// the disk already UpToDate, the role still Primary, and the volume never
// Activated (a consumer's promote latches Activated on the pass that
// observes it, so an unactivated Primary here can only be the mint's).
func (r *VolumeReconciler) restoreSeedStuckPrimary(vol *miroirv1alpha1.MiroirVolume, st drbd.Status, localDiskless bool) bool {
	if localDiskless || vol.Spec.DRBD == nil ||
		vol.Spec.Source == nil || !vol.Spec.Source.PadForMetadata {
		return false
	}
	seed := vol.Spec.FirstDiskfulReplica()
	if seed == nil || seed.Node != r.NodeName || seed.FullSync {
		return false
	}
	return st.Primary && st.DiskState == drbd.DiskUpToDate && !vol.Status.Activated
}

// restoreSeedPending gates the restore seed mint on proof that this leg
// is the data-bearing clone waiting on its first generation, and nothing
// else: only the padded-restore seed (first diskful, never FullSync — the
// clone), only fresh self-created metadata (adopted metadata carries a
// real generation; see Driver.MetadataAdopted), only while Inconsistent,
// and only while no peer has ever reported UpToDate — a seed leg
// recreated after a node wipe wears the same fresh-metadata signature,
// but its peers hold the data now and it must join as a plain full
// SyncTarget, not declare its blank disk the source.
func (r *VolumeReconciler) restoreSeedPending(vol *miroirv1alpha1.MiroirVolume, st drbd.Status, localDiskless bool) bool {
	if localDiskless || vol.Spec.DRBD == nil ||
		vol.Spec.Source == nil || !vol.Spec.Source.PadForMetadata {
		return false
	}
	seed := vol.Spec.FirstDiskfulReplica()
	if seed == nil || seed.Node != r.NodeName || seed.FullSync {
		return false
	}
	if st.DiskState != drbd.DiskInconsistent || r.DRBD.MetadataAdopted(vol.Name) {
		return false
	}
	for _, ds := range st.PeerDiskState {
		if ds == drbd.DiskUpToDate {
			return false
		}
	}
	for node, ps := range vol.Status.PerNode {
		if node != r.NodeName && ps.DiskState == drbd.DiskUpToDate {
			return false
		}
	}
	return true
}

// birthInitPending reports whether this leg is waiting on the one-time
// birth generation: a never-written replicated volume whose local disk and
// every diskful peer's disk sit Inconsistent over established connections.
// The Activated/Formatted gate means divergent real data can never be
// declared clean; a FullSync joiner means the volume already has a
// generation, so this is birth only. Fail closed: every snapshot-derived
// volume is ineligible, even if its metadata appears Inconsistent.
func birthInitPending(vol *miroirv1alpha1.MiroirVolume, st drbd.Status, self string, localDiskless bool) bool {
	if vol.Spec.DRBD == nil || vol.Spec.Source != nil || localDiskless {
		return false
	}
	if vol.Status.Activated || vol.Status.Formatted {
		return false
	}
	if st.DiskState != drbd.DiskInconsistent {
		return false
	}
	for _, rep := range vol.Spec.Replicas {
		if rep.FullSync {
			return false
		}
		if rep.Node == self || rep.Diskless || rep.Address == "" {
			continue
		}
		if !st.PeerConnected[rep.NodeID] || st.PeerDiskState[rep.NodeID] != drbd.DiskInconsistent {
			return false
		}
	}
	return true
}
